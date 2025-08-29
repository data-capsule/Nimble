use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use num_bigint::BigInt;
use psl::crypto::{CryptoService, CryptoServiceConnector, KeyStore};
use psl::proto::checkpoint::{ProtoAuthSenderType, ProtoBackfillQuery};
use psl::proto::consensus::{ProtoAppendEntries, ProtoBlock};
use psl::proto::execution::ProtoTransactionOpType;
use psl::rpc::client::{Client, PinnedClient};
use psl::storage_server::logserver::LogServer;
use psl::utils::channel::{make_channel, Receiver};
use psl::utils::{deserialize_proto_block, AtomicStruct, RemoteStorageEngine, StorageService};
use psl::worker::block_broadcaster::BlockBroadcaster;
use psl::worker::block_sequencer::{BlockSequencer, SequencerCommand};
use psl::worker::cache_manager::{CacheCommand, CachedValue};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tonic::async_trait;
use tracing::{debug, error, info, warn};
use prost_new::Message as _;

use psl::config::{AtomicPSLWorkerConfig, PSLWorkerConfig};
use psl::proto::rpc::ProtoPayload;
use psl::rpc::server::{MsgAckChan, RespType, Server, ServerContextType};
use psl::rpc::MessageRef;
use psl::{config::AtomicConfig, crypto::AtomicKeyStore, utils::channel::Sender};
use psl::worker::staging::{Staging, VoteWithSender};
use crate::storage::{ConfigType, StorageCommand};
use crate::{PslError, StorageBackend};

pub type AtomicHashMap<K, V> = AtomicStruct<HashMap<K, V>>;

pub struct PSLWorkerServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    pub staging_txs: AtomicHashMap<u64, Sender<VoteWithSender>>,
}

#[derive(Clone)]
pub struct PinnedPSLWorkerServerContext (pub Arc<Pin<Box<PSLWorkerServerContext>>>);

impl Deref for PinnedPSLWorkerServerContext {
    type Target = PSLWorkerServerContext;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl ServerContextType for PinnedPSLWorkerServerContext {
    fn get_server_keys(&self) -> std::sync::Arc<Box<psl::crypto::KeyStore>> {
        self.keystore.get()
    }

    async fn handle_rpc(
        &self,
        m: MessageRef<'_>,
        ack_chan: MsgAckChan,
    ) -> Result<RespType, std::io::Error> {
        let sender = match m.2 {
            psl::rpc::SenderType::Anon => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "unauthenticated message",
                )); // Anonymous replies shouldn't come here
            }
            _sender @ psl::rpc::SenderType::Auth(_, _) => _sender.clone(),
        };

        
        let body = match ProtoPayload::decode(&m.0.as_slice()[0..m.1]) {
            Ok(b) => b,
            Err(e) => {
                warn!("Parsing problem: {} ... Dropping connection", e.to_string());
                debug!("Original message: {:?} {:?}", &m.0, &m.1);
                return Err(Error::new(ErrorKind::InvalidData, e));
            }
        };

        let msg = match body.message {
            Some(m) => m,
            None => {
                warn!("Nil message: {}", m.1);
                return Ok(RespType::NoResp);
            }
        };

        match msg {
            psl::proto::rpc::proto_payload::Message::Vote(vote) => {
                let staging_txs = self.staging_txs.get();
                let Some(staging_tx) = staging_txs.get(&vote.chain_id) else {
                    warn!("No staging tx for chain {}", vote.chain_id);
                    return Ok(RespType::NoResp);
                };
                staging_tx.send((sender, vote)).await.expect("Channel send error");
                return Ok(RespType::NoResp);
            },
            _ => {}
        }

        Ok(RespType::NoResp)
    }
}

impl PinnedPSLWorkerServerContext {
    fn new(config: AtomicConfig, keystore: AtomicKeyStore) -> Self {
        let context = PSLWorkerServerContext {
            config,
            keystore,
            staging_txs: AtomicHashMap::new(HashMap::new()),
        };
        Self(Arc::new(Box::pin(context)))
    }
}

pub struct PSLWorkerPerChain {
    chain_id: u64,
    cmd_rx: mpsc::Receiver<StorageCommand>,
    cache_manager_tx: Sender<SequencerCommand>,
    backfill_request_tx: Sender<(ProtoBackfillQuery, oneshot::Sender<Option<ProtoAppendEntries>>)>,
    client_reply_rx: broadcast::Receiver<u64>,
    block_sequencer: Arc<Mutex<BlockSequencer>>,
    block_broadcaster: Arc<Mutex<BlockBroadcaster>>,
    staging: Arc<Mutex<Staging>>,
    logserver: Arc<Mutex<LogServer>>,
    config: PSLWorkerConfig,
}

const CHAN_DEPTH: usize = 10000;

impl PSLWorkerPerChain {

    /// Must be called in tokio runtime.
    async fn new(chain_id: u64, config: PSLWorkerConfig, client: PinnedClient, vote_rx: Receiver<VoteWithSender>, cmd_rx: mpsc::Receiver<StorageCommand>, crypto: CryptoServiceConnector) -> Self {
        let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);
        let key_store = AtomicKeyStore::new(key_store);
        let worker_config = AtomicPSLWorkerConfig::new(config.clone());
        let server_config = AtomicConfig::new(config.to_config());


        let (cache_manager_tx, cache_manager_rx) = make_channel(CHAN_DEPTH);
        let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(CHAN_DEPTH);
        let (unused_block_broadcaster_tx, unused_block_broadcaster_rx) = make_channel(CHAN_DEPTH);
        let (staging_tx, staging_rx) = make_channel(CHAN_DEPTH);
        let (unused_block_broadcaster_delivery_tx, unused_block_broadcaster_delivery_rx) = make_channel(CHAN_DEPTH);
        let (logserver_tx, logserver_rx) = make_channel(CHAN_DEPTH);
        let (client_reply_tx, client_reply_rx) = broadcast::channel(CHAN_DEPTH);
        let (gc_tx, gc_rx) = make_channel(CHAN_DEPTH);
        let (backfill_request_tx, backfill_request_rx) = make_channel(CHAN_DEPTH);
        let (unused_backfill_request_tx, unused_backfill_request_rx) = make_channel(CHAN_DEPTH);

        let block_sequencer = BlockSequencer::new(
            worker_config.clone(), crypto.clone(),
            cache_manager_rx,
            unused_block_broadcaster_tx,
            block_broadcaster_tx,
            chain_id,
        );

        let block_broadcaster = BlockBroadcaster::new(
            psl::worker::block_broadcaster::BroadcasterConfig::WorkerConfig(worker_config.clone()),
            client,
            psl::worker::block_broadcaster::BroadcastMode::StorageStar,
            true, false,
            block_broadcaster_rx,
            None,
            Some(staging_tx),
        );

        let staging = Staging::new(
            worker_config.clone(), chain_id, crypto.clone(),
            vote_rx,
            staging_rx,
            unused_block_broadcaster_delivery_tx,
            logserver_tx,
            client_reply_tx,
            gc_tx,
        );

        let remote_storage = StorageService::<RemoteStorageEngine>::new(
            server_config.clone(),
            RemoteStorageEngine {
                config: server_config.clone(),
            },
            CHAN_DEPTH
        );

        let logserver = LogServer::new(
            server_config.clone(),
            key_store.clone(),
            remote_storage.get_connector(crypto.clone()),
            gc_rx,
            logserver_rx,
            unused_backfill_request_rx,
            Some(backfill_request_rx),
        );

        tokio::spawn(async move {
            let _tx = unused_backfill_request_tx.clone();
            loop {
                tokio::select! {
                    _ = unused_block_broadcaster_rx.recv() => {
                        // Black hole
                    }
                    _ = unused_block_broadcaster_delivery_rx.recv() => {
                        // Black hole
                    }
                }
            }

        });
        
        let worker = Self {
            chain_id,
            cmd_rx,
            cache_manager_tx,
            backfill_request_tx,
            client_reply_rx,
            block_sequencer: Arc::new(Mutex::new(block_sequencer)),
            block_broadcaster: Arc::new(Mutex::new(block_broadcaster)),
            staging: Arc::new(Mutex::new(staging)),
            logserver: Arc::new(Mutex::new(logserver)),
            config,
        };

        worker
    }

    async fn run(&mut self) {
        let block_sequencer = self.block_sequencer.clone();
        let block_broadcaster = self.block_broadcaster.clone();
        let staging = self.staging.clone();
        let logserver = self.logserver.clone();

        tokio::spawn(async move {
            BlockSequencer::run(block_sequencer).await;
        });

        tokio::spawn(async move {
            BlockBroadcaster::run(block_broadcaster).await;
        });

        tokio::spawn(async move {
            Staging::run(staging).await;
        });

        tokio::spawn(async move {
            LogServer::run(logserver).await;
        });

        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                StorageCommand::Store(_origin_id, seq_num, data, tx) => {
                    warn!("Received store command for chain {}", _origin_id);
                    let value = CachedValue::new_with_seq_num(data, seq_num, BigInt::from(1));

                    let (_tx, _rx) = oneshot::channel();
                    warn!("Sending self write op");
                    let _ = self.cache_manager_tx.send(SequencerCommand::SelfWriteOp { 
                        key: seq_num.to_be_bytes().to_vec(), value,
                        seq_num_query: psl::worker::block_sequencer::BlockSeqNumQuery::WaitForSeqNum(_tx)
                    }).await;

                    warn!("Sending force make new block");
                    let _ = self.cache_manager_tx.send(SequencerCommand::ForceMakeNewBlock).await;

                    warn!("Receiving client reply");
                    let _ = self.client_reply_rx.recv().await;
                    warn!("Received client reply2");
                    let _ = _rx.await;

                    warn!("Sending response");
                    let _ = tx.send(Ok(()));
                },
                StorageCommand::Read(origin_id, seq_num, tx) => {
                    self.handle_read_with_retries(origin_id, seq_num, tx).await;
                },
                _ => {
                    unimplemented!();
                }
            }
        }

        error!("PSLWorkerPerChain run loop ended");
    
    }

    async fn handle_read_with_retries(&self, origin_id: u64, seq_num: u64, tx: oneshot::Sender<Result<Option<Vec<u8>>, PslError>>) {
        let mut retries = 0;
        loop {
            warn!("Read value after {} retries", retries);
            let res = self.handle_read(origin_id, seq_num).await;
            warn!("Read done");
            
            if res.is_ok() {
                tx.send(res).unwrap();

                return;
            }

            retries += 1;
            if retries > 100 {
                error!("Failed to read value after 100 retries");
                tx.send(Err(PslError::Storage("Failed to read value".to_string()))).unwrap();
                return;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn handle_read(&self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        let my_name = self.config.net_config.name.clone();
        let sub_id = origin_id;

        let backfill_query = ProtoBackfillQuery {
            reply_name: my_name.clone(),
            origin: Some(ProtoAuthSenderType {
                name: my_name,
                sub_id,
            }),
            start_index: seq_num,
            end_index: seq_num,
        };

        let (resp_tx, resp_rx) = oneshot::channel();
        self.backfill_request_tx.send((backfill_query, resp_tx)).await.expect("Channel send error");

        let resp = match resp_rx.await {
            Ok(Some(resp)) => {
                resp
            },
            val @ _ => {
                return Err(PslError::Storage("No response from logserver".to_string()));
            }
        };
        

        let Some(fork) = resp.fork else {
            return Err(PslError::Storage("No fork from logserver".to_string()));
        };

        if fork.serialized_blocks.len() != 1 {
            return Err(PslError::Storage("No block from logserver".to_string()));
        }
        let block = &fork.serialized_blocks[0];
        let Ok(block) = deserialize_proto_block(&block.serialized_body) else {
            return Err(PslError::Storage("Failed to decode block".to_string()));
        };

        // There is only one Write Tx with the value that I want.
        if block.tx_list.len() != 1 {
            return Ok(None);
        }

        let transaction = &block.tx_list[0];
        let Some(phase) = &transaction.on_crash_commit else {
            return Err(PslError::Storage("No on-crash commit".to_string()));
        };

        if phase.ops.len() != 1 {
            return Err(PslError::Storage("No ops".to_string()));
        }

        if phase.ops[0].op_type() != ProtoTransactionOpType::Write
        || phase.ops[0].operands.len() < 2 {
            return Err(PslError::Storage("No write tx".to_string()));
        }

        let value = phase.ops[0].operands[1].clone();
        
        // This is bincode serialized.
        let Ok(value) = bincode::deserialize::<CachedValue>(&value) else {
            return Err(PslError::Storage("Failed to deserialize value".to_string()));
        };

        Ok(Some(value.get_value()))
    }
}

pub struct PSLVoteController {
    staging_rx: Receiver<VoteWithSender>,
    staging_txs: HashMap<u64, Sender<VoteWithSender>>,
}

pub struct PSLWorker {
    config: PSLWorkerConfig,

    cmd_rx: mpsc::Receiver<StorageCommand>,
    server: Arc<Server<PinnedPSLWorkerServerContext>>,
    staging_txs: AtomicHashMap<u64, Sender<VoteWithSender>>,
    cmd_txs: HashMap<u64, mpsc::Sender<StorageCommand>>,
    crypto_connector: CryptoServiceConnector,
    crypto: CryptoService,

    all_clients: Vec<PinnedClient>,


}

#[derive(Clone)]
pub struct WorkerConfig<'a> {
    worker_config: PSLWorkerConfig,
    server_config: AtomicConfig,
    keystore: AtomicKeyStore,

    marker: PhantomData<&'a PSLWorker>,
}

impl<'a> ConfigType<'a> for WorkerConfig<'a> {
    fn num_tasks(&self) -> usize {
        1
    }

    fn commit_threshold(&self) -> usize {
        self.worker_config.commit_threshold()
    }
}

impl<'a> From<psl::config::PSLWorkerConfig> for WorkerConfig<'a> {
    fn from(config: psl::config::PSLWorkerConfig) -> Self {
        let og_config = config.to_config();
        let og_config = AtomicConfig::new(og_config);
        let key_store = AtomicKeyStore::new(KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        ));
        Self {
            worker_config: config,
            server_config: og_config,
            keystore: key_store,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<'a> StorageBackend<'a> for PSLWorker {
    type Config = WorkerConfig<'a>;
    fn new(cmd_rx: mpsc::Receiver<StorageCommand>, config: Self::Config) -> Self {
        let ctx = PinnedPSLWorkerServerContext::new(config.server_config.clone(), config.keystore.clone());
        let staging_txs = ctx.staging_txs.clone();

        let key_store = KeyStore::new(
            &config.worker_config.rpc_config.allowed_keylist_path,
            &config.worker_config.rpc_config.signing_priv_key_path,
        );

        let og_config = config.worker_config.to_config();
        let og_config = AtomicConfig::new(og_config);
        let key_store = AtomicKeyStore::new(key_store);
        let mut crypto_service = CryptoService::new(config.worker_config.worker_config.num_crypto_workers, key_store.clone(), og_config);
        crypto_service.run();

        let crypto_connector = crypto_service.get_connector();

        // TODO: Prefill.

        let mut all_clients = Vec::new();
        for i in 0..10 {
            let client = Client::new_atomic(config.server_config.clone(), key_store.clone(), false, i);
            all_clients.push(client.into());
        }
        
        Self {
            config: config.worker_config.clone(),
            cmd_rx,
            cmd_txs: HashMap::new(),
            server: Arc::new(Server::new_atomic(config.server_config.clone(), ctx, config.keystore.clone())),
            staging_txs,
            crypto_connector,
            crypto: crypto_service,
            all_clients,
        }
    }

    async fn run(&mut self) {
        let server = self.server.clone();
        tokio::spawn(async move {
            let _ = Server::<PinnedPSLWorkerServerContext>::run(server).await;
        });
        while let Some(cmd) = self.cmd_rx.recv().await {
            warn!("Received command. Pending commands: {}", self.cmd_rx.len());
            match cmd {
                StorageCommand::Store(origin_id, seq_num, data, tx) => {
                    let res = self.store(origin_id, seq_num, data).await;
                    tx.send(res).unwrap();
                },
                StorageCommand::Read(origin_id, seq_num, tx) => {
                    let data = self.read(origin_id, seq_num).await;
                    tx.send(data).unwrap();
                },
                StorageCommand::HealthCheck(tx) => {
                    let data = self.health_check().await;
                    tx.send(data).unwrap();
                },
            }
        }
    }

    async fn store(&mut self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError> {
        let staging_txs = self.staging_txs.get();
        if !staging_txs.contains_key(&origin_id) {
            // This is super inefficient.
            warn!("Creating new staging tx for chain {}", origin_id);
            let (tx, rx) = make_channel(CHAN_DEPTH);
            let mut staging_txs = HashMap::from_iter(staging_txs.iter().map(|(k, v)| (k.clone(), v.clone())));
            staging_txs.insert(origin_id, tx);
            self.staging_txs.set(Box::new(staging_txs));

            let (cmd_tx, cmd_rx) = mpsc::channel(CHAN_DEPTH);
            self.cmd_txs.insert(origin_id, cmd_tx);

            let mut worker = PSLWorkerPerChain::new(
                origin_id,
                self.config.clone(),
                self.all_clients[origin_id as usize % self.all_clients.len()].clone(),
                rx,
                cmd_rx,
                self.crypto_connector.clone(),
            ).await;
            warn!("Spawning worker for chain {}", origin_id);
            tokio::spawn(async move {
                worker.run().await;
            });
        }

        let cmd_tx = self.cmd_txs.get(&origin_id).unwrap();

        warn!("Sending store command to worker for chain {}", origin_id);
        let (resp_tx, resp_rx) = oneshot::channel();
        cmd_tx.send(StorageCommand::Store(origin_id, seq_num, data, resp_tx)).await.expect("Channel send error");

        warn!("Waiting for response from worker for chain {}", origin_id);
        let resp = resp_rx.await.expect("Channel send error");
        warn!("Received response from worker for chain {}", origin_id);
        resp
    }

    async fn read(&mut self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        let staging_txs = self.staging_txs.get();
        if !staging_txs.contains_key(&origin_id) {
            return Err(PslError::Storage(format!("No staging tx for chain {}", origin_id)));
        }

        let cmd_tx = self.cmd_txs.get(&origin_id).unwrap();
        let (resp_tx, resp_rx) = oneshot::channel();
        cmd_tx.send(StorageCommand::Read(origin_id, seq_num, resp_tx)).await.expect("Channel send error");
        let resp = resp_rx.await.expect("Channel send error");
        resp
    }

    async fn health_check(&mut self) -> Result<(), PslError> {
        Ok(())
    }
}



