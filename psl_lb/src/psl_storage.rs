use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use psl::crypto::KeyStore;
use psl::utils::channel::{make_channel, Receiver};
use psl::utils::AtomicStruct;
use psl::worker::block_broadcaster::BlockBroadcaster;
use psl::worker::block_sequencer::BlockSequencer;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tonic::async_trait;
use tracing::{debug, warn};
use prost_new::Message as _;

use psl::config::PSLWorkerConfig;
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
    block_sequencer: Arc<Mutex<BlockSequencer>>,
    block_broadcaster: Arc<Mutex<BlockBroadcaster>>,
    staging: Arc<Mutex<Staging>>,
}

impl PSLWorkerPerChain {
    fn new(chain_id: u64, config: PSLWorkerConfig, vote_rx: Receiver<VoteWithSender>, cmd_rx: mpsc::Receiver<StorageCommand>) -> Self {
        let worker = Self {
            chain_id,
            cmd_rx,
            block_sequencer: Arc::new(Mutex::new(BlockSequencer::new(chain_id, config))),
            block_broadcaster: Arc::new(Mutex::new(BlockBroadcaster::new(chain_id, config))),
            staging: Arc::new(Mutex::new(Staging::new(chain_id, config))),
        };

        worker
    }

    async fn run(&mut self) {
        let block_sequencer = self.block_sequencer.clone();
        let block_broadcaster = self.block_broadcaster.clone();
        let staging = self.staging.clone();

        tokio::spawn(async move {
            BlockSequencer::run(block_sequencer).await;
        });

        tokio::spawn(async move {
            BlockBroadcaster::run(block_broadcaster).await;
        });

        tokio::spawn(async move {
            Staging::run(staging).await;
        });

        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                StorageCommand::Store(origin_id, seq_num, data, tx) => {
                    tx.send(Ok(())).unwrap();
                },
                StorageCommand::Read(origin_id, seq_num, tx) => {
                    tx.send(Ok(None)).unwrap();
                },
                _ => {
                    unimplemented!();
                }
            }
        }
    
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

impl<'a> From<psl::config::Config> for WorkerConfig<'a> {
    fn from(config: psl::config::Config) -> Self {
        Self {
            worker_config: PSLWorkerConfig::deserialize(&config.serialize()),
            server_config: AtomicConfig::new(config.clone()),
            keystore: AtomicKeyStore::new(KeyStore::new(
                &config.rpc_config.allowed_keylist_path,
                &config.rpc_config.signing_priv_key_path,
            )),
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

        // TODO: Prefill.
        
        Self {
            config: config.worker_config.clone(),
            cmd_rx,
            cmd_txs: HashMap::new(),
            server: Arc::new(Server::new_atomic(config.server_config.clone(), ctx, config.keystore.clone())),
            staging_txs,
        }
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                StorageCommand::Store(origin_id, seq_num, data, tx) => {
                    let _ = self.store(origin_id, seq_num, data).await;
                    tx.send(Ok(())).unwrap();
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

            let (tx, mut rx) = make_channel(1000);
            let mut staging_txs = HashMap::from_iter(staging_txs.iter().map(|(k, v)| (k.clone(), v.clone())));
            staging_txs.insert(origin_id, tx);
            self.staging_txs.set(Box::new(staging_txs));

            let (cmd_tx, cmd_rx) = mpsc::channel(1000);
            self.cmd_txs.insert(origin_id, cmd_tx);

            let mut worker = PSLWorkerPerChain::new(origin_id, self.config.clone(), rx, cmd_rx);
            tokio::spawn(async move {
                worker.run().await;
            });
        }

        let cmd_tx = self.cmd_txs.get(&origin_id).unwrap();

        let (resp_tx, resp_rx) = oneshot::channel();
        cmd_tx.send(StorageCommand::Store(origin_id, seq_num, data, resp_tx)).await.expect("Channel send error");

        let resp = resp_rx.await.expect("Channel send error");
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



