use eth_trie::{MemoryDB, EthTrie, Trie};
use psl::{config::{AtomicConfig, Config}, crypto::{AtomicKeyStore, KeyStore}, proto::rpc::ProtoPayload, rpc::{server::{MsgAckChan, RespType, Server, ServerContextType}, MessageRef}, utils::channel::{make_channel, Sender}, worker::TxWithAckChanTag};
use tokio::{sync::Mutex, task::JoinSet};
use std::{ops::Deref, pin::Pin, sync::Arc};
use prost::Message as _;
use std::io::{Error, ErrorKind};
use tracing::{debug, warn, info, error};
use clap::Parser;
use std::fs;
use anyhow::{Result, Context};

mod client_reply_handler;
mod kvs_manager;
mod nimble_client;

use client_reply_handler::ClientReplyHandler;
use kvs_manager::KVSManager;
use nimble_client::NimbleClient;

// pub struct NimbleKVServerContext{
//   store: EthTrie<MemoryDB>,
// }

// impl NimbleKVServerContext {
//   pub fn new() -> Result<Self, anyhow::Error> {
//     let memdb = Arc::new(MemoryDB::new(false));
//     let store = EthTrie::new(memdb.clone());
//     Ok(Self {
//       store,
//     })
//   }


// }

pub struct NimbleKVServerContext {
  keystore: AtomicKeyStore,
  batch_proposal_tx: Sender<TxWithAckChanTag>,
}


#[derive(Clone)]
pub struct PinnedNimbleKVServerContext(pub Arc<Pin<Box<NimbleKVServerContext>>>);

impl PinnedNimbleKVServerContext {
    pub fn new(
        keystore: AtomicKeyStore,
        batch_proposal_tx: Sender<TxWithAckChanTag>,
    ) -> Self {
        Self(Arc::new(Box::pin(NimbleKVServerContext {
            keystore, batch_proposal_tx,
        })))
    }
}

impl Deref for PinnedNimbleKVServerContext {
    type Target = NimbleKVServerContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl ServerContextType for PinnedNimbleKVServerContext {
    fn get_server_keys(&self) -> std::sync::Arc<Box<psl::crypto::KeyStore>> {
        self.keystore.get()
    }

    async fn handle_rpc(&self, m: MessageRef<'_>, ack_chan: MsgAckChan) -> Result<RespType, std::io::Error> {
        let sender = match m.2 {
            psl::rpc::SenderType::Anon => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unauthenticated message",
                )); // Anonymous replies shouldn't come here
            }
            _sender @ psl::rpc::SenderType::Auth(_, _) => _sender.clone()
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
            psl::proto::rpc::proto_payload::Message::ClientRequest(proto_client_request) => {
                let client_tag = proto_client_request.client_tag;
                tracing::info!("Proposal from {:?}", sender);
                self.batch_proposal_tx.send((proto_client_request.tx, (ack_chan, client_tag, sender))).await
                    .expect("Channel send error");
                return Ok(RespType::Resp);
            },
            _ => {
                // Drop the message.
            }
        }
        Ok(RespType::NoResp)
    }
}

pub struct NimbleKVSNode {
  config: AtomicConfig,
  keystore: AtomicKeyStore,

  server: Arc<Server<PinnedNimbleKVServerContext>>,
  client_reply_handlers: Vec<Arc<Mutex<ClientReplyHandler>>>,
  kvs_manager: Arc<Mutex<KVSManager>>,
  nimble_client: Arc<Mutex<NimbleClient>>, 
}

const NUM_CLIENT_REPLY_HANDLERS: usize = 20;

impl NimbleKVSNode {
  pub fn new(config: AtomicConfig, keystore: AtomicKeyStore, nimble_endpoint_url: String) -> Self {
    let chan_depth = config.get().rpc_config.channel_depth as usize;
    let (batch_proposal_tx, batch_proposal_rx) = make_channel(chan_depth);
    // let (reply_tx, reply_rx) = make_channel(chan_depth);
    let (nimble_tx, nimble_rx) = make_channel(chan_depth);
    
    let ctx = PinnedNimbleKVServerContext::new(keystore.clone(), batch_proposal_tx);
    let server = Arc::new(Server::new_atomic(config.clone(), ctx, keystore.clone()));

    let mut client_reply_handlers = Vec::new();
    let mut reply_txs = Vec::new();
    for _ in 0..NUM_CLIENT_REPLY_HANDLERS {
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel();
        client_reply_handlers.push(Arc::new(Mutex::new(ClientReplyHandler::new(config.clone(), reply_rx))));
        reply_txs.push(reply_tx.clone());
    }
    
    let kvs_manager = Arc::new(Mutex::new(KVSManager::new(config.clone(), batch_proposal_rx, reply_txs, nimble_tx)));
    let nimble_client = Arc::new(Mutex::new(NimbleClient::new(config.clone(), keystore.clone(), nimble_endpoint_url, nimble_rx)));

    Self {
      config,
      keystore,
      server,
      client_reply_handlers,
      kvs_manager,
      nimble_client,
    }
  }

  pub async fn run(&self) -> JoinSet<()> {
    let mut handles = JoinSet::new();

    let server = self.server.clone();
    let kvs_manager = self.kvs_manager.clone();
    let nimble_client = self.nimble_client.clone();
    let mut client_reply_handlers = self.client_reply_handlers.iter().map(|handler| handler.clone()).collect::<Vec<_>>();

    handles.spawn(async move {
      let _ = Server::<PinnedNimbleKVServerContext>::run(server).await;
    });

    handles.spawn(async move {
      let _ = KVSManager::run(kvs_manager).await;
    });

    handles.spawn(async move {
      let _ = NimbleClient::run(nimble_client).await;
    });

    for handler in client_reply_handlers.drain(..) {
      handles.spawn(async move {
        let _ = ClientReplyHandler::run(handler).await;
      });
    }
    handles
  }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the JSON configuration file
    #[arg(short, long)]
    config: String,

    /// HTTP Endpoint for Nimble.
    #[arg(short, long)]
    nimble_endpoint_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("kvs=info,psl=warn")
        .init();

    let args = Args::parse();
    
    info!("Loading configuration from: {}", args.config);
    
    // Read and parse the JSON config file directly into psl::config::Config
    let config_content = fs::read_to_string(&args.config)
        .with_context(|| format!("Failed to read config file: {}", args.config))?;
    
    let psl_config: Config = serde_json::from_str(&config_content)
        .with_context(|| "Failed to parse JSON config")?;
    
    info!("Configuration loaded: {:?}", psl_config);
    
    // Create the keystore using the parsed config
    let keystore = KeyStore::new(
        &psl_config.rpc_config.allowed_keylist_path,
        &psl_config.rpc_config.signing_priv_key_path,
    );
    let atomic_keystore = AtomicKeyStore::new(keystore);
    
    // Create AtomicConfig from the parsed psl config
    let atomic_config = AtomicConfig::new(psl_config);
    
    info!("Creating NimbleKVSNode...");
    
    // Create the NimbleKVSNode
    let node = NimbleKVSNode::new(atomic_config, atomic_keystore, args.nimble_endpoint_url);
    
    info!("Starting NimbleKVSNode...");
    
    // Run the node
    let mut handles = node.run().await;
    
    info!("NimbleKVSNode is running. Waiting for completion...");
    
    // Wait for all tasks to complete
    while let Some(result) = handles.join_next().await {
        match result {
            Ok(_) => info!("Task completed successfully"),
            Err(e) => error!("Task failed: {:?}", e),
        }
    }
    
    info!("NimbleKVSNode has stopped");
    Ok(())
}