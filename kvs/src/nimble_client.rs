use psl::{config::AtomicConfig, crypto::{hash, AtomicKeyStore, HashType}, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::{Duration, Instant}};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};

pub struct NimbleClient {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    nimble_rx: Receiver<(tokio::sync::oneshot::Sender<()>, Vec<u8>)>,
    nimble_endpoint_url: String,

    current_hash: HashType,
    current_counter: usize,
    buffered_ops: usize,
    buffered_replies: Vec<tokio::sync::oneshot::Sender<()>>,

    handle: String,
    client: reqwest::Client,
    last_batch_time: Instant,

    batch_timer: Arc<Pin<Box<ResettableTimer>>>,
}

#[derive(bincode::Encode)]
struct NimblePayload {
    counter: usize,
    state_hash: Vec<u8>,
}


#[derive(Serialize)]
struct CreateRequest {
    Tag: String,
}

#[derive(Serialize)]
struct IncrementRequest {
    Tag: String,
    ExpectedCounter: u64,
}

#[derive(Serialize)]
struct ReadRequest {
    Tag: String,
}

impl NimbleClient {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        nimble_endpoint_url: String,
        nimble_rx: Receiver<(tokio::sync::oneshot::Sender<()>, Vec<u8>)>
    ) -> Self {
        let my_name = &config.get().net_config.name;
        let pub_key = keystore.get().get_pubkey(my_name).unwrap().clone();
        let mut handle_bytes = b"nimble_kvs".to_vec();
        handle_bytes.extend_from_slice(pub_key.as_bytes());
        let handle = hash(&handle_bytes);
        let handle = URL_SAFE.encode(handle.as_slice());

        let client = reqwest::Client::new();

        let batch_timer = ResettableTimer::new(Duration::from_millis(config.get().consensus_config.batch_max_delay_ms));
        
        Self {
            config, keystore, nimble_rx,
            nimble_endpoint_url,
            current_hash: vec![0u8; 32], current_counter: 0,
            buffered_ops: 0, buffered_replies: vec![],
            handle,
            client,
            last_batch_time: Instant::now(),
            batch_timer,
        }
    }

    pub async fn run(client: Arc<Mutex<Self>>) -> Option<()> {
        info!("NimbleClient started");
        let mut client = client.lock().await;

        client.propose_new_counter().await;

        client.batch_timer.run().await;

        loop {
            tokio::select! {
                Some((tx, new_hash)) = client.nimble_rx.recv() => {
                    client.buffered_ops += 1;
                    client.buffered_replies.push(tx);
                    client.current_hash = new_hash;
                },

                _ = client.batch_timer.wait() => {
                }    
            }

            let batch_size = 1; client.config.get().consensus_config.max_backlog_batch_size;
            // let batch_timeout = client.config.get().consensus_config.batch_max_delay_ms as u128;

            if client.buffered_ops >= batch_size
            // || (client.last_batch_time.elapsed().as_millis() >= batch_timeout && client.buffered_replies.len() > 0)
            {
                client.current_counter += 1;
                client.propose_new_counter().await;
            }

        }

        Some(())
    }


    async fn propose_new_counter(&mut self) {
        let payload = NimblePayload {
            counter: self.current_counter,
            state_hash: self.current_hash.clone().try_into().unwrap(),
        };

        let payload_bytes = bincode::encode_to_vec(&payload, bincode::config::standard()).unwrap();

        let payload_signature = self.keystore.get().sign(&payload_bytes);

        let mut tag = Vec::new();
        tag.extend_from_slice(&payload_bytes);
        tag.extend_from_slice(&payload_signature);

        self.send_to_nimble(tag).await;


        self.clear_buffered_replies().await;

        self.buffered_ops = 0;
        self.last_batch_time = Instant::now();

        info!("Proposed new counter: {}", self.current_counter);
    }

    async fn clear_buffered_replies(&mut self) {
        for reply in self.buffered_replies.drain(..) {
            let _ = reply.send(());
        }
    }

    async fn send_to_nimble(&mut self, tag: Vec<u8>) {
        let addr = format!("{}/counters/{}", self.nimble_endpoint_url, self.handle);
        let request = match self.current_counter {
            0 => {
                let json = CreateRequest {
                    Tag: URL_SAFE.encode(tag),
                };

                self.client.put(addr) // PUT => Create
                    .json(&json)


            },
            _ => {
                let json = IncrementRequest {
                    Tag: URL_SAFE.encode(tag),
                    ExpectedCounter: self.current_counter as u64,
                };

                self.client.post(addr) // POST => Increment
                    .json(&json)
            },
        };

        let res = request.send().await;
        if res.is_err() {
            error!("failed to send request to nimble: {:?}", res);
            return;
        }

        let res = res.unwrap();
        if res.status() != reqwest::StatusCode::OK && res.status() != reqwest::StatusCode::CREATED {
            warn!("Nimble error: {:?}", res);
        }

    }
}
