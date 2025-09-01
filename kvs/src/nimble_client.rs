use psl::{config::AtomicConfig, utils::channel::{Receiver, Sender}};
use tokio::sync::{oneshot, Mutex};
use tracing::debug;
use std::sync::Arc;

pub struct NimbleClient {
    config: AtomicConfig,
    nimble_rx: Receiver<(Sender<()>, (Vec<u8>, Vec<u8>))>,
}

impl NimbleClient {
    pub fn new(config: AtomicConfig, nimble_rx: Receiver<(Sender<()>, (Vec<u8>, Vec<u8>))>) -> Self {
        Self { config, nimble_rx }
    }

    pub async fn run(client: Arc<Mutex<Self>>) -> Option<()> {
        debug!("NimbleClient started");
        let mut client = client.lock().await;
        loop {
            let Some((tx, (key, value))) = client.nimble_rx.recv().await else {
                debug!("No message received");
                continue;
            };
            let _ = tx.send(()).await;
        }

        Some(())
    }
}
