use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::error;
use tracing::warn;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use crate::config::InMemoryStorageConfig;
use crate::error::PslError;

pub trait ConfigType<'a>: Clone + Send + Sync {
    fn num_tasks(&self) -> usize;
    fn commit_threshold(&self) -> usize;
}

#[async_trait]
pub trait StorageBackend<'a>: Send + Sync {
    type Config: ConfigType<'a>;

    fn new(cmd_rx: mpsc::Receiver<StorageCommand>, config: Self::Config) -> Self;
    async fn run(&mut self);
    async fn store(&mut self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError>;
    async fn read(&mut self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError>;
    async fn health_check(&mut self) -> Result<(), PslError>;
}

pub enum StorageCommand {
    Store(u64, u64, Vec<u8>, oneshot::Sender<Result<(), PslError>>),
    Read(u64, u64, oneshot::Sender<Result<Option<Vec<u8>>, PslError>>),
    HealthCheck(oneshot::Sender<Result<(), PslError>>),
}

#[derive(Debug)]
pub struct InMemoryStorage {
    data: HashMap<(u64, u64), Vec<u8>>,
    cmd_rx: mpsc::Receiver<StorageCommand>,
}


#[async_trait]
impl<'a> StorageBackend<'a> for InMemoryStorage {
    type Config = InMemoryStorageConfig;
    fn new(cmd_rx: mpsc::Receiver<StorageCommand>, config: Self::Config) -> Self {
        Self {
            data: HashMap::new(),
            cmd_rx,
        }
    }

    async fn store(&mut self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError> {
        self.data.insert((origin_id, seq_num), data);
        Ok(())
    }

    async fn read(&mut self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        Ok(self.data.get(&(origin_id, seq_num)).cloned())
    }

    async fn health_check(&mut self) -> Result<(), PslError> {
        // In-memory storage is always healthy
        Ok(())
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                StorageCommand::Store(origin_id, seq_num, data, tx) => {
                    let _ = self.store(origin_id, seq_num, data).await;
                    let _ = tx.send(Ok(()));
                },
                StorageCommand::Read(origin_id, seq_num, tx) => {
                    warn!("Received read command for chain {}", origin_id);
                    let data = self.read(origin_id, seq_num).await;
                    warn!("Read command for chain {} returned", origin_id);
                    let _ = tx.send(data);
                },
                StorageCommand::HealthCheck(tx) => {
                    let data = self.health_check().await;
                    let _ = tx.send(data);
                },
            }
        }

        error!("InMemoryStorage run loop ended");
    }
}

pub struct StorageManager<'a, T: StorageBackend<'a>> {
    backend_txs: Vec<mpsc::Sender<StorageCommand>>,
    graveyard_tx: mpsc::Sender<oneshot::Receiver<Result<(), PslError>>>,
    num_tasks: usize,
    __rr_cnt: AtomicUsize,
    marker: PhantomData<&'a T>,
}

impl<'a, T: StorageBackend<'a>> StorageManager<'a, T> 
    where 'a: 'static
{
    pub async fn new(config: T::Config) -> Self {
        let num_tasks = config.num_tasks();
        assert!(num_tasks > 0);
        let mut backend_txs = Vec::new();
        for _ in 0..num_tasks {
            let (tx, rx) = mpsc::channel(1000);
            backend_txs.push(tx);

            let _config = config.clone();
            tokio::spawn(async move {
                let mut backend = T::new(rx, _config);
                backend.run().await;
            });
        }

        let (graveyard_tx, mut graveyard_rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            while let Some(recv_tx) = graveyard_rx.recv().await {
                let _ = timeout(Duration::from_millis(100), recv_tx).await;
            }
        });

        Self { backend_txs, graveyard_tx, num_tasks, __rr_cnt: AtomicUsize::new(0), marker: PhantomData }
    }

    pub async fn store(&self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError> {
        // Store command gets forwarded to all backends
        let mut resp_rxs = Vec::new();
        for tx in self.backend_txs.iter() {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(StorageCommand::Store(origin_id, seq_num, data.clone(), resp_tx)).await.unwrap();
            resp_rxs.push(resp_rx);
        }

        // Wait for majority responses.
        let mut votes = 0;

        while votes < self.num_tasks / 2 {
            let rx = resp_rxs.pop().unwrap();
            let _ = rx.await.unwrap();
            votes += 1;
        }

        for rx in resp_rxs.drain(..) {
            let _ = self.graveyard_tx.send(rx).await;
        }

        Ok(())
    }

    pub async fn read(&self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        // Read command gets forwarded to one backend only, in most cases.
        // But since we only wait for majority responses while writing, we may be unfortunate in a reading before writing case.

        let _idx = self.__rr_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for i in 0..self.num_tasks {
            let (resp_tx, resp_rx) = oneshot::channel();
            
            warn!("Sending read command to backend {}", i);
            self.backend_txs[(_idx + i) % self.num_tasks].send(StorageCommand::Read(origin_id, seq_num, resp_tx)).await.unwrap();
            let data = resp_rx.await.unwrap().unwrap();

            if data.is_some() {
                return Ok(data);
            }
        }

        // If we reached here, the data is truly not present.
        Ok(None)
    }

    pub async fn health_check(&self) -> Result<(), PslError> {
        // Health check command gets forwarded to all backends
        let mut resp_rxs = Vec::new();
        for tx in self.backend_txs.iter() {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(StorageCommand::HealthCheck(resp_tx)).await.unwrap();
            resp_rxs.push(resp_rx);
        }

        // Wait for all responses
        for rx in resp_rxs.drain(..) {
            let _ = rx.await.unwrap();
        }
        Ok(())
    }
}
