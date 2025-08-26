use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use core::num;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use crate::error::PslError;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    fn new(cmd_rx: mpsc::Receiver<StorageCommand>) -> Self;
    async fn run(&mut self);
    async fn store(&mut self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError>;
    async fn read(&mut self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError>;
    async fn health_check(&mut self) -> Result<(), PslError>;
}

enum StorageCommand {
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
impl StorageBackend for InMemoryStorage {
    fn new(cmd_rx: mpsc::Receiver<StorageCommand>) -> Self {
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
                    self.store(origin_id, seq_num, data).await;
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
}

pub struct StorageManager<T: StorageBackend> {
    backend_txs: Vec<mpsc::Sender<StorageCommand>>,
    graveyard_tx: mpsc::Sender<oneshot::Receiver<Result<(), PslError>>>,
    num_tasks: usize,
    __rr_cnt: AtomicUsize,
    marker: PhantomData<T>,
}

impl<T: StorageBackend> StorageManager<T> {
    pub async fn new(num_tasks: usize) -> Self {
        assert!(num_tasks > 0);
        let mut backend_txs = Vec::new();
        for _ in 0..num_tasks {
            let (tx, rx) = mpsc::channel(1000);
            backend_txs.push(tx);

            tokio::spawn(async move {
                let mut backend = T::new(rx);
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
