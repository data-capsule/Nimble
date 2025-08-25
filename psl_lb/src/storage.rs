use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::error::PslError;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn store(&self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError>;
    async fn read(&self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError>;
    async fn health_check(&self) -> Result<(), PslError>;
}

#[derive(Debug)]
pub struct InMemoryStorage {
    data: Arc<RwLock<HashMap<(u64, u64), Vec<u8>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn store(&self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError> {
        let mut storage = self.data.write().await;
        storage.insert((origin_id, seq_num), data);
        Ok(())
    }

    async fn read(&self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        let storage = self.data.read().await;
        Ok(storage.get(&(origin_id, seq_num)).cloned())
    }

    async fn health_check(&self) -> Result<(), PslError> {
        // In-memory storage is always healthy
        Ok(())
    }
}

pub struct StorageManager {
    backend: Box<dyn StorageBackend>,
}

impl StorageManager {
    pub fn new(backend: Box<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    pub async fn store(&self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError> {
        self.backend.store(origin_id, seq_num, data).await
    }

    pub async fn read(&self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        self.backend.read(origin_id, seq_num).await
    }

    pub async fn health_check(&self) -> Result<(), PslError> {
        self.backend.health_check().await
    }
}
