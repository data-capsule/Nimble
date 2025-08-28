use psl::{config::PSLWorkerConfig, worker::{PSLWorkerServerContext}};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use crate::psl_storage::WorkerConfig;
use crate::storage::ConfigType;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageBackendConfig {
    InMemory(InMemoryStorageConfig),
    PSL(psl::config::PSLWorkerConfig),
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        Self::InMemory(InMemoryStorageConfig {
            num_tasks: Some(8),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageBackendConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub addr: String,
    pub max_concurrent_streams: Option<u32>,
    pub max_frame_size: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InMemoryStorageConfig {
    pub num_tasks: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                addr: "[::1]:50051".to_string(),
                max_concurrent_streams: Some(500),
                max_frame_size: Some(1024 * 1024 * 1024), // 1GB
            },
            storage: StorageBackendConfig::default(),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        let mut config = config::Config::default();
        
        // Start with default values
        config.merge(config::Config::try_from(&Config::default())?)?;
        
        // Override with environment variables
        config.merge(config::Environment::default().separator("__"))?;
        
        // Override with config file if it exists
        if let Ok(config_file) = std::env::var("PSL_CONFIG_FILE") {
            config.merge(config::File::with_name(&config_file))?;
        }

        
        let mut config: Config = config.try_into()?;

        if let Ok(worker_config_path) = std::env::var("PSL_WORKER_CONFIG") {
            let s = std::fs::read_to_string(worker_config_path).map_err(|e| config::ConfigError::Message(e.to_string()))?;
            config.storage = StorageBackendConfig::PSL(psl::config::PSLWorkerConfig::deserialize(&s));
        }


        Ok(config)
    }
    
    pub fn server_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.server.addr.parse()
    }
}

impl TryFrom<config::Config> for Config {
    type Error = config::ConfigError;

    fn try_from(config: config::Config) -> Result<Self, Self::Error> {
        Ok(config.try_deserialize::<Config>()?)
    }
}

impl<'a> ConfigType<'a> for InMemoryStorageConfig {
    fn num_tasks(&self) -> usize {
        self.num_tasks.unwrap_or(8)
    }

    fn commit_threshold(&self) -> usize {
        self.num_tasks() / 2 + 1
    }
}

impl<'a> ConfigType<'a> for PSLWorkerConfig {
    fn num_tasks(&self) -> usize {
        1
    }

    fn commit_threshold(&self) -> usize {
        self.worker_config.storage_list.len() / 2 + 1
    }
}