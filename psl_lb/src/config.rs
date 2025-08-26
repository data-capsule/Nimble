use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub addr: String,
    pub max_concurrent_streams: Option<u32>,
    pub max_frame_size: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub backend: String,
    pub connection_string: Option<String>,
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
            storage: StorageConfig {
                backend: "memory".to_string(),
                connection_string: None,
                num_tasks: Some(8),
            },
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
        
        config.try_into()
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