use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum PslError {
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<PslError> for Status {
    fn from(err: PslError) -> Self {
        match err {
            PslError::Storage(_) => Status::unavailable(err.to_string()),
            PslError::Config(_) => Status::internal(err.to_string()),
            PslError::Network(_) => Status::unavailable(err.to_string()),
            PslError::InvalidRequest(_) => Status::invalid_argument(err.to_string()),
            PslError::Internal(_) => Status::internal(err.to_string()),
        }
    }
}

impl From<std::net::AddrParseError> for PslError {
    fn from(err: std::net::AddrParseError) -> Self {
        PslError::Config(format!("Invalid address: {}", err))
    }
}

impl From<config::ConfigError> for PslError {
    fn from(err: config::ConfigError) -> Self {
        PslError::Config(err.to_string())
    }
}
