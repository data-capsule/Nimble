use tonic::{transport::Server, Request, Response, Status};
use clap::Parser;
use std::net::SocketAddr;
use tracing::{debug, error, info, warn};

use psl_lb::{
    config::Config, error::PslError, psl_proto::{psl_storage_call_server::{PslStorageCall, PslStorageCallServer}, ReadRemoteReq, ReadRemoteResp, StoreRemoteReq, StoreRemoteResp}, storage::{InMemoryStorage, StorageManager}, StorageBackend
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The address to bind the gRPC server to
    #[arg(short, long)]
    addr: Option<String>,
    
    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,
}

pub struct PslStorageCallService<T: StorageBackend> {
    storage: StorageManager<T>,
    config: Config,
}

impl<T: StorageBackend> PslStorageCallService<T> {
    pub async fn new(config: Config) -> Self {
        let storage = StorageManager::<T>::new(config.storage.num_tasks.unwrap_or(1)).await;
        Self { storage, config }
    }
}

#[tonic::async_trait]
impl<T: StorageBackend + 'static> PslStorageCall for PslStorageCallService<T> {
    async fn store_remote(
        &self,
        request: Request<StoreRemoteReq>,
    ) -> Result<Response<StoreRemoteResp>, Status> {
        let req = request.into_inner();
        debug!("StoreRemote called: origin_id={}, seq_num={}, data_size={}", 
              req.origin_id, req.seq_num, req.data.len());
        
        // // Validate request
        // if req.data.is_empty() {
        //     return Err(Status::invalid_argument("Data cannot be empty"));
        // }
        
        // Store the data
        match self.storage.store(req.origin_id, req.seq_num, req.data).await {
            Ok(()) => {
                debug!("Successfully stored data for origin_id={}, seq_num={}", req.origin_id, req.seq_num);
                let response = StoreRemoteResp { success: true };
                Ok(Response::new(response))
            }
            Err(e) => {
                error!("Failed to store data: {}", e);
                Err(e.into())
            }
        }
    }

    async fn read_remote(
        &self,
        request: Request<ReadRemoteReq>,
    ) -> Result<Response<ReadRemoteResp>, Status> {
        let req = request.into_inner();
        debug!("ReadRemote called: origin_id={}, seq_num={}", req.origin_id, req.seq_num);
        
        // Retrieve the data
        match self.storage.read(req.origin_id, req.seq_num).await {
            Ok(Some(data)) => {
                debug!("Successfully retrieved data for origin_id={}, seq_num={}, size={}", 
                      req.origin_id, req.seq_num, data.len());
                let response = ReadRemoteResp { data };
                Ok(Response::new(response))
            }
            Ok(None) => {
                warn!("No data found for origin_id={}, seq_num={}", req.origin_id, req.seq_num);
                Err(Status::not_found("Data not found"))
            }
            Err(e) => {
                error!("Failed to read data: {}", e);
                Err(e.into())
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("psl_lb=warn,tower=warn,tonic=warn")
        .init();

    let args = Args::parse();
    
    // Load configuration
    let config = if let Some(config_file) = args.config {
        std::env::set_var("PSL_CONFIG_FILE", config_file);
        Config::from_env()?
    } else {
        Config::from_env().unwrap_or_default()
    };
    
    // Override address if provided via command line
    let addr = if let Some(addr) = args.addr {
        addr
    } else {
        config.server.addr.clone()
    };
    
    info!("Starting PSL Load Balancer gRPC server...");
    info!("Configuration: {:?}", config);
    info!("Binding to address: {}", addr);

    let socket_addr: SocketAddr = addr.parse()?;
    
    let service = PslStorageCallService::<InMemoryStorage>::new(config).await;
    
    let svc = PslStorageCallServer::new(service);
    
    info!("Server listening on {}", socket_addr);
    
    Server::builder()
        .add_service(svc)
        .serve(socket_addr)
        .await?;

    Ok(())
}
