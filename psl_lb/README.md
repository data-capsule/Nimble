# PSL Load Balancer gRPC Server

A high-performance gRPC server implementation for PSL (Proof of Stake Ledger) storage operations using Tonic and Rust.

## Features

- **gRPC Server**: Implements the `PSLStorageCall` service with `StoreRemote` and `ReadRemote` operations
- **Configurable**: Supports configuration via environment variables, config files, and command-line arguments
- **Storage Backend**: Pluggable storage interface with in-memory implementation
- **Logging**: Comprehensive logging with tracing and structured logging
- **Error Handling**: Proper error handling with gRPC status codes
- **Async/Await**: Full async support using Tokio runtime

## Prerequisites

- Rust 1.70+ 
- Cargo

## Building

```bash
cargo build --release
```

## Running

### Basic Usage

```bash
# Run with default configuration (localhost:50051)
cargo run

# Run on specific address
cargo run -- --addr 0.0.0.0:50051

# Run with custom config file
cargo run -- --config config.toml
```

### Configuration

The server can be configured using:

1. **Environment Variables**: Use `PSL__` prefix
   ```bash
   export PSL__SERVER__ADDR="0.0.0.0:50051"
   export PSL__STORAGE__BACKEND="memory"
   ```

2. **Configuration File**: Create a `config.toml` file
   ```toml
   [server]
   addr = "0.0.0.0:50051"
   max_concurrent_streams = 100
   max_frame_size = 1048576

   [storage]
   backend = "memory"
   ```

3. **Command Line Arguments**:
   ```bash
   cargo run -- --addr 0.0.0.0:50051 --config config.toml
   ```

## API

### StoreRemote

Stores data for a specific origin and sequence number.

```protobuf
rpc StoreRemote(StoreRemoteReq) returns (StoreRemoteResp);

message StoreRemoteReq {
    uint64 origin_id = 1;
    bytes data = 2;
    uint64 seq_num = 3;
}

message StoreRemoteResp {
    bool success = 1;
}
```

### ReadRemote

Retrieves data for a specific origin and sequence number.

```protobuf
rpc ReadRemote(ReadRemoteReq) returns (ReadRemoteResp);

message ReadRemoteReq {
    uint64 origin_id = 1;
    uint64 seq_num = 2;
}

message ReadRemoteResp {
    bytes data = 1;
}
```

## Testing

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo test
```

## Development

### Project Structure

```
src/
├── main.rs          # Main server entry point
├── lib.rs           # Library exports and module organization
├── config.rs        # Configuration management
├── error.rs         # Error handling and types
└── storage.rs       # Storage backend interface and implementations
```

### Adding New Storage Backends

Implement the `StorageBackend` trait:

```rust
use async_trait::async_trait;
use crate::error::PslError;

#[async_trait]
impl StorageBackend for YourStorageBackend {
    async fn store(&self, origin_id: u64, seq_num: u64, data: Vec<u8>) -> Result<(), PslError> {
        // Your implementation
    }
    
    async fn read(&self, origin_id: u64, seq_num: u64) -> Result<Option<Vec<u8>>, PslError> {
        // Your implementation
    }
    
    async fn health_check(&self) -> Result<(), PslError> {
        // Your implementation
    }
}
```

## Logging

The server uses structured logging with the following log levels:

- `error`: Error conditions
- `warn`: Warning conditions  
- `info`: General information
- `debug`: Detailed debug information

Set log level via environment variable:

```bash
RUST_LOG=psl_lb=debug cargo run
```

## Performance

- **Concurrent Streams**: Configurable via `max_concurrent_streams`
- **Frame Size**: Configurable via `max_frame_size`
- **Async I/O**: Full async/await support with Tokio runtime
- **Memory Management**: Efficient memory usage with Arc and RwLock

## Security Considerations

- The server binds to localhost by default
- Use proper authentication and authorization in production
- Consider TLS/TLS termination for production deployments
- Validate all input data and implement rate limiting

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the same license as the parent project.
