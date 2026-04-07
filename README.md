<div align="center">

# Chaincraft Rust

**Chaincraft: A high-performance framework for blockchain prototyping and production-ready decentralized protocols**

[![Crates.io](https://img.shields.io/crates/v/chaincraft.svg)](https://crates.io/crates/chaincraft)
[![Documentation](https://docs.rs/chaincraft/badge.svg)](https://docs.rs/chaincraft)
[![CI](https://github.com/jose-blockchain/chaincraft-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/jose-blockchain/chaincraft-rust/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

[![Rust](https://img.shields.io/badge/rust-1.82%2B-orange.svg)](https://www.rust-lang.org/)
[![Crates.io Downloads](https://img.shields.io/crates/d/chaincraft.svg)](https://crates.io/crates/chaincraft)

</div>

## Overview

Chaincraft: A high-performance library for blockchain prototyping and production-ready decentralized protocols. Chaincraft Rust provides a clean, well-documented implementation of core blockchain concepts with a focus on performance, security, and production-stability.

## Features

- **High Performance**: Built with Rust for maximum performance and memory safety
- **Educational Focus**: Well-documented code with clear explanations of blockchain concepts
- **Modular Design**: Pluggable consensus mechanisms, storage backends, and network protocols
- **Cryptographic Primitives**: Ed25519, ECDSA/secp256k1, VRF, Proof-of-Work, VDF, symmetric encryption (Fernet)
- **Network Protocol**: P2P networking with peer discovery and message propagation
- **Flexible Storage**: Memory and persistent storage options with optional SQLite indexing
- **CLI Interface**: Easy-to-use command-line interface for node management

## Quick Start

### Installation

#### From Crates.io

```bash
cargo install chaincraft
```

#### From Source

```bash
git clone https://github.com/jose-blockchain/chaincraft-rust.git
cd chaincraft-rust
cargo build --release
```

### Running a Node

Start a Chaincraft node with default settings:

```bash
chaincraft-cli start
```

Or with custom configuration:

```bash
chaincraft-cli start --port 8080 --max-peers 20 --debug
```

### Generate a Keypair

```bash
chaincraft-cli keygen
```

## Usage as a Library

Add Chaincraft Rust to your `Cargo.toml`:

```toml
[dependencies]
chaincraft = "0.3.2"
```

### Basic Example

```rust
use chaincraft::{ChaincraftNode, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut node = ChaincraftNode::builder()
        .port(21000)
        .max_peers(10)
        .build()?;

    println!("Starting node {} on port {}", node.id(), node.port());
    
    node.start().await?;
    
    // Your application logic here
    
    node.stop().await?;
    Ok(())
}
```

### Advanced Configuration

```rust
use chaincraft::{ChaincraftNode, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut node = ChaincraftNode::builder()
        .port(21000)
        .max_peers(50)
        .with_persistent_storage(true)  // requires feature "persistent"
        .local_discovery(true)
        .persist_peers(true)
        .build()?;

    node.start().await?;
    
    // Node is now running with persistent storage and local discovery
    
    Ok(())
}
```

## Architecture

Chaincraft Rust is built with a modular architecture:

- **Core**: Basic blockchain data structures and validation logic
- **Consensus**: Pluggable consensus mechanisms (Tendermint-style, randomness beacon)
- **Network**: P2P networking layer with peer discovery
- **Storage**: Flexible storage backends (memory, persistent, indexed)
- **Crypto**: Cryptographic primitives and utilities
- **CLI**: Command-line interface for node management

### Network Architecture (Rust Version)

The Rust implementation uses **raw UDP sockets** (no libp2p) for simplicity and control:

1. **Bind**: Each node binds a UDP socket to `host:port` (port `0` = ephemeral).
2. **Gossip Loop**: A background task periodically rebroadcasts stored message hashes to all known peers.
3. **Receive Loop**: Incoming datagrams are parsed as JSON `SharedMessage`, deduplicated, stored, and rebroadcast.
4. **Local Discovery**: In-process nodes register in a static `LOCAL_NODES` map for automatic peer discovery within the same process.
5. **Message Flow**: `create_shared_message_with_data()` stores a message, processes it through `ApplicationObject`s, and broadcasts to peers.

### Running Multi-Node Demos

Start multiple nodes in separate terminals or use the multi-node script:

```bash
# Terminal 1
chaincraft-cli start --port 9001

# Terminal 2
chaincraft-cli start --port 9002 --peer 127.0.0.1:9001

# Terminal 3
chaincraft-cli start --port 9003 --peer 127.0.0.1:9001 --peer 127.0.0.1:9002
```

Or run the shared objects example (in-process multi-node):

```bash
cargo run --example shared_objects_example
```

### Python vs Rust Feature Mapping

| Python (chaincraft)   | Rust (chaincraft)  |
|-----------------------|-------------------------|
| `node.py`             | `src/node.rs`           |
| `SharedMessage`       | `shared::SharedMessage` |
| `ApplicationObject`   | `shared_object::ApplicationObject` |
| `SimpleSharedNumber`  | `shared_object::SimpleSharedNumber` |
| `SimpleChainObject`   | `shared_object::MerkelizedChain`    |
| Message chain         | `shared_object::MessageChain`       |
| ECDSA ledger          | `examples::ecdsa_ledger::ECDSALedgerObject` |
| `dbm` / on-disk storage | `sled` (feature `persistent`) |
| `local_discovery`     | `NodeConfig::local_discovery` + `LOCAL_NODES` registry |
| `gossip` loop         | Gossip task in `start_networking()` |
| UDP broadcast         | `broadcast_bytes()` via `UdpSocket` |

## Features

### Default Features

- `compression`: Enable message compression for network efficiency

### Optional Features

- `persistent`: Enable persistent storage using sled
- `indexing`: Enable SQLite-based transaction indexing
- `vdf-crypto`: Enable VDF (Verifiable Delay Function) support via [vdf](https://crates.io/crates/vdf) crate. Requires GMP: `apt install libgmp-dev` or `brew install gmp`

Enable features in your `Cargo.toml`:

```toml
[dependencies]
chaincraft = { version = "0.3.2", features = ["persistent", "indexing"] }
```

## Development

### Prerequisites

- Rust 1.82 or later
- Git

### Building

```bash
git clone https://github.com/jose-blockchain/chaincraft-rust.git
cd chaincraft-rust
cargo build
```

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with all features enabled
cargo test --all-features

# Run example integration tests
cargo test --test examples_integration
```

### Running Benchmarks

```bash
cargo bench
```

### Code Coverage

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --out Html
```

## API Documentation

Full API documentation is available on [docs.rs](https://docs.rs/chaincraft).

To build documentation locally:

```bash
cargo doc --open --all-features
```

## Examples

Check out the `examples/` directory for more usage examples:

- `basic_node.rs`: Simple node setup and operation
- `keypair_generation.rs`: Cryptographic keypair generation and signing
- `chatroom_example.rs`: Decentralized chatroom protocol (create rooms, post messages)
- `randomness_beacon_example.rs`: Verifiable randomness beacon
- `shared_objects_example.rs`: Multi-node network with shared object propagation
- `proof_of_work_example.rs`: Proof of Work mining and verification

Run examples with:

```bash
cargo run --example basic_node
cargo run --example chatroom_example
cargo run --example shared_objects_example
```

## Contributing

We welcome contributions via pull requests.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for your changes
5. Ensure all tests pass (`cargo test --all-features`)
6. Run clippy (`cargo clippy --all-features`)
7. Format your code (`cargo fmt`)
8. Commit your changes (`git commit -am 'Add amazing feature'`)
9. Push to the branch (`git push origin feature/amazing-feature`)
10. Open a Pull Request

## Performance

Chaincraft Rust is designed for high performance:

- Zero-copy serialization where possible
- Efficient async networking with tokio
- Optimized cryptographic operations
- Configurable resource limits

## Security

Security is a top priority:

- Memory-safe Rust implementation
- Cryptographic operations use well-audited libraries
- Network protocol includes message authentication
- Input validation and sanitization throughout

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Tokio](https://tokio.rs/) for async runtime
- Cryptography powered by [RustCrypto](https://github.com/RustCrypto)
- Custom UDP-based P2P networking (no libp2p dependency)

## Roadmap

- [X] Advanced consensus mechanisms (PBFT)
- [ ] Smart contract support
- [ ] Enhanced monitoring and metrics
- [ ] WebAssembly runtime integration

---

For more information, visit our [documentation](https://docs.rs/chaincraft) or [repository](https://github.com/jose-blockchain/chaincraft-rust). 
