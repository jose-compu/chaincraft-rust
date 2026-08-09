# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1] - 2026-08-09

### Fixed
- Serialize local-discovery tests with an async mutex to avoid flaky races on the shared `LOCAL_NODES` registry
- Add Homebrew GMP link path for Apple Silicon (`vdf-crypto`)
- Allow `clippy::double_must_use` false positives from `async_trait` expansion
- Ignore coverage artifacts and local `node_*.db/` databases

### Changed
- Bump crate version to 0.4.1

## [0.3.0] - 2026-04-07

### Changed
- Bump crate version to 0.3.0.
- Migrate Python-aligned core shared objects into Rust.

## [0.2.4] - 2025-03-12

### Fixed
- Remove corrupted test_slush.rs:*:* files from git

## [0.2.2] - 2025-03-12

### Added
- Slush protocol example from Avalanche paper (Section 2.2) as ApplicationObject
- `examples/slush_example.rs`: 10-node metastable consensus demo over real UDP
- `tests/test_slush.rs`: 13 tests (unit + integration)

### Changed
- Package and crate renamed from `chaincraft-rust` to `chaincraft` (crates.io/docs.rs)
- GitHub repository remains `chaincraft-rust`
- Expose `ApplicationObjectRegistry.objects` for protocol examples

## [0.2.1] - 2025-03-10

### Fixed
- VDF tests for CI (`--all-features`): iteration minimum 66, conditional test for feature-off case
- CI: install libgmp-dev for vdf-crypto feature

## [0.2.0] - 2025-03-05

### Added
- ECDSA-based VRF primitive (matches Python `crypto_primitives/vrf.py`)
- Symmetric encryption (Fernet, Python-compatible) in `SymmetricEncryption`
- `rand_core` feature for ed25519-dalek key generation

### Changed
- Removed libp2p dependency; all P2P/socket logic is in-house (UDP)
- Updated `openssl-tls` feature (no longer depends on libp2p)

### Removed
- libp2p and libp2p-websocket dependencies

## [0.1.0] - 2025-06-11

### Added
- Initial release of ChainCraft Rust
- Core blockchain data structures and validation
- P2P networking with peer discovery
- Cryptographic primitives (Ed25519, secp256k1)
- Modular consensus framework
- Flexible storage backends (memory, persistent)
- CLI interface for node management
- Comprehensive test suite
- Documentation and examples
- GitHub Actions for CI/CD
- Automated publishing to crates.io

### Features
- **Node Management**: Start, stop, and configure blockchain nodes
- **Cryptography**: Key generation, signing, and verification
- **Networking**: P2P communication and peer discovery  
- **Storage**: Multiple storage backend options
- **CLI**: Command-line interface for easy node operations
- **Modularity**: Pluggable components for customization

### Security
- Memory-safe Rust implementation
- Cryptographic operations using audited libraries
- Input validation throughout the codebase

### Performance
- Async networking with tokio
- Efficient serialization with bincode
- Configurable resource limits
- Optimized cryptographic operations

[Unreleased]: https://github.com/jose-blockchain/chaincraft-rust/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/jose-blockchain/chaincraft-rust/compare/v0.2.4...v0.3.0
[0.2.4]: https://github.com/jose-blockchain/chaincraft-rust/compare/v0.2.2...v0.2.4
[0.2.2]: https://github.com/jose-blockchain/chaincraft-rust/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/jose-blockchain/chaincraft-rust/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/jose-blockchain/chaincraft-rust/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jose-blockchain/chaincraft-rust/releases/tag/v0.1.0 