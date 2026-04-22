//! Chaincraft - A blockchain education and prototyping platform
//!
//! This library provides a clean, well-documented implementation of core blockchain concepts
//! with a focus on performance, security, and educational value.

#![allow(incomplete_features)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

// Modules
pub mod consensus;
pub mod core_objects;
pub mod crypto;
pub mod discovery;
pub mod error;
pub mod examples;
pub mod network;
pub mod node;
pub mod shared;
pub mod shared_object;
pub mod state_memento;
pub mod storage;
pub mod types;
pub mod utils;

// Re-exports
pub use error::{ChaincraftError, Result};
pub use network::{PeerId, PeerInfo};
pub use node::{clear_local_registry, ChaincraftNode};
pub use shared::{SharedMessage, SharedObject, SharedObjectId, SharedObjectRegistry};
pub use state_memento::{normalize_state_memento, StateMemento};

// Application object re-exports
pub use core_objects::{
    BalanceLedger, Blockchain, CacheObject, CoreSharedObject, DAGObject, DocumentCache, Mempool,
    MerkelizedObject, MerkleizedObject, NativeSharedObject, NonMerkelizedObject, TransactionChain,
    UTXOLedger,
};
pub use examples::blockchain::{
    helpers as blockchain_helpers, BlockchainNode, BlockchainObject, MempoolObject,
};
pub use shared_object::{
    ApplicationObject, ApplicationObjectRegistry, MerkelizedChain, MessageChain, SimpleSharedNumber,
};

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = env!("CARGO_PKG_NAME");
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

/// Default network port for Chaincraft nodes
pub const DEFAULT_PORT: u16 = 8080;

/// Maximum number of peers by default
pub const DEFAULT_MAX_PEERS: usize = 10;

/// Default gossip interval in milliseconds
pub const DEFAULT_GOSSIP_INTERVAL_MS: u64 = 500;
