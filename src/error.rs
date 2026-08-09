//! Error types for the Chaincraft library

use std::net::SocketAddr;
use thiserror::Error;

/// Result type alias for Chaincraft operations
pub type Result<T> = std::result::Result<T, ChaincraftError>;

/// Main error type for Chaincraft operations
#[derive(Error, Debug)]
pub enum ChaincraftError {
    /// Network-related errors
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    /// Cryptographic errors
    #[error("Cryptographic error: {0}")]
    Crypto(#[from] CryptoError),

    /// Storage-related errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),

    /// Validation errors
    #[error("Validation error: {0}")]
    Validation(String),

    /// Consensus-related errors
    #[error("Consensus error: {0}")]
    Consensus(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Generic IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic errors with message
    #[error("{0}")]
    Generic(String),
}

/// Network-specific error types
#[derive(Error, Debug)]
pub enum NetworkError {
    /// Failed to bind to socket
    #[error("Failed to bind to {addr}: {source}")]
    BindFailed {
        addr: SocketAddr,
        source: std::io::Error,
    },

    /// Failed to connect to peer
    #[error("Failed to connect to {addr}: {source}")]
    ConnectionFailed {
        addr: SocketAddr,
        source: std::io::Error,
    },

    /// Peer is banned
    #[error("Peer {addr} is banned until {expires_at}")]
    PeerBanned {
        addr: SocketAddr,
        expires_at: chrono::DateTime<chrono::Utc>,
    },

    /// Message too large
    #[error("Message size {size} exceeds maximum {max_size}")]
    MessageTooLarge { size: usize, max_size: usize },

    /// Invalid message format
    #[error("Invalid message format: {reason}")]
    InvalidMessage { reason: String },

    /// Timeout occurred
    #[error("Operation timed out after {duration:?}")]
    Timeout { duration: std::time::Duration },

    /// No peers available
    #[error("No peers available for operation")]
    NoPeersAvailable,

    /// NAT traversal discovery failed
    #[error("NAT traversal discovery failed: {reason}")]
    NatDiscoveryFailed { reason: String },

    /// Hole punch session failed
    #[error("Hole punch to {addr} failed: {reason}")]
    HolePunchFailed { addr: SocketAddr, reason: String },
}

/// Cryptographic error types
#[derive(Error, Debug)]
pub enum CryptoError {
    /// Invalid signature
    #[error("Invalid signature")]
    InvalidSignature,

    /// Invalid public key
    #[error("Invalid public key: {reason}")]
    InvalidPublicKey { reason: String },

    /// Invalid private key
    #[error("Invalid private key: {reason}")]
    InvalidPrivateKey { reason: String },

    /// Hash verification failed
    #[error("Hash verification failed")]
    HashVerificationFailed,

    /// Proof of work verification failed
    #[error("Proof of work verification failed")]
    ProofOfWorkFailed,

    /// VRF verification failed
    #[error("VRF verification failed")]
    VrfVerificationFailed,

    /// VDF verification failed
    #[error("VDF verification failed")]
    VdfVerificationFailed,

    /// VDF error (solve/verify)
    #[error("VDF error: {reason}")]
    VdfError { reason: String },

    /// Key generation failed
    #[error("Key generation failed: {reason}")]
    KeyGenerationFailed { reason: String },

    /// Encryption failed
    #[error("Encryption failed: {reason}")]
    EncryptionFailed { reason: String },

    /// Decryption failed
    #[error("Decryption failed: {reason}")]
    DecryptionFailed { reason: String },
}

/// Storage-related error types
#[derive(Error, Debug)]
pub enum StorageError {
    /// Database operation failed
    #[error("Database operation failed: {reason}")]
    DatabaseOperation { reason: String },

    /// Key not found
    #[error("Key not found: {key}")]
    KeyNotFound { key: String },

    /// Serialization failed
    #[error("Failed to serialize data: {reason}")]
    SerializationFailed { reason: String },

    /// Deserialization failed
    #[error("Failed to deserialize data: {reason}")]
    DeserializationFailed { reason: String },

    /// Database corruption detected
    #[error("Database corruption detected: {reason}")]
    Corruption { reason: String },

    /// Database is read-only
    #[error("Attempted to write to read-only database")]
    ReadOnly,

    /// Transaction failed
    #[error("Transaction failed: {reason}")]
    TransactionFailed { reason: String },
}

/// Serialization error types
#[derive(Error, Debug)]
pub enum SerializationError {
    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Binary serialization error
    #[error("Binary serialization error: {0}")]
    Binary(#[from] bincode::Error),

    /// Invalid message format
    #[error("Invalid message format: expected {expected}, got {actual}")]
    InvalidFormat { expected: String, actual: String },

    /// Missing required field
    #[error("Missing required field: {field}")]
    MissingField { field: String },

    /// Field validation failed
    #[error("Field validation failed for {field}: {reason}")]
    FieldValidation { field: String, reason: String },
}

impl ChaincraftError {
    /// Create a validation error
    pub fn validation<T: Into<String>>(msg: T) -> Self {
        ChaincraftError::Validation(msg.into())
    }

    /// Create a consensus error
    pub fn consensus<T: Into<String>>(msg: T) -> Self {
        ChaincraftError::Consensus(msg.into())
    }

    /// Create a configuration error
    pub fn config<T: Into<String>>(msg: T) -> Self {
        ChaincraftError::Config(msg.into())
    }

    /// Create a generic error
    pub fn generic<T: Into<String>>(msg: T) -> Self {
        ChaincraftError::Generic(msg.into())
    }
}

impl From<serde_json::Error> for ChaincraftError {
    fn from(err: serde_json::Error) -> Self {
        ChaincraftError::Serialization(SerializationError::Json(err))
    }
}
