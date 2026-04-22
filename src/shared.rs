//! Shared objects and messages for distributed state management

use crate::error::{ChaincraftError, CryptoError, Result, SerializationError};
use crate::state_memento::StateMemento;
use async_trait::async_trait;
use bincode;
use hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Unique identifier for shared objects
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SharedObjectId(Uuid);

impl SharedObjectId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn into_uuid(self) -> Uuid {
        self.0
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl fmt::Display for SharedObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for SharedObjectId {
    fn default() -> Self {
        Self::new()
    }
}

/// Message types for inter-node communication
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageType {
    /// Peer discovery message
    PeerDiscovery,
    /// Request for local peers
    RequestLocalPeers,
    /// Response with local peers
    LocalPeers,
    /// Request for shared object update
    RequestSharedObjectUpdate,
    /// Response with shared object data
    SharedObjectUpdate,
    /// Request to get an object
    Get,
    /// Request to set an object
    Set,
    /// Request to delete an object
    Delete,
    /// Response containing requested data
    Response,
    /// Notification of changes
    Notification,
    /// Heartbeat/ping message
    Heartbeat,
    /// Error response
    Error,
    /// Request latest digest for digest-based sync
    RequestDigest,
    /// Request messages since a digest
    RequestMessagesSince,
    /// Response with latest digest
    DigestResponse,
    /// Response with messages since digest
    MessagesResponse,
    /// Custom application message
    Custom(String),
}

impl Serialize for MessageType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MessageType::Custom(name) => {
                #[derive(Serialize)]
                struct Custom {
                    #[serde(rename = "Custom")]
                    custom: String,
                }
                Custom {
                    custom: name.clone(),
                }
                .serialize(serializer)
            },
            _ => serializer.serialize_str(&self.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for MessageType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct MessageTypeVisitor;

        impl<'de> Visitor<'de> for MessageTypeVisitor {
            type Value = MessageType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a message type string or Custom object")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<MessageType, E>
            where
                E: Error,
            {
                match value {
                    "PEER_DISCOVERY" => Ok(MessageType::PeerDiscovery),
                    "REQUEST_LOCAL_PEERS" => Ok(MessageType::RequestLocalPeers),
                    "LOCAL_PEERS" => Ok(MessageType::LocalPeers),
                    "REQUEST_SHARED_OBJECT_UPDATE" => Ok(MessageType::RequestSharedObjectUpdate),
                    "SHARED_OBJECT_UPDATE" => Ok(MessageType::SharedObjectUpdate),
                    "GET" => Ok(MessageType::Get),
                    "SET" => Ok(MessageType::Set),
                    "DELETE" => Ok(MessageType::Delete),
                    "RESPONSE" => Ok(MessageType::Response),
                    "NOTIFICATION" => Ok(MessageType::Notification),
                    "HEARTBEAT" => Ok(MessageType::Heartbeat),
                    "ERROR" => Ok(MessageType::Error),
                    "REQUEST_DIGEST" => Ok(MessageType::RequestDigest),
                    "REQUEST_MESSAGES_SINCE" => Ok(MessageType::RequestMessagesSince),
                    "DIGEST_RESPONSE" => Ok(MessageType::DigestResponse),
                    "MESSAGES_RESPONSE" => Ok(MessageType::MessagesResponse),
                    _ => Ok(MessageType::Custom(value.to_string())),
                }
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<MessageType, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                if let Some(key) = map.next_key::<String>()? {
                    if key == "Custom" {
                        let value: String = map.next_value()?;
                        Ok(MessageType::Custom(value))
                    } else {
                        Err(Error::unknown_field(&key, &["Custom"]))
                    }
                } else {
                    Err(Error::missing_field("Custom"))
                }
            }
        }

        deserializer.deserialize_any(MessageTypeVisitor)
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageType::PeerDiscovery => write!(f, "PEER_DISCOVERY"),
            MessageType::RequestLocalPeers => write!(f, "REQUEST_LOCAL_PEERS"),
            MessageType::LocalPeers => write!(f, "LOCAL_PEERS"),
            MessageType::RequestSharedObjectUpdate => write!(f, "REQUEST_SHARED_OBJECT_UPDATE"),
            MessageType::SharedObjectUpdate => write!(f, "SHARED_OBJECT_UPDATE"),
            MessageType::Get => write!(f, "GET"),
            MessageType::Set => write!(f, "SET"),
            MessageType::Delete => write!(f, "DELETE"),
            MessageType::Response => write!(f, "RESPONSE"),
            MessageType::Notification => write!(f, "NOTIFICATION"),
            MessageType::Heartbeat => write!(f, "HEARTBEAT"),
            MessageType::Error => write!(f, "ERROR"),
            MessageType::RequestDigest => write!(f, "REQUEST_DIGEST"),
            MessageType::RequestMessagesSince => write!(f, "REQUEST_MESSAGES_SINCE"),
            MessageType::DigestResponse => write!(f, "DIGEST_RESPONSE"),
            MessageType::MessagesResponse => write!(f, "MESSAGES_RESPONSE"),
            MessageType::Custom(name) => write!(f, "{name}"),
        }
    }
}

/// A message that can be shared between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMessage {
    /// Unique message identifier
    pub id: SharedObjectId,
    /// Message type
    pub message_type: MessageType,
    /// Target object ID (if applicable)
    pub target_id: Option<SharedObjectId>,
    /// Message payload
    pub data: serde_json::Value,
    /// Timestamp when message was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Optional signature for authenticated messages
    #[serde(with = "serde_bytes", default)]
    pub signature: Option<Vec<u8>>,
    /// Hash of the message content
    pub hash: String,
}

impl SharedMessage {
    /// Create a new shared message
    pub fn new(message_type: MessageType, data: serde_json::Value) -> Self {
        let mut message = Self {
            id: SharedObjectId::new(),
            message_type,
            target_id: None,
            data,
            timestamp: chrono::Utc::now(),
            signature: None,
            hash: String::new(),
        };
        message.hash = message.calculate_hash();
        message
    }

    /// Create a new message with a target
    pub fn new_with_target(
        message_type: MessageType,
        target_id: SharedObjectId,
        data: serde_json::Value,
    ) -> Self {
        let mut message = Self {
            id: SharedObjectId::new(),
            message_type,
            target_id: Some(target_id),
            data,
            timestamp: chrono::Utc::now(),
            signature: None,
            hash: String::new(),
        };
        message.hash = message.calculate_hash();
        message
    }

    /// Create a custom message
    pub fn custom<T: Serialize>(message_type: impl Into<String>, data: T) -> Result<Self> {
        let data = serde_json::to_value(data)
            .map_err(|e| ChaincraftError::Serialization(SerializationError::Json(e)))?;
        Ok(Self::new(MessageType::Custom(message_type.into()), data))
    }

    /// Sign this message with the given private key
    pub fn sign(&mut self, private_key: &crate::crypto::PrivateKey) -> Result<()> {
        let message_bytes = self.to_bytes()?;
        let signature = private_key.sign(&message_bytes)?;
        self.signature = Some(signature.to_bytes());
        Ok(())
    }

    /// Verify the signature of this message
    pub fn verify_signature(&self, public_key: &crate::crypto::PublicKey) -> Result<bool> {
        if let Some(sig_bytes) = &self.signature {
            // Create a copy without signature for verification
            let mut message_copy = self.clone();
            message_copy.signature = None;
            let message_bytes = message_copy.to_bytes()?;

            // Different approach for each key type to avoid the signature creation issues
            match public_key {
                crate::crypto::PublicKey::Ed25519(pk) => {
                    if sig_bytes.len() != 64 {
                        return Err(ChaincraftError::Crypto(CryptoError::InvalidSignature));
                    }

                    // Create the signature bytes array safely
                    let mut sig_array = [0u8; 64];
                    sig_array.copy_from_slice(&sig_bytes[0..64]);

                    // In ed25519_dalek 2.0, from_bytes returns a Signature directly
                    let signature = ed25519_dalek::Signature::from_bytes(&sig_array);

                    use ed25519_dalek::Verifier;
                    Ok(pk.verify(&message_bytes, &signature).is_ok())
                },
                crate::crypto::PublicKey::Secp256k1(pk) => {
                    let sig_result = k256::ecdsa::Signature::from_slice(sig_bytes.as_slice());
                    if sig_result.is_err() {
                        return Err(ChaincraftError::Crypto(CryptoError::InvalidSignature));
                    }

                    use k256::ecdsa::{signature::Verifier, VerifyingKey};
                    let verifying_key = VerifyingKey::from(pk);
                    Ok(verifying_key
                        .verify(&message_bytes, &sig_result.unwrap())
                        .is_ok())
                },
            }
        } else {
            Ok(false)
        }
    }

    /// Calculate the hash of this message
    pub fn calculate_hash(&self) -> String {
        let mut hasher = Sha256::new();
        if let Ok(id_bytes) = bincode::serialize(self.id.as_uuid()) {
            hasher.update(&id_bytes);
        }
        hasher.update(self.message_type.to_string().as_bytes());
        hasher.update(self.data.to_string().as_bytes());
        hasher.update(self.timestamp.to_rfc3339().as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Verify the message hash
    pub fn verify_hash(&self) -> bool {
        self.hash == self.calculate_hash()
    }

    /// Get the message size in bytes
    pub fn size(&self) -> usize {
        bincode::serialized_size(self).unwrap_or(0) as usize
    }

    /// Convert message to bytes for signing/verification
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| ChaincraftError::Serialization(SerializationError::Binary(e)))
    }

    /// Create message from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
            .map_err(|e| ChaincraftError::Serialization(SerializationError::Binary(e)))
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| ChaincraftError::Serialization(SerializationError::Json(e)))
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| ChaincraftError::Serialization(SerializationError::Json(e)))
    }
}

impl PartialEq for SharedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.hash == other.hash
    }
}

impl Eq for SharedMessage {}

/// Trait for objects that can be shared and synchronized across nodes
#[async_trait]
pub trait SharedObject: Send + Sync + Debug {
    /// Get the unique identifier for this shared object
    fn id(&self) -> SharedObjectId;

    /// Get the type name of this shared object
    fn type_name(&self) -> &'static str;

    /// Validate if a message is valid for this shared object
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool>;

    /// Process a validated message and update the object state
    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>>;

    /// Check if this object supports merkleized synchronization
    fn is_merkleized(&self) -> bool;

    /// Get the latest state digest for synchronization
    async fn get_latest_digest(&self) -> Result<String>;

    /// Check if the object has a specific digest
    async fn has_digest(&self, digest: &str) -> Result<bool>;

    /// Validate if a digest is valid
    async fn is_valid_digest(&self, digest: &str) -> Result<bool>;

    /// Add a digest to the object
    async fn add_digest(&mut self, digest: String) -> Result<bool>;

    /// Get messages for gossip protocol
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>>;

    /// Get messages since a specific digest
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>>;

    /// Get the current state as a serializable value
    async fn get_state(&self) -> Result<serde_json::Value>;

    /// Reset the object to a clean state
    async fn reset(&mut self) -> Result<()>;

    /// Serialize the object to JSON
    fn to_json(&self) -> Result<serde_json::Value>;

    /// Update the object from JSON data
    async fn apply_json(&mut self, data: serde_json::Value) -> Result<()>;

    /// Clone the object as a trait object
    fn clone_box(&self) -> Box<dyn SharedObject>;

    /// Get a reference to self as Any for downcasting
    fn as_any(&self) -> &dyn Any;

    /// Get a mutable reference to self as Any for downcasting
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Called when the object is accessed
    async fn on_access(&mut self) -> Result<()>;

    /// Called when the object is modified
    async fn on_modify(&mut self) -> Result<()>;

    /// Called when the object is deleted
    async fn on_delete(&mut self) -> Result<()>;

    /// Validate the object's current state
    async fn validate(&self) -> Result<bool>;

    /// Get object metadata
    fn metadata(&self) -> HashMap<String, String>;
}

impl Clone for Box<dyn SharedObject> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Digest for tracking shared object state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDigest {
    /// The digest hash
    pub hash: String,
    /// Timestamp when digest was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Number of messages included in this digest
    pub message_count: u64,
}

impl StateDigest {
    /// Create a new state digest
    pub fn new(hash: String, message_count: u64) -> Self {
        Self {
            hash,
            timestamp: chrono::Utc::now(),
            message_count,
        }
    }

    /// Calculate a digest from messages
    pub fn from_messages(messages: &[SharedMessage]) -> Self {
        let mut hasher = Sha256::new();
        for message in messages {
            hasher.update(message.hash.as_bytes());
        }
        let hash = hex::encode(hasher.finalize());
        Self::new(hash, messages.len() as u64)
    }
}

/// Registry for managing shared objects
pub struct SharedObjectRegistry {
    objects: HashMap<SharedObjectId, Box<dyn SharedObject>>,
    #[allow(dead_code)]
    lock: Arc<RwLock<()>>,
}

impl Default for SharedObjectRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedObjectRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            objects: HashMap::new(),
            lock: Arc::new(RwLock::new(())),
        }
    }

    /// Register a shared object
    pub fn register(&mut self, object: Box<dyn SharedObject>) -> SharedObjectId {
        let id = object.id();
        self.objects.insert(id.clone(), object);
        id
    }

    /// Get a reference to an object by ID
    pub fn get(&self, id: &SharedObjectId) -> Option<&dyn SharedObject> {
        self.objects.get(id).map(|obj| obj.as_ref())
    }

    /// Remove an object from the registry
    pub fn remove(&mut self, id: &SharedObjectId) -> Option<Box<dyn SharedObject>> {
        self.objects.remove(id)
    }

    /// Get all object IDs
    pub fn ids(&self) -> Vec<SharedObjectId> {
        self.objects.keys().cloned().collect()
    }

    /// Get the number of registered objects
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Clear all objects from the registry
    pub fn clear(&mut self) {
        self.objects.clear();
    }

    /// Get objects by type
    pub fn get_by_type(&self, type_name: &str) -> Vec<&dyn SharedObject> {
        self.objects
            .values()
            .filter(|obj| obj.type_name() == type_name)
            .map(|obj| obj.as_ref())
            .collect()
    }

    /// Check if an object exists
    pub fn contains(&self, id: &SharedObjectId) -> bool {
        self.objects.contains_key(id)
    }
}

impl Debug for SharedObjectRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedObjectRegistry")
            .field("object_count", &self.objects.len())
            .field("object_ids", &self.ids())
            .finish()
    }
}
