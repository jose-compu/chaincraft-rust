//! Enhanced shared object implementation with application-specific logic

pub use crate::shared::SharedObjectId;
use crate::{
    error::{ChaincraftError, Result},
    shared::{MessageType, SharedMessage, SharedObject},
    state_memento::{normalize_state_memento, StateMemento},
};
use async_trait::async_trait;
use chrono;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Enhanced shared object trait with application-specific functionality
#[async_trait]
pub trait ApplicationObject: Send + Sync + std::fmt::Debug {
    /// Get the object's unique identifier
    fn id(&self) -> &SharedObjectId;

    /// Get the object's type name
    fn type_name(&self) -> &'static str;

    /// Validate if a message is valid for this object
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool>;

    /// Add a validated message to the object and optionally emit updated frontier state.
    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>>;

    /// Check if this object supports merkleized synchronization
    fn is_merkleized(&self) -> bool;

    /// Get the latest state digest
    async fn get_latest_digest(&self) -> Result<String>;

    /// Check if object has a specific digest
    async fn has_digest(&self, digest: &str) -> Result<bool>;

    /// Validate if a digest is valid
    async fn is_valid_digest(&self, digest: &str) -> Result<bool>;

    /// Add a digest to the object
    async fn add_digest(&mut self, digest: String) -> Result<bool>;

    /// Get messages for gossip protocol
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>>;

    /// Get messages since a specific digest
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>>;

    /// Return frontier digests for multi-head structures.
    fn get_state_digests(&self) -> Vec<String> {
        vec![]
    }

    /// Emit a normalized shared pipeline state memento.
    async fn emit_state_memento(&self) -> Result<StateMemento> {
        let latest_digest = self.get_latest_digest().await?;
        let frontier_digests = self.get_state_digests();
        Ok(normalize_state_memento(&latest_digest, Some(frontier_digests)))
    }

    /// Get the current state as JSON
    async fn get_state(&self) -> Result<Value>;

    /// Reset the object to initial state
    async fn reset(&mut self) -> Result<()>;

    /// Clone the object
    fn clone_box(&self) -> Box<dyn ApplicationObject>;

    /// Get reference as Any for downcasting
    fn as_any(&self) -> &dyn Any;

    /// Get mutable reference as Any for downcasting
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Simple shared number object for testing (equivalent to Python SimpleSharedNumber)
#[derive(Debug, Clone)]
pub struct SimpleSharedNumber {
    id: SharedObjectId,
    number: i64,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    locked: bool,
    messages: Vec<SharedMessage>,
    seen_hashes: HashSet<String>,
    digests: Vec<String>,
}

impl SimpleSharedNumber {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            number: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            locked: false,
            messages: Vec::new(),
            seen_hashes: HashSet::new(),
            digests: Vec::new(),
        }
    }

    pub fn get_number(&self) -> i64 {
        self.number
    }

    pub fn get_messages(&self) -> &[SharedMessage] {
        &self.messages
    }

    fn calculate_message_hash(data: &Value) -> String {
        let data_str = serde_json::to_string(&serde_json::json!({
            "content": data
        }))
        .unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(data_str.as_bytes());
        hex::encode(hasher.finalize())
    }
}

impl Default for SimpleSharedNumber {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for SimpleSharedNumber {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "SimpleSharedNumber"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        // We only accept integer data
        Ok(message.data.is_i64())
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        _frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        // Deduplicate by hashing the message's data field
        let msg_hash = Self::calculate_message_hash(&message.data);

        if self.seen_hashes.contains(&msg_hash) {
            // Already processed this data
            return Ok(None);
        }

        self.seen_hashes.insert(msg_hash);

        // Extract the integer value and add to our number
        if let Some(value) = message.data.as_i64() {
            self.number += value;
            self.messages.push(message);
            tracing::info!("SimpleSharedNumber: Added message with data: {}", value);
        }

        Ok(Some(self.emit_state_memento().await?))
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.number.to_string())
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.digests.contains(&digest.to_string()))
    }

    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(true)
    }

    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        self.digests.push(digest);
        Ok(true)
    }

    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new())
    }

    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new())
    }

    fn get_state_digests(&self) -> Vec<String> {
        let digest = self.number.to_string();
        if digest.is_empty() {
            vec![]
        } else {
            vec![digest]
        }
    }

    async fn get_state(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "number": self.number,
            "message_count": self.messages.len(),
            "seen_hashes_count": self.seen_hashes.len()
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.number = 0;
        self.messages.clear();
        self.seen_hashes.clear();
        self.digests.clear();
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ApplicationObject> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Merkelized chain object (Rust analogue of Python SimpleChainObject)
///
/// Maintains an append-only chain of SHA256 hashes where each hash is derived from
/// the previous hash. Supports digest-based synchronization for efficient
/// state sync between nodes.
#[derive(Debug, Clone)]
pub struct MerkelizedChain {
    id: SharedObjectId,
    /// The chain of hashes (genesis is at index 0)
    chain: Vec<String>,
    /// Messages corresponding to each chain entry (optional payload)
    messages: Vec<SharedMessage>,
    /// Set of all hashes in the chain for O(1) lookup
    hash_set: HashSet<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl MerkelizedChain {
    /// Create a new MerkelizedChain with a genesis hash
    pub fn new() -> Self {
        let genesis = Self::calculate_hash("genesis");
        Self {
            id: SharedObjectId::new(),
            chain: vec![genesis.clone()],
            messages: vec![SharedMessage::new(
                MessageType::Custom("genesis".to_string()),
                serde_json::json!("genesis"),
            )],
            hash_set: {
                let mut set = HashSet::new();
                set.insert(genesis);
                set
            },
            created_at: chrono::Utc::now(),
        }
    }

    /// Calculate the SHA256 hash of data (consistent with Python)
    pub fn calculate_hash(data: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Calculate what the next hash should be given a previous hash
    pub fn calculate_next_hash(prev_hash: &str) -> String {
        Self::calculate_hash(prev_hash)
    }

    /// Get the current chain length (including genesis)
    pub fn chain_length(&self) -> usize {
        self.chain.len()
    }

    /// Get the genesis hash
    pub fn genesis_hash(&self) -> &str {
        &self.chain[0]
    }

    /// Get the latest (tip) hash
    pub fn latest_hash(&self) -> &str {
        self.chain.last().expect("chain is never empty")
    }

    /// Get a specific hash by index
    pub fn hash_at(&self, index: usize) -> Option<&str> {
        self.chain.get(index).map(|s| s.as_str())
    }

    /// Check if a hash is valid as the next hash in the chain
    pub fn is_valid_next_hash(&self, hash: &str) -> bool {
        let expected = Self::calculate_next_hash(self.latest_hash());
        hash == expected
    }

    /// Add a next hash to the chain (returns the new hash)
    pub fn add_next_hash(&mut self) -> String {
        let next_hash = Self::calculate_next_hash(self.latest_hash());
        self.chain.push(next_hash.clone());
        self.hash_set.insert(next_hash.clone());

        // Create a message for this hash
        let msg = SharedMessage::new(
            MessageType::Custom("chain_update".to_string()),
            serde_json::json!(next_hash),
        );
        self.messages.push(msg);

        next_hash
    }

    /// Try to add an existing hash to the chain (for sync from peers)
    /// Returns true if the hash was added, false if invalid or duplicate
    pub fn try_add_hash(&mut self, hash: &str) -> bool {
        // Skip if already in chain
        if self.hash_set.contains(hash) {
            return false;
        }

        // Check if this hash follows from any hash in our chain
        for i in 0..self.chain.len() {
            let expected_next = Self::calculate_next_hash(&self.chain[i]);
            if hash == expected_next {
                self.chain.push(hash.to_string());
                self.hash_set.insert(hash.to_string());

                let msg = SharedMessage::new(
                    MessageType::Custom("chain_update".to_string()),
                    serde_json::json!(hash),
                );
                self.messages.push(msg);

                return true;
            }
        }

        false
    }

    /// Get the chain as a slice of hashes
    pub fn chain(&self) -> &[String] {
        &self.chain
    }

    /// Find the index of a hash in the chain
    pub fn find_hash_index(&self, hash: &str) -> Option<usize> {
        self.chain.iter().position(|h| h == hash)
    }
}

impl Default for MerkelizedChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for MerkelizedChain {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "MerkelizedChain"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        // Accept string messages that are valid hashes
        let Some(hash) = message.data.as_str() else {
            return Ok(false);
        };

        // If already in chain, it's valid (for deduplication)
        if self.hash_set.contains(hash) {
            return Ok(true);
        }

        // Check if it's a valid next hash from any existing hash
        for i in 0..self.chain.len() {
            let expected_next = Self::calculate_next_hash(&self.chain[i]);
            if hash == expected_next {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        let Some(hash) = message.data.as_str() else {
            return Ok(None);
        };

        // Skip if already in chain
        if self.hash_set.contains(hash) {
            return Ok(None);
        }

        // Try to add to chain
        if self.try_add_hash(hash) {
            tracing::info!(
                "MerkelizedChain: Added hash {} to chain (length: {})",
                &hash[..8.min(hash.len())],
                self.chain.len()
            );
        }

        Ok(None)
    }

    fn is_merkleized(&self) -> bool {
        true
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.latest_hash().to_string())
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.hash_set.contains(digest))
    }

    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.hash_set.contains(digest) || self.is_valid_next_hash(digest))
    }

    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.try_add_hash(&digest) {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        let start_index = match digest {
            Some(hash) => {
                match self.find_hash_index(hash) {
                    Some(idx) => idx + 1,          // Start after the given digest
                    None => return Ok(Vec::new()), // Unknown digest
                }
            },
            None => 1, // Skip genesis, return all subsequent
        };

        let mut result = Vec::new();
        for i in start_index..self.chain.len() {
            let msg = SharedMessage::new(
                MessageType::Custom("chain_update".to_string()),
                serde_json::json!(self.chain[i]),
            );
            result.push(msg);
        }

        Ok(result)
    }

    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        self.gossip_messages(Some(digest)).await
    }

    async fn get_state(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "chain_length": self.chain.len(),
            "latest_hash": self.latest_hash(),
            "genesis_hash": self.genesis_hash(),
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        let genesis = Self::calculate_hash("genesis");
        self.chain = vec![genesis.clone()];
        self.hash_set = {
            let mut set = HashSet::new();
            set.insert(genesis);
            set
        };
        self.messages = vec![SharedMessage::new(
            MessageType::Custom("genesis".to_string()),
            serde_json::json!("genesis"),
        )];
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ApplicationObject> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Message chain - append-only ordered log of messages.
/// Mirrors Python message chain concept: ordered sequence of messages
/// with digest-based sync support.
#[derive(Debug, Clone)]
pub struct MessageChain {
    id: SharedObjectId,
    messages: Vec<SharedMessage>,
    seen_hashes: HashSet<String>,
    digests: Vec<String>,
}

impl MessageChain {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            messages: Vec::new(),
            seen_hashes: HashSet::new(),
            digests: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn messages(&self) -> &[SharedMessage] {
        &self.messages
    }

    fn msg_hash(msg: &SharedMessage) -> String {
        msg.hash.clone()
    }
}

impl Default for MessageChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for MessageChain {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "MessageChain"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        Ok(!message.data.is_null())
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        let h = Self::msg_hash(&message);
        if self.seen_hashes.contains(&h) {
            return Ok(None);
        }
        self.seen_hashes.insert(h);
        self.messages.push(message);
        Ok(None)
    }

    fn is_merkleized(&self) -> bool {
        true
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self
            .messages
            .last()
            .map(|m| m.hash.clone())
            .unwrap_or_else(|| "genesis".to_string()))
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.digests.contains(&digest.to_string())
            || self.messages.iter().any(|m| m.hash == digest))
    }

    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.has_digest(digest).await?
            || digest == "genesis"
            || self.seen_hashes.contains(digest))
    }

    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(false)
    }

    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        let start = match digest {
            Some(d) if d != "genesis" => self
                .messages
                .iter()
                .position(|m| m.hash == d)
                .map(|i| i + 1)
                .unwrap_or(0),
            _ => 0,
        };
        Ok(self.messages[start..].to_vec())
    }

    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        self.gossip_messages(Some(digest)).await
    }

    async fn get_state(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "length": self.messages.len(),
            "message_count": self.messages.len()
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.messages.clear();
        self.seen_hashes.clear();
        self.digests.clear();
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ApplicationObject> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Registry for managing application objects
#[derive(Debug)]
pub struct ApplicationObjectRegistry {
    pub objects: HashMap<SharedObjectId, Box<dyn ApplicationObject>>,
    objects_by_type: HashMap<String, Vec<SharedObjectId>>,
    registration_order: Vec<SharedObjectId>,
}

impl ApplicationObjectRegistry {
    pub fn new() -> Self {
        Self {
            objects: HashMap::new(),
            objects_by_type: HashMap::new(),
            registration_order: Vec::new(),
        }
    }

    /// Register a new application object
    pub fn register(&mut self, object: Box<dyn ApplicationObject>) -> SharedObjectId {
        let id = object.id().clone();
        let type_name = object.type_name().to_string();

        self.objects_by_type
            .entry(type_name)
            .or_default()
            .push(id.clone());

        self.registration_order.push(id.clone());
        self.objects.insert(id.clone(), object);
        id
    }

    /// Get an object by ID
    pub fn get(&self, id: &SharedObjectId) -> Option<&dyn ApplicationObject> {
        self.objects.get(id).map(|obj| obj.as_ref())
    }

    /// Get all objects of a specific type (returning owned clones for safety)
    pub fn get_by_type(&self, type_name: &str) -> Vec<Box<dyn ApplicationObject>> {
        self.objects_by_type
            .get(type_name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.objects.get(id))
                    .map(|obj| obj.clone_box())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove an object
    pub fn remove(&mut self, id: &SharedObjectId) -> Option<Box<dyn ApplicationObject>> {
        if let Some(object) = self.objects.remove(id) {
            let type_name = object.type_name().to_string();
            if let Some(type_list) = self.objects_by_type.get_mut(&type_name) {
                type_list.retain(|obj_id| obj_id != id);
                if type_list.is_empty() {
                    self.objects_by_type.remove(&type_name);
                }
            }
            self.registration_order.retain(|obj_id| obj_id != id);
            Some(object)
        } else {
            None
        }
    }

    /// Get all object IDs
    pub fn ids(&self) -> Vec<SharedObjectId> {
        self.registration_order.clone()
    }

    /// Get count of objects
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Clear all objects
    pub fn clear(&mut self) {
        self.objects.clear();
        self.objects_by_type.clear();
        self.registration_order.clear();
    }

    /// Process a message against all appropriate objects
    pub async fn process_message(&mut self, message: SharedMessage) -> Result<Vec<SharedObjectId>> {
        let mut processed_objects = Vec::new();

        // Get all object IDs first to avoid borrow checker issues.
        let ids: Vec<SharedObjectId> = self.registration_order.clone();

        // Validation-first: reject if any object rejects.
        for id in &ids {
            let is_valid = if let Some(object) = self.objects.get(id) {
                object.is_valid(&message).await?
            } else {
                false
            };
            if !is_valid {
                return Err(ChaincraftError::validation(
                    "message rejected by at least one application object",
                ));
            }
        }

        // Sequential pipeline with optional frontier state memento propagation.
        let mut frontier_state: Option<StateMemento> = None;
        for id in ids {
            if let Some(object) = self.objects.get_mut(&id) {
                let emitted = object
                    .add_message(message.clone(), frontier_state.clone())
                    .await?;
                frontier_state = match emitted {
                    Some(memento) => Some(memento),
                    None => Some(object.emit_state_memento().await?),
                };
                processed_objects.push(id);
            }
        }

        Ok(processed_objects)
    }
}

impl Default for ApplicationObjectRegistry {
    fn default() -> Self {
        Self::new()
    }
}
