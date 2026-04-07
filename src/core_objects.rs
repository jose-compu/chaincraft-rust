//! Core shared objects aligned with the Python implementation.

use crate::{
    error::Result,
    shared::{SharedMessage, SharedObjectId},
    shared_object::ApplicationObject,
};
use async_trait::async_trait;
use lru::LruCache;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

type SharedKvBackend = Arc<RwLock<HashMap<String, Value>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMode {
    Memory,
    Storage,
}

impl StorageMode {
    fn from_str(mode: &str) -> Self {
        if mode == "storage" {
            Self::Storage
        } else {
            Self::Memory
        }
    }
}

#[derive(Clone)]
struct CoreStorage {
    persistent: bool,
    storage_mode: StorageMode,
    enable_memory_cache: bool,
    memory_store: HashMap<String, Value>,
    storage_backend: SharedKvBackend,
    memory_cache: LruCache<String, Value>,
}

impl std::fmt::Debug for CoreStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreStorage")
            .field("persistent", &self.persistent)
            .field("storage_mode", &self.storage_mode)
            .field("enable_memory_cache", &self.enable_memory_cache)
            .finish()
    }
}

impl CoreStorage {
    fn new(
        persistent: bool,
        storage_mode: StorageMode,
        storage_backend: Option<SharedKvBackend>,
        enable_memory_cache: bool,
        memory_cache_size: usize,
    ) -> Self {
        let cache_size = memory_cache_size.max(1);
        let non_zero = NonZeroUsize::new(cache_size).expect("cache size must be non-zero");
        Self {
            persistent,
            storage_mode,
            enable_memory_cache,
            memory_store: HashMap::new(),
            storage_backend: storage_backend
                .unwrap_or_else(|| Arc::new(RwLock::new(HashMap::new()))),
            memory_cache: LruCache::new(non_zero),
        }
    }

    async fn read(&mut self, key: &str) -> Option<Value> {
        if self.enable_memory_cache {
            if let Some(value) = self.memory_cache.get(key) {
                return Some(value.clone());
            }
        }
        let value = match self.storage_mode {
            StorageMode::Memory => self.memory_store.get(key).cloned(),
            StorageMode::Storage => {
                let backend = self.storage_backend.read().await;
                backend.get(key).cloned()
            },
        };
        if self.enable_memory_cache {
            if let Some(v) = value.clone() {
                self.memory_cache.put(key.to_string(), v);
            }
        }
        value
    }

    async fn write(&mut self, key: &str, value: Value) {
        match self.storage_mode {
            StorageMode::Memory => {
                self.memory_store.insert(key.to_string(), value.clone());
            },
            StorageMode::Storage => {
                let mut backend = self.storage_backend.write().await;
                backend.insert(key.to_string(), value.clone());
            },
        }
        if self.enable_memory_cache {
            self.memory_cache.put(key.to_string(), value);
        }
    }

    async fn delete(&mut self, key: &str) {
        match self.storage_mode {
            StorageMode::Memory => {
                self.memory_store.remove(key);
            },
            StorageMode::Storage => {
                let mut backend = self.storage_backend.write().await;
                backend.remove(key);
            },
        }
        if self.enable_memory_cache {
            self.memory_cache.pop(key);
        }
    }
}

#[derive(Debug, Clone)]
struct MerkelizedState {
    messages: Vec<SharedMessage>,
    digests: Vec<String>,
    digest_index: HashMap<String, usize>,
    parents_by_digest: HashMap<String, Vec<String>>,
    children_by_digest: HashMap<String, HashSet<String>>,
    head_digests: BTreeSet<String>,
}

impl MerkelizedState {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            digests: Vec::new(),
            digest_index: HashMap::new(),
            parents_by_digest: HashMap::new(),
            children_by_digest: HashMap::new(),
            head_digests: BTreeSet::new(),
        }
    }

    fn latest_digest(&self) -> String {
        self.digests.last().cloned().unwrap_or_default()
    }

    fn contains_digest(&self, digest: &str) -> bool {
        self.digest_index.contains_key(digest)
    }

    fn head_digest_list(&self) -> Vec<String> {
        self.head_digests.iter().cloned().collect()
    }

    async fn record_message(
        &mut self,
        storage: &mut CoreStorage,
        message: SharedMessage,
        digest: String,
        parent_digests: Vec<String>,
    ) {
        self.messages.push(message.clone());
        self.digest_index.insert(digest.clone(), self.digests.len());
        self.digests.push(digest.clone());
        self.parents_by_digest
            .insert(digest.clone(), parent_digests.clone());

        for parent in &parent_digests {
            self.children_by_digest
                .entry(parent.clone())
                .or_default()
                .insert(digest.clone());
            self.head_digests.remove(parent);
        }
        self.children_by_digest.entry(digest.clone()).or_default();
        self.head_digests.insert(digest.clone());
        storage.write(&digest, message.data).await;
    }

    fn messages_since_digest(&self, digest: &str) -> Vec<SharedMessage> {
        if self.messages.is_empty() {
            return vec![];
        }
        if digest.is_empty() {
            return self.messages.clone();
        }
        let Some(idx) = self.digest_index.get(digest) else {
            return vec![];
        };
        self.messages[(*idx + 1)..].to_vec()
    }
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = std::collections::BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.clone(), canonical_json(v));
            }
            let mut out = Map::new();
            for (k, v) in sorted {
                out.insert(k, v);
            }
            Value::Object(out)
        },
        Value::Array(arr) => Value::Array(arr.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

fn compute_digest_value(value: &Value) -> String {
    let canonical = canonical_json(value);
    let payload = serde_json::to_string(&canonical).unwrap_or_else(|_| "null".to_string());
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    hex::encode(hasher.finalize())
}

fn normalize_parent_digests_from_value(value: Option<&Value>) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    if let Some(Value::Array(parents)) = value {
        for p in parents {
            if let Some(parent) = p.as_str() {
                if parent.is_empty() || parent == "genesis" {
                    continue;
                }
                if seen.insert(parent.to_string()) {
                    normalized.push(parent.to_string());
                }
            }
        }
    }
    normalized
}

fn extract_parent_digests(data: &Value) -> Vec<String> {
    let Some(obj) = data.as_object() else {
        return vec![];
    };
    if obj.contains_key("parents") {
        return normalize_parent_digests_from_value(obj.get("parents"));
    }
    if let Some(prev) = obj.get("previous_digest").and_then(|v| v.as_str()) {
        return normalize_parent_digests_from_value(Some(&Value::Array(vec![Value::String(
            prev.to_string(),
        )])));
    }
    vec![]
}

#[derive(Debug, Clone)]
pub struct CoreSharedObject {
    id: SharedObjectId,
    storage: CoreStorage,
}

impl CoreSharedObject {
    pub fn new(
        persistent: bool,
        storage_mode: &str,
        storage_backend: Option<SharedKvBackend>,
        enable_memory_cache: bool,
        memory_cache_size: usize,
    ) -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(
                persistent,
                StorageMode::from_str(storage_mode),
                storage_backend,
                enable_memory_cache,
                memory_cache_size,
            ),
        }
    }

    pub fn compute_digest(data: &Value) -> String {
        compute_digest_value(data)
    }
}

impl Default for CoreSharedObject {
    fn default() -> Self {
        Self::new(false, "memory", None, true, 1024)
    }
}

#[derive(Debug, Clone)]
pub struct NonMerkelizedObject {
    id: SharedObjectId,
    storage: CoreStorage,
}

impl NonMerkelizedObject {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
        }
    }
}

impl Default for NonMerkelizedObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for NonMerkelizedObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "NonMerkelizedObject"
    }

    async fn is_valid(&self, _message: &SharedMessage) -> Result<bool> {
        Ok(true)
    }

    async fn add_message(&mut self, _message: SharedMessage) -> Result<()> {
        Ok(())
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(String::new())
    }

    async fn has_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }

    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }

    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(false)
    }

    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }

    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }

    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"merkleized": false}))
    }

    async fn reset(&mut self) -> Result<()> {
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

#[derive(Debug, Clone)]
pub struct MerkelizedObject {
    id: SharedObjectId,
    storage: CoreStorage,
    state: MerkelizedState,
}

impl MerkelizedObject {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            state: MerkelizedState::new(),
        }
    }
}

impl Default for MerkelizedObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for MerkelizedObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "MerkelizedObject"
    }

    async fn is_valid(&self, _message: &SharedMessage) -> Result<bool> {
        Ok(true)
    }

    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let digest = compute_digest_value(&message.data);
        if self.state.contains_digest(&digest) {
            return Ok(());
        }
        let parents = extract_parent_digests(&message.data);
        self.state
            .record_message(&mut self.storage, message, digest, parents)
            .await;
        Ok(())
    }

    fn is_merkleized(&self) -> bool {
        true
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.state.latest_digest())
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }

    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }

    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.state.contains_digest(&digest) {
            return Ok(false);
        }
        self.state
            .digest_index
            .insert(digest.clone(), self.state.digests.len());
        self.state.digests.push(digest);
        Ok(true)
    }

    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest.unwrap_or_default()))
    }

    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest))
    }

    async fn get_state(&self) -> Result<Value> {
        Ok(json!({
            "digests": self.state.digests.len(),
            "messages": self.state.messages.len(),
            "head_digests": self.state.head_digest_list(),
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.state = MerkelizedState::new();
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

pub type MerkleizedObject = MerkelizedObject;
pub type NativeSharedObject = CoreSharedObject;

#[derive(Debug, Clone)]
pub struct UTXOLedger {
    id: SharedObjectId,
    storage: CoreStorage,
    state: MerkelizedState,
    pub utxos: HashMap<String, Value>,
}

impl UTXOLedger {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            state: MerkelizedState::new(),
            utxos: HashMap::new(),
        }
    }
}

impl Default for UTXOLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for UTXOLedger {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "UTXOLedger"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let Some(data) = message.data.as_object() else {
            return Ok(false);
        };
        match data.get("action").and_then(|v| v.as_str()) {
            Some("utxo_add") => Ok(data.contains_key("utxo_id")
                && data.contains_key("amount")
                && data.contains_key("owner")),
            Some("utxo_spend") => Ok(data
                .get("utxo_id")
                .and_then(|v| v.as_str())
                .map(|id| self.utxos.contains_key(id))
                .unwrap_or(false)),
            _ => Ok(false),
        }
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let digest = compute_digest_value(&message.data);
        if self.state.contains_digest(&digest) {
            return Ok(());
        }
        let parents = extract_parent_digests(&message.data);
        self.state
            .record_message(&mut self.storage, message.clone(), digest, parents)
            .await;
        let data = message.data.as_object().cloned().unwrap_or_default();
        let action = data
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if action == "utxo_add" {
            if let Some(utxo_id) = data.get("utxo_id").and_then(|v| v.as_str()) {
                let entry = json!({
                    "amount": data.get("amount").cloned().unwrap_or(Value::Null),
                    "owner": data.get("owner").cloned().unwrap_or(Value::Null),
                    "meta": data.get("meta").cloned().unwrap_or_else(|| json!({})),
                });
                self.utxos.insert(utxo_id.to_string(), entry.clone());
                self.storage.write(&format!("utxo:{utxo_id}"), entry).await;
            }
        } else if action == "utxo_spend" {
            if let Some(utxo_id) = data.get("utxo_id").and_then(|v| v.as_str()) {
                self.utxos.remove(utxo_id);
                self.storage.delete(&format!("utxo:{utxo_id}")).await;
            }
        }
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        true
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.state.latest_digest())
    }
    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.state.contains_digest(&digest) {
            return Ok(false);
        }
        self.state
            .digest_index
            .insert(digest.clone(), self.state.digests.len());
        self.state.digests.push(digest);
        Ok(true)
    }
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest.unwrap_or_default()))
    }
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest))
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"utxo_count": self.utxos.len()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.state = MerkelizedState::new();
        self.utxos.clear();
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

#[derive(Debug, Clone)]
pub struct BalanceLedger {
    id: SharedObjectId,
    storage: CoreStorage,
    state: MerkelizedState,
    pub balances: HashMap<String, f64>,
}

impl BalanceLedger {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            state: MerkelizedState::new(),
            balances: HashMap::new(),
        }
    }
}

impl Default for BalanceLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for BalanceLedger {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "BalanceLedger"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let Some(data) = message.data.as_object() else {
            return Ok(false);
        };
        let action = data
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let amount = data.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        match action {
            "credit" => Ok(data.contains_key("account") && data.get("amount").is_some()),
            "debit" => {
                let Some(account) = data.get("account").and_then(|v| v.as_str()) else {
                    return Ok(false);
                };
                Ok(self.balances.get(account).cloned().unwrap_or(0.0) >= amount)
            },
            "transfer" => {
                let Some(src) = data.get("from").and_then(|v| v.as_str()) else {
                    return Ok(false);
                };
                let has_dst = data.get("to").and_then(|v| v.as_str()).is_some();
                Ok(has_dst && self.balances.get(src).cloned().unwrap_or(0.0) >= amount)
            },
            _ => Ok(false),
        }
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let digest = compute_digest_value(&message.data);
        if self.state.contains_digest(&digest) {
            return Ok(());
        }
        let parents = extract_parent_digests(&message.data);
        self.state
            .record_message(&mut self.storage, message.clone(), digest, parents)
            .await;

        let data = message.data.as_object().cloned().unwrap_or_default();
        let action = data
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let amount = data.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        match action {
            "credit" => {
                if let Some(account) = data.get("account").and_then(|v| v.as_str()) {
                    let entry = self.balances.get(account).cloned().unwrap_or(0.0) + amount;
                    self.balances.insert(account.to_string(), entry);
                    self.storage
                        .write(&format!("balance:{account}"), json!(entry))
                        .await;
                }
            },
            "debit" => {
                if let Some(account) = data.get("account").and_then(|v| v.as_str()) {
                    let entry = self.balances.get(account).cloned().unwrap_or(0.0) - amount;
                    self.balances.insert(account.to_string(), entry);
                    self.storage
                        .write(&format!("balance:{account}"), json!(entry))
                        .await;
                }
            },
            "transfer" => {
                if let (Some(src), Some(dst)) = (
                    data.get("from").and_then(|v| v.as_str()),
                    data.get("to").and_then(|v| v.as_str()),
                ) {
                    let src_entry = self.balances.get(src).cloned().unwrap_or(0.0) - amount;
                    let dst_entry = self.balances.get(dst).cloned().unwrap_or(0.0) + amount;
                    self.balances.insert(src.to_string(), src_entry);
                    self.balances.insert(dst.to_string(), dst_entry);
                    self.storage
                        .write(&format!("balance:{src}"), json!(src_entry))
                        .await;
                    self.storage
                        .write(&format!("balance:{dst}"), json!(dst_entry))
                        .await;
                }
            },
            _ => {},
        }
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        true
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.state.latest_digest())
    }
    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.state.contains_digest(&digest) {
            return Ok(false);
        }
        self.state
            .digest_index
            .insert(digest.clone(), self.state.digests.len());
        self.state.digests.push(digest);
        Ok(true)
    }
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest.unwrap_or_default()))
    }
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest))
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"balance_count": self.balances.len(), "balances": self.balances}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.state = MerkelizedState::new();
        self.balances.clear();
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

#[derive(Debug, Clone)]
pub struct Blockchain {
    id: SharedObjectId,
    storage: CoreStorage,
    state: MerkelizedState,
    pub blocks: Vec<Value>,
}

impl Blockchain {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            state: MerkelizedState::new(),
            blocks: Vec::new(),
        }
    }
}

impl Default for Blockchain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for Blockchain {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "Blockchain"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let Some(data) = message.data.as_object() else {
            return Ok(false);
        };
        let prev = data
            .get("previous_digest")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if self.blocks.is_empty() {
            return Ok(prev.is_empty() || prev == "genesis");
        }
        let last = self
            .blocks
            .last()
            .and_then(|b| b.get("digest"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        Ok(prev == last)
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let digest = compute_digest_value(&message.data);
        if self.state.contains_digest(&digest) {
            return Ok(());
        }
        let parents = extract_parent_digests(&message.data);
        self.state
            .record_message(&mut self.storage, message.clone(), digest.clone(), parents)
            .await;
        let block = json!({
            "digest": digest,
            "payload": message.data
        });
        self.blocks.push(block.clone());
        let key = block
            .get("digest")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        self.storage.write(&format!("block:{key}"), block).await;
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        true
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.state.latest_digest())
    }
    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.state.contains_digest(&digest) {
            return Ok(false);
        }
        self.state
            .digest_index
            .insert(digest.clone(), self.state.digests.len());
        self.state.digests.push(digest);
        Ok(true)
    }
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest.unwrap_or_default()))
    }
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest))
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"block_count": self.blocks.len()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.state = MerkelizedState::new();
        self.blocks.clear();
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

#[derive(Debug, Clone)]
pub struct DAGObject {
    id: SharedObjectId,
    storage: CoreStorage,
    state: MerkelizedState,
    pub nodes: HashMap<String, Value>,
    frontier_state_index: HashMap<String, isize>,
    pub frontier_digest_version: u32,
}

impl DAGObject {
    pub fn new() -> Self {
        let mut frontier_state_index = HashMap::new();
        frontier_state_index.insert(String::new(), -1);
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            state: MerkelizedState::new(),
            nodes: HashMap::new(),
            frontier_state_index,
            frontier_digest_version: 1,
        }
    }

    pub fn get_head_digests(&self) -> Vec<String> {
        self.state.head_digest_list()
    }

    fn current_frontier_digest(&self) -> String {
        let heads = self.get_head_digests();
        if heads.is_empty() {
            return String::new();
        }
        compute_digest_value(&json!({
            "v": self.frontier_digest_version,
            "frontier_heads": heads
        }))
    }
}

impl Default for DAGObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for DAGObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "DAGObject"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let Some(data) = message.data.as_object() else {
            return Ok(false);
        };
        let parents = normalize_parent_digests_from_value(data.get("parents"));
        if !parents.iter().all(|p| self.nodes.contains_key(p)) {
            return Ok(false);
        }
        if !parents.is_empty() {
            let head_set: HashSet<String> = self.state.head_digest_list().into_iter().collect();
            if !parents.iter().all(|p| head_set.contains(p)) {
                return Ok(false);
            }
        }
        Ok(true)
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let digest = compute_digest_value(&message.data);
        if self.state.contains_digest(&digest) {
            return Ok(());
        }
        let parents = extract_parent_digests(&message.data);
        self.state
            .record_message(&mut self.storage, message.clone(), digest.clone(), parents.clone())
            .await;
        let node = json!({"digest": digest, "parents": parents, "data": message.data});
        let digest_key = node
            .get("digest")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        self.nodes.insert(digest_key.clone(), node.clone());
        self.storage.write(&format!("dag:{digest_key}"), node).await;
        let frontier = self.current_frontier_digest();
        self.frontier_state_index
            .insert(frontier, (self.state.messages.len() as isize) - 1);
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        true
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.current_frontier_digest())
    }
    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest) || self.frontier_state_index.contains_key(digest))
    }
    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        self.has_digest(digest).await
    }
    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.state.contains_digest(&digest) {
            return Ok(false);
        }
        self.state
            .digest_index
            .insert(digest.clone(), self.state.digests.len());
        self.state.digests.push(digest);
        Ok(true)
    }
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        self.get_messages_since_digest(digest.unwrap_or_default())
            .await
    }
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        if self.state.messages.is_empty() {
            return Ok(vec![]);
        }
        if digest.is_empty() {
            return Ok(self.state.messages.clone());
        }
        if let Some(idx) = self.state.digest_index.get(digest) {
            return Ok(self.state.messages[(*idx + 1)..].to_vec());
        }
        if let Some(idx) = self.frontier_state_index.get(digest) {
            let start = (*idx + 1).max(0) as usize;
            return Ok(self.state.messages[start..].to_vec());
        }
        Ok(vec![])
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"node_count": self.nodes.len(), "heads": self.get_head_digests()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.state = MerkelizedState::new();
        self.nodes.clear();
        self.frontier_state_index.clear();
        self.frontier_state_index.insert(String::new(), -1);
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

#[derive(Debug, Clone)]
pub struct TransactionChain {
    id: SharedObjectId,
    storage: CoreStorage,
    state: MerkelizedState,
    pub transactions: Vec<Value>,
}

impl TransactionChain {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            state: MerkelizedState::new(),
            transactions: Vec::new(),
        }
    }
}

impl Default for TransactionChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for TransactionChain {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "TransactionChain"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        Ok(message.data.is_object())
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let digest = compute_digest_value(&message.data);
        if self.state.contains_digest(&digest) {
            return Ok(());
        }
        let parents = extract_parent_digests(&message.data);
        self.state
            .record_message(&mut self.storage, message.clone(), digest.clone(), parents)
            .await;
        let tx = json!({"tx_digest": digest, "payload": message.data});
        self.transactions.push(tx.clone());
        let key = tx
            .get("tx_digest")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        self.storage.write(&format!("tx:{key}"), tx).await;
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        true
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.state.latest_digest())
    }
    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.state.contains_digest(digest))
    }
    async fn add_digest(&mut self, digest: String) -> Result<bool> {
        if self.state.contains_digest(&digest) {
            return Ok(false);
        }
        self.state
            .digest_index
            .insert(digest.clone(), self.state.digests.len());
        self.state.digests.push(digest);
        Ok(true)
    }
    async fn gossip_messages(&self, digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest.unwrap_or_default()))
    }
    async fn get_messages_since_digest(&self, digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(self.state.messages_since_digest(digest))
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"transaction_count": self.transactions.len()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.state = MerkelizedState::new();
        self.transactions.clear();
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

#[derive(Debug, Clone)]
pub struct CacheObject {
    id: SharedObjectId,
    storage: CoreStorage,
    pub cache: HashMap<String, Value>,
}

impl CacheObject {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            cache: HashMap::new(),
        }
    }
}

impl Default for CacheObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for CacheObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "CacheObject"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        Ok(message.data.is_object())
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let data = message.data.as_object().cloned().unwrap_or_default();
        let key = data
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| compute_digest_value(&message.data));

        if data.get("action").and_then(|v| v.as_str()) == Some("delete") {
            self.cache.remove(&key);
            self.storage.delete(&key).await;
            return Ok(());
        }

        let value = data.get("value").cloned().unwrap_or(message.data);
        self.cache.insert(key.clone(), value.clone());
        self.storage.write(&key, value).await;
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        false
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn has_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }
    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }
    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(false)
    }
    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"cache_size": self.cache.len()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.cache.clear();
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

#[derive(Debug, Clone)]
pub struct Mempool {
    id: SharedObjectId,
    storage: CoreStorage,
    pub cache: HashMap<String, Value>,
    pub transactions: HashMap<String, Value>,
}

impl Mempool {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            cache: HashMap::new(),
            transactions: HashMap::new(),
        }
    }
}

impl Default for Mempool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for Mempool {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "Mempool"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        Ok(message.data.is_object())
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let tx = message.data.as_object().cloned().unwrap_or_default();
        let tx_id = tx
            .get("tx_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| compute_digest_value(&message.data));

        if tx.get("action").and_then(|v| v.as_str()) == Some("remove") {
            self.transactions.remove(&tx_id);
            self.cache.remove(&tx_id);
            self.storage.delete(&format!("mempool:{tx_id}")).await;
            return Ok(());
        }
        let as_value = Value::Object(tx);
        self.transactions.insert(tx_id.clone(), as_value.clone());
        self.cache.insert(tx_id.clone(), as_value.clone());
        self.storage
            .write(&format!("mempool:{tx_id}"), as_value)
            .await;
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        false
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn has_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }
    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }
    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(false)
    }
    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"tx_count": self.transactions.len()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.cache.clear();
        self.transactions.clear();
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

#[derive(Debug, Clone)]
pub struct DocumentCache {
    id: SharedObjectId,
    storage: CoreStorage,
    pub cache: HashMap<String, Value>,
    pub documents: HashMap<String, Value>,
}

impl DocumentCache {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            storage: CoreStorage::new(false, StorageMode::Memory, None, true, 1024),
            cache: HashMap::new(),
            documents: HashMap::new(),
        }
    }
}

impl Default for DocumentCache {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for DocumentCache {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "DocumentCache"
    }
    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        Ok(message.data.is_object())
    }
    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let data = message.data.as_object().cloned().unwrap_or_default();
        let doc_id = data
            .get("doc_id")
            .or_else(|| data.get("document_id"))
            .or_else(|| data.get("key"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| compute_digest_value(&message.data));

        if data.get("action").and_then(|v| v.as_str()) == Some("delete") {
            self.documents.remove(&doc_id);
            self.cache.remove(&doc_id);
            self.storage.delete(&format!("doc:{doc_id}")).await;
            return Ok(());
        }

        let payload = data
            .get("document")
            .cloned()
            .or_else(|| data.get("value").cloned())
            .unwrap_or(Value::Object(data));
        self.documents.insert(doc_id.clone(), payload.clone());
        self.cache.insert(doc_id.clone(), payload.clone());
        self.storage.write(&format!("doc:{doc_id}"), payload).await;
        Ok(())
    }
    fn is_merkleized(&self) -> bool {
        false
    }
    async fn get_latest_digest(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn has_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }
    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }
    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(false)
    }
    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_state(&self) -> Result<Value> {
        Ok(json!({"document_count": self.documents.len()}))
    }
    async fn reset(&mut self) -> Result<()> {
        self.cache.clear();
        self.documents.clear();
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
