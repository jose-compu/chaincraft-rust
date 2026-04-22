//! Blockchain protocol example with Memento-aware pipeline state.

use crate::{
    error::{ChaincraftError, Result},
    network::PeerId,
    shared::{MessageType, SharedMessage, SharedObjectId},
    shared_object::ApplicationObject,
    state_memento::{normalize_state_memento, StateMemento},
    storage::MemoryStorage,
    ChaincraftNode,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRecord {
    pub height: u64,
    pub digest: String,
    pub previous_digest: String,
    pub miner: String,
    pub transactions: Vec<Value>,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct BlockchainObject {
    id: SharedObjectId,
    pub chain: Vec<BlockRecord>,
    pub balances: HashMap<String, i64>,
    pub mining_reward: i64,
    block_by_digest: HashMap<String, BlockRecord>,
    children_by_digest: HashMap<String, HashSet<String>>,
    tip_digests: HashSet<String>,
    state_by_digest: HashMap<String, HashMap<String, i64>>,
    seen_hashes: HashSet<String>,
}

impl BlockchainObject {
    pub fn new() -> Self {
        Self::with_reward(10)
    }

    pub fn with_reward(mining_reward: i64) -> Self {
        let genesis_digest = Self::compute_digest(&serde_json::json!({
            "height": 0u64,
            "previous_digest": "",
            "miner": "genesis",
            "transactions": [],
            "payload": "genesis"
        }));
        let genesis = BlockRecord {
            height: 0,
            digest: genesis_digest.clone(),
            previous_digest: String::new(),
            miner: "genesis".to_string(),
            transactions: vec![],
            payload: serde_json::json!("genesis"),
        };
        let mut block_by_digest = HashMap::new();
        block_by_digest.insert(genesis_digest.clone(), genesis.clone());
        let mut children_by_digest = HashMap::new();
        children_by_digest.insert(genesis_digest.clone(), HashSet::new());
        let mut tip_digests = HashSet::new();
        tip_digests.insert(genesis_digest.clone());
        let mut balances = HashMap::new();
        balances.insert("genesis".to_string(), 1000);
        let mut state_by_digest = HashMap::new();
        state_by_digest.insert(genesis_digest.clone(), balances.clone());

        Self {
            id: SharedObjectId::new(),
            chain: vec![genesis],
            balances,
            mining_reward,
            block_by_digest,
            children_by_digest,
            tip_digests,
            state_by_digest,
            seen_hashes: HashSet::new(),
        }
    }

    fn canonical_json(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut keys: Vec<String> = map.keys().cloned().collect();
                keys.sort();
                let mut out = serde_json::Map::new();
                for key in keys {
                    if let Some(v) = map.get(&key) {
                        out.insert(key, Self::canonical_json(v));
                    }
                }
                Value::Object(out)
            },
            Value::Array(arr) => Value::Array(arr.iter().map(Self::canonical_json).collect()),
            _ => value.clone(),
        }
    }

    fn compute_digest(value: &Value) -> String {
        let canonical = Self::canonical_json(value);
        let payload = serde_json::to_string(&canonical).unwrap_or_else(|_| "null".to_string());
        let mut hasher = Sha256::new();
        hasher.update(payload.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn latest_block(&self) -> &BlockRecord {
        self.chain
            .last()
            .expect("blockchain always has genesis block")
    }

    fn compute_next_state(
        &self,
        parent_state: &HashMap<String, i64>,
        block: &BlockRecord,
    ) -> Result<HashMap<String, i64>> {
        let mut next = parent_state.clone();
        *next.entry(block.miner.clone()).or_insert(0) += self.mining_reward;
        for tx in &block.transactions {
            let sender = tx
                .get("sender")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ChaincraftError::validation("transaction missing sender"))?
                .to_string();
            let recipient = tx
                .get("recipient")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ChaincraftError::validation("transaction missing recipient"))?
                .to_string();
            let amount = tx
                .get("amount")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| ChaincraftError::validation("transaction missing amount"))?;
            let fee = tx
                .get("fee")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| ChaincraftError::validation("transaction missing fee"))?;
            let sender_balance = *next.get(&sender).unwrap_or(&0);
            if sender_balance < amount + fee {
                return Err(ChaincraftError::validation(
                    "sender has insufficient balance for transaction",
                ));
            }
            next.insert(sender.clone(), sender_balance - amount - fee);
            *next.entry(recipient).or_insert(0) += amount;
            *next.entry(block.miner.clone()).or_insert(0) += fee;
        }
        Ok(next)
    }

    fn build_chain_to_tip(&self, tip_digest: &str) -> Vec<BlockRecord> {
        let mut reversed = vec![];
        let mut cursor = tip_digest.to_string();
        while let Some(block) = self.block_by_digest.get(&cursor) {
            reversed.push(block.clone());
            if block.height == 0 {
                break;
            }
            cursor = block.previous_digest.clone();
        }
        reversed.reverse();
        if reversed.first().map(|b| b.height) == Some(0) {
            reversed
        } else {
            vec![]
        }
    }

    fn is_better_tip(&self, candidate_digest: &str, current_digest: &str) -> bool {
        let Some(candidate) = self.block_by_digest.get(candidate_digest) else {
            return false;
        };
        let Some(current) = self.block_by_digest.get(current_digest) else {
            return true;
        };
        if candidate.height > current.height {
            return true;
        }
        if candidate.height < current.height {
            return false;
        }
        candidate_digest < current_digest
    }

    fn collect_transactions(&self, digests: &[String]) -> Vec<Value> {
        let mut txs = vec![];
        for digest in digests {
            if let Some(block) = self.block_by_digest.get(digest) {
                for tx in &block.transactions {
                    txs.push(tx.clone());
                }
            }
        }
        txs
    }

    fn build_memento(
        &self,
        reorg: bool,
        reorg_from_height: Option<u64>,
        reverted_txs: Vec<Value>,
        applied_tx_ids: Vec<String>,
    ) -> StateMemento {
        let latest = self.latest_block().digest.clone();
        let mut frontier = self
            .chain
            .iter()
            .rev()
            .take(8)
            .map(|b| b.digest.clone())
            .collect::<Vec<_>>();
        let canonical_set: HashSet<String> = frontier.iter().cloned().collect();
        let mut extras = self
            .tip_digests
            .iter()
            .filter(|d| !canonical_set.contains(*d))
            .cloned()
            .collect::<Vec<_>>();
        extras.sort();
        frontier.extend(extras);

        let mut memento = normalize_state_memento(&latest, Some(frontier));
        memento.revision = self.chain.len() as u64;
        let mut metadata = BTreeMap::new();
        metadata.insert("reorg".to_string(), Value::Bool(reorg));
        if let Some(height) = reorg_from_height {
            metadata.insert(
                "reorg_from_height".to_string(),
                Value::Number(serde_json::Number::from(height)),
            );
        }
        metadata.insert("reverted_txs".to_string(), Value::Array(reverted_txs));
        metadata.insert(
            "applied_tx_ids".to_string(),
            Value::Array(applied_tx_ids.into_iter().map(Value::String).collect()),
        );
        metadata.insert(
            "chain_length".to_string(),
            Value::Number(serde_json::Number::from(self.chain.len())),
        );
        memento.metadata = metadata;
        memento
    }
}

impl Default for BlockchainObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for BlockchainObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "BlockchainObject"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let data = &message.data;
        let Some(message_type) = data.get("message_type").and_then(|v| v.as_str()) else {
            return Ok(false);
        };
        match message_type {
            "NEW_TX" => Ok(data.get("tx").and_then(|v| v.get("tx_id")).is_some()),
            "NEW_BLOCK" => {
                let Some(height) = data.get("height").and_then(|v| v.as_u64()) else {
                    return Ok(false);
                };
                let Some(previous_digest) = data.get("previous_digest").and_then(|v| v.as_str())
                else {
                    return Ok(false);
                };
                let Some(miner) = data.get("miner").and_then(|v| v.as_str()) else {
                    return Ok(false);
                };
                if miner.is_empty() {
                    return Ok(false);
                }
                let Some(transactions) = data.get("transactions").and_then(|v| v.as_array()) else {
                    return Ok(false);
                };
                let Some(payload) = data.get("payload") else {
                    return Ok(false);
                };
                let computed = Self::compute_digest(&serde_json::json!({
                    "height": height,
                    "previous_digest": previous_digest,
                    "miner": miner,
                    "transactions": transactions,
                    "payload": payload
                }));
                let provided = data
                    .get("digest")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if computed != provided {
                    return Ok(false);
                }

                let Some(parent) = self.block_by_digest.get(previous_digest) else {
                    return Ok(false);
                };
                if height != parent.height + 1 {
                    return Ok(false);
                }
                let Some(parent_state) = self.state_by_digest.get(previous_digest) else {
                    return Ok(false);
                };
                let candidate = BlockRecord {
                    height,
                    digest: provided.to_string(),
                    previous_digest: previous_digest.to_string(),
                    miner: miner.to_string(),
                    transactions: transactions.clone(),
                    payload: payload.clone(),
                };
                Ok(self.compute_next_state(parent_state, &candidate).is_ok())
            },
            _ => Ok(false),
        }
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        _frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        if self.seen_hashes.contains(&message.hash) {
            return Ok(None);
        }
        self.seen_hashes.insert(message.hash.clone());

        let data = message.data;
        let message_type = data
            .get("message_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ChaincraftError::validation("missing message_type"))?;

        match message_type {
            "NEW_TX" => Ok(Some(self.build_memento(false, None, vec![], vec![]))),
            "NEW_BLOCK" => {
                let height = data
                    .get("height")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| ChaincraftError::validation("missing block height"))?;
                let previous_digest = data
                    .get("previous_digest")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ChaincraftError::validation("missing previous_digest"))?
                    .to_string();
                let payload = data
                    .get("payload")
                    .cloned()
                    .ok_or_else(|| ChaincraftError::validation("missing payload"))?;
                let miner = data
                    .get("miner")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ChaincraftError::validation("missing miner"))?
                    .to_string();
                let transactions = data
                    .get("transactions")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| ChaincraftError::validation("missing transactions"))?
                    .clone();
                let digest = data
                    .get("digest")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ChaincraftError::validation("missing digest"))?
                    .to_string();

                let block = BlockRecord {
                    height,
                    digest: digest.clone(),
                    previous_digest: previous_digest.clone(),
                    miner,
                    transactions: transactions.clone(),
                    payload,
                };
                if self.block_by_digest.contains_key(&digest) {
                    return Ok(Some(self.build_memento(false, None, vec![], vec![])));
                }
                let parent_state = self
                    .state_by_digest
                    .get(&previous_digest)
                    .ok_or_else(|| ChaincraftError::validation("missing parent state"))?
                    .clone();
                let next_state = self.compute_next_state(&parent_state, &block)?;

                let old_chain_hashes = self
                    .chain
                    .iter()
                    .map(|b| b.digest.clone())
                    .collect::<Vec<_>>();
                let old_tip = self.latest_block().digest.clone();

                self.block_by_digest.insert(digest.clone(), block.clone());
                self.children_by_digest
                    .entry(previous_digest.clone())
                    .or_default()
                    .insert(digest.clone());
                self.children_by_digest.entry(digest.clone()).or_default();
                self.state_by_digest.insert(digest.clone(), next_state);
                self.tip_digests.insert(digest.clone());
                self.tip_digests.remove(&previous_digest);

                let mut reorg = false;
                let mut reorg_from_height = None;
                if self.is_better_tip(&digest, &old_tip) {
                    let new_chain = self.build_chain_to_tip(&digest);
                    if !new_chain.is_empty() {
                        self.chain = new_chain;
                        self.balances = self
                            .state_by_digest
                            .get(&digest)
                            .cloned()
                            .unwrap_or_default();
                    }
                }

                let new_chain_hashes = self
                    .chain
                    .iter()
                    .map(|b| b.digest.clone())
                    .collect::<Vec<_>>();
                let old_set: HashSet<String> = old_chain_hashes.iter().cloned().collect();
                let new_set: HashSet<String> = new_chain_hashes.iter().cloned().collect();
                let removed = old_chain_hashes
                    .into_iter()
                    .filter(|h| !new_set.contains(h))
                    .collect::<Vec<_>>();
                let added = new_chain_hashes
                    .iter()
                    .filter(|h| !old_set.contains(*h))
                    .cloned()
                    .collect::<Vec<_>>();
                if !removed.is_empty() {
                    reorg = true;
                    let min_height = removed
                        .iter()
                        .filter_map(|h| self.block_by_digest.get(h).map(|b| b.height))
                        .min();
                    reorg_from_height = min_height;
                }
                let reverted_txs = self.collect_transactions(&removed);
                let applied_tx_ids = self
                    .collect_transactions(&added)
                    .iter()
                    .filter_map(|tx| tx.get("tx_id").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();

                Ok(Some(self.build_memento(reorg, reorg_from_height, reverted_txs, applied_tx_ids)))
            },
            _ => Ok(None),
        }
    }

    fn is_merkleized(&self) -> bool {
        true
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.latest_block().digest.clone())
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        Ok(self.block_by_digest.contains_key(digest))
    }

    async fn is_valid_digest(&self, digest: &str) -> Result<bool> {
        self.has_digest(digest).await
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

    fn get_state_digests(&self) -> Vec<String> {
        let mut canonical = self
            .chain
            .iter()
            .rev()
            .take(8)
            .map(|b| b.digest.clone())
            .collect::<Vec<_>>();
        canonical.reverse();
        let canonical_set: HashSet<String> = canonical.iter().cloned().collect();
        let mut extras = self
            .tip_digests
            .iter()
            .filter(|d| !canonical_set.contains(*d))
            .cloned()
            .collect::<Vec<_>>();
        extras.sort();
        canonical.extend(extras);
        canonical
    }

    async fn emit_state_memento(&self) -> Result<StateMemento> {
        Ok(self.build_memento(false, None, vec![], vec![]))
    }

    async fn get_state(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "chain_length": self.chain.len(),
            "latest_digest": self.latest_block().digest,
            "latest_height": self.latest_block().height,
            "balances": self.balances
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        let genesis = self.chain.first().cloned();
        self.chain.clear();
        if let Some(genesis) = genesis {
            self.chain.push(genesis.clone());
            self.block_by_digest.clear();
            self.block_by_digest
                .insert(genesis.digest.clone(), genesis.clone());
            self.children_by_digest.clear();
            self.children_by_digest
                .insert(genesis.digest.clone(), HashSet::new());
            self.tip_digests.clear();
            self.tip_digests.insert(genesis.digest.clone());
            self.state_by_digest.clear();
            let mut balances = HashMap::new();
            balances.insert("genesis".to_string(), 1000);
            self.balances = balances.clone();
            self.state_by_digest.insert(genesis.digest, balances);
        }
        self.seen_hashes.clear();
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
pub struct MempoolObject {
    id: SharedObjectId,
    pub transactions: BTreeMap<String, Value>,
}

impl MempoolObject {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            transactions: BTreeMap::new(),
        }
    }
}

impl Default for MempoolObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for MempoolObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "MempoolObject"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let msg_type = message
            .data
            .get("message_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        Ok(matches!(msg_type, "NEW_TX" | "NEW_BLOCK"))
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        let msg_type = message
            .data
            .get("message_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        match msg_type {
            "NEW_TX" => {
                if let Some(tx) = message.data.get("tx").cloned() {
                    if let Some(tx_id) = tx.get("tx_id").and_then(|v| v.as_str()) {
                        self.transactions.insert(tx_id.to_string(), tx);
                    }
                }
            },
            "NEW_BLOCK" => {
                let metadata = frontier_state.map(|m| m.metadata).unwrap_or_default();
                let reverted = metadata
                    .get("reverted_txs")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                for tx in reverted {
                    if let Some(tx_id) = tx.get("tx_id").and_then(|v| v.as_str()) {
                        self.transactions.insert(tx_id.to_string(), tx);
                    }
                }
                let applied = metadata
                    .get("applied_tx_ids")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                if !applied.is_empty() {
                    for tx_id in applied {
                        if let Some(tx_id) = tx_id.as_str() {
                            self.transactions.remove(tx_id);
                        }
                    }
                } else if let Some(block_txs) = message
                    .data
                    .get("transactions")
                    .and_then(|v| v.as_array())
                    .cloned()
                {
                    for tx in block_txs {
                        if let Some(tx_id) = tx.get("tx_id").and_then(|v| v.as_str()) {
                            self.transactions.remove(tx_id);
                        }
                    }
                }
            },
            _ => {},
        }
        Ok(Some(self.emit_state_memento().await?))
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.transactions.len().to_string())
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
        Ok(serde_json::json!({
            "mempool_size": self.transactions.len()
        }))
    }

    async fn reset(&mut self) -> Result<()> {
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

/// Typed node wrapper for Blockchain examples.
pub struct BlockchainNode {
    node: ChaincraftNode,
    ledger_object_id: SharedObjectId,
    mempool_object_id: SharedObjectId,
}

impl BlockchainNode {
    pub async fn new(port: u16) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let ledger_object_id = node
            .add_shared_object(Box::new(BlockchainObject::new()))
            .await?;
        let mempool_object_id = node
            .add_shared_object(Box::new(MempoolObject::new()))
            .await?;
        Ok(Self {
            node,
            ledger_object_id,
            mempool_object_id,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.node.start().await
    }

    pub async fn close(&mut self) -> Result<()> {
        self.node.close().await
    }

    pub async fn connect_to_peer(&mut self, addr: &str) -> Result<()> {
        self.node.connect_to_peer(addr).await
    }

    pub fn host(&self) -> &str {
        self.node.host()
    }

    pub fn port(&self) -> u16 {
        self.node.port()
    }

    pub async fn publish(&mut self, data: Value) -> Result<String> {
        self.node.create_shared_message_with_data(data).await
    }

    pub async fn chain_state(&self) -> Result<Value> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.ledger_object_id) else {
            return Err(ChaincraftError::validation("BlockchainObject not found"));
        };
        obj.get_state().await
    }

    pub async fn mempool_state(&self) -> Result<Value> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.mempool_object_id) else {
            return Err(ChaincraftError::validation("MempoolObject not found"));
        };
        obj.get_state().await
    }
}

pub mod helpers {
    use super::*;

    pub fn create_tx_message(tx: Value) -> Value {
        serde_json::json!({
            "message_type": "NEW_TX",
            "tx": tx
        })
    }

    pub fn create_block_message(height: u64, previous_digest: String, payload: Value) -> Value {
        create_block_message_with_transactions(
            height,
            previous_digest,
            "miner".to_string(),
            vec![],
            payload,
        )
    }

    pub fn create_block_message_with_transactions(
        height: u64,
        previous_digest: String,
        miner: String,
        transactions: Vec<Value>,
        payload: Value,
    ) -> Value {
        let digest = BlockchainObject::compute_digest(&serde_json::json!({
            "height": height,
            "previous_digest": previous_digest,
            "miner": miner,
            "transactions": transactions,
            "payload": payload
        }));
        serde_json::json!({
            "message_type": "NEW_BLOCK",
            "height": height,
            "previous_digest": previous_digest,
            "miner": miner,
            "transactions": transactions,
            "payload": payload,
            "digest": digest
        })
    }

    pub fn create_simple_transfer_tx(
        tx_id: &str,
        sender: &str,
        recipient: &str,
        amount: i64,
        fee: i64,
    ) -> Value {
        serde_json::json!({
            "tx_id": tx_id,
            "sender": sender,
            "recipient": recipient,
            "amount": amount,
            "fee": fee,
        })
    }
}
