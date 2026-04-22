//! ECDSA-signed transaction ledger - educational shared object
//!
//! Demonstrates ECDSA signature verification for a simple transfer ledger.
//! Transactions have { from, to, amount, nonce } and are signed by the sender.

use crate::{
    crypto::ecdsa::{ECDSASignature, ECDSAVerifier},
    error::{ChaincraftError, Result},
    network::PeerId,
    shared::{SharedMessage, SharedObjectId},
    shared_object::ApplicationObject,
    state_memento::StateMemento,
    storage::MemoryStorage,
    ChaincraftNode,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "message_type")]
pub enum LedgerMessageType {
    #[serde(rename = "TRANSFER")]
    Transfer {
        from: String,
        to: String,
        amount: u64,
        nonce: u64,
        public_key_pem: String,
        #[serde(default)]
        signature: String,
    },
}

#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub nonce: u64,
}

/// ECDSA-signed transaction ledger.
/// Accepts TRANSFER messages; validates signature before appending.
#[derive(Debug, Clone)]
pub struct ECDSALedgerObject {
    id: SharedObjectId,
    entries: Vec<LedgerEntry>,
    seen_tx_hashes: HashSet<String>,
    balances: HashMap<String, u64>,
    nonces: HashMap<String, u64>,
    verifier: ECDSAVerifier,
}

impl ECDSALedgerObject {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            entries: Vec::new(),
            seen_tx_hashes: HashSet::new(),
            balances: HashMap::new(),
            nonces: HashMap::new(),
            verifier: ECDSAVerifier::new(),
        }
    }

    pub fn entries(&self) -> &[LedgerEntry] {
        &self.entries
    }

    pub fn balance(&self, account: &str) -> u64 {
        *self.balances.get(account).unwrap_or(&0)
    }

    fn tx_hash(from: &str, to: &str, amount: u64, nonce: u64) -> String {
        format!("{from}:{to}:{amount}:{nonce}")
    }

    fn validate_signature(
        &self,
        msg_data: &Value,
        signature: &str,
        public_key_pem: &str,
    ) -> Result<bool> {
        let mut for_verify = msg_data.clone();
        if let Some(obj) = for_verify.as_object_mut() {
            obj.remove("signature");
        }
        let payload = serde_json::to_string(&for_verify).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        let sig_bytes = hex::decode(signature)
            .map_err(|_| ChaincraftError::validation("Invalid signature hex"))?;
        let ecdsa_sig = ECDSASignature::from_bytes(&sig_bytes)
            .map_err(|_| ChaincraftError::validation("Invalid signature format"))?;
        self.verifier
            .verify(payload.as_bytes(), &ecdsa_sig, public_key_pem)
    }
}

impl Default for ECDSALedgerObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for ECDSALedgerObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "ECDSALedger"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let msg: LedgerMessageType = serde_json::from_value(message.data.clone())
            .map_err(|_| ChaincraftError::validation("Invalid ledger message format"))?;

        match msg {
            LedgerMessageType::Transfer {
                from,
                to,
                amount,
                nonce,
                public_key_pem,
                signature,
            } => {
                if signature.is_empty() {
                    return Ok(false);
                }
                let tx_hash = Self::tx_hash(&from, &to, amount, nonce);
                if self.seen_tx_hashes.contains(&tx_hash) {
                    return Ok(true);
                }
                let msg_data = serde_json::to_value(&message.data).unwrap_or_default();
                self.validate_signature(&msg_data, &signature, &public_key_pem)
            },
        }
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        let msg: LedgerMessageType = serde_json::from_value(message.data.clone())
            .map_err(|_| ChaincraftError::validation("Invalid ledger message format"))?;

        match msg {
            LedgerMessageType::Transfer {
                from,
                to,
                amount,
                nonce,
                public_key_pem,
                signature,
            } => {
                let tx_hash = Self::tx_hash(&from, &to, amount, nonce);
                if self.seen_tx_hashes.contains(&tx_hash) {
                    return Ok(None);
                }

                let msg_data = message.data.clone();
                if !self.validate_signature(&msg_data, &signature, &public_key_pem)? {
                    return Ok(None);
                }

                let from_balance = *self.balances.get(&from).unwrap_or(&0);
                let expected_nonce = *self.nonces.get(&from).unwrap_or(&0);
                if nonce != expected_nonce {
                    return Ok(None);
                }
                // Allow first tx from new address (genesis/mint for demo)
                if from_balance < amount && from_balance > 0 {
                    return Ok(None);
                }

                self.seen_tx_hashes.insert(tx_hash);
                self.entries.push(LedgerEntry {
                    from: from.clone(),
                    to: to.clone(),
                    amount,
                    nonce,
                });
                if from_balance >= amount {
                    self.balances.insert(from.clone(), from_balance - amount);
                }
                *self.balances.entry(to).or_insert(0) += amount;
                self.nonces.insert(from, nonce + 1);
                Ok(None)
            },
        }
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(self.entries.len().to_string())
    }

    async fn has_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false)
    }

    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(true)
    }

    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(false)
    }

    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new())
    }

    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new())
    }

    async fn get_state(&self) -> Result<Value> {
        let balances: HashMap<&str, u64> = self
            .balances
            .iter()
            .map(|(k, v)| (k.as_str(), *v))
            .collect();
        Ok(serde_json::json!({
            "entry_count": self.entries.len(),
            "balances": balances
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.entries.clear();
        self.seen_tx_hashes.clear();
        self.balances.clear();
        self.nonces.clear();
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

/// Typed node wrapper for ECDSA ledger examples.
pub struct ECDSALedgerNode {
    node: ChaincraftNode,
    object_id: SharedObjectId,
}

impl ECDSALedgerNode {
    pub async fn new(port: u16) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let object_id = node
            .add_shared_object(Box::new(ECDSALedgerObject::new()))
            .await?;
        Ok(Self { node, object_id })
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

    pub async fn publish(&mut self, data: serde_json::Value) -> Result<String> {
        self.node.create_shared_message_with_data(data).await
    }

    pub async fn entry_count(&self) -> Result<usize> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("ECDSALedgerObject not found"));
        };
        let Some(ledger) = obj.as_any().downcast_ref::<ECDSALedgerObject>() else {
            return Err(ChaincraftError::validation("Object type mismatch for ECDSALedgerObject"));
        };
        Ok(ledger.entries().len())
    }

    pub async fn balance(&self, account: &str) -> Result<u64> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("ECDSALedgerObject not found"));
        };
        let Some(ledger) = obj.as_any().downcast_ref::<ECDSALedgerObject>() else {
            return Err(ChaincraftError::validation("Object type mismatch for ECDSALedgerObject"));
        };
        Ok(ledger.balance(account))
    }
}

/// Helpers for creating signed transfer messages
pub mod helpers {
    use super::*;
    use crate::crypto::ecdsa::ECDSASigner;
    use serde_json::json;

    /// Create a signed TRANSFER message
    pub fn create_transfer(
        from: String,
        to: String,
        amount: u64,
        nonce: u64,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let public_key_pem = signer.get_public_key_pem()?;
        let payload = json!({
            "message_type": "TRANSFER",
            "from": from,
            "to": to,
            "amount": amount,
            "nonce": nonce,
            "public_key_pem": public_key_pem,
            "signature": ""
        });
        let mut for_sign = payload.clone();
        if let Some(obj) = for_sign.as_object_mut() {
            obj.remove("signature");
        }
        let to_sign = serde_json::to_vec(&for_sign).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        let sig = signer.sign(&to_sign)?;
        let sig_hex = hex::encode(sig.to_bytes());
        let mut out = payload;
        out["signature"] = serde_json::json!(sig_hex);
        Ok(out)
    }
}
