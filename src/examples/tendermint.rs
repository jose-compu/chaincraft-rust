use crate::{
    crypto::{
        ecdsa::{ECDSASigner, ECDSAVerifier},
        KeyType, PrivateKey, PublicKey, Signature,
    },
    error::{ChaincraftError, Result},
    network::PeerId,
    shared::{MessageType, SharedMessage, SharedObjectId},
    shared_object::ApplicationObject,
    storage::MemoryStorage,
    ChaincraftNode,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Tendermint consensus message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TendermintMessageType {
    Proposal {
        height: u64,
        round: u32,
        block_hash: String,
        proposer: String,
        timestamp: DateTime<Utc>,
        signature: String,
    },
    Prevote {
        height: u64,
        round: u32,
        block_hash: Option<String>, // None for nil vote
        validator: String,
        signature: String,
    },
    Precommit {
        height: u64,
        round: u32,
        block_hash: Option<String>, // None for nil vote
        validator: String,
        signature: String,
    },
    ValidatorSet {
        validators: Vec<ValidatorInfo>,
        height: u64,
    },
    BlockCommit {
        height: u64,
        block_hash: String,
        commit_signatures: Vec<String>,
        timestamp: DateTime<Utc>,
    },
}

/// Validator information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValidatorInfo {
    pub address: String,
    pub public_key: String,
    pub voting_power: u64,
    pub active: bool,
}

/// Block data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub height: u64,
    pub hash: String,
    pub previous_hash: String,
    pub timestamp: DateTime<Utc>,
    pub proposer: String,
    pub transactions: Vec<serde_json::Value>,
    pub commit_signatures: Vec<String>,
}

/// Tendermint consensus state
#[derive(Debug, Clone, PartialEq)]
pub enum ConsensusState {
    Propose,
    Prevote,
    Precommit,
    Commit,
}

/// Vote information
#[derive(Debug, Clone)]
pub struct Vote {
    pub validator: String,
    pub block_hash: Option<String>,
    pub signature: String,
    pub timestamp: DateTime<Utc>,
}

/// Tendermint BFT consensus object
#[derive(Debug)]
pub struct TendermintObject {
    pub id: SharedObjectId,
    pub validators: HashMap<String, ValidatorInfo>,
    pub blocks: Vec<Block>,
    pub current_height: u64,
    pub current_round: u32,
    pub state: ConsensusState,
    pub proposals: HashMap<(u64, u32), TendermintMessageType>,
    pub prevotes: HashMap<(u64, u32), HashMap<String, Vote>>,
    pub precommits: HashMap<(u64, u32), HashMap<String, Vote>>,
    pub locked_block: Option<String>,
    pub locked_round: Option<u32>,
    pub my_validator_address: String,
    pub signer: ECDSASigner,
    pub verifier: ECDSAVerifier,
    pub messages: Vec<TendermintMessageType>,
}

impl TendermintObject {
    pub fn new() -> Result<Self> {
        let signer = ECDSASigner::new()?;
        let my_validator_address = signer.get_public_key_pem()?;

        // Create genesis block
        let genesis_block = Block {
            height: 0,
            hash: "genesis_hash".to_string(),
            previous_hash: "".to_string(),
            timestamp: Utc::now(),
            proposer: "genesis".to_string(),
            transactions: vec![],
            commit_signatures: vec![],
        };

        Ok(Self {
            id: SharedObjectId::new(),
            validators: HashMap::new(),
            blocks: vec![genesis_block],
            current_height: 1,
            current_round: 0,
            state: ConsensusState::Propose,
            proposals: HashMap::new(),
            prevotes: HashMap::new(),
            precommits: HashMap::new(),
            locked_block: None,
            locked_round: None,
            my_validator_address,
            signer,
            verifier: ECDSAVerifier::new(),
            messages: Vec::new(),
        })
    }

    /// Add a validator to the set
    pub fn add_validator(&mut self, address: String, public_key: String, voting_power: u64) {
        let validator = ValidatorInfo {
            address: address.clone(),
            public_key,
            voting_power,
            active: true,
        };
        self.validators.insert(address, validator);
    }

    /// Get total voting power of active validators
    pub fn total_voting_power(&self) -> u64 {
        self.validators
            .values()
            .filter(|v| v.active)
            .map(|v| v.voting_power)
            .sum()
    }

    /// Check if we have +2/3 majority
    pub fn has_majority(&self, voting_power: u64) -> bool {
        voting_power * 3 > self.total_voting_power() * 2
    }

    /// Process a proposal message
    pub fn process_proposal(&mut self, proposal: TendermintMessageType) -> Result<bool> {
        if let TendermintMessageType::Proposal { height, round, .. } = &proposal {
            if *height == self.current_height && *round == self.current_round {
                self.proposals.insert((*height, *round), proposal.clone());
                self.messages.push(proposal);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Process a prevote message
    pub fn process_prevote(&mut self, prevote: TendermintMessageType) -> Result<bool> {
        if let TendermintMessageType::Prevote {
            height,
            round,
            block_hash,
            validator,
            signature,
        } = &prevote
        {
            if *height == self.current_height && *round == self.current_round {
                let vote = Vote {
                    validator: validator.clone(),
                    block_hash: block_hash.clone(),
                    signature: signature.clone(),
                    timestamp: Utc::now(),
                };

                self.prevotes
                    .entry((*height, *round))
                    .or_default()
                    .insert(validator.clone(), vote);

                self.messages.push(prevote);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Process a precommit message
    pub fn process_precommit(&mut self, precommit: TendermintMessageType) -> Result<bool> {
        if let TendermintMessageType::Precommit {
            height,
            round,
            block_hash,
            validator,
            signature,
        } = &precommit
        {
            if *height == self.current_height && *round == self.current_round {
                let vote = Vote {
                    validator: validator.clone(),
                    block_hash: block_hash.clone(),
                    signature: signature.clone(),
                    timestamp: Utc::now(),
                };

                self.precommits
                    .entry((*height, *round))
                    .or_default()
                    .insert(validator.clone(), vote);

                self.messages.push(precommit);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check if we can commit a block
    pub fn can_commit(&self) -> Option<String> {
        if let Some(precommits) = self
            .precommits
            .get(&(self.current_height, self.current_round))
        {
            let mut vote_counts: HashMap<Option<String>, u64> = HashMap::new();

            for vote in precommits.values() {
                if let Some(validator) = self.validators.get(&vote.validator) {
                    *vote_counts.entry(vote.block_hash.clone()).or_insert(0) +=
                        validator.voting_power;
                }
            }

            for (block_hash, voting_power) in vote_counts {
                if let Some(hash) = block_hash {
                    if self.has_majority(voting_power) {
                        return Some(hash);
                    }
                }
            }
        }
        None
    }

    /// Commit a block
    pub fn commit_block(&mut self, block_hash: String) -> Result<()> {
        let block = Block {
            height: self.current_height,
            hash: block_hash.clone(),
            previous_hash: self.blocks.last().unwrap().hash.clone(),
            timestamp: Utc::now(),
            proposer: self.my_validator_address.clone(),
            transactions: vec![], // Would contain actual transactions
            commit_signatures: self
                .precommits
                .get(&(self.current_height, self.current_round))
                .map(|votes| votes.values().map(|v| v.signature.clone()).collect())
                .unwrap_or_default(),
        };

        self.blocks.push(block);
        self.current_height += 1;
        self.current_round = 0;
        self.state = ConsensusState::Propose;
        self.locked_block = None;
        self.locked_round = None;

        // Clean up old votes
        self.prevotes.retain(|&(h, _), _| h >= self.current_height);
        self.precommits
            .retain(|&(h, _), _| h >= self.current_height);
        self.proposals.retain(|&(h, _), _| h >= self.current_height);

        Ok(())
    }

    /// Create a proposal for the current height/round
    pub fn create_proposal(
        &self,
        transactions: Vec<serde_json::Value>,
    ) -> Result<TendermintMessageType> {
        let block_data = serde_json::json!({
            "height": self.current_height,
            "round": self.current_round,
            "previous_hash": self.blocks.last().unwrap().hash,
            "transactions": transactions,
            "timestamp": Utc::now().to_rfc3339()
        });

        let block_hash = format!("{:x}", sha2::Sha256::digest(block_data.to_string().as_bytes()));
        let signature_data =
            format!("proposal:{}:{}:{}", self.current_height, self.current_round, block_hash);
        let signature = self.signer.sign(signature_data.as_bytes())?;

        Ok(TendermintMessageType::Proposal {
            height: self.current_height,
            round: self.current_round,
            block_hash,
            proposer: self.my_validator_address.clone(),
            timestamp: Utc::now(),
            signature: hex::encode(signature.to_bytes()),
        })
    }

    /// Get current consensus state info
    pub fn get_consensus_info(&self) -> serde_json::Value {
        serde_json::json!({
            "height": self.current_height,
            "round": self.current_round,
            "state": format!("{:?}", self.state),
            "validators_count": self.validators.len(),
            "blocks_count": self.blocks.len(),
            "locked_block": self.locked_block,
            "locked_round": self.locked_round,
            "total_voting_power": self.total_voting_power()
        })
    }

    /// Get voting statistics for current round
    pub fn get_voting_stats(&self) -> serde_json::Value {
        let prevote_count = self
            .prevotes
            .get(&(self.current_height, self.current_round))
            .map(|votes| votes.len())
            .unwrap_or(0);
        let precommit_count = self
            .precommits
            .get(&(self.current_height, self.current_round))
            .map(|votes| votes.len())
            .unwrap_or(0);

        serde_json::json!({
            "height": self.current_height,
            "round": self.current_round,
            "prevotes": prevote_count,
            "precommits": precommit_count,
            "has_proposal": self.proposals.contains_key(&(self.current_height, self.current_round))
        })
    }
}

#[async_trait]
impl ApplicationObject for TendermintObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "TendermintBFT"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let msg_result: std::result::Result<TendermintMessageType, _> =
            serde_json::from_value(message.data.clone());
        Ok(msg_result.is_ok())
    }

    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let tendermint_msg: TendermintMessageType = serde_json::from_value(message.data.clone())
            .map_err(|e| {
                ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
            })?;

        let processed = match &tendermint_msg {
            TendermintMessageType::Proposal { .. } => {
                self.process_proposal(tendermint_msg.clone())?
            },
            TendermintMessageType::Prevote { .. } => {
                self.process_prevote(tendermint_msg.clone())?
            },
            TendermintMessageType::Precommit { .. } => {
                self.process_precommit(tendermint_msg.clone())?
            },
            TendermintMessageType::ValidatorSet { validators, .. } => {
                for validator in validators {
                    self.add_validator(
                        validator.address.clone(),
                        validator.public_key.clone(),
                        validator.voting_power,
                    );
                }
                true
            },
            TendermintMessageType::BlockCommit { block_hash, .. } => {
                self.commit_block(block_hash.clone())?;
                true
            },
        };

        if processed {
            tracing::debug!("Successfully processed Tendermint message: {:?}", tendermint_msg);

            // Check if we can advance consensus
            if let Some(commit_hash) = self.can_commit() {
                self.commit_block(commit_hash)?;
            }
        }

        Ok(())
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(format!("{}:{}", self.current_height, self.current_round))
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        let current_digest = format!("{}:{}", self.current_height, self.current_round);
        Ok(digest == current_digest)
    }

    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(true)
    }

    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(true)
    }

    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new())
    }

    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new())
    }

    async fn get_state(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "type": "TendermintBFT",
            "height": self.current_height,
            "round": self.current_round,
            "state": format!("{:?}", self.state),
            "validators": self.validators.len(),
            "blocks": self.blocks.len(),
            "messages": self.messages.len(),
            "consensus_info": self.get_consensus_info(),
            "voting_stats": self.get_voting_stats()
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.current_height = 1;
        self.current_round = 0;
        self.state = ConsensusState::Propose;
        self.proposals.clear();
        self.prevotes.clear();
        self.precommits.clear();
        self.locked_block = None;
        self.locked_round = None;
        self.messages.clear();
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ApplicationObject> {
        // Create a new instance with same configuration
        let new_obj = TendermintObject::new().unwrap_or_else(|_| {
            // Fallback if creation fails
            let signer = ECDSASigner::new().unwrap();
            let my_validator_address = signer.get_public_key_pem().unwrap();
            TendermintObject {
                id: SharedObjectId::new(),
                validators: HashMap::new(),
                blocks: vec![],
                current_height: 1,
                current_round: 0,
                state: ConsensusState::Propose,
                proposals: HashMap::new(),
                prevotes: HashMap::new(),
                precommits: HashMap::new(),
                locked_block: None,
                locked_round: None,
                my_validator_address,
                signer,
                verifier: ECDSAVerifier::new(),
                messages: Vec::new(),
            }
        });
        Box::new(new_obj)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Typed node wrapper for Tendermint examples.
pub struct TendermintNode {
    node: ChaincraftNode,
    object_id: SharedObjectId,
}

impl TendermintNode {
    pub async fn new(port: u16) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let object_id = node
            .add_shared_object(Box::new(TendermintObject::new()?))
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

    pub async fn state(&self) -> Result<(u64, u32, ConsensusState)> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("TendermintObject not found"));
        };
        let Some(tm) = obj.as_any().downcast_ref::<TendermintObject>() else {
            return Err(ChaincraftError::validation("Object type mismatch for TendermintObject"));
        };
        Ok((tm.current_height, tm.current_round, tm.state.clone()))
    }
}

/// Helper functions for creating Tendermint messages
pub mod helpers {
    use super::*;

    pub fn create_validator_set_message(
        validators: Vec<ValidatorInfo>,
        height: u64,
    ) -> Result<serde_json::Value> {
        let validator_msg = TendermintMessageType::ValidatorSet { validators, height };
        serde_json::to_value(validator_msg)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }

    pub fn create_proposal_message(
        height: u64,
        round: u32,
        block_hash: String,
        proposer: String,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data = format!("proposal:{height}:{round}:{block_hash}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let proposal = TendermintMessageType::Proposal {
            height,
            round,
            block_hash,
            proposer,
            timestamp: Utc::now(),
            signature: hex::encode(signature.to_bytes()),
        };

        serde_json::to_value(proposal)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }

    pub fn create_prevote_message(
        height: u64,
        round: u32,
        block_hash: Option<String>,
        validator: String,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data = format!("prevote:{height}:{round}:{block_hash:?}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let prevote = TendermintMessageType::Prevote {
            height,
            round,
            block_hash,
            validator,
            signature: hex::encode(signature.to_bytes()),
        };

        serde_json::to_value(prevote)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }

    pub fn create_precommit_message(
        height: u64,
        round: u32,
        block_hash: Option<String>,
        validator: String,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data = format!("precommit:{height}:{round}:{block_hash:?}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let precommit = TendermintMessageType::Precommit {
            height,
            round,
            block_hash,
            validator,
            signature: hex::encode(signature.to_bytes()),
        };

        serde_json::to_value(precommit)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }
}
