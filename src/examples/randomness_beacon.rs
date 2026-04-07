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

/// Randomness beacon message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BeaconMessageType {
    /// VRF proof submission
    VrfProof {
        round: u64,
        input: String,
        proof: String,
        output: String,
        validator: String,
        signature: String,
        timestamp: DateTime<Utc>,
    },
    /// Partial signature for threshold signature
    PartialSignature {
        round: u64,
        validator: String,
        partial_sig: String,
        signature: String,
        timestamp: DateTime<Utc>,
    },
    /// Finalized randomness for a round
    FinalizedBeacon {
        round: u64,
        randomness: String,
        vrf_proofs: Vec<String>,
        threshold_sig: String,
        participants: Vec<String>,
        timestamp: DateTime<Utc>,
    },
    /// Validator registration for beacon participation
    ValidatorRegistration {
        validator: String,
        public_key: String,
        vrf_key: String,
        stake: u64,
        signature: String,
    },
    /// Challenge for bias resistance
    BiasChallenge {
        round: u64,
        challenger: String,
        target_validator: String,
        challenge_data: String,
        signature: String,
    },
}

/// VRF (Verifiable Random Function) proof
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VrfProof {
    pub validator: String,
    pub input: String,
    pub proof: String,
    pub output: String,
    pub signature: String,
    pub timestamp: DateTime<Utc>,
}

/// Beacon validator information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeaconValidator {
    pub address: String,
    pub public_key: String,
    pub vrf_key: String,
    pub stake: u64,
    pub active: bool,
    pub last_participation: Option<u64>,
}

/// Finalized beacon round
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeaconRound {
    pub round: u64,
    pub randomness: String,
    pub participants: Vec<String>,
    pub vrf_proofs: Vec<VrfProof>,
    pub threshold_signature: String,
    pub finalized_at: DateTime<Utc>,
    pub bias_challenges: Vec<String>,
}

/// Randomness beacon implementation
#[derive(Debug)]
pub struct RandomnessBeaconObject {
    pub id: SharedObjectId,
    pub validators: HashMap<String, BeaconValidator>,
    pub rounds: HashMap<u64, BeaconRound>,
    pub current_round: u64,
    pub round_duration_secs: u64,
    pub last_round_time: DateTime<Utc>,
    pub pending_vrf_proofs: HashMap<u64, Vec<VrfProof>>,
    pub pending_partial_sigs: HashMap<u64, HashMap<String, String>>,
    pub threshold: u64, // Minimum number of participants needed
    pub my_validator_address: String,
    pub signer: ECDSASigner,
    pub verifier: ECDSAVerifier,
    pub messages: Vec<BeaconMessageType>,
    pub bias_resistance_enabled: bool,
    pub challenges: HashMap<u64, Vec<BeaconMessageType>>,
}

impl RandomnessBeaconObject {
    pub fn new(round_duration_secs: u64, threshold: u64) -> Result<Self> {
        let signer = ECDSASigner::new()?;
        let my_validator_address = signer.get_public_key_pem()?;

        Ok(Self {
            id: SharedObjectId::new(),
            validators: HashMap::new(),
            rounds: HashMap::new(),
            current_round: 1,
            round_duration_secs,
            last_round_time: Utc::now(),
            pending_vrf_proofs: HashMap::new(),
            pending_partial_sigs: HashMap::new(),
            threshold,
            my_validator_address,
            signer,
            verifier: ECDSAVerifier::new(),
            messages: Vec::new(),
            bias_resistance_enabled: true,
            challenges: HashMap::new(),
        })
    }

    /// Register a validator for beacon participation
    pub fn register_validator(&mut self, validator: BeaconValidator) -> Result<()> {
        // Verify validator signature (simplified)
        self.validators.insert(validator.address.clone(), validator);
        Ok(())
    }

    /// Check if enough time has passed to advance to next round
    pub fn should_advance_round(&self) -> bool {
        let elapsed = Utc::now().signed_duration_since(self.last_round_time);
        elapsed.num_seconds() >= self.round_duration_secs as i64
    }

    /// Generate VRF proof for current round
    pub fn generate_vrf_proof(&self, input: &str) -> Result<VrfProof> {
        // Simplified VRF implementation
        let vrf_input = format!("round_{}_{}", self.current_round, input);
        let mut hasher = Sha256::new();
        hasher.update(vrf_input.as_bytes());
        hasher.update(self.my_validator_address.as_bytes());
        let hash = hasher.finalize();

        let proof = hex::encode(&hash[0..16]); // First 16 bytes as proof
        let output = hex::encode(&hash[16..32]); // Last 16 bytes as output

        let signature_data = format!("vrf:{}:{}:{}:{}", self.current_round, input, proof, output);
        let signature = self.signer.sign(signature_data.as_bytes())?;

        Ok(VrfProof {
            validator: self.my_validator_address.clone(),
            input: input.to_string(),
            proof,
            output,
            signature: hex::encode(signature.to_bytes()),
            timestamp: Utc::now(),
        })
    }

    /// Process VRF proof submission
    pub fn process_vrf_proof(&mut self, msg: BeaconMessageType) -> Result<bool> {
        if let BeaconMessageType::VrfProof {
            round,
            input,
            proof,
            output,
            validator,
            signature,
            timestamp,
        } = msg
        {
            if round != self.current_round {
                return Ok(false);
            }

            // Verify validator is registered
            if !self.validators.contains_key(&validator) {
                return Ok(false);
            }

            let vrf_proof = VrfProof {
                validator: validator.clone(),
                input,
                proof,
                output,
                signature,
                timestamp,
            };

            self.pending_vrf_proofs
                .entry(round)
                .or_default()
                .push(vrf_proof.clone());

            self.messages.push(BeaconMessageType::VrfProof {
                round,
                input: vrf_proof.input,
                proof: vrf_proof.proof,
                output: vrf_proof.output,
                validator,
                signature: vrf_proof.signature,
                timestamp,
            });

            return Ok(true);
        }
        Ok(false)
    }

    /// Process partial signature
    pub fn process_partial_signature(&mut self, msg: BeaconMessageType) -> Result<bool> {
        if let BeaconMessageType::PartialSignature {
            round,
            validator,
            partial_sig,
            signature,
            timestamp,
        } = msg
        {
            if round != self.current_round {
                return Ok(false);
            }

            if !self.validators.contains_key(&validator) {
                return Ok(false);
            }

            self.pending_partial_sigs
                .entry(round)
                .or_default()
                .insert(validator.clone(), partial_sig.clone());

            self.messages.push(BeaconMessageType::PartialSignature {
                round,
                validator,
                partial_sig,
                signature,
                timestamp,
            });

            return Ok(true);
        }
        Ok(false)
    }

    /// Check if we can finalize the current round
    pub fn can_finalize_round(&self) -> bool {
        let vrf_count = self
            .pending_vrf_proofs
            .get(&self.current_round)
            .map(|proofs| proofs.len())
            .unwrap_or(0);

        let partial_sig_count = self
            .pending_partial_sigs
            .get(&self.current_round)
            .map(|sigs| sigs.len())
            .unwrap_or(0);

        vrf_count >= self.threshold as usize && partial_sig_count >= self.threshold as usize
    }

    /// Finalize the current round and generate randomness
    pub fn finalize_round(&mut self) -> Result<String> {
        if !self.can_finalize_round() {
            return Err(ChaincraftError::validation(
                "Insufficient proofs/signatures for finalization",
            ));
        }

        let vrf_proofs = self
            .pending_vrf_proofs
            .get(&self.current_round)
            .cloned()
            .unwrap_or_default();

        let partial_sigs = self
            .pending_partial_sigs
            .get(&self.current_round)
            .cloned()
            .unwrap_or_default();

        // Combine VRF outputs to create final randomness
        let mut combined_randomness = String::new();
        for proof in &vrf_proofs {
            combined_randomness.push_str(&proof.output);
        }

        let mut hasher = Sha256::new();
        hasher.update(combined_randomness.as_bytes());
        hasher.update(self.current_round.to_string().as_bytes());
        let final_randomness = hex::encode(hasher.finalize());

        // Create threshold signature (simplified)
        let mut threshold_sig = String::new();
        for (validator, sig) in &partial_sigs {
            threshold_sig.push_str(&format!("{validator}:{sig};"));
        }

        let participants: Vec<String> = vrf_proofs.iter().map(|p| p.validator.clone()).collect();

        let beacon_round = BeaconRound {
            round: self.current_round,
            randomness: final_randomness.clone(),
            participants: participants.clone(),
            vrf_proofs,
            threshold_signature: threshold_sig,
            finalized_at: Utc::now(),
            bias_challenges: self
                .challenges
                .get(&self.current_round)
                .map(|challenges| challenges.iter().map(|c| format!("{c:?}")).collect())
                .unwrap_or_default(),
        };

        self.rounds.insert(self.current_round, beacon_round);

        // Clean up and advance to next round
        self.pending_vrf_proofs.remove(&self.current_round);
        self.pending_partial_sigs.remove(&self.current_round);
        self.challenges.remove(&self.current_round);

        self.current_round += 1;
        self.last_round_time = Utc::now();

        Ok(final_randomness)
    }

    /// Process bias challenge
    pub fn process_bias_challenge(&mut self, msg: BeaconMessageType) -> Result<bool> {
        if !self.bias_resistance_enabled {
            return Ok(false);
        }

        if let BeaconMessageType::BiasChallenge { round, .. } = &msg {
            self.challenges.entry(*round).or_default().push(msg.clone());

            self.messages.push(msg);
            return Ok(true);
        }
        Ok(false)
    }

    /// Get beacon statistics
    pub fn get_beacon_stats(&self) -> serde_json::Value {
        let current_vrf_count = self
            .pending_vrf_proofs
            .get(&self.current_round)
            .map(|proofs| proofs.len())
            .unwrap_or(0);

        let current_partial_sigs = self
            .pending_partial_sigs
            .get(&self.current_round)
            .map(|sigs| sigs.len())
            .unwrap_or(0);

        serde_json::json!({
            "current_round": self.current_round,
            "total_rounds": self.rounds.len(),
            "active_validators": self.validators.values().filter(|v| v.active).count(),
            "threshold": self.threshold,
            "current_vrf_proofs": current_vrf_count,
            "current_partial_signatures": current_partial_sigs,
            "can_finalize": self.can_finalize_round(),
            "should_advance": self.should_advance_round(),
            "bias_resistance": self.bias_resistance_enabled,
            "total_challenges": self.challenges.values().map(|c| c.len()).sum::<usize>()
        })
    }

    /// Get latest randomness
    pub fn get_latest_randomness(&self) -> Option<String> {
        if self.current_round > 1 {
            self.rounds
                .get(&(self.current_round - 1))
                .map(|round| round.randomness.clone())
        } else {
            None
        }
    }

    /// Get randomness history
    pub fn get_randomness_history(&self, count: usize) -> Vec<(u64, String)> {
        let mut history: Vec<_> = self
            .rounds
            .iter()
            .map(|(round, data)| (*round, data.randomness.clone()))
            .collect();
        history.sort_by_key(|(round, _)| *round);
        history.into_iter().rev().take(count).collect()
    }
}

#[async_trait]
impl ApplicationObject for RandomnessBeaconObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "RandomnessBeacon"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let msg_result: std::result::Result<BeaconMessageType, _> =
            serde_json::from_value(message.data.clone());
        Ok(msg_result.is_ok())
    }

    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let beacon_msg: BeaconMessageType =
            serde_json::from_value(message.data.clone()).map_err(|e| {
                ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
            })?;

        let processed = match &beacon_msg {
            BeaconMessageType::VrfProof { .. } => self.process_vrf_proof(beacon_msg.clone())?,
            BeaconMessageType::PartialSignature { .. } => {
                self.process_partial_signature(beacon_msg.clone())?
            },
            BeaconMessageType::ValidatorRegistration {
                validator,
                public_key,
                vrf_key,
                stake,
                ..
            } => {
                let beacon_validator = BeaconValidator {
                    address: validator.clone(),
                    public_key: public_key.clone(),
                    vrf_key: vrf_key.clone(),
                    stake: *stake,
                    active: true,
                    last_participation: None,
                };
                self.register_validator(beacon_validator)?;
                true
            },
            BeaconMessageType::BiasChallenge { .. } => {
                self.process_bias_challenge(beacon_msg.clone())?
            },
            BeaconMessageType::FinalizedBeacon { .. } => {
                // Already finalized beacon rounds are informational
                self.messages.push(beacon_msg.clone());
                true
            },
        };

        if processed {
            tracing::debug!("Successfully processed beacon message: {:?}", beacon_msg);

            // Check if we can finalize the current round
            if self.can_finalize_round() {
                if let Ok(randomness) = self.finalize_round() {
                    tracing::info!(
                        "Finalized beacon round {} with randomness: {}",
                        self.current_round - 1,
                        randomness
                    );
                }
            }
        }

        Ok(())
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(format!("beacon_round:{}", self.current_round))
    }

    async fn has_digest(&self, digest: &str) -> Result<bool> {
        let current_digest = format!("beacon_round:{}", self.current_round);
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
            "type": "RandomnessBeacon",
            "current_round": self.current_round,
            "validators": self.validators.len(),
            "finalized_rounds": self.rounds.len(),
            "messages": self.messages.len(),
            "latest_randomness": self.get_latest_randomness(),
            "beacon_stats": self.get_beacon_stats(),
            "randomness_history": self.get_randomness_history(10)
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.rounds.clear();
        self.current_round = 1;
        self.last_round_time = Utc::now();
        self.pending_vrf_proofs.clear();
        self.pending_partial_sigs.clear();
        self.challenges.clear();
        self.messages.clear();
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn ApplicationObject> {
        // Create a new instance with same configuration
        let new_obj = RandomnessBeaconObject::new(self.round_duration_secs, self.threshold)
            .unwrap_or_else(|_| {
                // Fallback if creation fails
                let signer = ECDSASigner::new().unwrap();
                let my_validator_address = signer.get_public_key_pem().unwrap();
                RandomnessBeaconObject {
                    id: SharedObjectId::new(),
                    validators: HashMap::new(),
                    rounds: HashMap::new(),
                    current_round: 1,
                    round_duration_secs: 60,
                    last_round_time: Utc::now(),
                    pending_vrf_proofs: HashMap::new(),
                    pending_partial_sigs: HashMap::new(),
                    threshold: 3,
                    my_validator_address,
                    signer,
                    verifier: ECDSAVerifier::new(),
                    messages: Vec::new(),
                    bias_resistance_enabled: true,
                    challenges: HashMap::new(),
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

/// Typed node wrapper for Randomness Beacon examples.
pub struct RandomnessBeaconNode {
    node: ChaincraftNode,
    object_id: SharedObjectId,
}

impl RandomnessBeaconNode {
    pub async fn new(port: u16, round_duration_secs: u64, threshold: u64) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let beacon = RandomnessBeaconObject::new(round_duration_secs, threshold)?;
        let object_id = node.add_shared_object(Box::new(beacon)).await?;
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

    pub async fn validator_address(&self) -> Result<String> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("RandomnessBeaconObject not found"));
        };
        let Some(beacon) = obj.as_any().downcast_ref::<RandomnessBeaconObject>() else {
            return Err(ChaincraftError::validation(
                "Object type mismatch for RandomnessBeaconObject",
            ));
        };
        Ok(beacon.my_validator_address.clone())
    }

    pub async fn beacon_state(&self) -> Result<serde_json::Value> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("RandomnessBeaconObject not found"));
        };
        obj.get_state().await
    }
}

/// Helper functions for creating beacon messages
pub mod helpers {
    use super::*;

    pub fn create_validator_registration(
        validator: String,
        public_key: String,
        vrf_key: String,
        stake: u64,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data = format!("register:{validator}:{public_key}:{vrf_key}:{stake}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let registration = BeaconMessageType::ValidatorRegistration {
            validator,
            public_key,
            vrf_key,
            stake,
            signature: hex::encode(signature.to_bytes()),
        };

        serde_json::to_value(registration)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }

    pub fn create_vrf_proof_message(
        round: u64,
        input: String,
        proof: String,
        output: String,
        validator: String,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data = format!("vrf:{round}:{input}:{proof}:{output}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let vrf_msg = BeaconMessageType::VrfProof {
            round,
            input,
            proof,
            output,
            validator,
            signature: hex::encode(signature.to_bytes()),
            timestamp: Utc::now(),
        };

        serde_json::to_value(vrf_msg)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }

    pub fn create_partial_signature_message(
        round: u64,
        validator: String,
        partial_sig: String,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data = format!("partial_sig:{round}:{validator}:{partial_sig}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let partial_sig_msg = BeaconMessageType::PartialSignature {
            round,
            validator,
            partial_sig,
            signature: hex::encode(signature.to_bytes()),
            timestamp: Utc::now(),
        };

        serde_json::to_value(partial_sig_msg)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }

    pub fn create_bias_challenge(
        round: u64,
        challenger: String,
        target_validator: String,
        challenge_data: String,
        signer: &ECDSASigner,
    ) -> Result<serde_json::Value> {
        let signature_data =
            format!("challenge:{round}:{challenger}:{target_validator}:{challenge_data}");
        let signature = signer.sign(signature_data.as_bytes())?;

        let challenge = BeaconMessageType::BiasChallenge {
            round,
            challenger,
            target_validator,
            challenge_data,
            signature: hex::encode(signature.to_bytes()),
        };

        serde_json::to_value(challenge)
            .map_err(|e| ChaincraftError::Serialization(crate::error::SerializationError::Json(e)))
    }
}
