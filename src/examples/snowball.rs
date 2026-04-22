//! Snowball Protocol - Avalanche family
//!
//! Extends Snowflake by keeping confidence counters per color plus
//! a consecutive-success counter for fast finalization.

use crate::{
    error::Result,
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
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Color {
    Red,
    Blue,
}

impl std::fmt::Display for Color {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Color::Red => write!(f, "R"),
            Color::Blue => write!(f, "B"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnowballVote {
    pub message_type: String,
    pub node_id: String,
    pub round: u32,
    pub color: Color,
}

#[derive(Debug, Clone)]
pub struct SnowballObject {
    id: SharedObjectId,
    pub node_id: String,
    pub preference: Option<Color>,
    pub accepted: Option<Color>,
    pub k: usize,
    pub alpha: f64,
    pub beta: u32,
    pub current_round: u32,
    pub confidence_red: u32,
    pub confidence_blue: u32,
    pub last_color: Option<Color>,
    pub consecutive_count: u32,
    votes: Vec<SnowballVote>,
    seen_hashes: HashSet<String>,
}

impl SnowballObject {
    pub fn new(node_id: String, k: usize, alpha: f64, beta: u32) -> Self {
        Self {
            id: SharedObjectId::new(),
            node_id,
            preference: None,
            accepted: None,
            k,
            alpha,
            beta,
            current_round: 0,
            confidence_red: 0,
            confidence_blue: 0,
            last_color: None,
            consecutive_count: 0,
            votes: Vec::new(),
            seen_hashes: HashSet::new(),
        }
    }

    pub fn count_votes_for_round(&self, round: u32) -> (usize, usize) {
        let mut red = 0usize;
        let mut blue = 0usize;
        for v in &self.votes {
            if v.round == round && v.node_id != self.node_id {
                match v.color {
                    Color::Red => red += 1,
                    Color::Blue => blue += 1,
                }
            }
        }
        (red, blue)
    }

    pub fn process_round(&mut self, round: u32) -> bool {
        let (red, blue) = self.count_votes_for_round(round);
        let threshold = ((self.alpha * self.k as f64) as usize).max(1);

        let sample_majority = if red >= threshold && red > blue {
            Some(Color::Red)
        } else if blue >= threshold && blue > red {
            Some(Color::Blue)
        } else {
            None
        };

        let Some(majority) = sample_majority else {
            return false;
        };

        match majority {
            Color::Red => self.confidence_red += 1,
            Color::Blue => self.confidence_blue += 1,
        }

        let mut flipped = false;
        if self.preference.is_none() {
            self.preference = Some(majority);
        } else if let Some(pref) = self.preference {
            let pref_conf = match pref {
                Color::Red => self.confidence_red,
                Color::Blue => self.confidence_blue,
            };
            let maj_conf = match majority {
                Color::Red => self.confidence_red,
                Color::Blue => self.confidence_blue,
            };
            if maj_conf > pref_conf {
                self.preference = Some(majority);
                flipped = pref != majority;
            }
        }

        match self.last_color {
            Some(last) if last == majority => self.consecutive_count += 1,
            _ => {
                self.last_color = Some(majority);
                self.consecutive_count = 1;
            },
        }

        if self.consecutive_count > self.beta {
            self.accepted = self.preference.or(Some(majority));
        }

        flipped
    }
}

pub fn create_vote_message(node_id: &str, round: u32, color: Color) -> Value {
    serde_json::json!({
        "message_type": "SNOWBALL_VOTE",
        "node_id": node_id,
        "round": round,
        "color": color,
    })
}

/// Typed node wrapper for Snowball examples.
pub struct SnowballNode {
    node: ChaincraftNode,
    object_id: SharedObjectId,
}

impl SnowballNode {
    pub async fn new(node_id: String, port: u16, k: usize, alpha: f64, beta: u32) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let object_id = node
            .add_shared_object(Box::new(SnowballObject::new(node_id, k, alpha, beta)))
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

    pub async fn publish_vote(&mut self, round: u32, color: Color) -> Result<()> {
        let vote = create_vote_message(&self.node_id().await?, round, color);
        self.node.create_shared_message_with_data(vote).await?;
        Ok(())
    }

    pub async fn node_id(&self) -> Result<String> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowballObject not found"));
        };
        let Some(snowball) = obj.as_any().downcast_ref::<SnowballObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowballObject",
            ));
        };
        Ok(snowball.node_id.clone())
    }

    pub async fn preference(&self) -> Result<Option<Color>> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowballObject not found"));
        };
        let Some(snowball) = obj.as_any().downcast_ref::<SnowballObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowballObject",
            ));
        };
        Ok(snowball.preference)
    }

    pub async fn accepted(&self) -> Result<Option<Color>> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowballObject not found"));
        };
        let Some(snowball) = obj.as_any().downcast_ref::<SnowballObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowballObject",
            ));
        };
        Ok(snowball.accepted)
    }

    pub async fn process_round(&mut self, round: u32) -> Result<bool> {
        let mut registry = self.node.app_objects.write().await;
        let Some(obj) = registry.objects.get_mut(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowballObject not found"));
        };
        let Some(snowball) = obj.as_any_mut().downcast_mut::<SnowballObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowballObject",
            ));
        };
        let flipped = snowball.process_round(round);
        snowball.current_round = round;
        Ok(flipped)
    }

    pub async fn state_snapshot(&self) -> Result<(Option<Color>, Option<Color>, u32, u32, u32)> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowballObject not found"));
        };
        let Some(snowball) = obj.as_any().downcast_ref::<SnowballObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowballObject",
            ));
        };
        Ok((
            snowball.preference,
            snowball.accepted,
            snowball.confidence_red,
            snowball.confidence_blue,
            snowball.consecutive_count,
        ))
    }
}

#[async_trait]
impl ApplicationObject for SnowballObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "SnowballObject"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let vote: std::result::Result<SnowballVote, _> =
            serde_json::from_value(message.data.clone());
        if let Ok(v) = vote {
            return Ok(v.message_type == "SNOWBALL_VOTE");
        }
        Ok(false)
    }

    async fn add_message(
        &mut self,
        message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> Result<Option<StateMemento>> {
        if self.seen_hashes.contains(&message.hash) {
            return Ok(None);
        }
        self.seen_hashes.insert(message.hash.clone());

        let vote: SnowballVote = match serde_json::from_value(message.data.clone()) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        if self.preference.is_none() {
            self.preference = Some(vote.color);
            self.last_color = Some(vote.color);
        }

        self.votes.push(vote);
        Ok(None)
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(format!("{:?}", self.accepted.or(self.preference)))
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
        Ok(serde_json::json!({
            "node_id": self.node_id,
            "preference": format!("{:?}", self.preference),
            "accepted": format!("{:?}", self.accepted),
            "round": self.current_round,
            "confidence_red": self.confidence_red,
            "confidence_blue": self.confidence_blue,
            "consecutive_count": self.consecutive_count,
            "votes": self.votes.len(),
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.preference = None;
        self.accepted = None;
        self.current_round = 0;
        self.confidence_red = 0;
        self.confidence_blue = 0;
        self.last_color = None;
        self.consecutive_count = 0;
        self.votes.clear();
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
