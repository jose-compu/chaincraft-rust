//! Snowflake Protocol - Avalanche paper Section 2.3
//!
//! BFT single-decree consensus with a consecutive-success counter.
//! This object uses Chaincraft gossip messages for vote exchange.

use crate::{
    error::Result,
    network::PeerId,
    shared::{SharedMessage, SharedObjectId},
    shared_object::ApplicationObject,
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
pub struct SnowflakeVote {
    pub message_type: String,
    pub node_id: String,
    pub round: u32,
    pub color: Color,
}

#[derive(Debug, Clone)]
pub struct SnowflakeObject {
    id: SharedObjectId,
    pub node_id: String,
    pub color: Option<Color>,
    pub accepted: Option<Color>,
    pub k: usize,
    pub alpha: f64,
    pub beta: u32,
    pub current_round: u32,
    pub consecutive_count: u32,
    votes: Vec<SnowflakeVote>,
    seen_hashes: HashSet<String>,
}

impl SnowflakeObject {
    pub fn new(node_id: String, k: usize, alpha: f64, beta: u32) -> Self {
        Self {
            id: SharedObjectId::new(),
            node_id,
            color: None,
            accepted: None,
            k,
            alpha,
            beta,
            current_round: 0,
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

        let sampled_majority = if red >= threshold && red > blue {
            Some(Color::Red)
        } else if blue >= threshold && blue > red {
            Some(Color::Blue)
        } else {
            None
        };

        let Some(majority) = sampled_majority else {
            return false;
        };

        let flipped = match self.color {
            Some(current) if current != majority => {
                self.color = Some(majority);
                self.consecutive_count = 1;
                true
            },
            Some(_) => {
                self.consecutive_count += 1;
                false
            },
            None => {
                self.color = Some(majority);
                self.consecutive_count = 1;
                false
            },
        };

        if self.consecutive_count > self.beta {
            self.accepted = self.color;
        }

        flipped
    }
}

pub fn create_vote_message(node_id: &str, round: u32, color: Color) -> Value {
    serde_json::json!({
        "message_type": "SNOWFLAKE_VOTE",
        "node_id": node_id,
        "round": round,
        "color": color,
    })
}

/// Typed node wrapper for Snowflake examples.
pub struct SnowflakeNode {
    node: ChaincraftNode,
    object_id: SharedObjectId,
}

impl SnowflakeNode {
    pub async fn new(node_id: String, port: u16, k: usize, alpha: f64, beta: u32) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let object_id = node
            .add_shared_object(Box::new(SnowflakeObject::new(node_id, k, alpha, beta)))
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
            return Err(crate::error::ChaincraftError::validation("SnowflakeObject not found"));
        };
        let Some(snowflake) = obj.as_any().downcast_ref::<SnowflakeObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowflakeObject",
            ));
        };
        Ok(snowflake.node_id.clone())
    }

    pub async fn color(&self) -> Result<Option<Color>> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowflakeObject not found"));
        };
        let Some(snowflake) = obj.as_any().downcast_ref::<SnowflakeObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowflakeObject",
            ));
        };
        Ok(snowflake.color)
    }

    pub async fn accepted(&self) -> Result<Option<Color>> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowflakeObject not found"));
        };
        let Some(snowflake) = obj.as_any().downcast_ref::<SnowflakeObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowflakeObject",
            ));
        };
        Ok(snowflake.accepted)
    }

    pub async fn process_round(&mut self, round: u32) -> Result<bool> {
        let mut registry = self.node.app_objects.write().await;
        let Some(obj) = registry.objects.get_mut(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowflakeObject not found"));
        };
        let Some(snowflake) = obj.as_any_mut().downcast_mut::<SnowflakeObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowflakeObject",
            ));
        };
        let flipped = snowflake.process_round(round);
        snowflake.current_round = round;
        Ok(flipped)
    }

    pub async fn state_snapshot(&self) -> Result<(Option<Color>, Option<Color>, u32)> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(crate::error::ChaincraftError::validation("SnowflakeObject not found"));
        };
        let Some(snowflake) = obj.as_any().downcast_ref::<SnowflakeObject>() else {
            return Err(crate::error::ChaincraftError::validation(
                "Object type mismatch for SnowflakeObject",
            ));
        };
        Ok((snowflake.color, snowflake.accepted, snowflake.consecutive_count))
    }
}

#[async_trait]
impl ApplicationObject for SnowflakeObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "SnowflakeObject"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        let vote: std::result::Result<SnowflakeVote, _> =
            serde_json::from_value(message.data.clone());
        if let Ok(v) = vote {
            return Ok(v.message_type == "SNOWFLAKE_VOTE");
        }
        Ok(false)
    }

    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        if self.seen_hashes.contains(&message.hash) {
            return Ok(());
        }
        self.seen_hashes.insert(message.hash.clone());

        let vote: SnowflakeVote = match serde_json::from_value(message.data.clone()) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        if self.color.is_none() {
            self.color = Some(vote.color);
        }

        self.votes.push(vote);
        Ok(())
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        Ok(format!("{:?}", self.accepted.or(self.color)))
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
            "color": format!("{:?}", self.color),
            "accepted": format!("{:?}", self.accepted),
            "round": self.current_round,
            "consecutive_count": self.consecutive_count,
            "votes": self.votes.len(),
        }))
    }

    async fn reset(&mut self) -> Result<()> {
        self.color = None;
        self.accepted = None;
        self.current_round = 0;
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
