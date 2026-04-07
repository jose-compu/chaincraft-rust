//! Chatroom Protocol - A dapp example for Chaincraft
//!
//! This demonstrates how to build a decentralized application on top of Chaincraft.
//! The chatroom protocol allows:
//! - Creating chatrooms with admin control
//! - Requesting to join chatrooms
//! - Admin approval of new members
//! - Posting messages to chatrooms
//! - Message validation and signature verification

use crate::{
    crypto::{
        ecdsa::{ECDSASignature, ECDSAVerifier},
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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Chatroom message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "message_type")]
pub enum ChatroomMessageType {
    #[serde(rename = "CREATE_CHATROOM")]
    CreateChatroom {
        chatroom_name: String,
        public_key_pem: String,
        #[serde(default)]
        timestamp: f64,
        #[serde(default)]
        signature: String,
    },
    #[serde(rename = "REQUEST_JOIN")]
    RequestJoin {
        chatroom_name: String,
        public_key_pem: String,
        #[serde(default)]
        timestamp: f64,
        #[serde(default)]
        signature: String,
    },
    #[serde(rename = "ACCEPT_MEMBER")]
    AcceptMember {
        chatroom_name: String,
        public_key_pem: String,
        requester_key_pem: String,
        #[serde(default)]
        timestamp: f64,
        #[serde(default)]
        signature: String,
    },
    #[serde(rename = "POST_MESSAGE")]
    PostMessage {
        chatroom_name: String,
        public_key_pem: String,
        text: String,
        #[serde(default)]
        timestamp: f64,
        #[serde(default)]
        signature: String,
    },
}

/// A chatroom structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chatroom {
    pub name: String,
    pub admin: String,              // Admin's public key
    pub members: Vec<String>,       // Member public keys
    pub messages: Vec<ChatMessage>, // All messages including metadata
}

/// A chat message with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub message_type: String,
    pub chatroom_name: String,
    pub public_key_pem: String,
    pub text: Option<String>,
    pub requester_key_pem: Option<String>,
    pub timestamp: f64,
    pub signature: String,
}

/// Chatroom application object
#[derive(Debug, Clone)]
pub struct ChatroomObject {
    id: SharedObjectId,
    chatrooms: HashMap<String, Chatroom>,
    users: HashMap<String, String>,
    verifier: ECDSAVerifier,
}

impl ChatroomObject {
    pub fn new() -> Self {
        Self {
            id: SharedObjectId::new(),
            chatrooms: HashMap::new(),
            users: HashMap::new(),
            verifier: ECDSAVerifier::new(),
        }
    }

    /// Get all chatrooms
    pub fn get_chatrooms(&self) -> &HashMap<String, Chatroom> {
        &self.chatrooms
    }

    /// Get a specific chatroom
    pub fn get_chatroom(&self, name: &str) -> Option<&Chatroom> {
        self.chatrooms.get(name)
    }

    /// Validate message signature
    fn validate_signature(
        &self,
        msg_data: &Value,
        signature: &str,
        public_key_pem: &str,
    ) -> Result<bool> {
        // Extract message data without signature for verification
        let mut msg_for_verification = msg_data.clone();
        if let Some(obj) = msg_for_verification.as_object_mut() {
            obj.remove("signature");
        }

        let payload = serde_json::to_string(&msg_for_verification).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;

        // Decode signature from hex
        let signature_bytes = hex::decode(signature)
            .map_err(|_| ChaincraftError::validation("Invalid signature hex"))?;

        // Verify signature
        let ecdsa_sig = ECDSASignature::from_bytes(&signature_bytes)
            .map_err(|_| ChaincraftError::validation("Invalid signature format"))?;

        self.verifier
            .verify(payload.as_bytes(), &ecdsa_sig, public_key_pem)
    }

    /// Check if timestamp is recent (within 15 seconds)
    fn is_timestamp_recent(&self, timestamp: f64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let diff = (now - timestamp).abs();
        diff <= 15.0
    }

    /// Process a create chatroom message
    async fn process_create_chatroom(
        &mut self,
        msg: ChatroomMessageType,
        msg_data: &Value,
    ) -> Result<bool> {
        if let ChatroomMessageType::CreateChatroom {
            chatroom_name,
            public_key_pem,
            timestamp,
            signature,
        } = msg
        {
            // Validate signature
            if !self.validate_signature(msg_data, &signature, &public_key_pem)? {
                return Ok(false);
            }

            // Check timestamp
            if !self.is_timestamp_recent(timestamp) {
                return Ok(false);
            }

            // Check if chatroom already exists
            if self.chatrooms.contains_key(&chatroom_name) {
                return Ok(false);
            }

            // Create new chatroom
            let chatroom = Chatroom {
                name: chatroom_name.clone(),
                admin: public_key_pem.clone(),
                members: vec![public_key_pem.clone()], // Admin is automatically a member
                messages: Vec::new(),
            };

            self.chatrooms.insert(chatroom_name, chatroom);
            tracing::info!("Created chatroom with admin: {}", public_key_pem);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Process a request join message
    async fn process_request_join(
        &mut self,
        msg: ChatroomMessageType,
        msg_data: &Value,
    ) -> Result<bool> {
        if let ChatroomMessageType::RequestJoin {
            chatroom_name,
            public_key_pem,
            timestamp,
            signature,
        } = msg
        {
            // Validate signature
            if !self.validate_signature(msg_data, &signature, &public_key_pem)? {
                return Ok(false);
            }

            // Check timestamp
            if !self.is_timestamp_recent(timestamp) {
                return Ok(false);
            }

            // Check if chatroom exists
            if !self.chatrooms.contains_key(&chatroom_name) {
                return Ok(false);
            }

            // Add to pending requests (for now, just log)
            tracing::info!(
                "Join request for chatroom '{}' from: {}",
                chatroom_name,
                public_key_pem
            );

            // Store the request message
            if let Some(chatroom) = self.chatrooms.get_mut(&chatroom_name) {
                let chat_msg = ChatMessage {
                    message_type: "REQUEST_JOIN".to_string(),
                    chatroom_name: chatroom_name.clone(),
                    public_key_pem: public_key_pem.clone(),
                    text: None,
                    requester_key_pem: None,
                    timestamp,
                    signature,
                };
                chatroom.messages.push(chat_msg);
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Process an accept member message
    async fn process_accept_member(
        &mut self,
        msg: ChatroomMessageType,
        msg_data: &Value,
    ) -> Result<bool> {
        if let ChatroomMessageType::AcceptMember {
            chatroom_name,
            public_key_pem,
            requester_key_pem,
            timestamp,
            signature,
        } = msg
        {
            // Validate signature
            if !self.validate_signature(msg_data, &signature, &public_key_pem)? {
                return Ok(false);
            }

            // Check timestamp
            if !self.is_timestamp_recent(timestamp) {
                return Ok(false);
            }

            // Check if chatroom exists and sender is admin
            if let Some(chatroom) = self.chatrooms.get_mut(&chatroom_name) {
                if chatroom.admin != public_key_pem {
                    return Ok(false); // Only admin can accept members
                }

                // Add member if not already present
                if !chatroom.members.contains(&requester_key_pem) {
                    chatroom.members.push(requester_key_pem.clone());
                    tracing::info!(
                        "Added member {} to chatroom '{}'",
                        requester_key_pem,
                        chatroom_name
                    );
                }

                // Store the accept message
                let chat_msg = ChatMessage {
                    message_type: "ACCEPT_MEMBER".to_string(),
                    chatroom_name: chatroom_name.clone(),
                    public_key_pem: public_key_pem.clone(),
                    text: None,
                    requester_key_pem: Some(requester_key_pem),
                    timestamp,
                    signature,
                };
                chatroom.messages.push(chat_msg);

                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Process a post message
    async fn process_post_message(
        &mut self,
        msg: ChatroomMessageType,
        msg_data: &Value,
    ) -> Result<bool> {
        if let ChatroomMessageType::PostMessage {
            chatroom_name,
            public_key_pem,
            text,
            timestamp,
            signature,
        } = msg
        {
            // Validate signature
            if !self.validate_signature(msg_data, &signature, &public_key_pem)? {
                return Ok(false);
            }

            // Check timestamp
            if !self.is_timestamp_recent(timestamp) {
                return Ok(false);
            }

            // Check if chatroom exists and sender is a member
            if let Some(chatroom) = self.chatrooms.get_mut(&chatroom_name) {
                if !chatroom.members.contains(&public_key_pem) {
                    return Ok(false); // Only members can post messages
                }

                // Add the message
                let chat_msg = ChatMessage {
                    message_type: "POST_MESSAGE".to_string(),
                    chatroom_name: chatroom_name.clone(),
                    public_key_pem: public_key_pem.clone(),
                    text: Some(text),
                    requester_key_pem: None,
                    timestamp,
                    signature,
                };
                chatroom.messages.push(chat_msg);

                tracing::info!("Message posted to '{}' by: {}", chatroom_name, public_key_pem);

                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

impl Default for ChatroomObject {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ApplicationObject for ChatroomObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "ChatroomObject"
    }

    async fn is_valid(&self, message: &SharedMessage) -> Result<bool> {
        // Check if this is a chatroom message
        let msg_result: std::result::Result<ChatroomMessageType, _> =
            serde_json::from_value(message.data.clone());
        Ok(msg_result.is_ok())
    }

    async fn add_message(&mut self, message: SharedMessage) -> Result<()> {
        let msg: ChatroomMessageType =
            serde_json::from_value(message.data.clone()).map_err(|e| {
                ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
            })?;

        let processed = match &msg {
            ChatroomMessageType::CreateChatroom { .. } => {
                self.process_create_chatroom(msg.clone(), &message.data)
                    .await?
            },
            ChatroomMessageType::RequestJoin { .. } => {
                self.process_request_join(msg.clone(), &message.data)
                    .await?
            },
            ChatroomMessageType::AcceptMember { .. } => {
                self.process_accept_member(msg.clone(), &message.data)
                    .await?
            },
            ChatroomMessageType::PostMessage { .. } => {
                self.process_post_message(msg.clone(), &message.data)
                    .await?
            },
        };

        if processed {
            tracing::debug!("Successfully processed chatroom message: {:?}", msg);
        } else {
            tracing::warn!("Failed to process chatroom message: {:?}", msg);
        }

        Ok(())
    }

    fn is_merkleized(&self) -> bool {
        false
    }

    async fn get_latest_digest(&self) -> Result<String> {
        let mut hasher = Sha256::new();

        for (room_name, chatroom) in &self.chatrooms {
            hasher.update(room_name.as_bytes());
            hasher.update(chatroom.messages.len().to_le_bytes());
        }

        Ok(hex::encode(hasher.finalize()))
    }

    async fn has_digest(&self, _digest: &str) -> Result<bool> {
        Ok(false) // Simplified implementation
    }

    async fn is_valid_digest(&self, _digest: &str) -> Result<bool> {
        Ok(true)
    }

    async fn add_digest(&mut self, _digest: String) -> Result<bool> {
        Ok(true)
    }

    async fn gossip_messages(&self, _digest: Option<&str>) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new()) // Simplified implementation
    }

    async fn get_messages_since_digest(&self, _digest: &str) -> Result<Vec<SharedMessage>> {
        Ok(Vec::new()) // Simplified implementation
    }

    async fn get_state(&self) -> Result<Value> {
        let state = serde_json::json!({
            "chatroom_count": self.chatrooms.len(),
            "chatrooms": self.chatrooms.keys().collect::<Vec<_>>(),
            "total_messages": self.chatrooms.values().map(|c| c.messages.len()).sum::<usize>()
        });
        Ok(state)
    }

    async fn reset(&mut self) -> Result<()> {
        self.chatrooms.clear();
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

/// Typed node wrapper for Chatroom examples.
pub struct ChatroomNode {
    node: ChaincraftNode,
    object_id: SharedObjectId,
}

impl ChatroomNode {
    pub async fn new(port: u16) -> Result<Self> {
        let mut node = ChaincraftNode::new(PeerId::new(), Arc::new(MemoryStorage::new()));
        node.set_port(port);
        let object_id = node
            .add_shared_object(Box::new(ChatroomObject::new()))
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

    pub fn id(&self) -> &crate::network::PeerId {
        self.node.id()
    }

    pub async fn publish(&mut self, data: Value) -> Result<String> {
        self.node.create_shared_message_with_data(data).await
    }

    pub async fn chatroom_message_count(&self, room: &str) -> Result<usize> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("ChatroomObject not found"));
        };
        let Some(chatroom) = obj.as_any().downcast_ref::<ChatroomObject>() else {
            return Err(ChaincraftError::validation("Object type mismatch for ChatroomObject"));
        };
        Ok(chatroom
            .get_chatroom(room)
            .map(|r| r.messages.len())
            .unwrap_or(0))
    }

    pub async fn chatroom_names(&self) -> Result<Vec<String>> {
        let registry = self.node.app_objects.read().await;
        let Some(obj) = registry.get(&self.object_id) else {
            return Err(ChaincraftError::validation("ChatroomObject not found"));
        };
        let Some(chatroom) = obj.as_any().downcast_ref::<ChatroomObject>() else {
            return Err(ChaincraftError::validation("Object type mismatch for ChatroomObject"));
        };
        Ok(chatroom.get_chatrooms().keys().cloned().collect())
    }
}

/// Helper functions for creating chatroom messages
pub mod helpers {
    use super::*;
    use crate::crypto::ecdsa::ECDSASigner;

    /// Create a create chatroom message
    pub fn create_chatroom_message(chatroom_name: String, signer: &ECDSASigner) -> Result<Value> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let public_key_pem = signer.get_public_key_pem()?;

        let mut msg = serde_json::json!({
            "message_type": "CREATE_CHATROOM",
            "chatroom_name": chatroom_name,
            "public_key_pem": public_key_pem,
            "timestamp": timestamp
        });

        // Sign the message
        let payload = serde_json::to_string(&msg).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        let signature = signer.sign(payload.as_bytes())?;

        msg["signature"] = serde_json::Value::String(hex::encode(signature.to_bytes()));

        Ok(msg)
    }

    /// Create a post message
    pub fn create_post_message(
        chatroom_name: String,
        text: String,
        signer: &ECDSASigner,
    ) -> Result<Value> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let public_key_pem = signer.get_public_key_pem()?;

        let mut msg = serde_json::json!({
            "message_type": "POST_MESSAGE",
            "chatroom_name": chatroom_name,
            "public_key_pem": public_key_pem,
            "text": text,
            "timestamp": timestamp
        });

        // Sign the message
        let payload = serde_json::to_string(&msg).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        let signature = signer.sign(payload.as_bytes())?;

        msg["signature"] = serde_json::Value::String(hex::encode(signature.to_bytes()));

        Ok(msg)
    }
}
