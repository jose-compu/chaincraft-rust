use async_trait::async_trait;
use chaincraft::shared::{MessageType, SharedMessage, SharedObjectId};
use chaincraft::shared_object::{ApplicationObject, ApplicationObjectRegistry};
use chaincraft::{blockchain_helpers, normalize_state_memento, BlockchainObject, StateMemento};
use serde_json::Value;
use std::any::Any;

#[derive(Debug, Clone)]
struct EmitterObject {
    id: SharedObjectId,
}

#[async_trait]
impl ApplicationObject for EmitterObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "EmitterObject"
    }

    async fn is_valid(&self, _message: &SharedMessage) -> chaincraft::Result<bool> {
        Ok(true)
    }

    async fn add_message(
        &mut self,
        _message: SharedMessage,
        _frontier_state: Option<StateMemento>,
    ) -> chaincraft::Result<Option<StateMemento>> {
        Ok(Some(normalize_state_memento("digest-a", Some(vec!["digest-a".to_string()]))))
    }

    fn is_merkleized(&self) -> bool {
        false
    }
    async fn get_latest_digest(&self) -> chaincraft::Result<String> {
        Ok("digest-a".to_string())
    }
    async fn has_digest(&self, _digest: &str) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn is_valid_digest(&self, _digest: &str) -> chaincraft::Result<bool> {
        Ok(true)
    }
    async fn add_digest(&mut self, _digest: String) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn gossip_messages(
        &self,
        _digest: Option<&str>,
    ) -> chaincraft::Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_messages_since_digest(
        &self,
        _digest: &str,
    ) -> chaincraft::Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    fn get_state_digests(&self) -> Vec<String> {
        vec!["digest-a".to_string()]
    }
    async fn get_state(&self) -> chaincraft::Result<Value> {
        Ok(serde_json::json!({}))
    }
    async fn reset(&mut self) -> chaincraft::Result<()> {
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
struct ReceiverObject {
    id: SharedObjectId,
    last_frontier: Option<StateMemento>,
}

#[async_trait]
impl ApplicationObject for ReceiverObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }

    fn type_name(&self) -> &'static str {
        "ReceiverObject"
    }

    async fn is_valid(&self, _message: &SharedMessage) -> chaincraft::Result<bool> {
        Ok(true)
    }

    async fn add_message(
        &mut self,
        _message: SharedMessage,
        frontier_state: Option<StateMemento>,
    ) -> chaincraft::Result<Option<StateMemento>> {
        self.last_frontier = frontier_state.clone();
        Ok(frontier_state)
    }

    fn is_merkleized(&self) -> bool {
        false
    }
    async fn get_latest_digest(&self) -> chaincraft::Result<String> {
        Ok(self
            .last_frontier
            .as_ref()
            .map(|m| m.canonical_digest.clone())
            .unwrap_or_default())
    }
    async fn has_digest(&self, _digest: &str) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn is_valid_digest(&self, _digest: &str) -> chaincraft::Result<bool> {
        Ok(true)
    }
    async fn add_digest(&mut self, _digest: String) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn gossip_messages(
        &self,
        _digest: Option<&str>,
    ) -> chaincraft::Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_messages_since_digest(
        &self,
        _digest: &str,
    ) -> chaincraft::Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_state(&self) -> chaincraft::Result<Value> {
        Ok(serde_json::json!({}))
    }
    async fn reset(&mut self) -> chaincraft::Result<()> {
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
struct RejectorObject {
    id: SharedObjectId,
}

#[async_trait]
impl ApplicationObject for RejectorObject {
    fn id(&self) -> &SharedObjectId {
        &self.id
    }
    fn type_name(&self) -> &'static str {
        "RejectorObject"
    }
    async fn is_valid(&self, _message: &SharedMessage) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn add_message(
        &mut self,
        _message: SharedMessage,
        _frontier_state: Option<StateMemento>,
    ) -> chaincraft::Result<Option<StateMemento>> {
        Ok(None)
    }
    fn is_merkleized(&self) -> bool {
        false
    }
    async fn get_latest_digest(&self) -> chaincraft::Result<String> {
        Ok(String::new())
    }
    async fn has_digest(&self, _digest: &str) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn is_valid_digest(&self, _digest: &str) -> chaincraft::Result<bool> {
        Ok(true)
    }
    async fn add_digest(&mut self, _digest: String) -> chaincraft::Result<bool> {
        Ok(false)
    }
    async fn gossip_messages(
        &self,
        _digest: Option<&str>,
    ) -> chaincraft::Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_messages_since_digest(
        &self,
        _digest: &str,
    ) -> chaincraft::Result<Vec<SharedMessage>> {
        Ok(vec![])
    }
    async fn get_state(&self) -> chaincraft::Result<Value> {
        Ok(serde_json::json!({}))
    }
    async fn reset(&mut self) -> chaincraft::Result<()> {
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

fn msg(data: Value) -> SharedMessage {
    SharedMessage::new(MessageType::Custom("TEST".to_string()), data)
}

#[tokio::test]
async fn test_pipeline_propagates_frontier_memento_between_objects() {
    let mut registry = ApplicationObjectRegistry::new();
    registry.register(Box::new(EmitterObject {
        id: SharedObjectId::new(),
    }));
    let receiver_id = registry.register(Box::new(ReceiverObject {
        id: SharedObjectId::new(),
        last_frontier: None,
    }));

    registry
        .process_message(msg(serde_json::json!({"k":"v"})))
        .await
        .unwrap();

    let receiver = registry
        .objects
        .get(&receiver_id)
        .unwrap()
        .as_any()
        .downcast_ref::<ReceiverObject>()
        .unwrap();
    let frontier = receiver.last_frontier.as_ref().unwrap();
    assert_eq!(frontier.canonical_digest, "digest-a");
    assert!(frontier.frontier_digests.iter().any(|d| d == "digest-a"));
}

#[tokio::test]
async fn test_pipeline_rejects_message_when_any_object_invalid() {
    let mut registry = ApplicationObjectRegistry::new();
    registry.register(Box::new(RejectorObject {
        id: SharedObjectId::new(),
    }));
    registry.register(Box::new(EmitterObject {
        id: SharedObjectId::new(),
    }));

    let result = registry
        .process_message(msg(serde_json::json!({"k":"v"})))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_blockchain_memento_reorg_metadata_shape() {
    let mut blockchain = BlockchainObject::with_reward(10);
    let genesis = blockchain.latest_block().digest.clone();
    let block1 = blockchain_helpers::create_block_message_with_transactions(
        1,
        genesis.clone(),
        "miner-a".to_string(),
        vec![],
        serde_json::json!({"id":"b1"}),
    );
    let msg1 = SharedMessage::new(MessageType::Custom("NEW_BLOCK".to_string()), block1);
    let _ = blockchain.add_message(msg1, None).await.unwrap();

    let tx1 = blockchain_helpers::create_simple_transfer_tx("tx-1", "miner-a", "bob", 2, 1);
    let block2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        blockchain.latest_block().digest.clone(),
        "miner-a".to_string(),
        vec![tx1.clone()],
        serde_json::json!({"id":"b2"}),
    );
    let msg2 = SharedMessage::new(MessageType::Custom("NEW_BLOCK".to_string()), block2);
    let _ = blockchain.add_message(msg2, None).await.unwrap();

    let c2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        blockchain.chain[1].digest.clone(),
        "miner-c".to_string(),
        vec![],
        serde_json::json!({"id":"c2"}),
    );
    let c2_msg = SharedMessage::new(MessageType::Custom("NEW_BLOCK".to_string()), c2);
    let _ = blockchain.add_message(c2_msg, None).await.unwrap();

    let c3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        blockchain
            .get_state_digests()
            .iter()
            .find(|d| **d != blockchain.latest_block().digest)
            .cloned()
            .unwrap_or_default(),
        "miner-c".to_string(),
        vec![],
        serde_json::json!({"id":"c3"}),
    );
    let c3_msg = SharedMessage::new(MessageType::Custom("NEW_BLOCK".to_string()), c3);
    let memento = blockchain.add_message(c3_msg, None).await.unwrap().unwrap();

    assert_eq!(memento.metadata.get("reorg"), Some(&serde_json::json!(true)));
    assert!(memento.metadata.contains_key("reorg_from_height"));
    assert_eq!(memento.metadata.get("reverted_txs"), Some(&serde_json::json!([tx1])));
    assert_eq!(memento.metadata.get("applied_tx_ids"), Some(&serde_json::json!([])));
    assert!(memento.metadata.contains_key("chain_length"));
}
