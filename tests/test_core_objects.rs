use chaincraft::shared::{MessageType, SharedMessage};
use chaincraft::shared_object::ApplicationObject;
use chaincraft::{
    BalanceLedger, Blockchain, CacheObject, CoreSharedObject, DAGObject, DocumentCache, Mempool,
    MerkelizedObject, NonMerkelizedObject, TransactionChain, UTXOLedger,
};

fn msg(data: serde_json::Value) -> SharedMessage {
    SharedMessage::new(MessageType::Custom("test".to_string()), data)
}

#[tokio::test]
async fn test_core_digest_stability() {
    let a = serde_json::json!({"b": 2, "a": 1});
    let b = serde_json::json!({"a": 1, "b": 2});
    assert_eq!(CoreSharedObject::compute_digest(&a), CoreSharedObject::compute_digest(&b));
}

#[tokio::test]
async fn test_non_merkelized_stubs() {
    for obj in &mut [
        Box::new(NonMerkelizedObject::new()) as Box<dyn ApplicationObject>,
        Box::new(CacheObject::new()),
        Box::new(Mempool::new()),
        Box::new(DocumentCache::new()),
    ] {
        assert!(!obj.is_merkleized());
        assert_eq!(obj.get_latest_digest().await.unwrap(), "");
        assert!(!obj.has_digest("abc").await.unwrap());
        assert!(!obj.is_valid_digest("abc").await.unwrap());
        assert!(!obj.add_digest("abc".to_string()).await.unwrap());
        assert!(obj.gossip_messages(Some("abc")).await.unwrap().is_empty());
        assert!(obj
            .get_messages_since_digest("abc")
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn test_merkelized_object_basic_digest_flow() {
    let mut obj = MerkelizedObject::new();
    obj.add_message(msg(serde_json::json!({"i": 1})))
        .await
        .unwrap();
    let d1 = obj.get_latest_digest().await.unwrap();
    obj.add_message(msg(serde_json::json!({"i": 2})))
        .await
        .unwrap();
    let d2 = obj.get_latest_digest().await.unwrap();
    assert_ne!(d1, "");
    assert_ne!(d1, d2);
    assert_eq!(obj.get_messages_since_digest(&d1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_utxo_add_spend_flow() {
    let mut ledger = UTXOLedger::new();
    let add = msg(serde_json::json!({
        "action": "utxo_add",
        "utxo_id": "u1",
        "amount": 5,
        "owner": "alice"
    }));
    assert!(ledger.is_valid(&add).await.unwrap());
    ledger.add_message(add).await.unwrap();
    assert!(ledger.utxos.contains_key("u1"));

    let spend = msg(serde_json::json!({"action": "utxo_spend", "utxo_id": "u1"}));
    assert!(ledger.is_valid(&spend).await.unwrap());
    ledger.add_message(spend).await.unwrap();
    assert!(!ledger.utxos.contains_key("u1"));
}

#[tokio::test]
async fn test_balance_ledger_credit_debit_transfer() {
    let mut ledger = BalanceLedger::new();
    ledger
        .add_message(msg(serde_json::json!({
            "action": "credit",
            "account": "alice",
            "amount": 10.0
        })))
        .await
        .unwrap();
    let debit = msg(serde_json::json!({"action": "debit", "account": "alice", "amount": 3.0}));
    assert!(ledger.is_valid(&debit).await.unwrap());
    ledger.add_message(debit).await.unwrap();

    let transfer = msg(serde_json::json!({
        "action": "transfer",
        "from": "alice",
        "to": "bob",
        "amount": 4.0
    }));
    assert!(ledger.is_valid(&transfer).await.unwrap());
    ledger.add_message(transfer).await.unwrap();
    assert_eq!(ledger.balances.get("alice").cloned().unwrap_or(0.0), 3.0);
    assert_eq!(ledger.balances.get("bob").cloned().unwrap_or(0.0), 4.0);
}

#[tokio::test]
async fn test_blockchain_previous_digest_rules() {
    let mut chain = Blockchain::new();
    let genesis = msg(serde_json::json!({"previous_digest": "genesis", "height": 1}));
    assert!(chain.is_valid(&genesis).await.unwrap());
    chain.add_message(genesis).await.unwrap();
    let prev = chain.blocks[0]["digest"].as_str().unwrap().to_string();
    let next = msg(serde_json::json!({"previous_digest": prev, "height": 2}));
    assert!(chain.is_valid(&next).await.unwrap());
}

#[tokio::test]
async fn test_dag_frontier_and_since_digest() {
    let mut dag = DAGObject::new();
    dag.add_message(msg(serde_json::json!({"parents": [], "payload": "root"})))
        .await
        .unwrap();
    let root = dag.get_head_digests()[0].clone();
    dag.add_message(msg(serde_json::json!({"parents": [root], "payload": "child"})))
        .await
        .unwrap();
    let checkpoint = dag.get_latest_digest().await.unwrap();
    let head = dag.get_head_digests()[0].clone();
    dag.add_message(msg(serde_json::json!({"parents": [head], "payload": "child-2"})))
        .await
        .unwrap();
    let delta = dag.get_messages_since_digest(&checkpoint).await.unwrap();
    assert_eq!(delta.len(), 1);
}

#[tokio::test]
async fn test_transaction_chain_tracks_order() {
    let mut chain = TransactionChain::new();
    chain
        .add_message(msg(serde_json::json!({"tx_id": "a", "amount": 1})))
        .await
        .unwrap();
    let d1 = chain.get_latest_digest().await.unwrap();
    chain
        .add_message(msg(serde_json::json!({"tx_id": "b", "amount": 2})))
        .await
        .unwrap();
    assert_eq!(chain.transactions.len(), 2);
    assert_eq!(chain.get_messages_since_digest(&d1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_mempool_and_document_cache_ops() {
    let mut mempool = Mempool::new();
    mempool
        .add_message(msg(serde_json::json!({"from": "a", "to": "b", "amount": 1})))
        .await
        .unwrap();
    assert_eq!(mempool.transactions.len(), 1);
    let tx_id = mempool.transactions.keys().next().unwrap().clone();
    mempool
        .add_message(msg(serde_json::json!({"tx_id": tx_id, "action": "remove"})))
        .await
        .unwrap();
    assert!(mempool.transactions.is_empty());

    let mut docs = DocumentCache::new();
    docs.add_message(msg(serde_json::json!({
        "document_id": "d1",
        "value": {"title": "hello"}
    })))
    .await
    .unwrap();
    assert_eq!(docs.documents["d1"]["title"], "hello");
}
