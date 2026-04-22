use chaincraft::shared::{MessageType, SharedMessage};
use chaincraft::{blockchain_helpers, ApplicationObject, BlockchainObject, MempoolObject};
use serde_json::Value;

fn msg(data: Value) -> SharedMessage {
    SharedMessage::new(MessageType::Custom("BLOCKCHAIN".to_string()), data)
}

fn digest_of(data: &Value) -> String {
    data.get("digest")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

async fn apply_pipeline(
    ledger: &mut BlockchainObject,
    mempool: &mut MempoolObject,
    payload: Value,
) -> chaincraft::StateMemento {
    let message = msg(payload);
    assert!(ledger.is_valid(&message).await.unwrap());
    assert!(mempool.is_valid(&message).await.unwrap());
    let state = ledger
        .add_message(message.clone(), None)
        .await
        .unwrap()
        .unwrap();
    let _ = mempool
        .add_message(message, Some(state.clone()))
        .await
        .unwrap();
    state
}

#[tokio::test]
async fn test_deep_reorg_reintroduces_reverted_transactions() {
    let mut ledger = BlockchainObject::with_reward(10);
    let mut mempool = MempoolObject::new();

    let miner_a = "miner-a";
    let miner_c = "miner-c";
    let wallet_b = "wallet-b";

    let genesis = ledger.latest_block().digest.clone();
    let b1 = blockchain_helpers::create_block_message_with_transactions(
        1,
        genesis,
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"b1"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, b1).await;

    let tx1 = blockchain_helpers::create_simple_transfer_tx("tx-1", miner_a, wallet_b, 3, 1);
    let tx1_msg = blockchain_helpers::create_tx_message(tx1.clone());
    let _ = apply_pipeline(&mut ledger, &mut mempool, tx1_msg).await;
    assert!(mempool.transactions.contains_key("tx-1"));

    let block2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        ledger.latest_block().digest.clone(),
        miner_a.to_string(),
        vec![tx1.clone()],
        serde_json::json!({"id":"b2"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, block2).await;
    assert!(!mempool.transactions.contains_key("tx-1"));

    let block3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        ledger.latest_block().digest.clone(),
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"b3"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, block3).await;

    let fork_b2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        ledger.chain[1].digest.clone(),
        miner_c.to_string(),
        vec![],
        serde_json::json!({"id":"c2"}),
    );
    let fork_b2_digest = digest_of(&fork_b2);
    let m2 = apply_pipeline(&mut ledger, &mut mempool, fork_b2).await;

    let fork_b3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        fork_b2_digest.clone(),
        miner_c.to_string(),
        vec![],
        serde_json::json!({"id":"c3"}),
    );
    let fork_b3_digest = digest_of(&fork_b3);
    let m3 = apply_pipeline(&mut ledger, &mut mempool, fork_b3).await;

    let fork_b4 = blockchain_helpers::create_block_message_with_transactions(
        4,
        fork_b3_digest.clone(),
        miner_c.to_string(),
        vec![],
        serde_json::json!({"id":"c4"}),
    );
    let fork_b4_digest = digest_of(&fork_b4);
    let m4 = apply_pipeline(&mut ledger, &mut mempool, fork_b4).await;

    let did_reorg = [m2, m3, m4]
        .iter()
        .any(|m| m.metadata.get("reorg") == Some(&serde_json::json!(true)));
    assert!(did_reorg);
    assert_eq!(ledger.latest_block().digest, fork_b4_digest);
    assert!(mempool.transactions.contains_key("tx-1"));
}

#[tokio::test]
async fn test_deep_reorg_does_not_reintroduce_reapplied_transaction() {
    let mut ledger = BlockchainObject::with_reward(10);
    let mut mempool = MempoolObject::new();

    let miner_a = "miner-a";
    let miner_c = "miner-c";
    let wallet_b = "wallet-b";

    let genesis = ledger.latest_block().digest.clone();
    let b1 = blockchain_helpers::create_block_message_with_transactions(
        1,
        genesis,
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"b1"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, b1).await;

    let tx1 = blockchain_helpers::create_simple_transfer_tx("tx-1", miner_a, wallet_b, 2, 1);
    let tx1_msg = blockchain_helpers::create_tx_message(tx1.clone());
    let _ = apply_pipeline(&mut ledger, &mut mempool, tx1_msg).await;

    let b2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        ledger.latest_block().digest.clone(),
        miner_a.to_string(),
        vec![tx1.clone()],
        serde_json::json!({"id":"b2"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, b2).await;

    let b3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        ledger.latest_block().digest.clone(),
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"b3"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, b3).await;
    assert!(!mempool.transactions.contains_key("tx-1"));

    let c2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        ledger.chain[1].digest.clone(),
        miner_c.to_string(),
        vec![],
        serde_json::json!({"id":"c2"}),
    );
    let c2_digest = digest_of(&c2);
    let _ = apply_pipeline(&mut ledger, &mut mempool, c2).await;

    let c3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        c2_digest.clone(),
        miner_c.to_string(),
        vec![tx1.clone()],
        serde_json::json!({"id":"c3"}),
    );
    let c3_digest = digest_of(&c3);
    let _ = apply_pipeline(&mut ledger, &mut mempool, c3).await;

    let c4 = blockchain_helpers::create_block_message_with_transactions(
        4,
        c3_digest.clone(),
        miner_c.to_string(),
        vec![],
        serde_json::json!({"id":"c4"}),
    );
    let _ = apply_pipeline(&mut ledger, &mut mempool, c4).await;

    assert!(!mempool.transactions.contains_key("tx-1"));
}

#[tokio::test]
async fn test_coinbase_rewards_recomputed_after_deep_reorg() {
    let mut ledger = BlockchainObject::with_reward(10);

    let miner_a = "miner-a";
    let miner_b = "miner-b";
    let genesis = ledger.latest_block().digest.clone();

    let a1 = blockchain_helpers::create_block_message_with_transactions(
        1,
        genesis,
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"a1"}),
    );
    let a1_digest = digest_of(&a1);
    let _ = ledger.add_message(msg(a1), None).await.unwrap();
    let a2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        ledger.latest_block().digest.clone(),
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"a2"}),
    );
    let _ = ledger.add_message(msg(a2), None).await.unwrap();
    let a3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        ledger.latest_block().digest.clone(),
        miner_a.to_string(),
        vec![],
        serde_json::json!({"id":"a3"}),
    );
    let _ = ledger.add_message(msg(a3), None).await.unwrap();

    let b2 = blockchain_helpers::create_block_message_with_transactions(
        2,
        a1_digest,
        miner_b.to_string(),
        vec![],
        serde_json::json!({"id":"b2"}),
    );
    let b2_digest = digest_of(&b2);
    let _ = ledger.add_message(msg(b2), None).await.unwrap();
    let b3 = blockchain_helpers::create_block_message_with_transactions(
        3,
        b2_digest,
        miner_b.to_string(),
        vec![],
        serde_json::json!({"id":"b3"}),
    );
    let b3_digest = digest_of(&b3);
    let _ = ledger.add_message(msg(b3), None).await.unwrap();
    let b4 = blockchain_helpers::create_block_message_with_transactions(
        4,
        b3_digest,
        miner_b.to_string(),
        vec![],
        serde_json::json!({"id":"b4"}),
    );
    let _ = ledger.add_message(msg(b4), None).await.unwrap();

    assert_eq!(ledger.balances.get(miner_a).copied().unwrap_or(0), 10);
    assert_eq!(ledger.balances.get(miner_b).copied().unwrap_or(0), 30);
}

#[tokio::test]
async fn test_get_state_digests_includes_non_canonical_tip() {
    let mut ledger = BlockchainObject::new();
    let genesis = ledger.latest_block().digest.clone();

    let canonical = blockchain_helpers::create_block_message_with_transactions(
        1,
        genesis.clone(),
        "miner-a".to_string(),
        vec![],
        serde_json::json!({"id":"canonical"}),
    );
    let canonical_digest = digest_of(&canonical);
    let _ = ledger.add_message(msg(canonical), None).await.unwrap();

    let side = blockchain_helpers::create_block_message_with_transactions(
        1,
        genesis,
        "miner-b".to_string(),
        vec![],
        serde_json::json!({"id":"side"}),
    );
    let side_digest = digest_of(&side);
    let _ = ledger.add_message(msg(side), None).await.unwrap();

    let digests = ledger.get_state_digests();
    assert!(digests.contains(&canonical_digest));
    assert!(digests.contains(&side_digest));
}

#[tokio::test]
async fn test_mempool_block_fallback_without_memento() {
    let mut mempool = MempoolObject::new();
    let tx = blockchain_helpers::create_simple_transfer_tx("tx-1", "alice", "bob", 1, 1);
    let tx_msg = msg(blockchain_helpers::create_tx_message(tx.clone()));
    let _ = mempool.add_message(tx_msg, None).await.unwrap();
    assert!(mempool.transactions.contains_key("tx-1"));

    let block_msg = blockchain_helpers::create_block_message_with_transactions(
        1,
        "abc".to_string(),
        "miner".to_string(),
        vec![tx],
        serde_json::json!({"id":"payload"}),
    );
    let _ = mempool.add_message(msg(block_msg), None).await.unwrap();
    assert!(!mempool.transactions.contains_key("tx-1"));
}
