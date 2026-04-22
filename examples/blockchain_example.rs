//! Blockchain Example (Memento v2 pipeline)
//!
//! Run with: `cargo run --example blockchain_example`

use chaincraft::{blockchain_helpers as helpers, error::Result, BlockchainNode};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("Chaincraft Blockchain Example");
    println!("=============================\n");

    let mut node = BlockchainNode::new(0).await?;
    node.start().await?;

    let tx = helpers::create_tx_message(serde_json::json!({
        "from": "alice",
        "to": "bob",
        "amount": 5
    }));
    node.publish(tx).await?;
    sleep(Duration::from_millis(100)).await;

    let state = node.chain_state().await?;
    let previous_digest = state
        .get("latest_digest")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let height = state
        .get("latest_height")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
        + 1;

    let block = helpers::create_block_message(
        height,
        previous_digest,
        serde_json::json!({"transactions": [{"from":"alice","to":"bob","amount":5}]}),
    );
    node.publish(block).await?;
    sleep(Duration::from_millis(150)).await;

    println!(
        "Blockchain state: {}",
        serde_json::to_string_pretty(&node.chain_state().await?).unwrap_or_default()
    );

    node.close().await?;
    Ok(())
}
