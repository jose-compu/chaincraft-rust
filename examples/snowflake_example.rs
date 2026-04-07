//! Snowflake Consensus Example
//!
//! Port of the Python `examples/snowflake_protocol.py` into Rust-style
//! Chaincraft usage with gossip-based vote messages.
//!
//! Run with: `cargo run --example snowflake_example`

use chaincraft::{
    clear_local_registry,
    error::Result,
    examples::snowflake::{Color, SnowflakeNode},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    clear_local_registry();

    let num_nodes: usize = 10;
    let k: usize = 4;
    let alpha: f64 = 0.5;
    let beta: u32 = 5;
    let base_port: u16 = 9500;
    let initial_color = Color::Red;

    println!("Snowflake Consensus Example");
    println!("===========================");
    println!("Nodes: {num_nodes}, k: {k}, alpha: {alpha}, beta: {beta}");
    println!("Proposer (node-0) initial color: {initial_color}\n");

    let mut nodes: Vec<SnowflakeNode> = Vec::new();
    for i in 0..num_nodes {
        let node_id = format!("node-{i}");
        let mut node = SnowflakeNode::new(node_id, base_port + i as u16, k, alpha, beta).await?;
        node.start().await?;
        nodes.push(node);
    }

    for i in 0..num_nodes {
        for j in 0..num_nodes {
            if i != j {
                let addr = format!("{}:{}", nodes[j].host(), nodes[j].port());
                nodes[i].connect_to_peer(&addr).await?;
            }
        }
    }
    sleep(Duration::from_millis(500)).await;

    nodes[0].publish_vote(0, initial_color).await?;
    println!("[node-0] Proposed {initial_color}");

    let max_rounds: u32 = 25;
    for round in 1..=max_rounds {
        for node in &mut nodes {
            let color = node.color().await?.unwrap_or(initial_color);
            node.publish_vote(round, color).await?;
        }

        sleep(Duration::from_millis(250)).await;

        for node in &mut nodes {
            node.process_round(round).await?;
        }
        let mut all_done = true;
        for node in &nodes {
            if node.accepted().await?.is_none() {
                all_done = false;
                break;
            }
        }
        if all_done {
            println!("All nodes accepted by round {round}");
            break;
        }
    }

    println!("\nFinal Snowflake state:");
    for (i, node) in nodes.iter_mut().enumerate() {
        let (color, accepted, count) = node.state_snapshot().await?;
        println!(
            "  node-{i}: color={}, accepted={}, cnt={}",
            color.map(|c| c.to_string()).unwrap_or("?".into()),
            accepted.map(|c| c.to_string()).unwrap_or("?".into()),
            count,
        );
    }

    for mut node in nodes {
        node.close().await?;
    }

    Ok(())
}
