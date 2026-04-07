//! Slush Consensus Example
//!
//! Demonstrates the Slush metastable consensus protocol from the Avalanche paper.
//! 10 ChaincraftNode instances reach agreement on a binary color (Red/Blue)
//! by broadcasting votes and adopting the majority each round.
//!
//! Run with: `cargo run --example slush_example`

use chaincraft::{
    clear_local_registry,
    error::Result,
    examples::slush::{Color, SlushNode},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    clear_local_registry();

    let num_nodes: usize = 10;
    let k: usize = 4;
    let alpha: f64 = 0.5;
    let m: u32 = 8;
    let base_port: u16 = 9400;
    let initial_color = Color::Red;

    println!("Slush Consensus Example (Avalanche paper)");
    println!("==========================================");
    println!("Nodes: {num_nodes}, k: {k}, alpha: {alpha}, rounds: {m}");
    println!("Proposer (node-0) initial color: {initial_color}\n");

    let mut nodes: Vec<SlushNode> = Vec::new();
    for i in 0..num_nodes {
        let node_id = format!("node-{i}");
        let mut node = SlushNode::new(node_id, base_port + i as u16, k, alpha, m).await?;
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
    sleep(Duration::from_millis(500)).await;

    // Run m rounds
    for round in 1..=m {
        // Each node broadcasts its current color
        for node in &mut nodes {
            let color = node.color().await?.unwrap_or(initial_color);
            node.publish_vote(round, color).await?;
        }

        sleep(Duration::from_millis(300)).await;

        // Each node processes votes and potentially flips
        for (i, node) in nodes.iter_mut().enumerate() {
            let flipped = node.process_round(round).await?;
            if flipped {
                let color = node.color().await?.unwrap_or(initial_color);
                println!("  [node-{i}] Round {round}: flipped to {color}");
            }
        }
    }

    // Finalize
    println!("\n=== Slush consensus complete ===");
    for (i, node) in nodes.iter_mut().enumerate() {
        let accepted = node.finalize().await?;
        println!("  node-{i}: accepted={}", accepted.map(|c| c.to_string()).unwrap_or("?".into()));
    }

    for mut node in nodes {
        node.close().await?;
    }

    Ok(())
}
