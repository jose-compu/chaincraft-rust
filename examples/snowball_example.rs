//! Snowball Consensus Example
//!
//! Port of the Python `examples/snowball_protocol.py` into Rust-style
//! Chaincraft usage with gossip-based vote messages.
//!
//! Run with: `cargo run --example snowball_example`

use chaincraft::{
    clear_local_registry,
    error::Result,
    examples::snowball::{Color, SnowballNode},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    clear_local_registry();

    let num_nodes: usize = 10;
    let k: usize = 4;
    let alpha: f64 = 0.5;
    let beta: u32 = 8;
    let base_port: u16 = 9600;
    let initial_color = Color::Red;

    println!("Snowball Consensus Example");
    println!("==========================");
    println!("Nodes: {num_nodes}, k: {k}, alpha: {alpha}, beta: {beta}");
    println!("Proposer (node-0) initial color: {initial_color}\n");

    let mut nodes: Vec<SnowballNode> = Vec::new();
    for i in 0..num_nodes {
        let node_id = format!("node-{i}");
        let mut node = SnowballNode::new(node_id, base_port + i as u16, k, alpha, beta).await?;
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

    let max_rounds: u32 = 35;
    for round in 1..=max_rounds {
        for node in &mut nodes {
            let pref = node.preference().await?.unwrap_or(initial_color);
            node.publish_vote(round, pref).await?;
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

    println!("\nFinal Snowball state:");
    for (i, node) in nodes.iter_mut().enumerate() {
        let (pref, accepted, conf_r, conf_b, cnt) = node.state_snapshot().await?;
        println!(
            "  node-{i}: pref={}, accepted={}, conf(R/B)=({}/{}), cnt={}",
            pref.map(|c| c.to_string()).unwrap_or("?".into()),
            accepted.map(|c| c.to_string()).unwrap_or("?".into()),
            conf_r,
            conf_b,
            cnt,
        );
    }

    for mut node in nodes {
        node.close().await?;
    }

    Ok(())
}
