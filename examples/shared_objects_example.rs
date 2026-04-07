//! Shared Objects Example
//!
//! Demonstrates creating a network of nodes with shared objects that sync via gossip.
//! Ported from the Python chaincraft example (example.py).
//!
//! Features:
//! - Create a multi-node network
//! - Connect nodes in a ring topology
//! - Create and propagate shared objects across nodes
//!
//! Run with: `cargo run --example shared_objects_example`

use chaincraft::{
    error::Result, network::PeerId, shared_object::ApplicationObject, storage::MemoryStorage,
    BalanceLedger, ChaincraftNode,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Create a network of nodes with shared objects.
async fn create_network(num_nodes: usize) -> Result<Vec<ChaincraftNode>> {
    let mut nodes = Vec::new();

    for _ in 0..num_nodes {
        let id = PeerId::new();
        let storage = Arc::new(MemoryStorage::new());
        let mut node = ChaincraftNode::new(id, storage);

        let ledger: Box<dyn ApplicationObject> = Box::new(BalanceLedger::new());
        node.add_shared_object(ledger).await?;
        node.start().await?;
        nodes.push(node);
    }

    Ok(nodes)
}

/// Connect nodes in a ring topology.
async fn connect_nodes(nodes: &mut [ChaincraftNode]) -> Result<()> {
    let num_nodes = nodes.len();
    for i in 0..num_nodes {
        let next = (i + 1) % num_nodes;
        let addr = format!("{}:{}", nodes[next].host(), nodes[next].port());
        nodes[i].connect_to_peer(&addr).await?;
    }
    Ok(())
}

/// Print network status for all nodes.
async fn print_network_status(nodes: &[ChaincraftNode], label: &str) {
    println!("{label}");
    for (i, node) in nodes.iter().enumerate() {
        let peers = node.peers();
        let obj_count = node.shared_object_count().await;
        println!(
            "  Node {} ({}:{}): peers={}, shared_objects={}",
            i,
            node.host(),
            node.port(),
            peers.len(),
            obj_count
        );
    }
    println!();
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("Chaincraft Shared Objects Example");
    println!("==================================\n");

    let num_nodes = 5;
    let mut nodes = create_network(num_nodes).await?;
    connect_nodes(&mut nodes).await?;

    sleep(Duration::from_secs(1)).await;

    println!("Initial network status:");
    print_network_status(&nodes, "Network created").await;

    for i in 0..3 {
        let node_idx = i % nodes.len();
        let value = (i + 1) as f64;
        let data = serde_json::json!({
            "action": "credit",
            "account": format!("user-{node_idx}"),
            "amount": value
        });
        nodes[node_idx]
            .create_shared_message_with_data(data)
            .await?;
        println!("Node {node_idx} credited user-{node_idx} with {value}");
        sleep(Duration::from_millis(500)).await;
    }

    sleep(Duration::from_secs(2)).await;

    println!("Final network status:");
    print_network_status(&nodes, "After creating objects").await;

    println!("Closing nodes...");
    for mut node in nodes {
        node.close().await?;
    }
    println!("Done.");

    Ok(())
}
