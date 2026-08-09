//! Tests for local discovery (in-process peer registry)
//!
//! Mirrors Python's test_local_discovery behavior.
//!
//! These tests share the process-global LOCAL_NODES registry, so they must not
//! run concurrently with each other.

use chaincraft::{
    clear_local_registry, network::PeerId, shared_object::SimpleSharedNumber,
    storage::MemoryStorage, ApplicationObject, ChaincraftNode,
};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

fn local_discovery_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn create_network(num_nodes: usize) -> Vec<ChaincraftNode> {
    clear_local_registry();
    let mut nodes = Vec::new();
    for _ in 0..num_nodes {
        let id = PeerId::new();
        let storage = Arc::new(MemoryStorage::new());
        let mut node = ChaincraftNode::new(id, storage);
        node.set_port(0);
        let shared: Box<dyn ApplicationObject> = Box::new(SimpleSharedNumber::new());
        node.add_shared_object(shared).await.unwrap();
        node.start().await.unwrap();
        nodes.push(node);
    }
    nodes
}

async fn wait_for_peers(nodes: &[ChaincraftNode], min_peers: usize, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        let mut ok = true;
        for n in nodes {
            if n.get_peers().await.len() < min_peers {
                ok = false;
                break;
            }
        }
        if ok {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn test_local_discovery_three_nodes() {
    let _guard = local_discovery_lock().lock().await;

    let mut nodes = create_network(3).await;
    assert!(
        wait_for_peers(&nodes, 2, 10).await,
        "Local discovery should discover peers within 10s"
    );

    let mut peer_counts = Vec::new();
    for n in &nodes {
        peer_counts.push(n.get_peers().await.len());
    }
    assert!(
        peer_counts.iter().all(|&c| c >= 2),
        "Each node should discover at least 2 other nodes via local discovery; got {peer_counts:?}"
    );

    nodes[0]
        .create_shared_message_with_data(serde_json::json!(42))
        .await
        .unwrap();
    sleep(Duration::from_secs(2)).await;

    let counts: Vec<usize> = nodes.iter().map(|n| n.db_size()).collect();
    assert!(
        counts.iter().all(|&c| c >= 1),
        "Message should propagate to all nodes; db_sizes: {counts:?}"
    );

    for mut node in nodes {
        node.close().await.unwrap();
    }
    clear_local_registry();
}

#[tokio::test]
async fn test_local_discovery_five_nodes() {
    let _guard = local_discovery_lock().lock().await;

    let nodes = create_network(5).await;
    assert!(
        wait_for_peers(&nodes, 4, 15).await,
        "Local discovery should discover peers within 15s"
    );

    let mut peer_counts = Vec::new();
    for n in &nodes {
        peer_counts.push(n.get_peers().await.len());
    }
    assert!(
        peer_counts.iter().all(|&c| c >= 4),
        "Each node should discover 4 other nodes; got {peer_counts:?}"
    );

    for mut node in nodes {
        node.close().await.unwrap();
    }
    clear_local_registry();
}
