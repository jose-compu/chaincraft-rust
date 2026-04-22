//! Multi-node Randomness Beacon tests over real UDP/gossip layer

use chaincraft::{
    clear_local_registry,
    crypto::ecdsa::ECDSASigner,
    examples::randomness_beacon::{helpers, RandomnessBeaconObject},
    network::PeerId,
    shared_object::ApplicationObject,
    storage::MemoryStorage,
    ChaincraftNode,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn create_beacon_network(num_nodes: usize) -> Vec<ChaincraftNode> {
    let mut nodes = Vec::new();
    for _ in 0..num_nodes {
        let id = PeerId::new();
        let storage = Arc::new(MemoryStorage::new());
        let mut node = ChaincraftNode::new(id, storage);
        node.set_port(0);
        let beacon = RandomnessBeaconObject::new(60, num_nodes as u64).unwrap();
        let app_obj: Box<dyn ApplicationObject> = Box::new(beacon);
        node.add_shared_object(app_obj).await.unwrap();
        node.start().await.unwrap();
        nodes.push(node);
    }
    nodes
}

async fn connect_mesh(nodes: &mut [ChaincraftNode]) {
    let n = nodes.len();
    for i in 0..n {
        for j in 0..n {
            if i != j {
                let addr = format!("{}:{}", nodes[j].host(), nodes[j].port());
                let _ = nodes[i].connect_to_peer(&addr).await;
            }
        }
    }
}

async fn wait_for_sync(nodes: &[ChaincraftNode], min_msgs: usize, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        if nodes.iter().all(|n| n.db_size() >= min_msgs) {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn test_beacon_three_node_network() {
    clear_local_registry();
    let mut nodes = create_beacon_network(3).await;
    connect_mesh(&mut nodes).await;
    sleep(Duration::from_secs(2)).await;

    let signer = ECDSASigner::new().unwrap();
    let msg = helpers::create_validator_registration(
        signer.get_public_key_pem().unwrap(),
        signer.get_public_key_pem().unwrap(),
        "vrf_key_0".to_string(),
        100,
        &signer,
    )
    .unwrap();
    nodes[0].create_shared_message_with_data(msg).await.unwrap();

    assert!(wait_for_sync(&nodes, 1, 10).await);
    for mut node in nodes {
        node.close().await.unwrap();
    }
}

#[tokio::test]
async fn test_beacon_multi_contribution_propagation() {
    clear_local_registry();
    let mut nodes = create_beacon_network(5).await;
    connect_mesh(&mut nodes).await;
    sleep(Duration::from_secs(2)).await;

    for (i, node) in nodes.iter_mut().enumerate() {
        let signer = ECDSASigner::new().unwrap();
        let msg = helpers::create_validator_registration(
            signer.get_public_key_pem().unwrap(),
            signer.get_public_key_pem().unwrap(),
            format!("vrf_key_{i}"),
            100,
            &signer,
        )
        .unwrap();
        node.create_shared_message_with_data(msg).await.unwrap();
        sleep(Duration::from_millis(100)).await;
    }

    assert!(wait_for_sync(&nodes, 5, 15).await);
    for mut node in nodes {
        node.close().await.unwrap();
    }
}
