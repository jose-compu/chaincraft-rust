//! Tests for MerkelizedChain object and digest-based synchronization
//!
//! Mirrors Python's test_shared_object_updates.py structure.

use chaincraft::{
    clear_local_registry,
    network::PeerId,
    shared::MessageType,
    shared::SharedMessage,
    shared_object::{ApplicationObject, MerkelizedChain},
    storage::MemoryStorage,
    ChaincraftNode,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Create a network of nodes with MerkelizedChain objects attached
async fn create_network_ephemeral(num_nodes: usize) -> Vec<ChaincraftNode> {
    clear_local_registry();
    let mut nodes = Vec::new();

    for _ in 0..num_nodes {
        let id = PeerId::new();
        let storage = Arc::new(MemoryStorage::new());
        let mut node = ChaincraftNode::new(id, storage);

        node.set_port(0);
        node.disable_local_discovery();

        // Add a MerkelizedChain object to each node
        let chain: Box<dyn ApplicationObject> = Box::new(MerkelizedChain::new());
        node.add_shared_object(chain).await.unwrap();

        node.start().await.unwrap();
        nodes.push(node);
    }

    nodes
}

/// Connect nodes in a fully connected mesh
async fn connect_nodes(nodes: &mut [ChaincraftNode]) {
    let num_nodes = nodes.len();
    for i in 0..num_nodes {
        for j in 0..num_nodes {
            if i == j {
                continue;
            }
            let addr = format!("{}:{}", nodes[j].host(), nodes[j].port());
            let _ = nodes[i].connect_to_peer(&addr).await;
        }
    }
}

/// Wait for all nodes to reach at least min_length and match each other
async fn wait_for_chain_sync(
    nodes: &[ChaincraftNode],
    min_length: usize,
    timeout_secs: u64,
) -> bool {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout {
        let mut lengths = Vec::new();
        for node in nodes {
            let mut shared_objects = node.shared_objects().await;
            if let Some(mut obj) = shared_objects.pop() {
                let obj_any = obj.as_any_mut();
                if let Some(chain) = obj_any.downcast_mut::<MerkelizedChain>() {
                    lengths.push(chain.chain_length());
                }
            }
        }
        let all_at_min = lengths.iter().all(|&l| l >= min_length);
        let all_same = lengths.windows(2).all(|w| w[0] == w[1]);
        if all_at_min && all_same && !lengths.is_empty() {
            return true;
        }
        sleep(Duration::from_millis(500)).await;
    }

    false
}

/// Add a hash to the chain on a node and broadcast it to the network
async fn add_hash_and_broadcast(node: &mut ChaincraftNode, hash: &str) {
    // Create a message with the hash that will be broadcast
    let data = serde_json::json!(hash);
    node.create_shared_message_with_data(data).await.unwrap();
}

#[tokio::test]
async fn test_merkelized_chain_basic() {
    // Create a single node with MerkelizedChain
    let id = PeerId::new();
    let storage = Arc::new(MemoryStorage::new());
    let mut node = ChaincraftNode::new(id, storage);
    node.set_port(0);

    let chain: Box<dyn ApplicationObject> = Box::new(MerkelizedChain::new());
    let _chain_id = node.add_shared_object(chain).await.unwrap();

    node.start().await.unwrap();

    // Verify initial chain state by getting mutable access
    let mut shared_objects = node.shared_objects().await;
    let chain = shared_objects
        .first_mut()
        .unwrap()
        .as_any_mut()
        .downcast_mut::<MerkelizedChain>()
        .unwrap();

    assert_eq!(chain.chain_length(), 1); // Only genesis
    assert!(!chain.genesis_hash().is_empty());

    // Add a few hashes
    chain.add_next_hash();
    chain.add_next_hash();
    chain.add_next_hash();

    assert_eq!(chain.chain_length(), 4); // Genesis + 3

    node.close().await.unwrap();
}

#[tokio::test]
async fn test_shared_object_chain_propagation() {
    // Create 5 nodes with MerkelizedChain using ephemeral ports
    let num_nodes = 5;
    let mut nodes = create_network_ephemeral(num_nodes).await;

    // Fully connect the network
    connect_nodes(&mut nodes).await;

    // Wait for connections to establish
    sleep(Duration::from_secs(2)).await;

    // Get the current chain tip from node 0
    let next_hash = {
        let mut shared_objects = nodes[0].shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();
        MerkelizedChain::calculate_next_hash(chain.latest_hash())
    };

    // Broadcast the first hash
    add_hash_and_broadcast(&mut nodes[0], &next_hash).await;
    sleep(Duration::from_millis(100)).await;

    // Calculate and broadcast second hash
    let next_hash2 = {
        let mut shared_objects = nodes[0].shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();
        MerkelizedChain::calculate_next_hash(chain.latest_hash())
    };
    add_hash_and_broadcast(&mut nodes[0], &next_hash2).await;
    sleep(Duration::from_millis(100)).await;

    // Calculate and broadcast third hash
    let next_hash3 = {
        let mut shared_objects = nodes[0].shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();
        MerkelizedChain::calculate_next_hash(chain.latest_hash())
    };
    add_hash_and_broadcast(&mut nodes[0], &next_hash3).await;

    // Expected chain length = genesis + 3 broadcasts = 4
    let expected_length = 4;

    // Wait for all nodes to sync
    assert!(
        wait_for_chain_sync(&nodes, expected_length, 30).await,
        "Chain did not sync to expected length {expected_length} within timeout"
    );

    // Verify all nodes have the same chain
    let first_chain = {
        let mut shared_objects = nodes[0].shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();
        chain.chain().to_vec()
    };

    for (i, node) in nodes.iter().enumerate() {
        let mut shared_objects = node.shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();

        assert_eq!(chain.chain_length(), expected_length, "Node {i} has incorrect chain length");

        // Verify chain prefix matches
        let node_chain: Vec<String> = chain
            .chain()
            .iter()
            .take(expected_length)
            .cloned()
            .collect();
        assert_eq!(node_chain, first_chain, "Node {i} chain doesn't match expected prefix");
    }

    println!("All {num_nodes} nodes synced to chain length {expected_length}!");

    // Cleanup
    for mut node in nodes {
        node.close().await.unwrap();
    }
}

#[tokio::test]
async fn test_digest_based_sync() {
    // Test digest-based gossip and sync
    let num_nodes = 3;
    let mut nodes = create_network_ephemeral(num_nodes).await;
    connect_nodes(&mut nodes).await;

    sleep(Duration::from_secs(2)).await;

    // Add a couple of hashes
    let hash1 = {
        let mut shared_objects = nodes[0].shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();
        MerkelizedChain::calculate_next_hash(chain.latest_hash())
    };
    add_hash_and_broadcast(&mut nodes[0], &hash1).await;
    sleep(Duration::from_millis(100)).await;

    let hash2 = {
        let mut shared_objects = nodes[0].shared_objects().await;
        let chain = shared_objects
            .first_mut()
            .unwrap()
            .as_any_mut()
            .downcast_mut::<MerkelizedChain>()
            .unwrap();
        MerkelizedChain::calculate_next_hash(chain.latest_hash())
    };
    add_hash_and_broadcast(&mut nodes[0], &hash2).await;

    assert!(wait_for_chain_sync(&nodes, 3, 30).await, "Chain did not sync to length >= 3");

    // Test gossip_messages functionality
    let mut shared_objects = nodes[0].shared_objects().await;
    let obj = shared_objects.first_mut().unwrap();
    let genesis = {
        let any = obj.as_any_mut();
        let chain = any.downcast_mut::<MerkelizedChain>().unwrap();
        chain.genesis_hash().to_string()
    };
    let messages = obj.gossip_messages(Some(&genesis)).await.unwrap();

    assert!(!messages.is_empty(), "Should have messages after genesis");
    println!("Got {} messages since genesis", messages.len());

    // Cleanup
    for mut node in nodes {
        node.close().await.unwrap();
    }
}

#[tokio::test]
async fn test_chain_hash_validation() {
    // Test that only valid next hashes are accepted
    let id = PeerId::new();
    let storage = Arc::new(MemoryStorage::new());
    let mut node = ChaincraftNode::new(id, storage);
    node.set_port(0);

    let chain: Box<dyn ApplicationObject> = Box::new(MerkelizedChain::new());
    let _chain_id = node.add_shared_object(chain).await.unwrap();
    node.start().await.unwrap();

    let mut shared_objects = node.shared_objects().await;
    let chain = shared_objects
        .first_mut()
        .unwrap()
        .as_any_mut()
        .downcast_mut::<MerkelizedChain>()
        .unwrap();

    // Calculate a valid next hash
    let next_hash = MerkelizedChain::calculate_next_hash(chain.latest_hash());

    // Verify is_valid accepts it
    let valid_msg = SharedMessage::new(
        MessageType::Custom("chain_update".to_string()),
        serde_json::json!(next_hash),
    );

    assert!(chain.is_valid(&valid_msg).await.unwrap(), "Should accept valid next hash");

    // Verify invalid hash is rejected
    let invalid_hash = "invalid_hash_not_following_chain";
    let invalid_msg = SharedMessage::new(
        MessageType::Custom("chain_update".to_string()),
        serde_json::json!(invalid_hash),
    );

    assert!(!chain.is_valid(&invalid_msg).await.unwrap(), "Should reject invalid hash");

    node.close().await.unwrap();
}

#[tokio::test]
async fn test_deduplication() {
    // Test that duplicate hashes are properly handled
    let id = PeerId::new();
    let storage = Arc::new(MemoryStorage::new());
    let mut node = ChaincraftNode::new(id, storage);
    node.set_port(0);

    let chain: Box<dyn ApplicationObject> = Box::new(MerkelizedChain::new());
    let _chain_id = node.add_shared_object(chain).await.unwrap();
    node.start().await.unwrap();

    let mut shared_objects = node.shared_objects().await;
    let chain = shared_objects
        .first_mut()
        .unwrap()
        .as_any_mut()
        .downcast_mut::<MerkelizedChain>()
        .unwrap();

    // Add a hash
    chain.add_next_hash();
    let len_before = chain.chain_length();

    // Try to add the same hash again (should be deduplicated)
    let existing_hash = chain.latest_hash().to_string();
    let dup_msg = SharedMessage::new(
        MessageType::Custom("chain_update".to_string()),
        serde_json::json!(existing_hash),
    );

    chain.add_message(dup_msg.clone(), None).await.unwrap();

    // Length should not increase
    assert_eq!(
        chain.chain_length(),
        len_before,
        "Duplicate hash should not increase chain length"
    );

    node.close().await.unwrap();
}
