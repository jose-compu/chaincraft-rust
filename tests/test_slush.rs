//! Tests for Slush protocol (Avalanche paper Section 2.2).

use chaincraft::{
    clear_local_registry,
    examples::slush::{create_vote_message, Color, SlushObject},
    network::PeerId,
    shared::{MessageType, SharedMessage},
    shared_object::ApplicationObject,
    storage::MemoryStorage,
    ChaincraftNode,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_color_display() {
    assert_eq!(format!("{}", Color::Red), "R");
    assert_eq!(format!("{}", Color::Blue), "B");
}

#[tokio::test]
async fn test_slush_object_initial_state() {
    let slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    assert!(slush.color.is_none());
    assert!(slush.accepted.is_none());
    assert_eq!(slush.current_round, 0);
    assert_eq!(slush.votes().len(), 0);
}

#[tokio::test]
async fn test_slush_object_shared_object_stubs() {
    let slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    assert!(!slush.is_merkleized());
    assert!(slush.get_latest_digest().await.is_ok());
    assert!(!slush.has_digest("x").await.unwrap());
    assert_eq!(slush.gossip_messages(None).await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_slush_add_message_adopts_color() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    assert!(slush.color.is_none());

    let vote_data = create_vote_message("n2", 1, Color::Blue);
    let msg = SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), vote_data);
    assert!(slush.is_valid(&msg).await.unwrap());
    slush.add_message(msg, None).await.unwrap();

    assert_eq!(slush.color, Some(Color::Blue));
    assert_eq!(slush.votes().len(), 1);
}

#[tokio::test]
async fn test_slush_deduplication() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    let vote_data = create_vote_message("n2", 1, Color::Red);
    let msg = SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), vote_data);
    slush.add_message(msg.clone(), None).await.unwrap();
    slush.add_message(msg, None).await.unwrap();
    assert_eq!(slush.votes().len(), 1);
}

#[tokio::test]
async fn test_slush_count_votes() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    slush.color = Some(Color::Red);
    for i in 0..3 {
        let vote = create_vote_message(&format!("peer-{i}"), 1, Color::Blue);
        let msg = SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), vote);
        slush.add_message(msg, None).await.unwrap();
    }
    let vote = create_vote_message("peer-3", 1, Color::Red);
    let msg = SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), vote);
    slush.add_message(msg, None).await.unwrap();

    let (red, blue) = slush.count_votes_for_round(1);
    assert_eq!(red, 1);
    assert_eq!(blue, 3);
}

#[tokio::test]
async fn test_slush_process_round_flips() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    slush.color = Some(Color::Red);
    // 3 blue votes from peers => meets threshold alpha*k = 0.5*4 = 2
    for i in 0..3 {
        let vote = create_vote_message(&format!("peer-{i}"), 1, Color::Blue);
        let msg = SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), vote);
        slush.add_message(msg, None).await.unwrap();
    }
    let flipped = slush.process_round(1);
    assert!(flipped);
    assert_eq!(slush.color, Some(Color::Blue));
}

#[tokio::test]
async fn test_slush_process_round_no_flip() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    slush.color = Some(Color::Red);
    // 1 blue, 1 red => no majority
    let v1 = create_vote_message("peer-0", 1, Color::Blue);
    let v2 = create_vote_message("peer-1", 1, Color::Red);
    slush
        .add_message(SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), v1), None)
        .await
        .unwrap();
    slush
        .add_message(SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), v2), None)
        .await
        .unwrap();
    let flipped = slush.process_round(1);
    assert!(!flipped);
    assert_eq!(slush.color, Some(Color::Red));
}

#[tokio::test]
async fn test_slush_finalize() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    slush.color = Some(Color::Blue);
    assert!(slush.accepted.is_none());
    slush.finalize();
    assert_eq!(slush.accepted, Some(Color::Blue));
}

#[tokio::test]
async fn test_slush_reset() {
    let mut slush = SlushObject::new("n1".into(), 4, 0.5, 8);
    slush.color = Some(Color::Red);
    slush.accepted = Some(Color::Red);
    slush.current_round = 5;
    let vote = create_vote_message("peer-0", 1, Color::Red);
    slush
        .add_message(SharedMessage::new(MessageType::Custom("SLUSH_VOTE".into()), vote), None)
        .await
        .unwrap();
    slush.reset().await.unwrap();
    assert!(slush.color.is_none());
    assert!(slush.accepted.is_none());
    assert_eq!(slush.current_round, 0);
    assert_eq!(slush.votes().len(), 0);
}

// ---------------------------------------------------------------------------
// Integration: multi-node consensus over real UDP
// ---------------------------------------------------------------------------

async fn run_slush_consensus(
    num_nodes: usize,
    k: usize,
    alpha: f64,
    m: u32,
    initial_color: Color,
) -> Vec<Option<Color>> {
    clear_local_registry();

    let mut nodes: Vec<ChaincraftNode> = Vec::new();
    for i in 0..num_nodes {
        let id = PeerId::new();
        let storage = Arc::new(MemoryStorage::new());
        let mut node = ChaincraftNode::new(id, storage);
        node.set_port(0); // ephemeral port avoids conflicts between parallel tests
        node.disable_local_discovery();
        let slush: Box<dyn ApplicationObject> =
            Box::new(SlushObject::new(format!("node-{i}"), k, alpha, m));
        node.add_shared_object(slush).await.unwrap();
        node.start().await.unwrap();
        nodes.push(node);
    }

    // Fully connect all nodes
    for i in 0..num_nodes {
        for j in 0..num_nodes {
            if i != j {
                let addr = format!("{}:{}", nodes[j].host(), nodes[j].port());
                nodes[i].connect_to_peer(&addr).await.unwrap();
            }
        }
    }
    sleep(Duration::from_millis(500)).await;

    // Proposer broadcasts round 0
    let vote = create_vote_message("node-0", 0, initial_color);
    nodes[0]
        .create_shared_message_with_data(vote)
        .await
        .unwrap();
    sleep(Duration::from_millis(600)).await;

    // Run m rounds
    for round in 1..=m {
        for (i, node) in nodes.iter_mut().enumerate() {
            let color = {
                let objs = node.shared_objects().await;
                objs.first()
                    .and_then(|o| o.as_any().downcast_ref::<SlushObject>())
                    .and_then(|s| s.color)
                    .unwrap_or(initial_color)
            };
            let vote = create_vote_message(&format!("node-{i}"), round, color);
            node.create_shared_message_with_data(vote).await.unwrap();
        }
        sleep(Duration::from_millis(600)).await;

        for node in nodes.iter_mut() {
            let mut registry = node.app_objects.write().await;
            let ids = registry.ids();
            for id in ids {
                if let Some(obj) = registry.objects.get_mut(&id) {
                    if let Some(slush) = obj.as_any_mut().downcast_mut::<SlushObject>() {
                        slush.process_round(round);
                        slush.current_round = round;
                    }
                }
            }
        }
    }

    // Collect results
    let mut results = Vec::new();
    for node in nodes.iter_mut() {
        let mut registry = node.app_objects.write().await;
        let ids = registry.ids();
        let mut accepted = None;
        for id in ids {
            if let Some(obj) = registry.objects.get_mut(&id) {
                if let Some(slush) = obj.as_any_mut().downcast_mut::<SlushObject>() {
                    slush.finalize();
                    accepted = slush.accepted;
                }
            }
        }
        results.push(accepted);
    }

    for mut node in nodes {
        node.close().await.unwrap();
    }

    results
}

#[tokio::test]
async fn test_slush_5_nodes_red() {
    let results = run_slush_consensus(5, 3, 0.5, 6, Color::Red).await;
    assert_eq!(results.len(), 5);
    for (i, c) in results.iter().enumerate() {
        assert_eq!(*c, Some(Color::Red), "node-{i} did not accept Red");
    }
}

#[tokio::test]
async fn test_slush_5_nodes_blue() {
    let results = run_slush_consensus(5, 3, 0.5, 6, Color::Blue).await;
    assert_eq!(results.len(), 5);
    for (i, c) in results.iter().enumerate() {
        assert_eq!(*c, Some(Color::Blue), "node-{i} did not accept Blue");
    }
}

#[tokio::test]
async fn test_slush_10_nodes() {
    let results = run_slush_consensus(10, 4, 0.5, 8, Color::Red).await;
    assert_eq!(results.len(), 10);
    let decided: Vec<_> = results.iter().filter(|c| c.is_some()).collect();
    assert_eq!(decided.len(), 10, "All 10 nodes should decide");
    assert!(
        decided.iter().all(|c| **c == Some(Color::Red)),
        "All nodes should converge on Red"
    );
}
