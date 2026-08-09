//! Integration tests for the NAT traversal module.
//!
//! These tests exercise the NAT traversal feature end-to-end by wiring
//! `ChaincraftNode` instances together over loopback UDP sockets and verifying
//! that public-address discovery, UDP hole punching, and keep-alive all work
//! as expected.

use chaincraft::{
    clear_local_registry,
    nat_traversal::{HolePunchMessage, NatTraversalConfig, NatTraversalManager, NatType},
    network::PeerId,
    storage::MemoryStorage,
    ChaincraftNode,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::UdpSocket, time::sleep};

// ─── Helper ──────────────────────────────────────────────────────────────────

async fn create_node_with_nat() -> ChaincraftNode {
    clear_local_registry();
    let id = PeerId::new();
    let storage = Arc::new(MemoryStorage::new());
    let mut node = ChaincraftNode::new(id, storage);
    node.set_port(0);
    node.disable_local_discovery();
    // NAT traversal is on by default
    node
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn test_nat_type_all_variants() {
    let types = [
        NatType::Open,
        NatType::FullCone,
        NatType::RestrictedCone,
        NatType::PortRestrictedCone,
        NatType::Symmetric,
        NatType::Unknown,
    ];
    for t in &types {
        // Roundtrip through JSON
        let json = serde_json::to_string(t).unwrap();
        let back: NatType = serde_json::from_str(&json).unwrap();
        assert_eq!(*t, back);
    }
}

#[test]
fn test_all_hole_punch_message_variants_serialize() {
    let local: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let remote: SocketAddr = "1.2.3.4:5678".parse().unwrap();

    let messages = vec![
        HolePunchMessage::DiscoverRequest { nonce: 1 },
        HolePunchMessage::DiscoverResponse { nonce: 1, observed_addr: remote },
        HolePunchMessage::CoordinateHolePunch {
            peer_a_addr: local,
            peer_b_addr: remote,
            session_id: 42,
        },
        HolePunchMessage::HolePunchProbe { session_id: 42, sender_addr: local },
        HolePunchMessage::HolePunchAck { session_id: 42, sender_addr: local },
        HolePunchMessage::KeepAlive { sender_addr: local, timestamp: 0 },
    ];

    for msg in &messages {
        let bytes = serde_json::to_vec(msg).unwrap();
        let _back: HolePunchMessage = serde_json::from_slice(&bytes)
            .expect("failed to deserialize HolePunchMessage");
    }
}

#[tokio::test]
async fn test_nat_traversal_manager_on_node_after_start() {
    let mut node = create_node_with_nat().await;
    node.start().await.unwrap();

    // After start, the NAT traversal manager should be initialized (because
    // nat_traversal.enabled == true by default).
    assert!(
        node.nat_traversal_manager().is_some(),
        "Expected NAT traversal manager to be initialized after node start"
    );

    node.close().await.unwrap();
}

#[tokio::test]
async fn test_nat_traversal_disabled_on_node() {
    clear_local_registry();
    let id = PeerId::new();
    let storage = Arc::new(MemoryStorage::new());
    let mut node = ChaincraftNode::builder()
        .with_id(id)
        .with_storage(storage)
        .port(0)
        .local_discovery(false)
        .nat_traversal(false)
        .build()
        .unwrap();

    node.start().await.unwrap();

    // With NAT traversal disabled, the manager should NOT be initialized.
    assert!(
        node.nat_traversal_manager().is_none(),
        "Expected NAT traversal manager to be absent when disabled"
    );

    node.close().await.unwrap();
}

#[tokio::test]
async fn test_node_builder_nat_traversal_config() {
    let custom_config = NatTraversalConfig {
        discovery_timeout: Duration::from_secs(3),
        hole_punch_timeout: Duration::from_secs(7),
        keep_alive_interval: Duration::from_secs(20),
        hole_punch_attempts: 2,
        enabled: true,
    };

    clear_local_registry();
    let node = ChaincraftNode::builder()
        .port(0)
        .local_discovery(false)
        .with_nat_traversal_config(custom_config.clone())
        .build()
        .unwrap();

    assert_eq!(node.config.nat_traversal.hole_punch_attempts, 2);
    assert_eq!(
        node.config.nat_traversal.keep_alive_interval,
        Duration::from_secs(20)
    );
}

/// End-to-end: a reflector node serves a DiscoverRequest sent by a prober, and
/// the prober learns its public address.
#[tokio::test]
async fn test_public_addr_discovery_via_loopback() {
    // Reflector side: plain UDP socket + NatTraversalManager acting as reflector
    let reflector_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let reflector_addr = reflector_sock.local_addr().unwrap();

    let reflector_mgr = Arc::new(NatTraversalManager::new(
        reflector_addr,
        NatTraversalConfig {
            discovery_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    ));

    let rsock = reflector_sock.clone();
    let rmgr = reflector_mgr.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 512];
        if let Ok((len, src)) = rsock.recv_from(&mut buf).await {
            if let Ok(msg) = serde_json::from_slice::<HolePunchMessage>(&buf[..len]) {
                if let Some(resp) = rmgr.handle_message(msg, src).await {
                    let _ = rsock
                        .send_to(&serde_json::to_vec(&resp).unwrap(), src)
                        .await;
                }
            }
        }
    });

    // Prober side
    let prober_mgr = NatTraversalManager::new(
        "127.0.0.1:0".parse().unwrap(),
        NatTraversalConfig {
            discovery_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    );

    let discovered = prober_mgr
        .discover_public_addr(reflector_addr)
        .await
        .unwrap();

    assert_eq!(discovered.ip().to_string(), "127.0.0.1");
}

/// End-to-end: two peers perform a complete hole-punch handshake over loopback.
#[tokio::test]
async fn test_hole_punch_end_to_end() {
    let sock_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let sock_b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr_a = sock_a.local_addr().unwrap();
    let addr_b = sock_b.local_addr().unwrap();

    let config = NatTraversalConfig {
        hole_punch_timeout: Duration::from_secs(3),
        hole_punch_attempts: 3,
        ..Default::default()
    };

    let mgr_a = Arc::new(NatTraversalManager::new(addr_a, config.clone()));
    let mgr_b = Arc::new(NatTraversalManager::new(addr_b, config));

    // Peer B background listener
    let sb = sock_b.clone();
    let mb = mgr_b.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 512];
        loop {
            if let Ok((len, src)) = sb.recv_from(&mut buf).await {
                if let Ok(msg) = serde_json::from_slice::<HolePunchMessage>(&buf[..len]) {
                    if let Some(resp) = mb.handle_message(msg, src).await {
                        let _ = sb.send_to(&serde_json::to_vec(&resp).unwrap(), src).await;
                    }
                }
            }
        }
    });

    // Peer A initiates punch
    let session_id = mgr_a.initiate_hole_punch(&sock_a, addr_b).await.unwrap();

    // Session should be confirmed
    let sessions = mgr_a.get_sessions().await;
    let session = sessions
        .iter()
        .find(|s| s.session_id == session_id)
        .expect("session not found");
    assert!(session.confirmed, "expected confirmed session");
    assert_eq!(session.remote_addr, addr_b);
}

/// Keep-alive packets should arrive at the destination within a short window.
#[tokio::test]
async fn test_keep_alive_reaches_destination() {
    let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    let sender_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sender_addr = sender_sock.local_addr().unwrap();

    let mgr = NatTraversalManager::new(
        sender_addr,
        NatTraversalConfig { enabled: true, ..Default::default() },
    );
    mgr.add_keep_alive_peer(receiver_addr).await;
    mgr.send_keep_alive(&sender_sock).await.unwrap();

    let mut buf = vec![0u8; 512];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), receiver.recv_from(&mut buf))
        .await
        .expect("timed out")
        .expect("recv error");

    let msg: HolePunchMessage = serde_json::from_slice(&buf[..len]).unwrap();
    assert!(matches!(msg, HolePunchMessage::KeepAlive { .. }));
}

/// Verify that NAT traversal messages arriving on a running node's socket are
/// handled transparently (the node should not surface them as application
/// messages).
#[tokio::test]
async fn test_node_handles_nat_messages_transparently() {
    let mut node = create_node_with_nat().await;
    node.start().await.unwrap();

    let node_port = node.port();
    let node_addr: SocketAddr = format!("127.0.0.1:{node_port}").parse().unwrap();

    // Send a DiscoverRequest to the node and wait for a DiscoverResponse.
    let probe_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let nonce: u64 = 0x1234_5678;
    let req = HolePunchMessage::DiscoverRequest { nonce };
    probe_sock
        .send_to(&serde_json::to_vec(&req).unwrap(), node_addr)
        .await
        .unwrap();

    let mut buf = vec![0u8; 512];
    let recv_result = tokio::time::timeout(
        Duration::from_secs(3),
        probe_sock.recv_from(&mut buf),
    )
    .await;

    let (len, _) = recv_result
        .expect("timed out waiting for DiscoverResponse")
        .expect("recv_from failed");

    let resp: HolePunchMessage = serde_json::from_slice(&buf[..len]).unwrap();
    if let HolePunchMessage::DiscoverResponse { nonce: resp_nonce, .. } = resp {
        assert_eq!(resp_nonce, nonce);
    } else {
        panic!("expected DiscoverResponse, got: {:?}", resp);
    }

    // The node's own peer list should still be empty (NAT msg was not treated
    // as an application message).
    let peers = node.get_peers().await;
    assert_eq!(peers.len(), 0);

    node.close().await.unwrap();
}

/// Two nodes can still communicate normally when NAT traversal is enabled on
/// both sides.
#[tokio::test]
async fn test_two_nodes_with_nat_traversal_enabled() {
    let mut node1 = create_node_with_nat().await;
    let mut node2 = create_node_with_nat().await;

    node1.start().await.unwrap();
    node2.start().await.unwrap();

    // Connect node2 to node1
    let node1_addr = format!("{}:{}", node1.host(), node1.port());
    node2.connect_to_peer(&node1_addr).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    let node2_peers = node2.get_peers().await;
    assert!(!node2_peers.is_empty(), "node2 should know about node1");

    node1.close().await.unwrap();
    node2.close().await.unwrap();
}

/// set_nat_traversal_enabled works before node start.
#[tokio::test]
async fn test_set_nat_traversal_enabled_before_start() {
    let mut node = create_node_with_nat().await;
    node.set_nat_traversal_enabled(false);
    node.start().await.unwrap();
    assert!(node.nat_traversal_manager().is_none());
    node.close().await.unwrap();
}
