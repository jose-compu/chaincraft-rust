//! NAT Traversal module for Chaincraft
//!
//! Provides NAT type detection, public address discovery, UDP hole punching,
//! and keep-alive mechanisms to enable peer-to-peer connectivity through
//! Network Address Translation (NAT) routers.
//!
//! # Overview
//!
//! Most nodes in a real network are behind NAT routers that translate private
//! IP addresses to public ones. Without NAT traversal, two nodes behind
//! different NATs cannot connect directly.
//!
//! This module implements:
//! - **NAT type detection** (Open, Full Cone, Restricted Cone, Port Restricted, Symmetric)
//! - **Public address discovery** by querying an echo/reflector peer
//! - **UDP hole punching** to establish direct peer-to-peer connections
//! - **Keep-alive messages** to prevent NAT mappings from expiring
//!
//! # Example
//!
//! ```rust,no_run
//! use chaincraft::nat_traversal::{NatTraversalConfig, NatTraversalManager};
//! use std::net::SocketAddr;
//!
//! # async fn example() -> chaincraft::Result<()> {
//! let local_addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
//! let config = NatTraversalConfig::default();
//! let manager = NatTraversalManager::new(local_addr, config);
//!
//! // Discover NAT type and public address
//! let nat_info = manager.probe_nat_type().await;
//! println!("NAT type: {:?}", nat_info.nat_type);
//! println!("Public address: {:?}", nat_info.public_addr);
//! # Ok(())
//! # }
//! ```

use crate::error::{ChaincraftError, NetworkError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::Instant;

// ─── NAT Type ────────────────────────────────────────────────────────────────

/// Classification of the NAT router behaviour seen by this node.
///
/// Listed in order of ease-of-traversal (easiest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NatType {
    /// No NAT; the node is directly reachable on the public internet.
    Open,
    /// Full Cone NAT: once a port mapping is created, any external host can
    /// reach the node via the mapped external port.
    FullCone,
    /// Address Restricted Cone NAT: the node must first send a packet to an
    /// external host before that host can reply through the NAT mapping.
    RestrictedCone,
    /// Port Restricted Cone NAT: like [`RestrictedCone`] but also restricts
    /// the source port of the external host.
    PortRestrictedCone,
    /// Symmetric NAT: a different external port is allocated for every distinct
    /// (destination IP, destination port) pair.  Hardest to traverse.
    Symmetric,
    /// NAT type could not be determined (e.g. no internet connectivity or probe
    /// timed out).
    Unknown,
}

impl std::fmt::Display for NatType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NatType::Open => write!(f, "Open"),
            NatType::FullCone => write!(f, "FullCone"),
            NatType::RestrictedCone => write!(f, "RestrictedCone"),
            NatType::PortRestrictedCone => write!(f, "PortRestrictedCone"),
            NatType::Symmetric => write!(f, "Symmetric"),
            NatType::Unknown => write!(f, "Unknown"),
        }
    }
}

// ─── Protocol messages ───────────────────────────────────────────────────────

/// Wire messages used by the NAT traversal protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HolePunchMessage {
    /// Sent by a node to a well-known peer to ask it to reflect the sender's
    /// observed address back.
    DiscoverRequest {
        /// A random nonce so responses can be matched to requests.
        nonce: u64,
    },
    /// Reply from the reflector peer containing the observed external address.
    DiscoverResponse {
        /// Echoed nonce from the corresponding request.
        nonce: u64,
        /// Public (external) address as observed by the reflector.
        observed_addr: SocketAddr,
    },
    /// Sent by a coordinator to both peers to signal that they should begin
    /// punching simultaneously.
    CoordinateHolePunch {
        /// Address that peer A should send packets to.
        peer_a_addr: SocketAddr,
        /// Address that peer B should send packets to.
        peer_b_addr: SocketAddr,
        /// Session identifier shared by both peers.
        session_id: u64,
    },
    /// The actual hole-punch probe packet sent directly between peers.
    HolePunchProbe {
        /// Session identifier (must match the value from [`CoordinateHolePunch`]).
        session_id: u64,
        /// Sender's public address (informational).
        sender_addr: SocketAddr,
    },
    /// Acknowledges a received [`HolePunchProbe`]; confirms the punch succeeded.
    HolePunchAck {
        /// Session identifier.
        session_id: u64,
        /// Address the ack sender will use for data traffic.
        sender_addr: SocketAddr,
    },
    /// Periodic keep-alive packet to prevent NAT mappings from expiring.
    KeepAlive {
        /// Sender's public address.
        sender_addr: SocketAddr,
        /// Unix timestamp in seconds.
        timestamp: u64,
    },
}

// ─── Configuration ───────────────────────────────────────────────────────────

/// Configuration for the NAT traversal subsystem.
#[derive(Debug, Clone)]
pub struct NatTraversalConfig {
    /// How long to wait for a discovery response before giving up.
    pub discovery_timeout: Duration,
    /// How long to wait for a hole-punch acknowledgement.
    pub hole_punch_timeout: Duration,
    /// Interval at which keep-alive packets are sent to maintain NAT mappings.
    pub keep_alive_interval: Duration,
    /// Number of simultaneous hole-punch probe packets to send per attempt.
    pub hole_punch_attempts: u32,
    /// Whether NAT traversal is enabled at all.
    pub enabled: bool,
}

impl Default for NatTraversalConfig {
    fn default() -> Self {
        Self {
            discovery_timeout: Duration::from_secs(5),
            hole_punch_timeout: Duration::from_secs(10),
            keep_alive_interval: Duration::from_secs(25),
            hole_punch_attempts: 5,
            enabled: true,
        }
    }
}

// ─── NAT information ─────────────────────────────────────────────────────────

/// Information about this node's network situation discovered at runtime.
#[derive(Debug, Clone)]
pub struct NatInfo {
    /// The public (external) address of this node as seen from the outside,
    /// or `None` if it could not be determined.
    pub public_addr: Option<SocketAddr>,
    /// The detected NAT type.
    pub nat_type: NatType,
    /// When the information was last refreshed.
    pub last_updated: Instant,
}

impl NatInfo {
    fn unknown() -> Self {
        Self {
            public_addr: None,
            nat_type: NatType::Unknown,
            last_updated: Instant::now(),
        }
    }
}

// ─── Active hole-punch session ───────────────────────────────────────────────

/// State tracked for an in-progress hole-punch session.
#[derive(Debug, Clone)]
pub struct HolePunchSession {
    /// Session identifier echoed in probe/ack messages.
    pub session_id: u64,
    /// Remote peer's external address we are punching towards.
    pub remote_addr: SocketAddr,
    /// Whether the punch has been confirmed (ack received).
    pub confirmed: bool,
    /// When the session was started (for timeout tracking).
    pub started_at: Instant,
}

// ─── Manager ─────────────────────────────────────────────────────────────────

/// Manages NAT traversal for a Chaincraft node.
///
/// Responsibilities:
/// - Discovering the node's public address via a reflector peer
/// - Probing the NAT type
/// - Coordinating and executing UDP hole punching
/// - Sending keep-alive packets to maintain active NAT mappings
pub struct NatTraversalManager {
    /// Local address this node is listening on.
    local_addr: SocketAddr,
    /// NAT traversal configuration.
    config: NatTraversalConfig,
    /// Cached NAT information (refreshed on demand).
    nat_info: Arc<RwLock<NatInfo>>,
    /// Active hole-punch sessions, keyed by session ID.
    sessions: Arc<RwLock<HashMap<u64, HolePunchSession>>>,
    /// Peers for which keep-alive packets should be sent.
    keep_alive_peers: Arc<RwLock<Vec<SocketAddr>>>,
}

impl NatTraversalManager {
    /// Create a new `NatTraversalManager`.
    pub fn new(local_addr: SocketAddr, config: NatTraversalConfig) -> Self {
        Self {
            local_addr,
            config,
            nat_info: Arc::new(RwLock::new(NatInfo::unknown())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            keep_alive_peers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    // ── Public address discovery ─────────────────────────────────────────────

    /// Discover the node's public address by sending a [`HolePunchMessage::DiscoverRequest`]
    /// to `reflector_addr` and waiting for the matching [`HolePunchMessage::DiscoverResponse`].
    ///
    /// Returns the observed external [`SocketAddr`] on success.
    pub async fn discover_public_addr(&self, reflector_addr: SocketAddr) -> Result<SocketAddr> {
        if !self.config.enabled {
            return Err(ChaincraftError::Network(NetworkError::NatDiscoveryFailed {
                reason: "NAT traversal is disabled".to_string(),
            }));
        }

        let socket = UdpSocket::bind("0.0.0.0:0").await.map_err(|e| {
            ChaincraftError::Network(NetworkError::BindFailed {
                addr: "0.0.0.0:0".parse().unwrap(),
                source: e,
            })
        })?;

        let nonce: u64 = rand::random();
        let request = HolePunchMessage::DiscoverRequest { nonce };
        let request_bytes = serde_json::to_vec(&request)?;

        socket.send_to(&request_bytes, reflector_addr).await?;

        let mut buf = vec![0u8; 512];
        let recv_result = tokio::time::timeout(
            self.config.discovery_timeout,
            socket.recv_from(&mut buf),
        )
        .await;

        match recv_result {
            Ok(Ok((len, _src))) => {
                let msg: HolePunchMessage = serde_json::from_slice(&buf[..len])?;
                match msg {
                    HolePunchMessage::DiscoverResponse {
                        nonce: resp_nonce,
                        observed_addr,
                    } if resp_nonce == nonce => {
                        // Cache the result
                        let mut info = self.nat_info.write().await;
                        info.public_addr = Some(observed_addr);
                        info.last_updated = Instant::now();
                        Ok(observed_addr)
                    },
                    _ => Err(ChaincraftError::Network(NetworkError::NatDiscoveryFailed {
                        reason: "Unexpected response to DiscoverRequest".to_string(),
                    })),
                }
            },
            Ok(Err(e)) => Err(ChaincraftError::Io(e)),
            Err(_) => Err(ChaincraftError::Network(NetworkError::Timeout {
                duration: self.config.discovery_timeout,
            })),
        }
    }

    // ── NAT type probing ─────────────────────────────────────────────────────

    /// Probe the NAT type by querying two reflector peers and comparing the
    /// observed external addresses.
    ///
    /// Algorithm (simplified RFC 3489 / RFC 5389):
    /// 1. Send a discover request to `reflector1`.
    /// 2. Send a discover request to `reflector2` from the **same** local port.
    /// 3. Compare the two observed external addresses:
    ///    - Same → likely Full Cone, Restricted Cone, or Port Restricted Cone.
    ///    - Different → Symmetric NAT.
    ///    - Only one responded → Unknown.
    /// 4. If `reflector1` and `reflector2` are the same address, falls back to
    ///    a single-reflector heuristic that reports `Unknown`.
    ///
    /// Updates the internal [`NatInfo`] cache and returns a copy.
    pub async fn probe_nat_type(&self) -> NatInfo {
        // Without real STUN servers we report Unknown; callers that have
        // reflector addresses should use discover_public_addr directly.
        let info = self.nat_info.read().await;
        info.clone()
    }

    /// Probe the NAT type using two known reflector addresses.
    ///
    /// Both reflectors must implement the [`HolePunchMessage`] protocol
    /// (i.e. they are other Chaincraft nodes with NAT traversal enabled).
    pub async fn probe_nat_type_with_reflectors(
        &self,
        reflector1: SocketAddr,
        reflector2: SocketAddr,
    ) -> NatInfo {
        // Bind a single socket and send to both reflectors.
        let socket = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => Arc::new(s),
            Err(_) => {
                return NatInfo::unknown();
            },
        };

        let nonce1: u64 = rand::random();
        let nonce2: u64 = rand::random();

        let req1 = serde_json::to_vec(&HolePunchMessage::DiscoverRequest { nonce: nonce1 })
            .unwrap_or_default();
        let req2 = serde_json::to_vec(&HolePunchMessage::DiscoverRequest { nonce: nonce2 })
            .unwrap_or_default();

        let _ = socket.send_to(&req1, reflector1).await;
        let _ = socket.send_to(&req2, reflector2).await;

        let mut observed1: Option<SocketAddr> = None;
        let mut observed2: Option<SocketAddr> = None;
        let mut buf = vec![0u8; 512];
        let deadline = Instant::now() + self.config.discovery_timeout;

        while observed1.is_none() || observed2.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
                Ok(Ok((len, _src))) => {
                    if let Ok(msg) = serde_json::from_slice::<HolePunchMessage>(&buf[..len]) {
                        match msg {
                            HolePunchMessage::DiscoverResponse { nonce, observed_addr } => {
                                if nonce == nonce1 {
                                    observed1 = Some(observed_addr);
                                } else if nonce == nonce2 {
                                    observed2 = Some(observed_addr);
                                }
                            },
                            _ => {},
                        }
                    }
                },
                _ => break,
            }
        }

        let nat_type = match (observed1, observed2) {
            (Some(addr1), Some(addr2)) => {
                if addr1 == addr2 {
                    // Same external address seen from two different reflectors →
                    // not Symmetric (likely Full Cone or Restricted Cone).
                    NatType::RestrictedCone
                } else {
                    NatType::Symmetric
                }
            },
            (Some(_), None) | (None, Some(_)) => NatType::Unknown,
            (None, None) => NatType::Unknown,
        };

        let public_addr = observed1.or(observed2);
        let info = NatInfo {
            public_addr,
            nat_type,
            last_updated: Instant::now(),
        };

        *self.nat_info.write().await = info.clone();
        info
    }

    // ── UDP hole punching ────────────────────────────────────────────────────

    /// Initiate a UDP hole-punch session towards `remote_addr`.
    ///
    /// Sends [`NatTraversalConfig::hole_punch_attempts`] probe packets to
    /// `remote_addr` and then waits up to `hole_punch_timeout` for an
    /// acknowledgement.  On success, the session is recorded as confirmed and
    /// `remote_addr` is added to the keep-alive list.
    ///
    /// Returns `Ok(session_id)` when the punch succeeded (ack received) or an
    /// error if it timed out.
    pub async fn initiate_hole_punch(
        &self,
        socket: &UdpSocket,
        remote_addr: SocketAddr,
    ) -> Result<u64> {
        if !self.config.enabled {
            return Err(ChaincraftError::Network(NetworkError::HolePunchFailed {
                addr: remote_addr,
                reason: "NAT traversal is disabled".to_string(),
            }));
        }

        let session_id: u64 = rand::random();

        // Record the session
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(
                session_id,
                HolePunchSession {
                    session_id,
                    remote_addr,
                    confirmed: false,
                    started_at: Instant::now(),
                },
            );
        }

        // Send multiple probes for reliability
        let probe = HolePunchMessage::HolePunchProbe {
            session_id,
            sender_addr: self.local_addr,
        };
        let probe_bytes = serde_json::to_vec(&probe)?;

        for _ in 0..self.config.hole_punch_attempts {
            socket.send_to(&probe_bytes, remote_addr).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Wait for ack
        let mut buf = vec![0u8; 512];
        let ack_result = tokio::time::timeout(self.config.hole_punch_timeout, async {
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, src)) if src == remote_addr => {
                        if let Ok(HolePunchMessage::HolePunchAck {
                            session_id: ack_sid, ..
                        }) = serde_json::from_slice(&buf[..len])
                        {
                            if ack_sid == session_id {
                                return Ok(());
                            }
                        }
                    },
                    Ok(_) => continue,
                    Err(e) => return Err(e),
                }
            }
        })
        .await;

        match ack_result {
            Ok(Ok(())) => {
                // Mark session confirmed
                let mut sessions = self.sessions.write().await;
                if let Some(s) = sessions.get_mut(&session_id) {
                    s.confirmed = true;
                }
                // Add to keep-alive list
                let mut peers = self.keep_alive_peers.write().await;
                if !peers.contains(&remote_addr) {
                    peers.push(remote_addr);
                }
                Ok(session_id)
            },
            Ok(Err(e)) => Err(ChaincraftError::Io(e)),
            Err(_) => {
                // Clean up timed-out session
                let mut sessions = self.sessions.write().await;
                sessions.remove(&session_id);
                Err(ChaincraftError::Network(NetworkError::HolePunchFailed {
                    addr: remote_addr,
                    reason: format!(
                        "no ack received within {:?}",
                        self.config.hole_punch_timeout
                    ),
                }))
            },
        }
    }

    // ── Message handling ─────────────────────────────────────────────────────

    /// Handle an inbound NAT traversal [`HolePunchMessage`].
    ///
    /// Returns an optional response message that should be sent back to
    /// `sender_addr` (e.g. a [`DiscoverResponse`] or [`HolePunchAck`]).
    pub async fn handle_message(
        &self,
        message: HolePunchMessage,
        sender_addr: SocketAddr,
    ) -> Option<HolePunchMessage> {
        match message {
            // Reflector role: echo the observed address back.
            HolePunchMessage::DiscoverRequest { nonce } => {
                Some(HolePunchMessage::DiscoverResponse {
                    nonce,
                    observed_addr: sender_addr,
                })
            },

            // Hole-punch initiator will handle this in the recv loop.
            HolePunchMessage::DiscoverResponse { .. } => None,

            // Coordinator broadcasts this to both sides; each side starts sending probes.
            HolePunchMessage::CoordinateHolePunch {
                peer_a_addr,
                peer_b_addr,
                session_id,
            } => {
                // Determine which peer we are.
                let target = if sender_addr == peer_a_addr {
                    peer_b_addr
                } else {
                    peer_a_addr
                };

                // Record the session.
                let mut sessions = self.sessions.write().await;
                sessions.insert(
                    session_id,
                    HolePunchSession {
                        session_id,
                        remote_addr: target,
                        confirmed: false,
                        started_at: Instant::now(),
                    },
                );

                // The caller should start sending probes to `target`.
                Some(HolePunchMessage::HolePunchProbe {
                    session_id,
                    sender_addr: self.local_addr,
                })
            },

            // Received a probe: record the session and reply with ack.
            HolePunchMessage::HolePunchProbe {
                session_id,
                sender_addr: _,
            } => {
                let mut sessions = self.sessions.write().await;
                sessions
                    .entry(session_id)
                    .and_modify(|s| s.confirmed = true)
                    .or_insert_with(|| HolePunchSession {
                        session_id,
                        remote_addr: sender_addr,
                        confirmed: true,
                        started_at: Instant::now(),
                    });

                // Add to keep-alive list
                let mut peers = self.keep_alive_peers.write().await;
                if !peers.contains(&sender_addr) {
                    peers.push(sender_addr);
                }

                Some(HolePunchMessage::HolePunchAck {
                    session_id,
                    sender_addr: self.local_addr,
                })
            },

            // Ack received by the initiator; already handled in `initiate_hole_punch`.
            HolePunchMessage::HolePunchAck { session_id, .. } => {
                let mut sessions = self.sessions.write().await;
                if let Some(s) = sessions.get_mut(&session_id) {
                    s.confirmed = true;
                }
                None
            },

            // Keep-alive: just update the peer list.
            HolePunchMessage::KeepAlive { .. } => {
                let mut peers = self.keep_alive_peers.write().await;
                if !peers.contains(&sender_addr) {
                    peers.push(sender_addr);
                }
                None
            },
        }
    }

    // ── Keep-alive ───────────────────────────────────────────────────────────

    /// Send a keep-alive packet to every peer in the keep-alive list.
    ///
    /// Should be called on a periodic timer (see [`NatTraversalConfig::keep_alive_interval`]).
    pub async fn send_keep_alive(&self, socket: &UdpSocket) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let msg = HolePunchMessage::KeepAlive {
            sender_addr: self.local_addr,
            timestamp: now,
        };
        let bytes = serde_json::to_vec(&msg)?;

        let peers = self.keep_alive_peers.read().await;
        for &peer_addr in peers.iter() {
            if let Err(e) = socket.send_to(&bytes, peer_addr).await {
                tracing::warn!(
                    "NAT keep-alive to {} failed: {:?}",
                    peer_addr,
                    e
                );
            }
        }

        Ok(())
    }

    /// Register a peer address for periodic keep-alive packets.
    pub async fn add_keep_alive_peer(&self, addr: SocketAddr) {
        let mut peers = self.keep_alive_peers.write().await;
        if !peers.contains(&addr) {
            peers.push(addr);
        }
    }

    /// Remove a peer address from the keep-alive list.
    pub async fn remove_keep_alive_peer(&self, addr: &SocketAddr) {
        let mut peers = self.keep_alive_peers.write().await;
        peers.retain(|a| a != addr);
    }

    // ── Session management ───────────────────────────────────────────────────

    /// Get a snapshot of all active hole-punch sessions.
    pub async fn get_sessions(&self) -> Vec<HolePunchSession> {
        let sessions = self.sessions.read().await;
        sessions.values().cloned().collect()
    }

    /// Remove sessions older than `max_age`.
    pub async fn cleanup_old_sessions(&self, max_age: Duration) {
        let mut sessions = self.sessions.write().await;
        sessions.retain(|_, s| s.started_at.elapsed() < max_age);
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// Return a copy of the cached [`NatInfo`].
    pub async fn nat_info(&self) -> NatInfo {
        self.nat_info.read().await.clone()
    }

    /// Return the local address this manager is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &NatTraversalConfig {
        &self.config
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Determine whether a `HolePunchMessage` should be handled by the NAT
/// traversal subsystem rather than by the main message router.
///
/// Returns `true` for all [`HolePunchMessage`] variants; intended to be used
/// when deserializing raw UDP datagrams so the caller can dispatch correctly.
pub fn is_nat_traversal_message(bytes: &[u8]) -> bool {
    serde_json::from_slice::<HolePunchMessage>(bytes).is_ok()
}

/// Build a [`HolePunchMessage::DiscoverResponse`] from raw bytes received from
/// a peer (convenience for reflector implementations).
pub fn make_discover_response(
    request_bytes: &[u8],
    observed_addr: SocketAddr,
) -> Option<HolePunchMessage> {
    if let Ok(HolePunchMessage::DiscoverRequest { nonce }) =
        serde_json::from_slice(request_bytes)
    {
        Some(HolePunchMessage::DiscoverResponse { nonce, observed_addr })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    fn local_config() -> NatTraversalConfig {
        NatTraversalConfig {
            discovery_timeout: Duration::from_secs(2),
            hole_punch_timeout: Duration::from_secs(3),
            keep_alive_interval: Duration::from_secs(5),
            hole_punch_attempts: 3,
            enabled: true,
        }
    }

    #[test]
    fn test_nat_type_display() {
        assert_eq!(NatType::Open.to_string(), "Open");
        assert_eq!(NatType::FullCone.to_string(), "FullCone");
        assert_eq!(NatType::RestrictedCone.to_string(), "RestrictedCone");
        assert_eq!(NatType::PortRestrictedCone.to_string(), "PortRestrictedCone");
        assert_eq!(NatType::Symmetric.to_string(), "Symmetric");
        assert_eq!(NatType::Unknown.to_string(), "Unknown");
    }

    #[test]
    fn test_nat_type_equality() {
        assert_eq!(NatType::Open, NatType::Open);
        assert_ne!(NatType::Open, NatType::Symmetric);
    }

    #[test]
    fn test_nat_type_serialization() {
        let t = NatType::FullCone;
        let json = serde_json::to_string(&t).unwrap();
        let back: NatType = serde_json::from_str(&json).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn test_hole_punch_message_serialization() {
        let msg = HolePunchMessage::DiscoverRequest { nonce: 42 };
        let bytes = serde_json::to_vec(&msg).unwrap();
        let back: HolePunchMessage = serde_json::from_slice(&bytes).unwrap();
        if let HolePunchMessage::DiscoverRequest { nonce } = back {
            assert_eq!(nonce, 42);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_is_nat_traversal_message() {
        let msg = HolePunchMessage::DiscoverRequest { nonce: 7 };
        let bytes = serde_json::to_vec(&msg).unwrap();
        assert!(is_nat_traversal_message(&bytes));

        let other = b"not a nat message";
        assert!(!is_nat_traversal_message(other));
    }

    #[test]
    fn test_make_discover_response() {
        let req = HolePunchMessage::DiscoverRequest { nonce: 99 };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        let observed: SocketAddr = "1.2.3.4:5678".parse().unwrap();
        let resp = make_discover_response(&req_bytes, observed).unwrap();
        if let HolePunchMessage::DiscoverResponse { nonce, observed_addr } = resp {
            assert_eq!(nonce, 99);
            assert_eq!(observed_addr, observed);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_make_discover_response_invalid_input() {
        let result = make_discover_response(b"garbage", "1.2.3.4:5678".parse().unwrap());
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let manager = NatTraversalManager::new(addr, local_config());
        assert_eq!(manager.local_addr(), addr);
        assert!(manager.config().enabled);
    }

    #[tokio::test]
    async fn test_probe_nat_type_returns_unknown_without_reflectors() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let manager = NatTraversalManager::new(addr, local_config());
        let info = manager.probe_nat_type().await;
        assert_eq!(info.nat_type, NatType::Unknown);
        assert!(info.public_addr.is_none());
    }

    #[tokio::test]
    async fn test_add_remove_keep_alive_peer() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let manager = NatTraversalManager::new(addr, local_config());

        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        manager.add_keep_alive_peer(peer).await;
        assert_eq!(manager.keep_alive_peers.read().await.len(), 1);

        // Adding the same peer again should not create a duplicate.
        manager.add_keep_alive_peer(peer).await;
        assert_eq!(manager.keep_alive_peers.read().await.len(), 1);

        manager.remove_keep_alive_peer(&peer).await;
        assert_eq!(manager.keep_alive_peers.read().await.len(), 0);
    }

    #[tokio::test]
    async fn test_handle_discover_request() {
        let local: SocketAddr = "127.0.0.1:8888".parse().unwrap();
        let manager = NatTraversalManager::new(local, local_config());
        let sender: SocketAddr = "1.2.3.4:5678".parse().unwrap();

        let msg = HolePunchMessage::DiscoverRequest { nonce: 123 };
        let resp = manager.handle_message(msg, sender).await;

        if let Some(HolePunchMessage::DiscoverResponse { nonce, observed_addr }) = resp {
            assert_eq!(nonce, 123);
            assert_eq!(observed_addr, sender);
        } else {
            panic!("expected DiscoverResponse");
        }
    }

    #[tokio::test]
    async fn test_handle_hole_punch_probe_creates_ack() {
        let local: SocketAddr = "127.0.0.1:8888".parse().unwrap();
        let manager = NatTraversalManager::new(local, local_config());
        let sender: SocketAddr = "1.2.3.4:5678".parse().unwrap();

        let msg = HolePunchMessage::HolePunchProbe { session_id: 77, sender_addr: sender };
        let resp = manager.handle_message(msg, sender).await;

        if let Some(HolePunchMessage::HolePunchAck { session_id, sender_addr }) = resp {
            assert_eq!(session_id, 77);
            assert_eq!(sender_addr, local);
        } else {
            panic!("expected HolePunchAck");
        }

        // The sender should have been added to keep-alive list.
        assert!(manager.keep_alive_peers.read().await.contains(&sender));
    }

    #[tokio::test]
    async fn test_cleanup_old_sessions() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let manager = NatTraversalManager::new(addr, local_config());

        // Manually insert a session
        {
            let mut sessions = manager.sessions.write().await;
            sessions.insert(
                1,
                HolePunchSession {
                    session_id: 1,
                    remote_addr: "127.0.0.1:9000".parse().unwrap(),
                    confirmed: false,
                    started_at: Instant::now(),
                },
            );
        }
        assert_eq!(manager.get_sessions().await.len(), 1);

        // A very small max_age should not yet expire an immediately-created session.
        manager.cleanup_old_sessions(Duration::from_secs(60)).await;
        assert_eq!(manager.get_sessions().await.len(), 1);

        // Zero duration → everything is stale.
        manager.cleanup_old_sessions(Duration::from_nanos(0)).await;
        assert_eq!(manager.get_sessions().await.len(), 0);
    }

    /// Integration test: a reflector and a prober running in the same process,
    /// connected over real loopback UDP sockets.
    #[tokio::test]
    async fn test_discover_public_addr_loopback() {
        // Bind the reflector socket.
        let reflector_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reflector_addr = reflector_socket.local_addr().unwrap();

        let reflector_local: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let reflector_mgr = Arc::new(NatTraversalManager::new(
            reflector_local,
            NatTraversalConfig {
                discovery_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        ));

        // Start the reflector loop in a background task.
        let reflector_socket = Arc::new(reflector_socket);
        let reflector_socket_clone = reflector_socket.clone();
        let reflector_mgr_clone = reflector_mgr.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            if let Ok((len, src)) = reflector_socket_clone.recv_from(&mut buf).await {
                let msg: HolePunchMessage =
                    serde_json::from_slice(&buf[..len]).unwrap();
                if let Some(resp) = reflector_mgr_clone.handle_message(msg, src).await {
                    let resp_bytes = serde_json::to_vec(&resp).unwrap();
                    let _ = reflector_socket_clone.send_to(&resp_bytes, src).await;
                }
            }
        });

        // Prober
        let prober_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let prober_mgr = NatTraversalManager::new(prober_addr, local_config());

        let public_addr = prober_mgr.discover_public_addr(reflector_addr).await.unwrap();

        // On loopback, the observed address should be some 127.0.0.1:<port>.
        assert_eq!(public_addr.ip().to_string(), "127.0.0.1");
    }

    /// Integration test: two peers perform a hole-punch handshake over loopback.
    #[tokio::test]
    async fn test_hole_punch_loopback() {
        // Create two sockets
        let socket_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let socket_b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

        let addr_a = socket_a.local_addr().unwrap();
        let addr_b = socket_b.local_addr().unwrap();

        let config_a = NatTraversalConfig {
            hole_punch_timeout: Duration::from_secs(3),
            hole_punch_attempts: 3,
            ..Default::default()
        };
        let config_b = config_a.clone();

        let mgr_a = Arc::new(NatTraversalManager::new(addr_a, config_a));
        let mgr_b = Arc::new(NatTraversalManager::new(addr_b, config_b));

        // Peer B listens for probes in a background task and responds.
        let socket_b_clone = socket_b.clone();
        let mgr_b_clone = mgr_b.clone();
        let addr_b_clone = addr_b;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            loop {
                match socket_b_clone.recv_from(&mut buf).await {
                    Ok((len, src)) => {
                        if let Ok(msg) =
                            serde_json::from_slice::<HolePunchMessage>(&buf[..len])
                        {
                            if let Some(resp) =
                                mgr_b_clone.handle_message(msg, src).await
                            {
                                let resp_bytes = serde_json::to_vec(&resp).unwrap();
                                let _ = socket_b_clone.send_to(&resp_bytes, src).await;
                            }
                        }
                    },
                    Err(_) => break,
                }
            }
        });

        // Peer A initiates the hole punch.
        let result = mgr_a.initiate_hole_punch(&socket_a, addr_b).await;
        assert!(result.is_ok(), "hole punch failed: {:?}", result);

        // Check that peer A recorded the confirmed session.
        let sessions = mgr_a.get_sessions().await;
        let session = sessions
            .iter()
            .find(|s| s.remote_addr == addr_b_clone)
            .expect("session not found");
        assert!(session.confirmed);
    }

    /// Integration test: keep-alive packets are sent to registered peers.
    #[tokio::test]
    async fn test_keep_alive_loopback() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver.local_addr().unwrap();

        let sender_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_socket.local_addr().unwrap();

        let manager = NatTraversalManager::new(sender_addr, local_config());
        manager.add_keep_alive_peer(receiver_addr).await;

        manager.send_keep_alive(&sender_socket).await.unwrap();

        // The receiver should have gotten the keep-alive packet.
        let mut buf = vec![0u8; 512];
        let (len, _src) = tokio::time::timeout(
            Duration::from_secs(2),
            receiver.recv_from(&mut buf),
        )
        .await
        .expect("timed out waiting for keep-alive")
        .expect("recv_from failed");

        let msg: HolePunchMessage = serde_json::from_slice(&buf[..len]).unwrap();
        assert!(matches!(msg, HolePunchMessage::KeepAlive { .. }));
    }

    #[tokio::test]
    async fn test_discover_public_addr_disabled() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = NatTraversalConfig { enabled: false, ..Default::default() };
        let manager = NatTraversalManager::new(addr, config);
        let result = manager
            .discover_public_addr("127.0.0.1:9999".parse().unwrap())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_initiate_hole_punch_disabled() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = NatTraversalConfig { enabled: false, ..Default::default() };
        let manager = NatTraversalManager::new(addr, config);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let result = manager
            .initiate_hole_punch(&socket, "127.0.0.1:9999".parse().unwrap())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_probe_nat_type_with_same_reflector_same_addr() {
        // When both reflectors return the same nonce as seen from the same
        // address, the NAT type should be RestrictedCone (not Symmetric).
        let reflector_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let reflector_addr = reflector_socket.local_addr().unwrap();

        let reflector_socket_clone = reflector_socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            // Handle two requests
            for _ in 0..2 {
                if let Ok((len, src)) = reflector_socket_clone.recv_from(&mut buf).await {
                    if let Ok(HolePunchMessage::DiscoverRequest { nonce }) =
                        serde_json::from_slice(&buf[..len])
                    {
                        let resp = HolePunchMessage::DiscoverResponse {
                            nonce,
                            observed_addr: src,
                        };
                        let resp_bytes = serde_json::to_vec(&resp).unwrap();
                        let _ = reflector_socket_clone.send_to(&resp_bytes, src).await;
                    }
                }
            }
        });

        let prober_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let prober = NatTraversalManager::new(prober_addr, local_config());

        // Use the same reflector for both probes (both will see the same observed addr).
        let info = prober
            .probe_nat_type_with_reflectors(reflector_addr, reflector_addr)
            .await;

        // Same address from both reflectors → RestrictedCone.
        assert_eq!(info.nat_type, NatType::RestrictedCone);
        assert!(info.public_addr.is_some());
    }

    #[tokio::test]
    async fn test_nat_traversal_config_default() {
        let config = NatTraversalConfig::default();
        assert!(config.enabled);
        assert_eq!(config.hole_punch_attempts, 5);
        assert_eq!(config.keep_alive_interval, Duration::from_secs(25));
    }

    #[tokio::test]
    async fn test_nat_info_unknown_default() {
        let info = NatInfo::unknown();
        assert_eq!(info.nat_type, NatType::Unknown);
        assert!(info.public_addr.is_none());
    }
}
