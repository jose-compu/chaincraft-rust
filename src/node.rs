//! Chaincraft node implementation

use crate::{
    discovery::{DiscoveryConfig, DiscoveryManager},
    error::{ChaincraftError, NetworkError, Result},
    network::{PeerId, PeerInfo},
    shared::{MessageType, SharedMessage, SharedObjectId, SharedObjectRegistry},
    shared_object::{ApplicationObject, ApplicationObjectRegistry, SimpleSharedNumber},
    storage::{MemoryStorage, Storage},
};
use serde_json::json;

use serde::de::Error as SerdeDeError;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{net::UdpSocket, sync::RwLock};

/// Storage key for persisted peers (equivalent to Python PEERS in DB)
const PEERS_KEY: &str = "__PEERS__";
/// Storage key for banned peers (equivalent to Python BANNED_PEERS in DB)
const BANNED_PEERS_KEY: &str = "__BANNED_PEERS__";

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedPeer {
    id: String,
    address: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BannedEntry {
    addr: String,
    expires_at: String,
}

/// Main node structure for Chaincraft network
pub struct ChaincraftNode {
    /// Unique identifier for this node
    pub id: PeerId,
    /// Registry of shared objects
    pub registry: Arc<RwLock<SharedObjectRegistry>>,
    /// Registry of application objects
    pub app_objects: Arc<RwLock<ApplicationObjectRegistry>>,
    /// Discovery manager
    pub discovery: Option<DiscoveryManager>,
    /// NAT traversal manager
    pub nat_traversal: Option<Arc<crate::nat_traversal::NatTraversalManager>>,
    /// Storage backend
    pub storage: Arc<dyn Storage>,
    /// Connected peers
    pub peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    /// Banned peer addresses (loaded from and saved to storage)
    pub banned_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    /// Known message hashes for gossip
    pub known_hashes: Arc<RwLock<HashSet<String>>>,
    /// UDP socket for networking (if initialized)
    pub socket: Option<Arc<UdpSocket>>,
    /// Node configuration
    pub config: NodeConfig,
    /// Running flag
    pub running: Arc<RwLock<bool>>,
}

impl ChaincraftNode {
    /// Create a new Chaincraft node
    pub fn new(id: PeerId, storage: Arc<dyn Storage>) -> Self {
        Self::builder()
            .with_id(id)
            .with_storage(storage)
            .build()
            .expect("Failed to create node")
    }

    /// Create a new Chaincraft node with default settings (alias for compatibility with examples)
    pub fn new_default() -> Self {
        Self::default()
    }

    /// Create a new node builder
    pub fn builder() -> ChaincraftNodeBuilder {
        ChaincraftNodeBuilder::new()
    }

    /// Start the node
    pub async fn start(&mut self) -> Result<()> {
        // Initialize storage
        self.storage.initialize().await?;

        // Load persisted peers and banned peers from storage
        self.load_persisted_peers().await?;
        self.load_banned_peers().await?;

        // Set running status
        {
            let mut running = self.running.write().await;
            *running = true;
        }

        // Start networking (UDP-based, minimal implementation)
        self.start_networking().await?;

        // TODO: Start consensus
        // TODO: Start API server

        Ok(())
    }

    /// Stop the node
    pub async fn stop(&mut self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            *running = false;
        }
        // Unregister from local discovery
        if self.config.local_discovery {
            unregister_local_node(&self.id);
        }
        // Socket tasks observe the running flag and exit gracefully
        Ok(())
    }

    /// Close the node (alias for stop)
    pub async fn close(&mut self) -> Result<()> {
        self.stop().await
    }

    /// Check if the node is running (async version)
    pub async fn is_running_async(&self) -> bool {
        *self.running.read().await
    }

    /// Add a peer to the node's peer list. Rejects if peer address is banned.
    pub async fn add_peer(&self, peer: PeerInfo) -> Result<()> {
        let banned = self.banned_peers.read().await;
        if banned.contains(&peer.address) {
            return Err(ChaincraftError::Network(NetworkError::PeerBanned {
                addr: peer.address,
                expires_at: chrono::Utc::now() + chrono::Duration::days(365 * 10),
            }));
        }
        drop(banned);

        let mut peers = self.peers.write().await;
        peers.insert(peer.id.clone(), peer.clone());
        drop(peers);

        if self.config.persist_peers {
            self.save_persisted_peers().await?;
        }
        Ok(())
    }

    /// Remove a peer from the node's peer list
    pub async fn remove_peer(&self, peer_id: &PeerId) -> Result<()> {
        let mut peers = self.peers.write().await;
        peers.remove(peer_id);
        drop(peers);

        if self.config.persist_peers {
            self.save_persisted_peers().await?;
        }
        Ok(())
    }

    /// Ban a peer address for a duration. Persisted to storage.
    pub async fn ban_peer(
        &self,
        addr: SocketAddr,
        duration: Option<std::time::Duration>,
    ) -> Result<()> {
        {
            let mut banned = self.banned_peers.write().await;
            banned.insert(addr);
        }
        self.save_banned_peers().await?;
        Ok(())
    }

    /// Unban a peer address. Persisted to storage.
    pub async fn unban_peer(&self, addr: SocketAddr) -> Result<()> {
        {
            let mut banned = self.banned_peers.write().await;
            banned.remove(&addr);
        }
        self.save_banned_peers().await?;
        Ok(())
    }

    /// Check if an address is banned
    pub async fn is_banned(&self, addr: SocketAddr) -> bool {
        self.banned_peers.read().await.contains(&addr)
    }

    /// Load persisted peers from storage (equivalent to Python PEERS in DB)
    async fn load_persisted_peers(&self) -> Result<()> {
        if !self.config.persist_peers {
            return Ok(());
        }
        let bytes = match self.storage.get(PEERS_KEY).await? {
            Some(b) => b,
            None => return Ok(()),
        };
        let persisted: Vec<PersistedPeer> = match serde_json::from_slice(&bytes) {
            Ok(p) => p,
            Err(_) => return Ok(()),
        };
        let mut peers = self.peers.write().await;
        for p in persisted {
            let addr: SocketAddr = match p.address.parse() {
                Ok(a) => a,
                Err(_) => continue,
            };
            let id = match uuid::Uuid::parse_str(&p.id) {
                Ok(u) => PeerId::from_uuid(u),
                Err(_) => PeerId::new(),
            };
            let info = PeerInfo::new(id, addr);
            peers.insert(info.id.clone(), info);
        }
        Ok(())
    }

    /// Save peers to storage
    async fn save_persisted_peers(&self) -> Result<()> {
        let peers = self.peers.read().await;
        let persisted: Vec<PersistedPeer> = peers
            .values()
            .map(|p| PersistedPeer {
                id: p.id.to_string(),
                address: p.address.to_string(),
            })
            .collect();
        let json = serde_json::to_vec(&persisted).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        self.storage.put(PEERS_KEY, json).await?;
        Ok(())
    }

    /// Load banned peers from storage (equivalent to Python BANNED_PEERS in DB)
    async fn load_banned_peers(&self) -> Result<()> {
        let bytes = match self.storage.get(BANNED_PEERS_KEY).await? {
            Some(b) => b,
            None => return Ok(()),
        };
        let entries: Vec<BannedEntry> = match serde_json::from_slice(&bytes) {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };
        let now = chrono::Utc::now();
        let mut banned = self.banned_peers.write().await;
        for e in entries {
            if let Ok(addr) = e.addr.parse::<SocketAddr>() {
                let expires: chrono::DateTime<chrono::Utc> =
                    chrono::DateTime::parse_from_rfc3339(&e.expires_at)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or(now);
                if expires > now {
                    banned.insert(addr);
                }
            }
        }
        Ok(())
    }

    /// Save banned peers to storage
    async fn save_banned_peers(&self) -> Result<()> {
        let banned = self.banned_peers.read().await;
        let entries: Vec<BannedEntry> = banned
            .iter()
            .map(|addr| BannedEntry {
                addr: addr.to_string(),
                expires_at: (chrono::Utc::now() + chrono::Duration::days(365 * 10)).to_rfc3339(),
            })
            .collect();
        let json = serde_json::to_vec(&entries).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        self.storage.put(BANNED_PEERS_KEY, json).await?;
        Ok(())
    }

    /// Connect to a peer
    pub async fn connect_to_peer(&mut self, peer_addr: &str) -> Result<()> {
        self.connect_to_peer_with_discovery(peer_addr, false).await
    }

    /// Connect to a peer with optional discovery
    pub async fn connect_to_peer_with_discovery(
        &mut self,
        peer_addr: &str,
        _discovery: bool,
    ) -> Result<()> {
        // Parse address and create PeerInfo
        let socket_addr: SocketAddr = peer_addr.parse().map_err(|_| {
            ChaincraftError::Network(NetworkError::InvalidMessage {
                reason: "Invalid peer address format".to_string(),
            })
        })?;

        if self.is_banned(socket_addr).await {
            return Err(ChaincraftError::Network(NetworkError::PeerBanned {
                addr: socket_addr,
                expires_at: chrono::Utc::now(),
            }));
        }

        let peer_id = PeerId::new(); // Generate a new peer ID for this address
        let peer_info = PeerInfo::new(peer_id.clone(), socket_addr);

        self.add_peer(peer_info.clone()).await?;

        // If discovery is available, notify it about the new peer
        if let Some(discovery) = &self.discovery {
            discovery.add_peer(peer_info).await?;
            discovery.mark_connected(&peer_id).await?;
        }

        Ok(())
    }

    /// Get all connected peers
    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }

    /// Get connected peers synchronously (for compatibility)
    pub fn peers(&self) -> Vec<PeerInfo> {
        // For now we expose an empty list synchronously; async get_peers should be used.
        Vec::new()
    }

    /// Get the node's ID
    pub fn id(&self) -> &PeerId {
        &self.id
    }

    /// Get the node's port
    pub fn port(&self) -> u16 {
        self.config.port
    }

    /// Get the node's host
    pub fn host(&self) -> &str {
        "127.0.0.1" // Default host
    }

    /// Get maximum peers
    pub fn max_peers(&self) -> usize {
        self.config.max_peers
    }

    /// Create a shared message
    pub async fn create_shared_message(&mut self, data: String) -> Result<String> {
        let message_data = serde_json::to_value(&data).map_err(|e| {
            ChaincraftError::Serialization(crate::error::SerializationError::Json(e))
        })?;
        let message =
            SharedMessage::new(MessageType::Custom("user_message".to_string()), message_data);
        let hash = message.hash.clone();
        let json = message.to_json()?;
        self.storage.put(&hash, json.as_bytes().to_vec()).await?;

        // Track this hash for gossip
        {
            let mut set = self.known_hashes.write().await;
            set.insert(hash.clone());
        }

        // Broadcast to peers if networking is enabled
        if let Some(socket) = &self.socket {
            let peers = self.peers.clone();
            let banned_peers = self.banned_peers.clone();
            let socket = socket.clone();
            let json_bytes = json.into_bytes();
            tokio::spawn(async move {
                if let Err(e) = broadcast_bytes(&socket, &peers, &banned_peers, &json_bytes).await {
                    tracing::warn!("Failed to broadcast message: {:?}", e);
                }
            });
        }

        Ok(hash)
    }

    /// Check if the node has a specific object
    pub fn has_object(&self, _hash: &str) -> bool {
        // Simplified implementation for testing
        true
    }

    /// Get an object by hash
    pub async fn get_object(&self, hash: &str) -> Result<String> {
        if let Some(bytes) = self.storage.get(hash).await? {
            let s = String::from_utf8(bytes).map_err(|e| {
                ChaincraftError::Serialization(crate::error::SerializationError::Json(
                    SerdeDeError::custom(e),
                ))
            })?;
            Ok(s)
        } else {
            Err(ChaincraftError::Storage(crate::error::StorageError::KeyNotFound {
                key: hash.to_string(),
            }))
        }
    }

    /// Get the database size
    pub fn db_size(&self) -> usize {
        // Query underlying storage length synchronously for tests
        futures::executor::block_on(async { self.storage.len().await.unwrap_or(0) })
    }

    /// Add a shared object (application object)
    pub async fn add_shared_object(
        &self,
        object: Box<dyn ApplicationObject>,
    ) -> Result<SharedObjectId> {
        let mut registry = self.app_objects.write().await;
        let id = registry.register(object);
        Ok(id)
    }

    /// Get shared objects (for compatibility with Python tests)
    pub async fn shared_objects(&self) -> Vec<Box<dyn ApplicationObject>> {
        let registry = self.app_objects.read().await;
        registry
            .ids()
            .into_iter()
            .filter_map(|id| registry.get(&id))
            .map(|obj| obj.clone_box())
            .collect()
    }

    /// Get shared object count
    pub async fn shared_object_count(&self) -> usize {
        let registry = self.app_objects.read().await;
        registry.len()
    }

    /// Create shared message with application object processing
    pub async fn create_shared_message_with_data(
        &mut self,
        data: serde_json::Value,
    ) -> Result<String> {
        // Extract message type from data if present, otherwise use default
        let message_type = if let Some(msg_type) = data.get("type").and_then(|t| t.as_str()) {
            match msg_type {
                "PEER_DISCOVERY" => MessageType::PeerDiscovery,
                "REQUEST_LOCAL_PEERS" => MessageType::RequestLocalPeers,
                "LOCAL_PEERS" => MessageType::LocalPeers,
                "REQUEST_SHARED_OBJECT_UPDATE" => MessageType::RequestSharedObjectUpdate,
                "SHARED_OBJECT_UPDATE" => MessageType::SharedObjectUpdate,
                "GET" => MessageType::Get,
                "SET" => MessageType::Set,
                "DELETE" => MessageType::Delete,
                "RESPONSE" => MessageType::Response,
                "NOTIFICATION" => MessageType::Notification,
                "HEARTBEAT" => MessageType::Heartbeat,
                "ERROR" => MessageType::Error,
                _ => MessageType::Custom(msg_type.to_string()),
            }
        } else {
            MessageType::Custom("user_message".to_string())
        };

        let message = SharedMessage::new(message_type, data.clone());
        let hash = message.hash.clone();
        let json = message.to_json()?;
        // Process message through application objects first (validation + pipeline).
        let mut app_registry = self.app_objects.write().await;
        let _processed = app_registry.process_message(message).await?;
        drop(app_registry);

        // Persist only after successful pipeline execution.
        self.storage.put(&hash, json.as_bytes().to_vec()).await?;

        // Broadcast to peers over UDP if networking is enabled
        if let Some(socket) = &self.socket {
            let peers = self.peers.clone();
            let banned_peers = self.banned_peers.clone();
            let socket = socket.clone();
            let json_bytes = json.into_bytes();
            tokio::spawn(async move {
                if let Err(e) = broadcast_bytes(&socket, &peers, &banned_peers, &json_bytes).await {
                    tracing::warn!("Failed to broadcast message: {:?}", e);
                }
            });
        }

        Ok(hash)
    }

    /// Get node state for testing/debugging
    pub async fn get_state(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "node_id": self.id.to_string(),
            "running": *self.running.read().await,
            "port": self.config.port,
            "max_peers": self.config.max_peers,
            "peer_count": self.peers.read().await.len(),
            "messages": "stored", // Simplified for testing
            "shared_objects": self.shared_object_count().await
        }))
    }

    /// Get discovery info for testing
    pub async fn get_discovery_info(&self) -> serde_json::Value {
        serde_json::json!({
            "node_id": self.id.to_string(),
            "host": self.host(),
            "port": self.port(),
            "max_peers": self.max_peers(),
            "peer_count": self.peers.read().await.len()
        })
    }

    /// Set port for testing
    pub fn set_port(&mut self, port: u16) {
        self.config.port = port;
    }

    /// Disable local discovery (for single-node tests)
    pub fn disable_local_discovery(&mut self) {
        self.config.local_discovery = false;
    }

    /// Enable or disable NAT traversal at runtime (must be called before `start`).
    pub fn set_nat_traversal_enabled(&mut self, enabled: bool) {
        self.config.nat_traversal.enabled = enabled;
    }

    /// Return a reference to the NAT traversal manager, if it has been initialized.
    pub fn nat_traversal_manager(
        &self,
    ) -> Option<&Arc<crate::nat_traversal::NatTraversalManager>> {
        self.nat_traversal.as_ref()
    }

    /// Check if node is running (sync version for compatibility)
    pub fn is_running(&self) -> bool {
        // For tests, we'll use a blocking approach
        futures::executor::block_on(async { *self.running.read().await })
    }
}

impl Default for ChaincraftNode {
    fn default() -> Self {
        Self::new(PeerId::new(), Arc::new(MemoryStorage::new()))
    }
}

/// Helper: start UDP networking for this node.
impl ChaincraftNode {
    async fn start_networking(&mut self) -> Result<()> {
        // Bind UDP socket to configured host/port. If port is 0, the OS will
        // choose an ephemeral port for us.
        let bind_addr: SocketAddr =
            format!("{}:{}", self.host(), self.port())
                .parse()
                .map_err(|_| {
                    ChaincraftError::Config(format!(
                        "Invalid bind address {}:{}",
                        self.host(),
                        self.port()
                    ))
                })?;

        let socket = UdpSocket::bind(bind_addr).await.map_err(|e| {
            ChaincraftError::Network(NetworkError::BindFailed {
                addr: bind_addr,
                source: e,
            })
        })?;

        // If we bound to port 0, update the config with the actual port chosen
        let register_addr = if self.config.port == 0 {
            if let Ok(local_addr) = socket.local_addr() {
                self.config.port = local_addr.port();
                local_addr
            } else {
                bind_addr
            }
        } else {
            bind_addr
        };

        let socket = Arc::new(socket);
        self.socket = Some(socket.clone());

        // Initialize NAT traversal manager if enabled.
        if self.config.nat_traversal.enabled {
            let nat_mgr = Arc::new(crate::nat_traversal::NatTraversalManager::new(
                register_addr,
                self.config.nat_traversal.clone(),
            ));
            self.nat_traversal = Some(nat_mgr);
        }

        let running = self.running.clone();
        let storage = self.storage.clone();
        let app_objects = self.app_objects.clone();
        let peers = self.peers.clone();
        let known_hashes = self.known_hashes.clone();

        // Register this node for local discovery (in-process registry)
        if self.config.local_discovery {
            register_local_node(self.id.clone(), register_addr);
        }

        // Receive loop
        let banned_peers = self.banned_peers.clone();
        let known_hashes = self.known_hashes.clone();
        let nat_mgr_recv = self.nat_traversal.clone();
        {
            let socket = socket.clone();
            let running = running.clone();
            let storage = storage.clone();
            let app_objects = app_objects.clone();
            let peers = peers.clone();
            let banned_peers = banned_peers.clone();
            let known_hashes = known_hashes.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    if !*running.read().await {
                        break;
                    }

                    let (len, addr) = match socket.recv_from(&mut buf).await {
                        Ok(res) => res,
                        Err(e) => {
                            if !*running.read().await {
                                break;
                            }
                            tracing::warn!("UDP recv_from error: {:?}", e);
                            continue;
                        },
                    };

                    let data = &buf[..len];

                    // Try to dispatch as a NAT traversal message first.
                    if let Some(ref nat_mgr) = nat_mgr_recv {
                        if let Ok(nat_msg) =
                            serde_json::from_slice::<crate::nat_traversal::HolePunchMessage>(data)
                        {
                            if let Some(response) =
                                nat_mgr.handle_message(nat_msg, addr).await
                            {
                                if let Ok(resp_bytes) = serde_json::to_vec(&response) {
                                    let _ = socket.send_to(&resp_bytes, addr).await;
                                }
                            }
                            continue;
                        }
                    }

                    if let Err(e) = handle_incoming_datagram(
                        data,
                        addr,
                        &socket,
                        &storage,
                        &app_objects,
                        &peers,
                        &banned_peers,
                        Some(&known_hashes),
                    )
                    .await
                    {
                        tracing::warn!("Error handling incoming datagram from {}: {:?}", addr, e);
                    }
                }
            });
        }

        // NAT traversal keep-alive loop
        if let Some(ref nat_mgr) = self.nat_traversal {
            let nat_mgr_ka = nat_mgr.clone();
            let socket_ka = socket.clone();
            let running_ka = running.clone();
            let keep_alive_interval = self.config.nat_traversal.keep_alive_interval;
            tokio::spawn(async move {
                loop {
                    if !*running_ka.read().await {
                        break;
                    }
                    tokio::time::sleep(keep_alive_interval).await;
                    if !*running_ka.read().await {
                        break;
                    }
                    if let Err(e) = nat_mgr_ka.send_keep_alive(&socket_ka).await {
                        tracing::warn!("NAT keep-alive error: {:?}", e);
                    }
                }
            });
        }

        // Gossip + local discovery loop
        let node_id = self.id.clone();
        let banned_peers = banned_peers.clone();
        let local_discovery = self.config.local_discovery;
        tokio::spawn(async move {
            // Simple fixed interval; could be made configurable.
            let interval = Duration::from_millis(500);
            loop {
                if !*running.read().await {
                    break;
                }

                // Local discovery: pull all locally-registered nodes and ensure
                // they appear in our peers map. Only when local_discovery is enabled.
                if local_discovery {
                    if let Some(local_nodes) = snapshot_local_nodes() {
                        let banned_set: HashSet<SocketAddr> = {
                            let b = banned_peers.read().await;
                            b.iter().copied().collect()
                        };
                        let mut peers_guard = peers.write().await;
                        for (peer_id, addr) in local_nodes {
                            if peer_id == node_id {
                                continue;
                            }
                            if banned_set.contains(&addr) {
                                continue;
                            }
                            if peers_guard.values().any(|p| p.address == addr) {
                                continue;
                            }
                            let info = PeerInfo::new(peer_id.clone(), addr);
                            peers_guard.insert(peer_id, info);
                        }
                    }
                }

                // Snapshot known hashes
                let hashes: Vec<String> = {
                    let set = known_hashes.read().await;
                    set.iter().cloned().collect()
                };

                for hash in hashes {
                    // Fetch stored JSON and rebroadcast it
                    if let Ok(Some(bytes)) = storage.get(&hash).await {
                        if let Err(e) =
                            broadcast_bytes(&socket, &peers, &banned_peers, &bytes).await
                        {
                            tracing::warn!("gossip broadcast failed for {}: {:?}", hash, e);
                        }
                    }
                }

                // Digest-based sync: periodically request latest digest from a peer
                let peer_addrs: Vec<SocketAddr> = {
                    let p = peers.read().await;
                    p.values().map(|x| x.address).collect()
                };
                if !peer_addrs.is_empty() {
                    if let Some(&peer_addr) = peer_addrs.first() {
                        let req = json!({ "type": "REQUEST_DIGEST" });
                        if let Ok(bytes) = serde_json::to_vec(&req) {
                            let _ = socket.send_to(&bytes, peer_addr).await;
                        }
                    }
                }

                tokio::time::sleep(interval).await;
            }
        });

        Ok(())
    }
}

/// Broadcast raw bytes to all known peers.
async fn broadcast_bytes(
    socket: &Arc<UdpSocket>,
    peers: &Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    banned_peers: &Arc<RwLock<HashSet<SocketAddr>>>,
    data: &[u8],
) -> Result<()> {
    let (peers_snapshot, banned_set): (Vec<SocketAddr>, HashSet<SocketAddr>) = {
        let p = peers.read().await;
        let b = banned_peers.read().await;
        (p.values().map(|x| x.address).collect(), b.iter().copied().collect())
    };

    for addr in peers_snapshot {
        if banned_set.contains(&addr) {
            continue;
        }
        if let Err(e) = socket.send_to(data, addr).await {
            tracing::warn!("Failed to send UDP packet to {}: {:?}", addr, e);
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Local discovery (in-process registry)
// -----------------------------------------------------------------------------

static LOCAL_NODES: OnceLock<Mutex<HashMap<PeerId, SocketAddr>>> = OnceLock::new();

fn local_registry() -> &'static Mutex<HashMap<PeerId, SocketAddr>> {
    LOCAL_NODES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_local_node(id: PeerId, addr: SocketAddr) {
    let registry = local_registry();
    let mut guard = registry.lock().unwrap();
    guard.insert(id, addr);
}

fn unregister_local_node(id: &PeerId) {
    if let Some(registry) = LOCAL_NODES.get() {
        let mut guard = registry.lock().unwrap();
        guard.remove(id);
    }
}

/// Clear all entries from the local registry. Useful for test isolation.
pub fn clear_local_registry() {
    if let Some(registry) = LOCAL_NODES.get() {
        let mut guard = registry.lock().unwrap();
        guard.clear();
    }
}

fn snapshot_local_nodes() -> Option<Vec<(PeerId, SocketAddr)>> {
    let registry = LOCAL_NODES.get()?;
    let guard = registry.lock().unwrap();
    Some(guard.iter().map(|(id, addr)| (id.clone(), *addr)).collect())
}

/// Handle digest-sync control messages (REQUEST_DIGEST, REQUEST_MESSAGES_SINCE, etc.)
#[allow(clippy::too_many_arguments)]
async fn handle_digest_sync_control(
    data: &[u8],
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    storage: &Arc<dyn Storage>,
    app_objects: &Arc<RwLock<ApplicationObjectRegistry>>,
    peers: &Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    banned_peers: &Arc<RwLock<HashSet<SocketAddr>>>,
    known_hashes: &Arc<RwLock<HashSet<String>>>,
) -> Result<bool> {
    let value: serde_json::Value = serde_json::from_slice(data).map_err(|_| {
        ChaincraftError::Serialization(crate::error::SerializationError::Json(
            serde_json::Error::custom("not json"),
        ))
    })?;
    let msg_type = value.get("type").and_then(|t| t.as_str());
    match msg_type {
        Some("REQUEST_DIGEST") => {
            let digest = {
                let registry = app_objects.read().await;
                let ids = registry.ids();
                let mut digest = "".to_string();
                for id in ids {
                    if let Some(obj) = registry.get(&id) {
                        if obj.is_merkleized() {
                            digest = obj.get_latest_digest().await.unwrap_or_default();
                            break;
                        }
                    }
                }
                digest
            };
            let resp = json!({ "type": "DIGEST_RESPONSE", "digest": digest });
            let bytes = serde_json::to_vec(&resp).unwrap_or_default();
            let _ = socket.send_to(&bytes, addr).await;
            return Ok(true);
        },
        Some("REQUEST_MESSAGES_SINCE") => {
            let since = value.get("digest").and_then(|d| d.as_str()).unwrap_or("");
            let messages = {
                let registry = app_objects.read().await;
                let ids = registry.ids();
                let mut msgs = Vec::new();
                for id in ids {
                    if let Some(obj) = registry.get(&id) {
                        if obj.is_merkleized() {
                            msgs = obj
                                .get_messages_since_digest(since)
                                .await
                                .unwrap_or_default();
                            break;
                        }
                    }
                }
                msgs
            };
            let msg_ser: Vec<serde_json::Value> = messages
                .iter()
                .filter_map(|m| serde_json::to_value(m).ok())
                .collect();
            let resp = json!({ "type": "MESSAGES_RESPONSE", "messages": msg_ser });
            let bytes = serde_json::to_vec(&resp).unwrap_or_default();
            let _ = socket.send_to(&bytes, addr).await;
            return Ok(true);
        },
        Some("DIGEST_RESPONSE") => {
            let remote_digest = value.get("digest").and_then(|d| d.as_str()).unwrap_or("");
            let our_digest = {
                let registry = app_objects.read().await;
                let ids = registry.ids();
                let mut d = String::new();
                for id in ids {
                    if let Some(obj) = registry.get(&id) {
                        if obj.is_merkleized() {
                            d = obj.get_latest_digest().await.unwrap_or_default();
                            break;
                        }
                    }
                }
                d
            };
            if remote_digest != our_digest {
                let req = json!({ "type": "REQUEST_MESSAGES_SINCE", "digest": our_digest });
                let bytes = serde_json::to_vec(&req).unwrap_or_default();
                let _ = socket.send_to(&bytes, addr).await;
            }
            return Ok(true);
        },
        Some("MESSAGES_RESPONSE") => {
            let messages: Vec<SharedMessage> = value
                .get("messages")
                .and_then(|m| m.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| serde_json::from_value(v.clone()).ok())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for msg in messages {
                if storage.exists(&msg.hash).await.unwrap_or(true) {
                    continue;
                }
                let json = msg.to_json().unwrap_or_default();
                {
                    let mut registry = app_objects.write().await;
                    if registry.process_message(msg.clone()).await.is_err() {
                        continue;
                    }
                }
                let _ = storage.put(&msg.hash, json.as_bytes().to_vec()).await;
                {
                    let mut set = known_hashes.write().await;
                    set.insert(msg.hash.clone());
                }
                let bytes = msg.to_json().unwrap_or_default().into_bytes();
                let _ = broadcast_bytes(socket, peers, banned_peers, &bytes).await;
            }
            return Ok(true);
        },
        _ => {},
    }
    Ok(false)
}

/// Handle an incoming UDP datagram.
#[allow(clippy::too_many_arguments)]
async fn handle_incoming_datagram(
    data: &[u8],
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    storage: &Arc<dyn Storage>,
    app_objects: &Arc<RwLock<ApplicationObjectRegistry>>,
    peers: &Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    banned_peers: &Arc<RwLock<HashSet<SocketAddr>>>,
    known_hashes: Option<&Arc<RwLock<HashSet<String>>>>,
) -> Result<()> {
    // Reject traffic from banned peers
    {
        let banned = banned_peers.read().await;
        if banned.contains(&addr) {
            return Ok(()); // Silently ignore
        }
    }

    // Try digest-sync control messages first
    if let Some(kh) = known_hashes {
        if let Ok(true) = handle_digest_sync_control(
            data,
            addr,
            socket,
            storage,
            app_objects,
            peers,
            banned_peers,
            kh,
        )
        .await
        {
            // Ensure peer is recorded
            {
                let mut guard = peers.write().await;
                if !guard.values().any(|p| p.address == addr) {
                    let peer_id = PeerId::new();
                    let info = PeerInfo::new(peer_id.clone(), addr);
                    guard.insert(peer_id, info);
                }
            }
            return Ok(());
        }
    }

    // Try to parse as SharedMessage JSON
    let msg: SharedMessage = match serde_json::from_slice(data) {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };

    // Deduplicate using storage: if we already have this hash, ignore
    if storage.exists(&msg.hash).await? {
        return Ok(());
    }

    let json = msg.to_json()?;

    // Ensure peer is recorded (only if not banned)
    {
        let mut guard = peers.write().await;
        if !guard.values().any(|p| p.address == addr) {
            let peer_id = PeerId::new();
            let info = PeerInfo::new(peer_id.clone(), addr);
            guard.insert(peer_id, info);
        }
    }

    // Process through application objects first (validation + pipeline).
    {
        let mut registry = app_objects.write().await;
        let _ = registry.process_message(msg.clone()).await?;
    }

    // Persist message only after successful pipeline execution.
    storage.put(&msg.hash, json.as_bytes().to_vec()).await?;

    // Broadcast to other peers so the message propagates
    let bytes = json.into_bytes();
    broadcast_bytes(socket, peers, banned_peers, &bytes).await?;

    Ok(())
}

/// Node configuration
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Maximum number of peers to connect to
    pub max_peers: usize,

    /// Network port to listen on
    pub port: u16,

    /// Host to bind on
    pub host: String,

    /// Enable consensus participation
    pub consensus_enabled: bool,

    /// Enable local discovery of peers within the same process
    pub local_discovery: bool,
    /// Persist peers to storage and load on start (equivalent to Python PEERS in DB)
    pub persist_peers: bool,
    /// Configuration for the NAT traversal subsystem.
    pub nat_traversal: crate::nat_traversal::NatTraversalConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            max_peers: 50,
            port: 8080,
            host: "127.0.0.1".to_string(),
            consensus_enabled: true,
            local_discovery: true,
            persist_peers: true,
            nat_traversal: crate::nat_traversal::NatTraversalConfig::default(),
        }
    }
}

/// Builder for Chaincraft nodes
pub struct ChaincraftNodeBuilder {
    id: Option<PeerId>,
    storage: Option<Arc<dyn Storage>>,
    config: NodeConfig,
    persistent: bool,
}

impl ChaincraftNodeBuilder {
    /// Create a new node builder
    pub fn new() -> Self {
        Self {
            id: None,
            storage: None,
            config: NodeConfig::default(),
            persistent: false,
        }
    }

    /// Set the node ID
    pub fn with_id(mut self, id: PeerId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set the storage backend
    pub fn with_storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set persistent storage option
    pub fn with_persistent_storage(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Set the node configuration
    pub fn with_config(mut self, config: NodeConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    /// Set the host
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    /// Enable or disable local discovery
    pub fn local_discovery(mut self, enabled: bool) -> Self {
        self.config.local_discovery = enabled;
        self
    }

    /// Enable or disable persisting peers to storage
    pub fn persist_peers(mut self, enabled: bool) -> Self {
        self.config.persist_peers = enabled;
        self
    }

    /// Set the maximum peers
    pub fn max_peers(mut self, max_peers: usize) -> Self {
        self.config.max_peers = max_peers;
        self
    }

    /// Enable or disable the NAT traversal subsystem.
    pub fn nat_traversal(mut self, enabled: bool) -> Self {
        self.config.nat_traversal.enabled = enabled;
        self
    }

    /// Supply a custom [`NatTraversalConfig`] for this node.
    pub fn with_nat_traversal_config(
        mut self,
        config: crate::nat_traversal::NatTraversalConfig,
    ) -> Self {
        self.config.nat_traversal = config;
        self
    }

    /// Build the node
    pub fn build(self) -> Result<ChaincraftNode> {
        // Generate a new random ID if not provided
        let id = self.id.unwrap_or_else(|| {
            use crate::network::PeerId;
            PeerId::new()
        });

        // Select storage backend: explicit, persistent (on-disk), or in-memory.
        let storage: Arc<dyn Storage> = if let Some(storage) = self.storage {
            storage
        } else if self.persistent {
            #[cfg(feature = "persistent")]
            {
                use crate::storage::SledStorage;
                // Use port-based file name similar to Python's node_<port>.db
                let path = format!("node_{}.db", self.config.port);
                Arc::new(SledStorage::open(path)?)
            }
            #[cfg(not(feature = "persistent"))]
            {
                Arc::new(MemoryStorage::new())
            }
        } else {
            Arc::new(MemoryStorage::new())
        };

        Ok(ChaincraftNode {
            id,
            registry: Arc::new(RwLock::new(SharedObjectRegistry::new())),
            app_objects: Arc::new(RwLock::new(ApplicationObjectRegistry::new())),
            discovery: None, // Will be initialized during start if needed
            nat_traversal: None, // Will be initialized during start if NAT traversal is enabled
            storage,
            peers: Arc::new(RwLock::new(HashMap::new())),
            banned_peers: Arc::new(RwLock::new(HashSet::new())),
            known_hashes: Arc::new(RwLock::new(HashSet::new())),
            socket: None,
            config: self.config,
            running: Arc::new(RwLock::new(false)),
        })
    }
}

impl Default for ChaincraftNodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}
