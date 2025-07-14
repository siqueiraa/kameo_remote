use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fmt, io};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

/// Errors that can occur in the gossip registry
#[derive(Error, Debug)]
pub enum GossipError {
    #[error("network error: {0}")]
    Network(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("connection timeout")]
    Timeout,

    #[error("peer not found: {0}")]
    PeerNotFound(SocketAddr),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("invalid node id: {0}")]
    InvalidNodeId(String),

    #[error("registry shutdown")]
    Shutdown,
}

pub type Result<T> = std::result::Result<T, GossipError>;

/// Node identifier using UUID for better uniqueness and performance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_simple())
    }
}

impl str::FromStr for NodeId {
    type Err = GossipError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(Self(
            Uuid::parse_str(s).map_err(|_| GossipError::InvalidNodeId(s.to_string()))?,
        ))
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the gossip registry
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    /// Maximum number of peers to gossip to in each round
    pub max_gossip_peers: usize,
    /// Time-to-live for actor entries
    pub actor_ttl: Duration,
    /// Cleanup interval for stale entries
    pub cleanup_interval: Duration,
    /// Connection timeout for outbound connections
    pub connection_timeout: Duration,
    /// Response timeout for gossip exchanges
    pub response_timeout: Duration,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Maximum number of failed connection attempts before marking peer as failed
    pub max_peer_failures: usize,
    /// Time to wait before retrying failed peers
    pub peer_retry_interval: Duration,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(5),
            max_gossip_peers: 3,
            actor_ttl: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(5),
            max_message_size: 10 * 1024 * 1024, // 10MB
            max_peer_failures: 3,
            peer_retry_interval: Duration::from_secs(60),
        }
    }
}

/// Message types for the gossip protocol
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryMessage {
    /// Gossip message containing actor registry state
    Gossip {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, (ActorLocation, u64)>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    },
    /// Response to gossip with our own state
    GossipResponse {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, (ActorLocation, u64)>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    },
    /// Health check ping
    Ping { sender_id: NodeId, timestamp: u64 },
    /// Health check pong
    Pong { sender_id: NodeId, timestamp: u64 },
}

/// Location of an actor in the network
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ActorLocation {
    pub node_id: NodeId,
    pub address: SocketAddr,
}

impl ActorLocation {
    pub fn new(node_id: NodeId, address: SocketAddr) -> Self {
        Self { node_id, address }
    }
}

/// Statistics about the gossip registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStats {
    pub node_id: NodeId,
    pub local_actors: usize,
    pub known_actors: usize,
    pub active_peers: usize,
    pub failed_peers: usize,
    pub total_gossip_rounds: u64,
    pub current_sequence: u64,
    pub uptime_seconds: u64,
    pub last_gossip_timestamp: u64,
}

/// Peer information with failure tracking
#[derive(Debug, Clone)]
struct PeerInfo {
    address: SocketAddr,
    failures: usize,
    last_attempt: u64,
    last_success: u64,
    last_sequence: u64,
}

/// Core gossip registry implementation
pub struct GossipRegistry {
    node_id: NodeId,
    bind_addr: SocketAddr,
    config: GossipConfig,
    local_actors: HashMap<String, ActorLocation>,
    known_actors: HashMap<String, (ActorLocation, u64)>,
    peers: HashMap<SocketAddr, PeerInfo>,
    gossip_sequence: u64,
    start_time: u64,
    shutdown: bool,
}

impl GossipRegistry {
    /// Create a new gossip registry
    pub fn new(node_id: NodeId, bind_addr: SocketAddr, config: GossipConfig) -> Self {
        info!(
            node_id = %node_id,
            bind_addr = %bind_addr,
            "creating new gossip registry"
        );

        Self {
            node_id,
            bind_addr,
            config,
            local_actors: HashMap::new(),
            known_actors: HashMap::new(),
            peers: HashMap::new(),
            gossip_sequence: 0,
            start_time: current_timestamp(),
            shutdown: false,
        }
    }

    /// Add bootstrap peers for initial connection
    pub fn add_bootstrap_peers(&mut self, bootstrap_peers: Vec<SocketAddr>) {
        for peer in bootstrap_peers {
            if peer != self.bind_addr {
                self.peers.insert(
                    peer,
                    PeerInfo {
                        address: peer,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                    },
                );
                debug!(peer = %peer, "added bootstrap peer");
            }
        }
        info!(peer_count = self.peers.len(), "added bootstrap peers");
    }

    /// Add a new peer (called when receiving connections)
    pub fn add_peer(&mut self, peer_addr: SocketAddr) {
        if peer_addr != self.bind_addr && !self.peers.contains_key(&peer_addr) {
            let current_time = current_timestamp();
            self.peers.insert(
                peer_addr,
                PeerInfo {
                    address: peer_addr,
                    failures: 0,
                    last_attempt: current_time,
                    last_success: current_time,
                    last_sequence: 0,
                },
            );
            debug!(peer = %peer_addr, "added new peer");
        }
    }

    /// Register a local actor
    pub fn register_actor(&mut self, name: String, location: ActorLocation) -> Result<()> {
        if self.shutdown {
            return Err(GossipError::Shutdown);
        }

        info!(
            actor_name = %name,
            node_id = %location.node_id,
            address = %location.address,
            "registering local actor"
        );

        self.local_actors.insert(name, location);
        Ok(())
    }

    /// Unregister a local actor
    pub fn unregister_actor(&mut self, name: &str) -> Result<Option<ActorLocation>> {
        if self.shutdown {
            return Err(GossipError::Shutdown);
        }

        let removed = self.local_actors.remove(name);
        if removed.is_some() {
            info!(actor_name = %name, "unregistered local actor");
        }
        Ok(removed)
    }

    /// Lookup an actor by name
    pub fn lookup_actor(&self, name: &str) -> Option<ActorLocation> {
        // Check local actors first
        if let Some(location) = self.local_actors.get(name) {
            debug!(actor_name = %name, location = "local", "actor found");
            return Some(location.clone());
        }

        // Check known remote actors
        if let Some((location, timestamp)) = self.known_actors.get(name) {
            let now = current_timestamp();
            let age_secs = now.saturating_sub(*timestamp);
            if age_secs < self.config.actor_ttl.as_secs() {
                debug!(
                    actor_name = %name,
                    location = "remote",
                    age_seconds = age_secs,
                    "actor found"
                );
                return Some(location.clone());
            } else {
                debug!(
                    actor_name = %name,
                    age_seconds = age_secs,
                    ttl_seconds = self.config.actor_ttl.as_secs(),
                    "actor found but expired"
                );
            }
        }

        debug!(actor_name = %name, "actor not found");
        None
    }

    /// Get registry statistics
    pub fn get_stats(&self) -> RegistryStats {
        let current_time = current_timestamp();
        let active_peers = self
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .count();
        let failed_peers = self.peers.len() - active_peers;

        RegistryStats {
            node_id: self.node_id,
            local_actors: self.local_actors.len(),
            known_actors: self.known_actors.len(),
            active_peers,
            failed_peers,
            total_gossip_rounds: self.gossip_sequence,
            current_sequence: self.gossip_sequence,
            uptime_seconds: current_time.saturating_sub(self.start_time),
            last_gossip_timestamp: current_time,
        }
    }

    /// Perform one round of gossip with random peers
    #[instrument(skip(self), fields(node_id = %self.node_id))]
    pub async fn gossip_round(&mut self) -> Result<()> {
        if self.shutdown {
            return Err(GossipError::Shutdown);
        }

        let current_time = current_timestamp();

        // Get available peers (not failed, or failed but ready for retry)
        let available_peers: Vec<SocketAddr> = self
            .peers
            .values()
            .filter(|peer| {
                peer.failures < self.config.max_peer_failures
                    || (current_time - peer.last_attempt)
                        > self.config.peer_retry_interval.as_secs()
            })
            .map(|peer| peer.address)
            .collect();

        if available_peers.is_empty() {
            debug!("no available peers for gossip");
            return Ok(());
        }

        // Select random peers for gossip using better randomization
        let num_peers = std::cmp::min(self.config.max_gossip_peers, available_peers.len());
        let mut selected_peers = Vec::new();

        // Use a more sophisticated peer selection algorithm
        for i in 0..num_peers {
            let idx = (self.gossip_sequence as usize * 7 + i * 11) % available_peers.len();
            selected_peers.push(available_peers[idx]);
        }

        self.gossip_sequence += 1;

        let gossip_msg = RegistryMessage::Gossip {
            local_actors: self.local_actors.clone(),
            known_actors: self.known_actors.clone(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            timestamp: current_time,
            sequence: self.gossip_sequence,
        };

        debug!(
            peer_count = selected_peers.len(),
            sequence = self.gossip_sequence,
            "starting gossip round"
        );

        for peer in selected_peers {
            if let Err(err) = self.send_gossip_to_peer(peer, &gossip_msg).await {
                warn!(peer = %peer, error = %err, "failed to gossip to peer");
                if let Some(peer_info) = self.peers.get_mut(&peer) {
                    peer_info.failures += 1;
                    peer_info.last_attempt = current_time;
                }
            } else if let Some(peer_info) = self.peers.get_mut(&peer) {
                peer_info.failures = 0;
                peer_info.last_success = current_time;
                peer_info.last_attempt = current_time;
            }
        }

        Ok(())
    }

    /// Send gossip to a specific peer and handle response
    #[instrument(skip(self, msg), fields(node_id = %self.node_id, peer = %addr))]
    async fn send_gossip_to_peer(&mut self, addr: SocketAddr, msg: &RegistryMessage) -> Result<()> {
        let mut stream =
            tokio::time::timeout(self.config.connection_timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| GossipError::Timeout)?
                .map_err(GossipError::Network)?;

        // Send our gossip
        send_message(&mut stream, msg, self.config.max_message_size).await?;

        // Wait for response
        match tokio::time::timeout(
            self.config.response_timeout,
            read_message(&mut stream, self.config.max_message_size),
        )
        .await
        {
            Ok(Ok(RegistryMessage::GossipResponse {
                local_actors,
                known_actors,
                sender_id,
                sender_addr,
                timestamp,
                sequence,
            })) => {
                debug!(
                    peer = %addr,
                    remote_node = %sender_id,
                    sequence = sequence,
                    "received gossip response"
                );

                self.merge_gossip(
                    local_actors,
                    known_actors,
                    sender_id,
                    sender_addr,
                    timestamp,
                    sequence,
                );
            }
            Ok(Ok(_)) => {
                warn!(peer = %addr, "received unexpected message type");
            }
            Ok(Err(err)) => {
                return Err(err);
            }
            Err(_) => {
                debug!(peer = %addr, "gossip response timeout");
                // Timeout is not necessarily an error for gossip
            }
        }

        Ok(())
    }

    /// Merge incoming gossip data with sequence-based deduplication
    #[instrument(skip(self, remote_local, remote_known), fields(
        node_id = %self.node_id,
        sender = %sender_id,
        local_count = remote_local.len(),
        known_count = remote_known.len(),
        sequence = sequence
    ))]
    pub fn merge_gossip(
        &mut self,
        remote_local: HashMap<String, ActorLocation>,
        remote_known: HashMap<String, (ActorLocation, u64)>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    ) {
        // Check if we've already processed this or a newer sequence from this peer
        // This prevents duplicate processing and ensures gossip efficiency
        if let Some(peer_info) = self.peers.get(&sender_addr) {
            if sequence <= peer_info.last_sequence {
                debug!(
                    last_sequence = peer_info.last_sequence,
                    received_sequence = sequence,
                    "ignoring duplicate or old gossip message"
                );
                return;
            }
        }

        // Add the sender as a peer and update sequence
        self.add_peer(sender_addr);
        if let Some(peer_info) = self.peers.get_mut(&sender_addr) {
            peer_info.last_sequence = sequence;
        }

        let mut new_actors = 0;
        let mut updated_actors = 0;

        // Merge remote local actors (treat as fresh with current timestamp)
        for (name, location) in remote_local {
            if !self.local_actors.contains_key(&name) {
                self.known_actors
                    .insert(name.clone(), (location, timestamp));
                new_actors += 1;
            }
        }

        // Merge remote known actors (keep newest based on timestamp)
        for (name, (location, ts)) in remote_known {
            if self.local_actors.contains_key(&name) {
                continue; // Don't override local actors
            }

            match self.known_actors.get(&name) {
                Some((_, existing_ts)) if ts > *existing_ts => {
                    self.known_actors.insert(name, (location, ts));
                    updated_actors += 1;
                }
                None => {
                    self.known_actors.insert(name, (location, ts));
                    new_actors += 1;
                }
                _ => {} // Our info is newer or same
            }
        }

        debug!(
            new_actors = new_actors,
            updated_actors = updated_actors,
            "merged gossip data"
        );
    }

    /// Create a gossip response message
    pub fn create_gossip_response(&self) -> RegistryMessage {
        RegistryMessage::GossipResponse {
            local_actors: self.local_actors.clone(),
            known_actors: self.known_actors.clone(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            timestamp: current_timestamp(),
            sequence: self.gossip_sequence,
        }
    }

    /// Clean up stale actor entries
    #[instrument(skip(self), fields(node_id = %self.node_id))]
    pub fn cleanup_stale_actors(&mut self) {
        let now = current_timestamp();
        let ttl_secs = self.config.actor_ttl.as_secs();

        let before_count = self.known_actors.len();
        self.known_actors
            .retain(|_, (_, timestamp)| now - *timestamp < ttl_secs);

        let removed = before_count - self.known_actors.len();
        if removed > 0 {
            info!(removed_count = removed, "cleaned up stale actor entries");
        }
    }

    /// Shutdown the registry
    pub fn shutdown(&mut self) {
        info!("shutting down gossip registry");
        self.shutdown = true;
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }
}

/// Message framing functions with size limits
async fn send_message(
    stream: &mut TcpStream,
    msg: &RegistryMessage,
    max_size: usize,
) -> Result<()> {
    let data = bincode::serialize(msg)?;

    if data.len() > max_size {
        return Err(GossipError::MessageTooLarge {
            size: data.len(),
            max: max_size,
        });
    }

    let len = data.len() as u32;

    // Send length header
    stream.write_all(&len.to_be_bytes()).await?;
    // Send message data
    stream.write_all(&data).await?;
    stream.flush().await?;

    Ok(())
}

async fn read_message(stream: &mut TcpStream, max_size: usize) -> Result<RegistryMessage> {
    // Read length header
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Check message size
    if len > max_size {
        return Err(GossipError::MessageTooLarge {
            size: len,
            max: max_size,
        });
    }

    // Read message data
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;

    let msg = bincode::deserialize(&data)?;
    Ok(msg)
}

/// Handle incoming TCP connections
#[instrument(skip(stream, registry), fields(peer = %peer_addr))]
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    registry: Arc<Mutex<GossipRegistry>>,
) {
    debug!("handling new connection");

    let max_message_size = {
        let reg = registry.lock().await;
        reg.config.max_message_size
    };

    match read_message(&mut stream, max_message_size).await {
        Ok(msg) => match msg {
            RegistryMessage::Gossip {
                local_actors,
                known_actors,
                sender_id,
                sender_addr,
                timestamp,
                sequence,
            } => {
                debug!(
                    sender = %sender_id,
                    sequence = sequence,
                    "received gossip message"
                );

                let response = {
                    let mut reg = registry.lock().await;
                    reg.merge_gossip(
                        local_actors,
                        known_actors,
                        sender_id,
                        sender_addr,
                        timestamp,
                        sequence,
                    );
                    reg.create_gossip_response()
                };

                if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                    warn!(error = %err, "failed to send gossip response");
                } else {
                    debug!("gossip exchange completed");
                }
            }
            RegistryMessage::GossipResponse {
                local_actors,
                known_actors,
                sender_id,
                sender_addr,
                timestamp,
                sequence,
            } => {
                debug!(
                    sender = %sender_id,
                    sequence = sequence,
                    "received gossip response"
                );

                let mut reg = registry.lock().await;
                reg.merge_gossip(
                    local_actors,
                    known_actors,
                    sender_id,
                    sender_addr,
                    timestamp,
                    sequence,
                );
            }
            RegistryMessage::Ping {
                sender_id,
                timestamp: _,
            } => {
                debug!(sender = %sender_id, "received ping");

                let node_id = {
                    let reg = registry.lock().await;
                    reg.node_id
                };

                let pong = RegistryMessage::Pong {
                    sender_id: node_id,
                    timestamp: current_timestamp(),
                };

                if let Err(err) = send_message(&mut stream, &pong, max_message_size).await {
                    warn!(error = %err, "failed to send pong");
                } else {
                    debug!("ping-pong completed");
                }
            }
            RegistryMessage::Pong {
                sender_id,
                timestamp: _,
            } => {
                debug!(sender = %sender_id, "received pong");
            }
        },
        Err(err) => {
            warn!(error = %err, "failed to read message");
        }
    }

    debug!("connection closed");
}

/// Start the gossip registry server
#[instrument(skip(registry))]
async fn start_gossip_server(registry: Arc<Mutex<GossipRegistry>>) -> Result<()> {
    let (bind_addr, node_id) = {
        let reg = registry.lock().await;
        (reg.bind_addr, reg.node_id)
    };

    let listener = TcpListener::bind(bind_addr).await?;
    info!(bind_addr = %bind_addr, node_id = %node_id, "gossip server started");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let registry_clone = registry.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer_addr, registry_clone).await;
                });
            }
            Err(err) => {
                error!(error = %err, "failed to accept connection");
            }
        }
    }
}

/// Start the gossip timer
#[instrument(skip(registry))]
async fn start_gossip_timer(registry: Arc<Mutex<GossipRegistry>>) {
    let (gossip_interval, cleanup_interval, _node_id) = {
        let reg = registry.lock().await;
        (
            reg.config.gossip_interval,
            reg.config.cleanup_interval,
            reg.node_id,
        )
    };

    let mut gossip_timer = interval(gossip_interval);
    let mut cleanup_timer = interval(cleanup_interval);

    info!(
        gossip_interval_secs = gossip_interval.as_secs(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
        "gossip timer started"
    );

    loop {
        tokio::select! {
            _ = gossip_timer.tick() => {
                let mut reg = registry.lock().await;
                if reg.is_shutdown() {
                    break;
                }
                if let Err(err) = reg.gossip_round().await {
                    error!(error = %err, "gossip round failed");
                }
            }
            _ = cleanup_timer.tick() => {
                let mut reg = registry.lock().await;
                if reg.is_shutdown() {
                    break;
                }
                reg.cleanup_stale_actors();
            }
        }
    }

    info!("gossip timer stopped");
}

/// Bootstrap by connecting to known peers and doing initial gossip
#[instrument(skip(registry))]
async fn bootstrap_peers(registry: Arc<Mutex<GossipRegistry>>) -> Result<()> {
    let (node_id, bind_addr, peers, config) = {
        let reg = registry.lock().await;
        (
            reg.node_id,
            reg.bind_addr,
            reg.peers.keys().cloned().collect::<Vec<_>>(),
            reg.config.clone(),
        )
    };

    info!(peer_count = peers.len(), "starting bootstrap");

    for peer in peers {
        let gossip_msg = {
            let reg = registry.lock().await;
            RegistryMessage::Gossip {
                local_actors: reg.local_actors.clone(),
                known_actors: reg.known_actors.clone(),
                sender_id: node_id,
                sender_addr: bind_addr,
                timestamp: current_timestamp(),
                sequence: reg.gossip_sequence,
            }
        };

        match tokio::time::timeout(config.connection_timeout, TcpStream::connect(peer)).await {
            Ok(Ok(mut stream)) => {
                debug!(peer = %peer, "connected to bootstrap peer");

                if let Err(err) =
                    send_message(&mut stream, &gossip_msg, config.max_message_size).await
                {
                    warn!(peer = %peer, error = %err, "failed to send bootstrap gossip");
                    continue;
                }

                match tokio::time::timeout(
                    config.response_timeout,
                    read_message(&mut stream, config.max_message_size),
                )
                .await
                {
                    Ok(Ok(RegistryMessage::GossipResponse {
                        local_actors,
                        known_actors,
                        sender_id: response_sender_id,
                        sender_addr: response_sender_addr,
                        timestamp,
                        sequence,
                    })) => {
                        debug!(
                            peer = %peer,
                            remote_node = %response_sender_id,
                            "received bootstrap response"
                        );

                        let mut reg = registry.lock().await;
                        reg.merge_gossip(
                            local_actors,
                            known_actors,
                            response_sender_id,
                            response_sender_addr,
                            timestamp,
                            sequence,
                        );
                    }
                    Ok(Ok(_)) => {
                        warn!(peer = %peer, "unexpected response from bootstrap peer");
                    }
                    Ok(Err(err)) => {
                        warn!(peer = %peer, error = %err, "failed to read bootstrap response");
                    }
                    Err(_) => {
                        debug!(peer = %peer, "bootstrap response timeout");
                    }
                }
            }
            Ok(Err(err)) => {
                warn!(peer = %peer, error = %err, "failed to connect to bootstrap peer");
            }
            Err(_) => {
                warn!(peer = %peer, "bootstrap connection timeout");
            }
        }
    }

    info!("bootstrap completed");
    Ok(())
}

/// Get current timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Main API for the gossip registry
pub struct GossipRegistryHandle {
    registry: Arc<Mutex<GossipRegistry>>,
    _server_handle: tokio::task::JoinHandle<()>,
    _timer_handle: tokio::task::JoinHandle<()>,
}

impl GossipRegistryHandle {
    /// Create and start a new gossip registry
    #[instrument(skip(peers, config))]
    pub async fn new(
        node_id: NodeId,
        bind_addr: SocketAddr,
        peers: Vec<SocketAddr>,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let config = config.unwrap_or_default();
        let mut registry = GossipRegistry::new(node_id, bind_addr, config);
        registry.add_bootstrap_peers(peers);

        let registry = Arc::new(Mutex::new(registry));

        // Start the server
        let server_registry = registry.clone();
        let server_handle = tokio::spawn(async move {
            if let Err(err) = start_gossip_server(server_registry).await {
                error!(error = %err, "server error");
            }
        });

        // Start the gossip timer
        let timer_registry = registry.clone();
        let timer_handle = tokio::spawn(async move {
            start_gossip_timer(timer_registry).await;
        });

        // Bootstrap after a brief delay to let server start
        let bootstrap_registry = registry.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if let Err(err) = bootstrap_peers(bootstrap_registry).await {
                error!(error = %err, "bootstrap error");
            }
        });

        info!(node_id = %node_id, bind_addr = %bind_addr, "gossip registry started");

        Ok(Self {
            registry,
            _server_handle: server_handle,
            _timer_handle: timer_handle,
        })
    }

    /// Register a local actor
    pub async fn register(&self, name: String, location: ActorLocation) -> Result<()> {
        let mut reg = self.registry.lock().await;
        reg.register_actor(name, location)
    }

    /// Unregister a local actor
    pub async fn unregister(&self, name: &str) -> Result<Option<ActorLocation>> {
        let mut reg = self.registry.lock().await;
        reg.unregister_actor(name)
    }

    /// Lookup an actor
    pub async fn lookup(&self, name: &str) -> Option<ActorLocation> {
        let reg = self.registry.lock().await;
        reg.lookup_actor(name)
    }

    /// Get registry statistics
    pub async fn stats(&self) -> RegistryStats {
        let reg = self.registry.lock().await;
        reg.get_stats()
    }

    /// Send a ping to a specific peer
    pub async fn ping(&self, peer: SocketAddr) -> Result<Duration> {
        let (node_id, config) = {
            let reg = self.registry.lock().await;
            (reg.node_id, reg.config.clone())
        };

        let start = std::time::Instant::now();

        let mut stream = tokio::time::timeout(config.connection_timeout, TcpStream::connect(peer))
            .await
            .map_err(|_| GossipError::Timeout)?
            .map_err(GossipError::Network)?;

        let ping = RegistryMessage::Ping {
            sender_id: node_id,
            timestamp: current_timestamp(),
        };

        send_message(&mut stream, &ping, config.max_message_size).await?;

        match tokio::time::timeout(
            config.response_timeout,
            read_message(&mut stream, config.max_message_size),
        )
        .await
        {
            Ok(Ok(RegistryMessage::Pong { .. })) => Ok(start.elapsed()),
            Ok(Ok(_)) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected response to ping",
            ))),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(GossipError::Timeout),
        }
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        let mut reg = self.registry.lock().await;
        reg.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;
    use tracing_test::traced_test;

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn test_basic_gossip() -> Result<()> {
        let node_id = NodeId::new();
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            ..Default::default()
        };

        // Start first node
        let node1_addr = "127.0.0.1:9095".parse().unwrap();
        let node1 =
            GossipRegistryHandle::new(node_id, node1_addr, vec![], Some(config.clone())).await?;

        // Register an actor on node1
        node1
            .register(
                "test_actor".to_string(),
                ActorLocation::new(node_id, node1_addr),
            )
            .await?;

        let _node2 = GossipRegistryHandle::new(
            NodeId::new(),
            "127.0.0.1:9096".parse().unwrap(),
            vec![node1_addr],
            Some(config),
        )
        .await?;

        // Wait a bit
        sleep(Duration::from_millis(200)).await;

        // Check stats
        let stats1 = node1.stats().await;
        assert_eq!(stats1.local_actors, 1);
        assert!(stats1.current_sequence > 0); // Should have done some gossip rounds

        // Lookup the actor
        let found = node1.lookup("test_actor").await;
        assert!(found.is_some());

        // Shutdown
        node1.shutdown().await;

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_node_id_conversion() -> Result<()> {
        let node_id = NodeId::new();
        let node_str = node_id.to_string();
        let parsed = node_str.parse()?;
        assert_eq!(node_id, parsed);

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_sequence_deduplication() -> Result<()> {
        let node_id1 = NodeId::new();
        let node_id2 = NodeId::new();

        let mut registry = GossipRegistry::new(
            node_id1,
            "127.0.0.1:8001".parse().unwrap(),
            GossipConfig::default(),
        );

        let peer_addr = "127.0.0.1:8002".parse().unwrap();
        let current_time = current_timestamp();

        // First gossip message with sequence 1
        registry.merge_gossip(
            HashMap::new(),
            HashMap::new(),
            node_id2,
            peer_addr,
            current_time,
            1,
        );

        let actors_count1 = registry.known_actors.len();

        // Same gossip message with sequence 1 again - should be ignored
        registry.merge_gossip(
            [("test".to_string(), ActorLocation::new(node_id2, peer_addr))].into(),
            HashMap::new(),
            node_id2,
            peer_addr,
            current_time + 1,
            1,
        );

        // Should not have added the actor due to sequence deduplication
        assert_eq!(registry.known_actors.len(), actors_count1);

        // New gossip message with sequence 2 - should be processed
        registry.merge_gossip(
            [("test".to_string(), ActorLocation::new(node_id2, peer_addr))].into(),
            HashMap::new(),
            node_id2,
            peer_addr,
            current_time + 2,
            2,
        );

        // Should have added the actor this time
        assert_eq!(registry.known_actors.len(), actors_count1 + 1);
        assert!(registry.lookup_actor("test").is_some());

        Ok(())
    }
}
