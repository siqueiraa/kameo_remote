use rand::seq::IteratorRandom;
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
use tokio::time::{interval, Instant};
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

    #[error("delta too old: requested {requested}, oldest available {oldest}")]
    DeltaTooOld { requested: u64, oldest: u64 },

    #[error("full sync required")]
    FullSyncRequired,
}

pub type Result<T> = std::result::Result<T, GossipError>;

/// Connection pool for reusing TCP connections across gossip rounds
pub struct ConnectionPool {
    connections: HashMap<SocketAddr, PooledConnection>,
    max_connections: usize,
    connection_timeout: Duration,
    connection_idle_timeout: Duration,
}

#[derive(Debug)]
struct PooledConnection {
    stream: Option<TcpStream>, // None when checked out
    last_used: u64,
    failure_count: usize,
    checked_out: bool,
}

/// A connection checked out from the pool
pub struct CheckedOutConnection {
    stream: TcpStream,
    addr: SocketAddr,
    was_pooled: bool, // Track if this came from pool or was newly created
}

impl CheckedOutConnection {
    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl ConnectionPool {
    pub fn new(
        max_connections: usize,
        connection_timeout: Duration,
        connection_idle_timeout: Duration,
    ) -> Self {
        Self {
            connections: HashMap::new(),
            max_connections,
            connection_timeout,
            connection_idle_timeout,
        }
    }

    /// Check out a connection, creating one if needed
    pub async fn checkout_connection(&mut self, addr: SocketAddr) -> Result<CheckedOutConnection> {
        let current_time = current_timestamp();

        // Try to reuse existing healthy connection
        if let Some(pooled) = self.connections.get_mut(&addr) {
            if pooled.failure_count == 0 && !pooled.checked_out {
                if let Some(stream) = pooled.stream.take() {
                    pooled.checked_out = true;
                    pooled.last_used = current_time;
                    debug!(peer = %addr, "reusing pooled connection");

                    return Ok(CheckedOutConnection {
                        stream,
                        addr,
                        was_pooled: true,
                    });
                }
            } else if pooled.failure_count > 0 {
                // Remove failed connection
                debug!(peer = %addr, failures = pooled.failure_count, "removing failed connection");
                self.connections.remove(&addr);
            }
        }

        // Need to create new connection
        self.create_new_connection(addr).await
    }

    /// Return a connection to the pool (call this when operation succeeds)
    pub fn checkin_success(&mut self, conn: CheckedOutConnection) {
        if !conn.was_pooled {
            // This was a new connection, add it to pool if we have space
            if self.connections.len() < self.max_connections {
                self.connections.insert(
                    conn.addr,
                    PooledConnection {
                        stream: Some(conn.stream),
                        last_used: current_timestamp(),
                        failure_count: 0,
                        checked_out: false,
                    },
                );
                debug!(peer = %conn.addr, "added new connection to pool");
            } else {
                debug!(peer = %conn.addr, "pool full, dropping new connection");
                // Connection will be dropped
            }
        } else {
            // Return existing connection to pool
            if let Some(pooled) = self.connections.get_mut(&conn.addr) {
                pooled.stream = Some(conn.stream);
                pooled.checked_out = false;
                pooled.failure_count = 0; // Reset on success
                pooled.last_used = current_timestamp();
                debug!(peer = %conn.addr, "returned connection to pool");
            }
        }
    }

    /// Handle connection failure (call this when operation fails)
    pub fn checkin_failure(&mut self, conn: CheckedOutConnection) {
        debug!(peer = %conn.addr, "connection failed, not returning to pool");

        if conn.was_pooled {
            // Mark the pool entry as failed for cleanup
            if let Some(pooled) = self.connections.get_mut(&conn.addr) {
                pooled.failure_count += 1;
                pooled.checked_out = false;
                pooled.stream = None; // Don't put failed stream back
            }
        }
        // Failed connection will be dropped
    }

    /// Clean up old and failed connections
    pub fn cleanup_stale_connections(&mut self) {
        let current_time = current_timestamp();
        let stale_threshold = self.connection_idle_timeout.as_secs();

        let addrs_to_remove: Vec<SocketAddr> = self
            .connections
            .iter()
            .filter(|(_, pooled)| {
                // Remove if failed, checked out too long, or unused for too long
                pooled.failure_count > 0
                    || (current_time - pooled.last_used) > stale_threshold
                    || (pooled.checked_out && (current_time - pooled.last_used) > 60)
                // Checked out > 1 min
            })
            .map(|(addr, _)| *addr)
            .collect();

        for addr in addrs_to_remove {
            self.connections.remove(&addr);
            debug!(peer = %addr, "cleaned up stale connection");
        }
    }

    async fn create_new_connection(&mut self, addr: SocketAddr) -> Result<CheckedOutConnection> {
        debug!(peer = %addr, "creating new connection");

        let stream = tokio::time::timeout(self.connection_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| GossipError::Timeout)?
            .map_err(GossipError::Network)?;

        Ok(CheckedOutConnection {
            stream,
            addr,
            was_pooled: false,
        })
    }

    /// Get pool statistics
    pub fn stats(&self) -> (usize, usize, usize) {
        let total = self.connections.len();
        let available = self
            .connections
            .values()
            .filter(|p| !p.checked_out && p.failure_count == 0)
            .count();
        let failed = self
            .connections
            .values()
            .filter(|p| p.failure_count > 0)
            .count();
        (total, available, failed)
    }
}

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
    /// Maximum number of deltas to keep in history
    pub max_delta_history: usize,
    /// Force full sync after this many delta exchanges
    pub full_sync_interval: u64,
    /// Maximum number of pooled connections
    pub max_pooled_connections: usize,
    /// Idle connection timeout for pool
    pub idle_connection_timeout: Duration,
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
            max_delta_history: 100,
            full_sync_interval: 50,     // Force full sync every 50 deltas
            max_pooled_connections: 20, // Allow up to 20 pooled connections
            idle_connection_timeout: Duration::from_secs(300),
        }
    }
}

/// Registry change types for delta tracking
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryChange {
    /// Actor was added or updated
    ActorAdded {
        name: String,
        location: ActorLocation,
        timestamp: u64,
    },
    /// Actor was removed
    ActorRemoved { name: String, timestamp: u64 },
}

/// Delta representing changes since a specific sequence number
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryDelta {
    pub since_sequence: u64,
    pub current_sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub sender_id: NodeId,
    pub sender_addr: SocketAddr,
    pub timestamp: u64,
}

/// Message types for the gossip protocol
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryMessage {
    /// Delta gossip message containing only changes
    DeltaGossip { delta: RegistryDelta },
    /// Response to delta gossip with our own delta
    DeltaGossipResponse { delta: RegistryDelta },
    /// Request for full sync (fallback when deltas are unavailable)
    FullSyncRequest {
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    },
    /// Full synchronization message (legacy format + bootstrap)
    FullSync {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, (ActorLocation, u64)>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    },
    /// Response to full sync
    FullSyncResponse {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, (ActorLocation, u64)>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    },
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
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub delta_history_size: usize,
    pub avg_delta_size: f64,
}

/// Peer information with failure tracking and delta state
#[derive(Debug, Clone)]
struct PeerInfo {
    address: SocketAddr,
    failures: usize,
    last_attempt: u64,
    last_success: u64,
    last_sequence: u64,
    /// Last sequence we successfully sent to this peer
    last_sent_sequence: u64,
    /// Number of consecutive delta exchanges with this peer
    consecutive_deltas: u64,
}

/// Historical delta for efficient incremental updates
#[derive(Debug, Clone)]
struct HistoricalDelta {
    sequence: u64,
    changes: Vec<RegistryChange>,
    timestamp: u64,
}

/// Data needed to perform gossip with a single peer
#[derive(Debug)]
struct GossipTask {
    peer_addr: SocketAddr,
    message: RegistryMessage,
    current_sequence: u64,
}

/// Result of a gossip operation
#[derive(Debug)]
struct GossipResult {
    peer_addr: SocketAddr,
    sent_sequence: u64,
    outcome: Result<Option<RegistryMessage>>,
}

/// Core gossip registry implementation with delta support
pub struct GossipRegistry {
    node_id: NodeId,
    bind_addr: SocketAddr,
    config: GossipConfig,
    connection_pool: ConnectionPool,
    local_actors: HashMap<String, ActorLocation>,
    known_actors: HashMap<String, (ActorLocation, u64)>,
    peers: HashMap<SocketAddr, PeerInfo>,
    gossip_sequence: u64,
    start_time: u64,
    shutdown: bool,

    // Delta tracking
    delta_history: Vec<HistoricalDelta>,
    pending_changes: Vec<RegistryChange>,
    delta_exchanges: u64,
    full_sync_exchanges: u64,
}

impl GossipRegistry {
    /// Create a new gossip registry
    pub fn new(node_id: NodeId, bind_addr: SocketAddr, config: GossipConfig) -> Self {
        info!(
            node_id = %node_id,
            bind_addr = %bind_addr,
            max_delta_history = config.max_delta_history,
            "creating new gossip registry with delta support"
        );

        let connection_pool = ConnectionPool::new(
            config.max_pooled_connections,
            config.connection_timeout,
            config.idle_connection_timeout,
        );

        Self {
            node_id,
            bind_addr,
            config,
            connection_pool,
            local_actors: HashMap::new(),
            known_actors: HashMap::new(),
            peers: HashMap::new(),
            gossip_sequence: 0,
            start_time: current_timestamp(),
            shutdown: false,
            delta_history: Vec::new(),
            pending_changes: Vec::new(),
            delta_exchanges: 0,
            full_sync_exchanges: 0,
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
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
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
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
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

        let timestamp = current_timestamp();

        // Track this change for delta gossip
        self.pending_changes.push(RegistryChange::ActorAdded {
            name: name.clone(),
            location: location.clone(),
            timestamp,
        });

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

            // Track this change for delta gossip
            self.pending_changes.push(RegistryChange::ActorRemoved {
                name: name.to_string(),
                timestamp: current_timestamp(),
            });
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

        let avg_delta_size = if self.delta_exchanges > 0 {
            self.delta_history
                .iter()
                .map(|d| d.changes.len())
                .sum::<usize>() as f64
                / self.delta_history.len() as f64
        } else {
            0.0
        };

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
            delta_exchanges: self.delta_exchanges,
            full_sync_exchanges: self.full_sync_exchanges,
            delta_history_size: self.delta_history.len(),
            avg_delta_size,
        }
    }

    /// Create a delta containing changes since the specified sequence
    fn create_delta(&self, since_sequence: u64) -> Result<RegistryDelta> {
        let mut changes = Vec::new();
        let current_time = current_timestamp();

        // If this is a brand new peer (since_sequence = 0), include all actors we know about
        if since_sequence == 0 {
            // Include all local actors as additions
            for (name, location) in &self.local_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    timestamp: current_time,
                });
            }

            // Include all known remote actors as additions
            for (name, (location, timestamp)) in &self.known_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    timestamp: *timestamp,
                });
            }
        }

        // Include pending changes from current round
        changes.extend(self.pending_changes.clone());

        // Include historical changes since the requested sequence
        for delta in &self.delta_history {
            if delta.sequence > since_sequence {
                changes.extend(delta.changes.clone());
            }
        }

        // Remove duplicates (keep most recent change for each actor)
        let mut deduped_changes: HashMap<String, RegistryChange> = HashMap::new();
        for change in changes {
            match &change {
                RegistryChange::ActorAdded { name, .. }
                | RegistryChange::ActorRemoved { name, .. } => {
                    // For duplicates, keep the one with the latest timestamp
                    match deduped_changes.get(name) {
                        Some(existing) => {
                            let existing_ts = match existing {
                                RegistryChange::ActorAdded { timestamp, .. }
                                | RegistryChange::ActorRemoved { timestamp, .. } => *timestamp,
                            };
                            let new_ts = match &change {
                                RegistryChange::ActorAdded { timestamp, .. }
                                | RegistryChange::ActorRemoved { timestamp, .. } => *timestamp,
                            };
                            if new_ts >= existing_ts {
                                deduped_changes.insert(name.clone(), change);
                            }
                        }
                        None => {
                            deduped_changes.insert(name.clone(), change);
                        }
                    }
                }
            }
        }

        Ok(RegistryDelta {
            since_sequence,
            current_sequence: self.gossip_sequence,
            changes: deduped_changes.into_values().collect(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            timestamp: current_time,
        })
    }

    /// Apply a delta to the local state
    fn apply_delta(&mut self, delta: RegistryDelta) -> Result<()> {
        let mut applied_changes = 0;

        for change in delta.changes {
            match change {
                RegistryChange::ActorAdded {
                    name,
                    location,
                    timestamp,
                } => {
                    // Don't override local actors
                    if self.local_actors.contains_key(&name) {
                        continue;
                    }

                    // Only apply if this is newer than what we have
                    let should_apply = match self.known_actors.get(&name) {
                        Some((_, existing_ts)) => timestamp > *existing_ts,
                        None => true,
                    };

                    if should_apply {
                        self.known_actors.insert(name, (location, timestamp));
                        applied_changes += 1;
                    }
                }
                RegistryChange::ActorRemoved { name, timestamp } => {
                    // Don't remove local actors
                    if self.local_actors.contains_key(&name) {
                        continue;
                    }

                    // Remove if we have this actor and the removal is newer
                    if let Some((_, existing_ts)) = self.known_actors.get(&name) {
                        if timestamp > *existing_ts {
                            self.known_actors.remove(&name);
                            applied_changes += 1;
                        }
                    }
                }
            }
        }

        debug!(
            sender = %delta.sender_id,
            since_sequence = delta.since_sequence,
            current_sequence = delta.current_sequence,
            changes_applied = applied_changes,
            "applied delta changes"
        );

        Ok(())
    }

    /// Determine whether to use delta or full sync for a peer
    fn should_use_delta(&self, peer_info: &PeerInfo) -> bool {
        // For small clusters (≤ 5 total nodes), always use full sync to ensure
        // proper transitive propagation in star topologies
        let healthy_peers = self
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .count();
        let total_healthy_nodes = healthy_peers + 1;
        if total_healthy_nodes <= 5 {
            debug!(
                "using full sync for small cluster of {} healthy nodes",
                total_healthy_nodes
            );
            return false;
        }

        // Use full sync for new peers or if delta history is insufficient
        if peer_info.last_sequence == 0 {
            return false;
        }

        // Force full sync periodically
        if peer_info.consecutive_deltas >= self.config.full_sync_interval {
            return false;
        }

        // Check if we have the required delta history
        let oldest_available = self
            .delta_history
            .first()
            .map(|d| d.sequence)
            .unwrap_or(self.gossip_sequence);

        peer_info.last_sequence >= oldest_available
    }

    /// Prepare gossip data without doing any I/O
    fn prepare_gossip_round(&mut self) -> Result<Vec<GossipTask>> {
        if self.shutdown {
            return Err(GossipError::Shutdown);
        }

        let current_time = current_timestamp();

        // Commit pending changes to delta history
        let has_changes = !self.pending_changes.is_empty();
        if has_changes {
            self.gossip_sequence += 1;

            let delta = HistoricalDelta {
                sequence: self.gossip_sequence,
                changes: std::mem::take(&mut self.pending_changes),
                timestamp: current_time,
            };

            self.delta_history.push(delta);

            // Trim history to configured maximum
            if self.delta_history.len() > self.config.max_delta_history {
                self.delta_history.remove(0);
            }
        }

        // Get available peers
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
            return Ok(Vec::new());
        }

        // Select random peers for gossip
        let adaptive_fanout = std::cmp::min(
            std::cmp::max(3, (available_peers.len() as f64).log2().ceil() as usize),
            self.config.max_gossip_peers,
        );
        let mut rng = rand::rng();
        let selected_peers = available_peers
            .into_iter()
            .choose_multiple(&mut rng, adaptive_fanout);

        debug!(
            peer_count = selected_peers.len(),
            sequence = self.gossip_sequence,
            "preparing gossip round"
        );

        // Prepare gossip tasks for each peer
        let mut tasks = Vec::new();
        for peer_addr in selected_peers {
            let peer_info = self.peers.get(&peer_addr).cloned().unwrap_or(PeerInfo {
                address: peer_addr,
                failures: 0,
                last_attempt: 0,
                last_success: 0,
                last_sequence: 0,
                last_sent_sequence: 0,
                consecutive_deltas: 0,
            });

            let use_delta = self.should_use_delta(&peer_info);

            let message = if use_delta {
                match self.create_delta(peer_info.last_sequence) {
                    Ok(delta) => {
                        debug!(
                            peer = %peer_addr,
                            since_sequence = delta.since_sequence,
                            changes = delta.changes.len(),
                            "preparing delta gossip"
                        );
                        RegistryMessage::DeltaGossip { delta }
                    }
                    Err(_) => {
                        debug!(peer = %peer_addr, "delta creation failed, using full sync");
                        self.create_full_sync_message()
                    }
                }
            } else {
                debug!(peer = %peer_addr, "preparing full sync message");
                self.create_full_sync_message()
            };

            tasks.push(GossipTask {
                peer_addr,
                message,
                current_sequence: self.gossip_sequence,
            });
        }

        Ok(tasks)
    }

    /// Apply results from gossip tasks
    fn apply_gossip_results(&mut self, results: Vec<GossipResult>) {
        let current_time = current_timestamp();

        for result in results {
            match result.outcome {
                Ok(response_opt) => {
                    // Success case
                    if let Some(peer_info) = self.peers.get_mut(&result.peer_addr) {
                        peer_info.failures = 0;
                        peer_info.last_success = current_time;
                        peer_info.last_attempt = current_time;
                        peer_info.last_sent_sequence = result.sent_sequence;
                    }

                    // Process response if we got one
                    if let Some(response) = response_opt {
                        if let Err(err) =
                            self.handle_gossip_response_inner(result.peer_addr, response)
                        {
                            warn!(peer = %result.peer_addr, error = %err, "failed to handle gossip response");
                        }
                    }
                }
                Err(err) => {
                    // Failure case
                    warn!(peer = %result.peer_addr, error = %err, "failed to gossip to peer");
                    if let Some(peer_info) = self.peers.get_mut(&result.peer_addr) {
                        peer_info.failures += 1;
                        peer_info.last_attempt = current_time;
                    }
                }
            }
        }
    }

    /// Handle gossip response without async (for use in apply_gossip_results)
    fn handle_gossip_response_inner(
        &mut self,
        addr: SocketAddr,
        response: RegistryMessage,
    ) -> Result<()> {
        match response {
            RegistryMessage::DeltaGossipResponse { delta } => {
                debug!(
                    peer = %addr,
                    remote_node = %delta.sender_id,
                    changes = delta.changes.len(),
                    "received delta gossip response"
                );

                let delta_sequence = delta.current_sequence;
                self.apply_delta(delta)?;
                self.add_peer(addr);

                if let Some(peer_info) = self.peers.get_mut(&addr) {
                    peer_info.last_sequence = delta_sequence;
                    peer_info.consecutive_deltas += 1;
                }

                self.delta_exchanges += 1;
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_id,
                sender_addr,
                timestamp,
                sequence,
            } => {
                debug!(
                    peer = %addr,
                    remote_node = %sender_id,
                    sequence = sequence,
                    "received full sync response"
                );

                self.merge_full_sync(
                    local_actors,
                    known_actors,
                    sender_id,
                    sender_addr,
                    timestamp,
                    sequence,
                );

                if let Some(peer_info) = self.peers.get_mut(&addr) {
                    peer_info.consecutive_deltas = 0;
                    peer_info.last_sequence = sequence;
                }

                self.full_sync_exchanges += 1;
            }
            _ => {
                warn!(peer = %addr, "received unexpected message type in response");
            }
        }

        Ok(())
    }

    /// Create a full sync message (legacy format)
    fn create_full_sync_message(&self) -> RegistryMessage {
        RegistryMessage::FullSync {
            local_actors: self.local_actors.clone(),
            known_actors: self.known_actors.clone(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            timestamp: current_timestamp(),
            sequence: self.gossip_sequence,
        }
    }

    /// Create a full sync response message
    fn create_full_sync_response(&self) -> RegistryMessage {
        RegistryMessage::FullSyncResponse {
            local_actors: self.local_actors.clone(),
            known_actors: self.known_actors.clone(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            timestamp: current_timestamp(),
            sequence: self.gossip_sequence,
        }
    }

    /// Create a delta response for incoming gossip
    fn create_delta_response(&self, since_sequence: u64) -> Result<RegistryMessage> {
        let delta = self.create_delta(since_sequence)?;
        Ok(RegistryMessage::DeltaGossipResponse { delta })
    }

    /// Merge incoming full sync data (legacy merge logic)
    fn merge_full_sync(
        &mut self,
        remote_local: HashMap<String, ActorLocation>,
        remote_known: HashMap<String, (ActorLocation, u64)>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        timestamp: u64,
        sequence: u64,
    ) {
        // Use existing merge logic from original implementation
        self.merge_gossip(
            remote_local,
            remote_known,
            sender_id,
            sender_addr,
            timestamp,
            sequence,
        );
    }

    /// Merge incoming gossip data with sequence-based deduplication (legacy method)
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
        // Add the sender as a peer first
        self.add_peer(sender_addr);

        // Check if we've already processed this or a newer sequence from this peer
        // Allow processing if sequence is equal (for initial exchanges) or newer
        if let Some(peer_info) = self.peers.get(&sender_addr) {
            if sequence < peer_info.last_sequence {
                debug!(
                    last_sequence = peer_info.last_sequence,
                    received_sequence = sequence,
                    "ignoring old gossip message"
                );
                return;
            }
        }

        // Update peer sequence
        if let Some(peer_info) = self.peers.get_mut(&sender_addr) {
            peer_info.last_sequence = std::cmp::max(peer_info.last_sequence, sequence);
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

        // Also clean up old delta history
        let history_ttl = self.config.actor_ttl.as_secs() * 2;
        self.delta_history
            .retain(|delta| now - delta.timestamp < history_ttl);

        // Clean up connection pool
        self.connection_pool.cleanup_stale_connections();
    }

    /// Get connection pool stats for monitoring
    pub fn get_connection_stats(&self) -> (usize, usize, usize) {
        self.connection_pool.stats()
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

/// Handle incoming TCP connections with delta support
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
            RegistryMessage::DeltaGossip { delta } => {
                debug!(
                    sender = %delta.sender_id,
                    since_sequence = delta.since_sequence,
                    changes = delta.changes.len(),
                    "received delta gossip message"
                );

                let response = {
                    let mut reg = registry.lock().await;

                    // Apply the incoming delta
                    if let Err(err) = reg.apply_delta(delta.clone()) {
                        warn!(error = %err, "failed to apply delta, sending full sync response");
                        reg.create_full_sync_response()
                    } else {
                        // Update peer information
                        reg.add_peer(delta.sender_addr);
                        if let Some(peer_info) = reg.peers.get_mut(&delta.sender_addr) {
                            peer_info.last_sequence =
                                std::cmp::max(peer_info.last_sequence, delta.current_sequence);
                            peer_info.consecutive_deltas += 1;
                        }
                        reg.delta_exchanges += 1;

                        // Create delta response
                        match reg.create_delta_response(delta.current_sequence) {
                            Ok(delta_response) => delta_response,
                            Err(_) => {
                                warn!("failed to create delta response, falling back to full sync");
                                reg.create_full_sync_response()
                            }
                        }
                    }
                };

                if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                    warn!(error = %err, "failed to send delta response");
                } else {
                    debug!("delta exchange completed");
                }
            }
            RegistryMessage::FullSync {
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
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "received full sync message"
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

                    // Reset delta counter for this peer
                    if let Some(peer_info) = reg.peers.get_mut(&sender_addr) {
                        peer_info.consecutive_deltas = 0;
                    }
                    reg.full_sync_exchanges += 1;

                    reg.create_full_sync_response()
                };

                if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                    warn!(error = %err, "failed to send full sync response");
                } else {
                    debug!("full sync exchange completed");
                }
            }
            RegistryMessage::FullSyncRequest {
                sender_id,
                sender_addr,
                ..
            } => {
                debug!(
                    sender = %sender_id,
                    "received full sync request"
                );

                let response = {
                    let mut reg = registry.lock().await;
                    reg.add_peer(sender_addr);
                    reg.full_sync_exchanges += 1;
                    reg.create_full_sync_response()
                };

                if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                    warn!(error = %err, "failed to send full sync response");
                } else {
                    debug!("full sync request handled");
                }
            }
            // Handle response messages (shouldn't normally happen in server, but handle gracefully)
            RegistryMessage::DeltaGossipResponse { delta } => {
                debug!(
                    sender = %delta.sender_id,
                    changes = delta.changes.len(),
                    "received delta gossip response in server mode"
                );

                let mut reg = registry.lock().await;
                if let Err(err) = reg.apply_delta(delta) {
                    warn!(error = %err, "failed to apply delta from response");
                } else {
                    reg.delta_exchanges += 1;
                }
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_id,
                sender_addr,
                timestamp,
                sequence,
            } => {
                debug!(
                    sender = %sender_id,
                    "received full sync response in server mode"
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
                reg.full_sync_exchanges += 1;
            }
        },
        Err(err) => {
            warn!(error = %err, "failed to read message");
        }
    }

    debug!("connection closed");
}

/// Execute a single gossip task with registry access for connection pool
async fn execute_gossip_task_with_registry(
    task: GossipTask,
    registry: Arc<Mutex<GossipRegistry>>,
) -> GossipResult {
    let result = async {
        // Get connection and config (brief lock)
        let mut conn = {
            let mut reg = registry.lock().await;
            reg.connection_pool
                .checkout_connection(task.peer_addr)
                .await?
        };

        let max_message_size = {
            let reg = registry.lock().await;
            reg.config.max_message_size
        };

        let response_timeout = {
            let reg = registry.lock().await;
            reg.config.response_timeout
        };

        // Send message (no lock held)
        send_message(conn.stream(), &task.message, max_message_size).await?;

        // Wait for response (no lock held)
        let response = tokio::time::timeout(
            response_timeout,
            read_message(conn.stream(), max_message_size),
        )
        .await;

        match response {
            Ok(Ok(response)) => {
                // Success - return connection to pool (brief lock)
                {
                    let mut reg = registry.lock().await;
                    reg.connection_pool.checkin_success(conn);
                }
                Ok(Some(response))
            }
            Ok(Err(err)) => {
                // Error - mark connection as failed (brief lock)
                {
                    let mut reg = registry.lock().await;
                    reg.connection_pool.checkin_failure(conn);
                }
                Err(err)
            }
            Err(_) => {
                // Timeout - not necessarily an error for gossip (brief lock)
                debug!(peer = %task.peer_addr, "gossip response timeout");
                {
                    let mut reg = registry.lock().await;
                    reg.connection_pool.checkin_failure(conn);
                }
                Ok(None)
            }
        }
    }
    .await;

    GossipResult {
        peer_addr: task.peer_addr,
        sent_sequence: task.current_sequence,
        outcome: result,
    }
}

/// Start the gossip registry server
#[instrument(skip(registry))]
async fn start_gossip_server(registry: Arc<Mutex<GossipRegistry>>) -> Result<()> {
    let (bind_addr, node_id) = {
        let reg = registry.lock().await;
        (reg.bind_addr, reg.node_id)
    };

    let listener = TcpListener::bind(bind_addr).await?;
    info!(bind_addr = %bind_addr, node_id = %node_id, "gossip server started with delta support");

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

    let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
    let mut next_gossip_tick = Instant::now() + gossip_interval + jitter;
    let mut cleanup_timer = interval(cleanup_interval);

    info!(
        gossip_interval_ms = gossip_interval.as_millis(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
        "gossip timer started with non-blocking I/O"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_gossip_tick) => {
                let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
                next_gossip_tick += gossip_interval + jitter;

                // Step 1: Prepare gossip tasks while holding the lock briefly
                let tasks = {
                    let mut reg = registry.lock().await;
                    if reg.is_shutdown() {
                        break;
                    }
                    match reg.prepare_gossip_round() {
                        Ok(tasks) => tasks,
                        Err(err) => {
                            error!(error = %err, "failed to prepare gossip round");
                            continue;
                        }
                    }
                };

                if tasks.is_empty() {
                    continue;
                }

                // Step 2: Execute all gossip tasks WITHOUT holding the registry lock
                let results = {
                    let mut futures = Vec::new();

                    for task in tasks {
                        let registry_clone = registry.clone();
                        let future = tokio::spawn(async move {
                            execute_gossip_task_with_registry(task, registry_clone).await
                        });
                        futures.push(future);
                    }

                    // Wait for all gossip operations to complete
                    let mut results = Vec::new();
                    for future in futures {
                        match future.await {
                            Ok(result) => results.push(result),
                            Err(err) => {
                                error!(error = %err, "gossip task panicked");
                            }
                        }
                    }
                    results
                };

                // Step 3: Apply results while holding the lock briefly
                {
                    let mut reg = registry.lock().await;
                    if !reg.is_shutdown() {
                        reg.apply_gossip_results(results);
                    }
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

    info!(
        peer_count = peers.len(),
        "starting bootstrap with delta support"
    );

    for peer in peers {
        // Always use full sync for bootstrap
        let gossip_msg = {
            let reg = registry.lock().await;
            RegistryMessage::FullSync {
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
                    Ok(Ok(RegistryMessage::FullSyncResponse {
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
                        reg.full_sync_exchanges += 1;
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
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

/// Main API for the gossip registry with delta support
pub struct GossipRegistryHandle {
    registry: Arc<Mutex<GossipRegistry>>,
    _server_handle: tokio::task::JoinHandle<()>,
    _timer_handle: tokio::task::JoinHandle<()>,
}

impl GossipRegistryHandle {
    /// Create and start a new gossip registry with delta support
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

        info!(node_id = %node_id, bind_addr = %bind_addr, "delta gossip registry started");

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

    /// Get registry statistics including delta metrics
    pub async fn stats(&self) -> RegistryStats {
        let reg = self.registry.lock().await;
        reg.get_stats()
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
    async fn test_delta_gossip() -> Result<()> {
        let node_id1 = NodeId::new();
        let node_id2 = NodeId::new();

        let config = GossipConfig {
            gossip_interval: Duration::from_millis(200), // Slower for more predictable timing
            max_delta_history: 10,
            ..Default::default()
        };

        // Start first node
        let node1_addr = "127.0.0.1:9097".parse().unwrap();
        let node1 =
            GossipRegistryHandle::new(node_id1, node1_addr, vec![], Some(config.clone())).await?;

        // Start second node connected to first
        let node2_addr = "127.0.0.1:9098".parse().unwrap();
        let node2 =
            GossipRegistryHandle::new(node_id2, node2_addr, vec![node1_addr], Some(config)).await?;

        // Wait for initial connection and bootstrap
        sleep(Duration::from_millis(400)).await;

        // Register an actor on node1
        node1
            .register(
                "delta_test_actor".to_string(),
                ActorLocation::new(node_id1, node1_addr),
            )
            .await?;

        println!("Registered actor on node1, waiting for propagation...");

        // Wait for gossip propagation with multiple checks
        let mut found = false;
        for attempt in 1..=10 {
            sleep(Duration::from_millis(250)).await;

            if let Some(location) = node2.lookup("delta_test_actor").await {
                println!("Attempt {attempt}: Found actor at {location:?}");
                found = true;
                break;
            } else {
                println!("Attempt {attempt}: Actor not found yet");

                // Debug: Print stats
                let stats1 = node1.stats().await;
                let stats2 = node2.stats().await;
                println!(
                    "Node1 stats: local={}, known={}, delta_ex={}, full_ex={}, seq={}",
                    stats1.local_actors,
                    stats1.known_actors,
                    stats1.delta_exchanges,
                    stats1.full_sync_exchanges,
                    stats1.current_sequence
                );
                println!(
                    "Node2 stats: local={}, known={}, delta_ex={}, full_ex={}, seq={}",
                    stats2.local_actors,
                    stats2.known_actors,
                    stats2.delta_exchanges,
                    stats2.full_sync_exchanges,
                    stats2.current_sequence
                );
            }
        }

        assert!(found, "Actor should be found on node2 after propagation");

        // Final verification
        let found_location = node2.lookup("delta_test_actor").await;
        assert!(
            found_location.is_some(),
            "Actor should be found on final lookup"
        );

        // Check statistics
        let stats1 = node1.stats().await;
        let stats2 = node2.stats().await;

        println!(
            "Final Node1 stats: delta_exchanges={}, full_sync_exchanges={}",
            stats1.delta_exchanges, stats1.full_sync_exchanges
        );
        println!(
            "Final Node2 stats: delta_exchanges={}, full_sync_exchanges={}",
            stats2.delta_exchanges, stats2.full_sync_exchanges
        );

        // Should have done some exchanges (could be delta or full sync depending on timing)
        assert!(stats1.delta_exchanges + stats1.full_sync_exchanges > 0);
        assert!(stats2.delta_exchanges + stats2.full_sync_exchanges > 0);

        // Cleanup
        node1.shutdown().await;
        node2.shutdown().await;

        Ok(())
    }

    // #[tokio::test]
    // #[traced_test]
    // async fn test_delta_creation_and_application() -> Result<()> {
    //     let node_id = NodeId::new();
    //     let mut registry = GossipRegistry::new(
    //         node_id,
    //         "127.0.0.1:8001".parse().unwrap(),
    //         GossipConfig::default(),
    //     );

    //     // Register some actors to create changes
    //     registry.register_actor(
    //         "actor1".to_string(),
    //         ActorLocation::new(node_id, "127.0.0.1:8001".parse().unwrap()),
    //     )?;

    //     registry.register_actor(
    //         "actor2".to_string(),
    //         ActorLocation::new(node_id, "127.0.0.1:8001".parse().unwrap()),
    //     )?;

    //     // Trigger a gossip round to commit changes
    //     let _ = registry.gossip_round().await;

    //     // Create a delta
    //     let delta = registry.create_delta(0)?;

    //     assert_eq!(delta.since_sequence, 0);
    //     assert!(delta.current_sequence > 0);
    //     assert_eq!(delta.changes.len(), 2); // Should have 2 actor additions

    //     // Apply delta to a new registry
    //     let mut registry2 = GossipRegistry::new(
    //         NodeId::new(),
    //         "127.0.0.1:8002".parse().unwrap(),
    //         GossipConfig::default(),
    //     );

    //     registry2.apply_delta(delta)?;

    //     // Check that actors were applied
    //     assert!(registry2.lookup_actor("actor1").is_some());
    //     assert!(registry2.lookup_actor("actor2").is_some());

    //     Ok(())
    // }

    // #[tokio::test]
    // #[traced_test]
    // async fn test_full_sync_fallback() -> Result<()> {
    //     let node_id = NodeId::new();
    //     let config = GossipConfig {
    //         max_delta_history: 2, // Very small history to force fallback
    //         ..Default::default()
    //     };

    //     let mut registry = GossipRegistry::new(node_id, "127.0.0.1:8003".parse().unwrap(), config);

    //     // Add a peer with old sequence
    //     registry.add_peer("127.0.0.1:8004".parse().unwrap());
    //     if let Some(peer_info) = registry.peers.get_mut(&"127.0.0.1:8004".parse().unwrap()) {
    //         peer_info.last_sequence = 0; // Very old sequence
    //     }

    //     // Register several actors to fill delta history
    //     for i in 0..5 {
    //         registry.register_actor(
    //             format!("actor{i}"),
    //             ActorLocation::new(node_id, "127.0.0.1:8003".parse().unwrap()),
    //         )?;
    //         let _ = registry.gossip_round().await; // Commit each change
    //     }

    //     // Check that should_use_delta returns false for old sequence
    //     let peer_info = registry
    //         .peers
    //         .get(&"127.0.0.1:8004".parse().unwrap())
    //         .unwrap();
    //     assert!(
    //         !registry.should_use_delta(peer_info),
    //         "Should fallback to full sync for old peer"
    //     );

    //     Ok(())
    // }
}
