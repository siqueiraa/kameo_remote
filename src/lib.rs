mod connection_pool;
mod handle;
mod registry;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fmt, io};
use thiserror::Error;
use tracing::error;
use uuid::Uuid;

pub use handle::GossipRegistryHandle;

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

/// Node identifier using UUID for better uniqueness and performance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
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

/// Vector clock for tracking causal relationships between events
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    clocks: HashMap<NodeId, u64>,
}

impl VectorClock {
    /// Create a new vector clock
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Create a vector clock with an initial entry for a node
    pub fn with_node(node_id: NodeId) -> Self {
        let mut clocks = HashMap::new();
        clocks.insert(node_id, 0);
        Self { clocks }
    }

    /// Increment the clock for a specific node (used for local events)
    pub fn increment(&mut self, node_id: NodeId) {
        let counter = self.clocks.entry(node_id).or_insert(0);
        *counter += 1;
    }

    /// Get the clock value for a specific node
    pub fn get(&self, node_id: NodeId) -> u64 {
        self.clocks.get(&node_id).copied().unwrap_or(0)
    }

    /// Merge with another vector clock (used when receiving remote events)
    pub fn merge(&mut self, other: &VectorClock) {
        for (&node_id, &other_clock) in &other.clocks {
            let our_clock = self.clocks.entry(node_id).or_insert(0);
            *our_clock = (*our_clock).max(other_clock);
        }
    }

    /// Create a new vector clock that is the result of merging this one with another
    pub fn merged_with(&self, other: &VectorClock) -> VectorClock {
        let mut result = self.clone();
        result.merge(other);
        result
    }

    /// Compare vector clocks to determine causal relationship
    pub fn compare(&self, other: &VectorClock) -> ClockOrdering {
        let mut self_greater = false;
        let mut other_greater = false;

        // Get all nodes that appear in either clock
        let all_nodes: std::collections::HashSet<NodeId> = self
            .clocks
            .keys()
            .chain(other.clocks.keys())
            .copied()
            .collect();

        for node_id in all_nodes {
            let self_clock = self.get(node_id);
            let other_clock = other.get(node_id);

            match self_clock.cmp(&other_clock) {
                std::cmp::Ordering::Greater => self_greater = true,
                std::cmp::Ordering::Less => other_greater = true,
                std::cmp::Ordering::Equal => {}
            }
        }

        match (self_greater, other_greater) {
            (true, false) => ClockOrdering::After,
            (false, true) => ClockOrdering::Before,
            (false, false) => ClockOrdering::Equal,
            (true, true) => ClockOrdering::Concurrent,
        }
    }

    /// Check if this vector clock happened before another
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::Before)
    }

    /// Check if this vector clock happened after another
    pub fn happens_after(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::After)
    }

    /// Check if this vector clock is concurrent with another
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), ClockOrdering::Concurrent)
    }

    /// Garbage collect entries for nodes not seen recently
    /// This prevents unbounded growth of vector clocks
    pub fn gc_old_nodes(&mut self, active_nodes: &HashSet<NodeId>) {
        // Never remove our own node entry or nodes that are still active
        self.clocks
            .retain(|node_id, _| active_nodes.contains(node_id));
    }

    /// Get all nodes referenced in this vector clock
    pub fn get_nodes(&self) -> HashSet<NodeId> {
        self.clocks.keys().copied().collect()
    }

    /// Check if this vector clock is "empty" (only has zero entries)
    pub fn is_effectively_empty(&self) -> bool {
        self.clocks.values().all(|&count| count == 0)
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Ordering relationship between vector clocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockOrdering {
    Before,     // This clock is before the other (causally precedes)
    After,      // This clock is after the other (causally follows)
    Equal,      // Clocks are identical
    Concurrent, // Clocks are concurrent (no causal relationship)
}

/// Location of an actor in the network with vector clock
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ActorLocation {
    pub node_id: NodeId,
    pub address: SocketAddr,
    pub vector_clock: VectorClock,
    pub wall_clock_time: u64, // Still needed for TTL calculations
}

impl ActorLocation {
    pub fn new(node_id: NodeId, address: SocketAddr, vector_clock: VectorClock) -> Self {
        Self {
            node_id,
            address,
            vector_clock,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create with current wall clock time
    pub fn new_with_wall_time(
        node_id: NodeId,
        address: SocketAddr,
        vector_clock: VectorClock,
        wall_time: u64,
    ) -> Self {
        Self {
            node_id,
            address,
            vector_clock,
            wall_clock_time: wall_time,
        }
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
    /// Timeout for connections checked out too long
    pub checkout_timeout: Duration,
    /// How often to run vector clock garbage collection  
    pub vector_clock_gc_frequency: Duration,
    /// How long to retain node entries in vector clocks after last seen
    pub vector_clock_retention_period: Duration,
    /// Threshold for small clusters
    pub small_cluster_threshold: usize,
    /// Maximum time to wait for server to become ready before bootstrap
    pub bootstrap_readiness_timeout: Duration,
    /// Interval between readiness checks
    pub bootstrap_readiness_check_interval: Duration,
    /// Maximum bootstrap retry attempts
    pub bootstrap_max_retries: usize,
    /// Delay between bootstrap retry attempts
    pub bootstrap_retry_delay: Duration,
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
            checkout_timeout: Duration::from_secs(60),
            vector_clock_gc_frequency: Duration::from_secs(300), // 5 minutes
            vector_clock_retention_period: Duration::from_secs(7200), // 2 hours (was 1 hour)
            small_cluster_threshold: 5,
            bootstrap_readiness_timeout: Duration::from_secs(30),
            bootstrap_readiness_check_interval: Duration::from_millis(100),
            bootstrap_max_retries: 3,
            bootstrap_retry_delay: Duration::from_secs(5),
        }
    }
}

/// Get current timestamp in seconds (still used for TTL)
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;
    use tracing_test::traced_test;

    use crate::handle::GossipRegistryHandle;

    use super::*;

    #[test]
    fn test_vector_clock_basic() {
        let node1 = NodeId::new();
        let node2 = NodeId::new();

        let mut clock1 = VectorClock::with_node(node1);
        let mut clock2 = VectorClock::with_node(node2);

        // Initial state: both should be concurrent
        assert!(clock1.is_concurrent(&clock2));

        // Node1 increments
        clock1.increment(node1);
        assert!(clock2.happens_before(&clock1));
        assert!(clock1.happens_after(&clock2));

        // Node2 increments
        clock2.increment(node2);
        assert!(clock1.is_concurrent(&clock2));

        // Node2 merges with node1's clock and increments
        clock2.merge(&clock1);
        clock2.increment(node2);
        assert!(clock1.happens_before(&clock2));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_delta_gossip_with_vector_clocks() -> Result<()> {
        let node_id1 = NodeId::new();
        let node_id2 = NodeId::new();

        let config = GossipConfig {
            gossip_interval: Duration::from_millis(200),
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
            .register("vector_clock_test_actor".to_string(), node_id1, node1_addr)
            .await?;

        println!("Registered actor on node1, waiting for propagation...");

        // Wait for gossip propagation
        let mut found = false;
        for attempt in 1..=10 {
            sleep(Duration::from_millis(250)).await;

            if let Some(location) = node2.lookup("vector_clock_test_actor").await {
                println!("Attempt {attempt}: Found actor at {location:?}");
                found = true;
                break;
            } else {
                println!("Attempt {attempt}: Actor not found yet");
            }
        }

        assert!(found, "Actor should be found on node2 after propagation");

        // Cleanup
        node1.shutdown().await;
        node2.shutdown().await;

        Ok(())
    }
}
