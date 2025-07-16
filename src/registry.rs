use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
};

use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::{
    connection_pool::ConnectionPool, current_timestamp, ActorLocation, ClockOrdering, GossipConfig,
    GossipError, NodeId, Result, VectorClock,
};

/// Registry change types for delta tracking with vector clocks
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryChange {
    /// Actor was added or updated
    ActorAdded {
        name: String,
        location: ActorLocation,
        vector_clock: VectorClock,
    },
    /// Actor was removed
    ActorRemoved {
        name: String,
        vector_clock: VectorClock,
    },
}

/// Delta representing changes since a specific sequence number with vector clock
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryDelta {
    pub since_sequence: u64,
    pub current_sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub sender_id: NodeId,
    pub sender_addr: SocketAddr,
    pub vector_clock: VectorClock,
    pub wall_clock_time: u64, // For debugging/monitoring only
}

/// Message types for the gossip protocol with vector clocks
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
        vector_clock: VectorClock,
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Full synchronization message with vector clocks
    FullSync {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, ActorLocation>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        vector_clock: VectorClock,
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Response to full sync
    FullSyncResponse {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, ActorLocation>,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        vector_clock: VectorClock,
        sequence: u64,
        wall_clock_time: u64,
    },
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
pub struct PeerInfo {
    pub address: SocketAddr,
    pub failures: usize,
    pub last_attempt: u64,
    pub last_success: u64,
    pub last_sequence: u64,
    /// Last sequence we successfully sent to this peer
    pub last_sent_sequence: u64,
    /// Number of consecutive delta exchanges with this peer
    pub consecutive_deltas: u64,
    /// Last known vector clock from this peer
    pub last_vector_clock: VectorClock,
}

/// Historical delta for efficient incremental updates with vector clock
#[derive(Debug, Clone)]
pub struct HistoricalDelta {
    pub sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub wall_clock_time: u64,
}

/// Data needed to perform gossip with a single peer
#[derive(Debug)]
pub struct GossipTask {
    pub peer_addr: SocketAddr,
    pub message: RegistryMessage,
    pub current_sequence: u64,
}

/// Result of a gossip operation
#[derive(Debug)]
pub struct GossipResult {
    pub peer_addr: SocketAddr,
    pub sent_sequence: u64,
    pub outcome: Result<Option<RegistryMessage>>,
}

/// Separated actor state for read-heavy operations (now with vector clocks)
#[derive(Debug)]
pub struct ActorState {
    pub local_actors: HashMap<String, ActorLocation>,
    pub known_actors: HashMap<String, ActorLocation>,
}

/// Gossip coordination state for write-heavy operations  
#[derive(Debug)]
pub struct GossipState {
    pub gossip_sequence: u64,
    pub pending_changes: Vec<RegistryChange>,
    pub delta_history: Vec<HistoricalDelta>,
    pub peers: HashMap<SocketAddr, PeerInfo>,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub shutdown: bool,
    /// Track when we last saw each node (for vector clock GC)
    pub node_last_seen: HashMap<NodeId, u64>,
}

/// Core gossip registry implementation with vector clocks and separated locks
pub struct GossipRegistry {
    // Immutable config
    pub node_id: NodeId,
    pub bind_addr: SocketAddr,
    pub config: GossipConfig,
    pub start_time: u64,

    // Vector clock for this node
    pub vector_clock: Arc<Mutex<VectorClock>>,

    // Separated lockable state
    pub actor_state: Arc<RwLock<ActorState>>,
    pub gossip_state: Arc<Mutex<GossipState>>,
    pub connection_pool: Arc<Mutex<ConnectionPool>>,
}

impl GossipRegistry {
    /// Create a new gossip registry with vector clock
    pub fn new(node_id: NodeId, bind_addr: SocketAddr, config: GossipConfig) -> Self {
        info!(
            node_id = %node_id,
            bind_addr = %bind_addr,
            "creating new gossip registry with vector clocks and separated locks"
        );

        let connection_pool = ConnectionPool::new(
            config.max_pooled_connections,
            config.connection_timeout,
            config.idle_connection_timeout,
            config.checkout_timeout,
        );

        let mut node_last_seen = HashMap::new();
        node_last_seen.insert(node_id, current_timestamp()); // Always keep our own node

        Self {
            node_id,
            bind_addr,
            config,
            start_time: current_timestamp(),
            vector_clock: Arc::new(Mutex::new(VectorClock::with_node(node_id))),
            actor_state: Arc::new(RwLock::new(ActorState {
                local_actors: HashMap::new(),
                known_actors: HashMap::new(),
            })),
            gossip_state: Arc::new(Mutex::new(GossipState {
                gossip_sequence: 0,
                pending_changes: Vec::new(),
                delta_history: Vec::new(),
                peers: HashMap::new(),
                delta_exchanges: 0,
                full_sync_exchanges: 0,
                shutdown: false,
                node_last_seen,
            })),
            connection_pool: Arc::new(Mutex::new(connection_pool)),
        }
    }

    /// Add bootstrap peers for initial connection
    pub async fn add_bootstrap_peers(&self, bootstrap_peers: Vec<SocketAddr>) {
        let mut gossip_state = self.gossip_state.lock().await;
        for peer in bootstrap_peers {
            if peer != self.bind_addr {
                gossip_state.peers.insert(
                    peer,
                    PeerInfo {
                        address: peer,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
                        last_vector_clock: VectorClock::new(),
                    },
                );
                debug!(peer = %peer, "added bootstrap peer");
            }
        }
        info!(
            peer_count = gossip_state.peers.len(),
            "added bootstrap peers"
        );
    }

    /// Add a new peer (called when receiving connections)
    pub async fn add_peer(&self, peer_addr: SocketAddr) {
        if peer_addr != self.bind_addr {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Entry::Vacant(e) = gossip_state.peers.entry(peer_addr) {
                let current_time = current_timestamp();
                e.insert(PeerInfo {
                    address: peer_addr,
                    failures: 0,
                    last_attempt: current_time,
                    last_success: current_time,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_vector_clock: VectorClock::new(),
                });
                debug!(peer = %peer_addr, "added new peer");
            }
        }
    }

    /// Register a local actor (fast path - minimal locking) with vector clock increment
    pub async fn register_actor(&self, name: String, mut location: ActorLocation) -> Result<()> {
        // Check shutdown first
        {
            let gossip_state = self.gossip_state.lock().await;
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }
        }

        // Increment our vector clock for this local event
        let current_vector_clock = {
            let mut vector_clock = self.vector_clock.lock().await;
            vector_clock.increment(self.node_id);
            vector_clock.clone()
        };

        // Update the location with current vector clock and wall time
        location.vector_clock = current_vector_clock.clone();
        location.wall_clock_time = current_timestamp();

        // Update actor state
        {
            let mut actor_state = self.actor_state.write().await;
            actor_state
                .local_actors
                .insert(name.clone(), location.clone());
        }

        // Update gossip state with pending change
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .pending_changes
                .push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location,
                    vector_clock: current_vector_clock,
                });
        }

        info!(
            actor_name = %name,
            node_id = %self.node_id,
            "registered local actor with vector clock"
        );

        Ok(())
    }

    /// Unregister a local actor with vector clock increment
    pub async fn unregister_actor(&self, name: &str) -> Result<Option<ActorLocation>> {
        // Check shutdown first
        {
            let gossip_state = self.gossip_state.lock().await;
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }
        }

        // Remove from actor state
        let removed = {
            let mut actor_state = self.actor_state.write().await;
            actor_state.local_actors.remove(name)
        };

        if removed.is_some() {
            info!(actor_name = %name, "unregistered local actor");

            // Increment our vector clock for this local event
            let current_vector_clock = {
                let mut vector_clock = self.vector_clock.lock().await;
                vector_clock.increment(self.node_id);
                vector_clock.clone()
            };

            // Track this change for delta gossip
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .pending_changes
                .push(RegistryChange::ActorRemoved {
                    name: name.to_string(),
                    vector_clock: current_vector_clock,
                });
        }
        Ok(removed)
    }

    /// Lookup an actor (read-only fast path)
    pub async fn lookup_actor(&self, name: &str) -> Option<ActorLocation> {
        let actor_state = self.actor_state.read().await;

        // Check local actors first
        if let Some(location) = actor_state.local_actors.get(name) {
            debug!(actor_name = %name, location = "local", "actor found");
            return Some(location.clone());
        }

        // Check known remote actors
        if let Some(location) = actor_state.known_actors.get(name) {
            let now = current_timestamp();
            let age_secs = now.saturating_sub(location.wall_clock_time);
            if age_secs < self.config.actor_ttl.as_secs() {
                debug!(
                    actor_name = %name,
                    location = "remote",
                    age_seconds = age_secs,
                    "actor found"
                );
                return Some(location.clone());
            }
        }

        debug!(actor_name = %name, "actor not found");
        None
    }

    /// Get registry statistics
    pub async fn get_stats(&self) -> RegistryStats {
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.len(),
                actor_state.known_actors.len(),
            )
        };

        let (
            gossip_sequence,
            active_peers,
            failed_peers,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
        ) = {
            let gossip_state = self.gossip_state.lock().await;
            let active_peers = gossip_state
                .peers
                .values()
                .filter(|p| p.failures < self.config.max_peer_failures)
                .count();
            let failed_peers = gossip_state.peers.len() - active_peers;

            (
                gossip_state.gossip_sequence,
                active_peers,
                failed_peers,
                gossip_state.delta_exchanges,
                gossip_state.full_sync_exchanges,
                gossip_state.delta_history.len(),
            )
        };

        let current_time = current_timestamp();
        let avg_delta_size = if delta_exchanges > 0 {
            // This is approximate since we don't hold the lock
            delta_history_size as f64
        } else {
            0.0
        };

        RegistryStats {
            node_id: self.node_id,
            local_actors,
            known_actors,
            active_peers,
            failed_peers,
            total_gossip_rounds: gossip_sequence,
            current_sequence: gossip_sequence,
            uptime_seconds: current_time.saturating_sub(self.start_time),
            last_gossip_timestamp: current_time,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
            avg_delta_size,
        }
    }

    /// Apply delta with vector clock-based conflict resolution (with race condition and error recovery fixes)
    pub async fn apply_delta(&self, delta: RegistryDelta) -> Result<()> {
        /// Internal enum for changes that have been validated and are ready to apply
        #[derive(Debug)]
        enum AppliedChange {
            AddActor {
                name: String,
                location: ActorLocation,
            },
            RemoveActor {
                name: String,
            },
        }

        // Record that we've seen this node
        self.record_node_activity(delta.sender_id).await;

        // Also record any nodes mentioned in the vector clock
        for &node_id in delta.vector_clock.get_nodes().iter() {
            self.record_node_activity(node_id).await;
        }

        // Step 1: Atomically update our vector clock with the incoming delta
        {
            let mut our_vector_clock = self.vector_clock.lock().await;
            our_vector_clock.merge(&delta.vector_clock);
        }

        let total_changes = delta.changes.len();

        // Step 2: Decision phase - determine what changes to apply without modifying state
        let changes_to_apply = {
            let actor_state = self.actor_state.read().await;
            let mut validated_changes = Vec::new();

            for change in delta.changes {
                match &change {
                    RegistryChange::ActorAdded {
                        name,
                        location,
                        vector_clock: change_vector_clock,
                    } => {
                        // Don't override local actors
                        if actor_state.local_actors.contains_key(name) {
                            debug!(
                                actor_name = %name,
                                "skipping remote actor update - actor is local"
                            );
                            continue;
                        }

                        // Use vector clock comparison for conflict resolution
                        let should_apply = match actor_state.known_actors.get(name) {
                            Some(existing_location) => {
                                match change_vector_clock.compare(&existing_location.vector_clock) {
                                    ClockOrdering::After => {
                                        debug!(
                                            actor_name = %name,
                                            "applying actor update - change is newer"
                                        );
                                        true
                                    }
                                    ClockOrdering::Concurrent => {
                                        // For concurrent updates, use node ID as tiebreaker
                                        let should_apply =
                                            location.node_id > existing_location.node_id;
                                        debug!(
                                            actor_name = %name,
                                            our_node = %existing_location.node_id,
                                            their_node = %location.node_id,
                                            applying = should_apply,
                                            "resolving concurrent actor update using node ID tiebreaker"
                                        );
                                        should_apply
                                    }
                                    ClockOrdering::Before => {
                                        debug!(
                                            actor_name = %name,
                                            "skipping actor update - change is older"
                                        );
                                        false
                                    }
                                    ClockOrdering::Equal => {
                                        debug!(
                                            actor_name = %name,
                                            "skipping actor update - change is identical"
                                        );
                                        false
                                    }
                                }
                            }
                            None => {
                                debug!(
                                    actor_name = %name,
                                    "applying new actor"
                                );
                                true // New actor
                            }
                        };

                        if should_apply {
                            validated_changes.push(AppliedChange::AddActor {
                                name: name.clone(),
                                location: location.clone(),
                            });
                        }
                    }
                    RegistryChange::ActorRemoved {
                        name,
                        vector_clock: removal_vector_clock,
                    } => {
                        // Don't remove local actors
                        if actor_state.local_actors.contains_key(name) {
                            debug!(
                                actor_name = %name,
                                "skipping actor removal - actor is local"
                            );
                            continue;
                        }

                        let should_remove = match actor_state.known_actors.get(name) {
                            Some(existing_location) => {
                                match removal_vector_clock.compare(&existing_location.vector_clock)
                                {
                                    ClockOrdering::After => {
                                        debug!(
                                            actor_name = %name,
                                            "applying actor removal - removal is newer"
                                        );
                                        true
                                    }
                                    ClockOrdering::Concurrent => {
                                        // For concurrent updates, removal wins if it has higher max node ID
                                        // This is arbitrary but consistent
                                        let removal_max_node = removal_vector_clock
                                            .clocks
                                            .keys()
                                            .max()
                                            .copied()
                                            .unwrap_or_else(|| {
                                                NodeId::from_str(
                                                    "00000000-0000-0000-0000-000000000000",
                                                )
                                                .unwrap()
                                            });
                                        let should_remove =
                                            removal_max_node > existing_location.node_id;
                                        debug!(
                                            actor_name = %name,
                                            removal_max_node = %removal_max_node,
                                            existing_node = %existing_location.node_id,
                                            removing = should_remove,
                                            "resolving concurrent removal using node ID tiebreaker"
                                        );
                                        should_remove
                                    }
                                    ClockOrdering::Before => {
                                        debug!(
                                            actor_name = %name,
                                            "skipping actor removal - removal is older"
                                        );
                                        false
                                    }
                                    ClockOrdering::Equal => {
                                        debug!(
                                            actor_name = %name,
                                            "skipping actor removal - removal is identical"
                                        );
                                        false
                                    }
                                }
                            }
                            None => {
                                debug!(
                                    actor_name = %name,
                                    "skipping actor removal - actor not found"
                                );
                                false // Nothing to remove
                            }
                        };

                        if should_remove {
                            validated_changes
                                .push(AppliedChange::RemoveActor { name: name.clone() });
                        }
                    }
                }
            }

            validated_changes
        };

        // Step 3: Application phase - atomically apply all validated changes
        let applied_count = {
            let mut actor_state = self.actor_state.write().await;
            let mut applied = 0;

            for change in changes_to_apply {
                match change {
                    AppliedChange::AddActor { name, location } => {
                        // Double-check that we still don't have this as a local actor
                        // (defensive programming - state could have changed between decision and application)
                        if !actor_state.local_actors.contains_key(&name) {
                            actor_state.known_actors.insert(name.clone(), location);
                            applied += 1;
                            debug!(actor_name = %name, "applied actor addition");
                        } else {
                            debug!(actor_name = %name, "skipped actor addition - became local during processing");
                        }
                    }
                    AppliedChange::RemoveActor { name } => {
                        // Double-check that this isn't a local actor
                        if !actor_state.local_actors.contains_key(&name) {
                            if actor_state.known_actors.remove(&name).is_some() {
                                applied += 1;
                                debug!(actor_name = %name, "applied actor removal");
                            } else {
                                debug!(actor_name = %name, "skipped actor removal - already removed");
                            }
                        } else {
                            debug!(actor_name = %name, "skipped actor removal - became local during processing");
                        }
                    }
                }
            }

            applied
        };

        debug!(
            sender = %delta.sender_id,
            total_changes,
            applied_changes = applied_count,
            "completed delta application with vector clock conflict resolution"
        );

        Ok(())
    }

    /// Perform vector clock garbage collection
    pub async fn gc_vector_clocks(&self) {
        let now = current_timestamp();

        // Use the dedicated retention period, which should be longer than actor TTL
        // This handles temporary network partitions and delayed messages
        let gc_cutoff = now.saturating_sub(self.config.vector_clock_retention_period.as_secs());

        // Determine which nodes are still considered "active"
        let active_nodes = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Update our own node as always active
            gossip_state.node_last_seen.insert(self.node_id, now);

            // Collect nodes that are still active (seen recently)
            let active_nodes: HashSet<NodeId> = gossip_state
                .node_last_seen
                .iter()
                .filter_map(|(&node_id, &last_seen)| {
                    if last_seen > gc_cutoff {
                        Some(node_id)
                    } else {
                        None
                    }
                })
                .collect();

            // Clean up the node_last_seen map itself, but use an even longer retention
            // Keep the tracking data for 2x the vector clock retention period
            let tracking_cutoff =
                now.saturating_sub(self.config.vector_clock_retention_period.as_secs() * 2);
            let before_tracking_size = gossip_state.node_last_seen.len();
            gossip_state
                .node_last_seen
                .retain(|_, &mut last_seen| last_seen > tracking_cutoff);

            let removed_tracking = before_tracking_size - gossip_state.node_last_seen.len();
            if removed_tracking > 0 {
                debug!(
                    removed_tracking_entries = removed_tracking,
                    "cleaned up old node tracking entries"
                );
            }

            active_nodes
        };

        // Conservative GC: only remove nodes that have been inactive for a long time
        let total_nodes_before = {
            let vector_clock = self.vector_clock.lock().await;
            vector_clock.clocks.len()
        };

        // GC our main vector clock
        {
            let mut vector_clock = self.vector_clock.lock().await;
            let before_size = vector_clock.clocks.len();
            vector_clock.gc_old_nodes(&active_nodes);
            let after_size = vector_clock.clocks.len();

            if before_size > after_size {
                debug!(
                    removed_nodes = before_size - after_size,
                    remaining_nodes = after_size,
                    retention_period_secs = self.config.vector_clock_retention_period.as_secs(),
                    "garbage collected main vector clock"
                );
            }
        }

        // GC vector clocks in actor locations
        {
            let mut actor_state = self.actor_state.write().await;
            let mut gc_count = 0;

            // GC local actors (but be very conservative - these are our actors)
            for location in actor_state.local_actors.values_mut() {
                let before_size = location.vector_clock.clocks.len();
                location.vector_clock.gc_old_nodes(&active_nodes);
                if location.vector_clock.clocks.len() < before_size {
                    gc_count += 1;
                }
            }

            // GC known actors
            for location in actor_state.known_actors.values_mut() {
                let before_size = location.vector_clock.clocks.len();
                location.vector_clock.gc_old_nodes(&active_nodes);
                if location.vector_clock.clocks.len() < before_size {
                    gc_count += 1;
                }
            }

            if gc_count > 0 {
                debug!(
                    gc_actor_count = gc_count,
                    "garbage collected vector clocks in actor locations"
                );
            }
        }

        // GC vector clocks in delta history (be more aggressive here since this is just history)
        {
            let mut gossip_state = self.gossip_state.lock().await;
            let mut gc_count = 0;

            for delta in &mut gossip_state.delta_history {
                for change in &mut delta.changes {
                    match change {
                        RegistryChange::ActorAdded { vector_clock, .. }
                        | RegistryChange::ActorRemoved { vector_clock, .. } => {
                            let before_size = vector_clock.clocks.len();
                            vector_clock.gc_old_nodes(&active_nodes);
                            if vector_clock.clocks.len() < before_size {
                                gc_count += 1;
                            }
                        }
                    }
                }
            }

            if gc_count > 0 {
                debug!(
                    gc_delta_count = gc_count,
                    "garbage collected vector clocks in delta history"
                );
            }
        }

        let total_nodes_after = {
            let vector_clock = self.vector_clock.lock().await;
            vector_clock.clocks.len()
        };

        info!(
            active_node_count = active_nodes.len(),
            nodes_before_gc = total_nodes_before,
            nodes_after_gc = total_nodes_after,
            retention_period_minutes = self.config.vector_clock_retention_period.as_secs() / 60,
            "vector clock garbage collection completed"
        );
    }

    /// Determine whether to use delta or full sync for a peer
    fn should_use_delta_state(&self, gossip_state: &GossipState, peer_info: &PeerInfo) -> bool {
        // For small clusters (≤ 5 total nodes), always use full sync to ensure
        // proper transitive propagation in star topologies
        let healthy_peers = gossip_state
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .count();
        let total_healthy_nodes = healthy_peers + 1;
        if total_healthy_nodes <= self.config.small_cluster_threshold {
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
        let oldest_available = gossip_state
            .delta_history
            .first()
            .map(|d| d.sequence)
            .unwrap_or(gossip_state.gossip_sequence);

        peer_info.last_sequence >= oldest_available
    }

    /// Create a delta containing changes since the specified sequence
    async fn create_delta_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        since_sequence: u64,
        vector_clock: &VectorClock,
    ) -> Result<RegistryDelta> {
        let mut changes = Vec::new();
        let current_time = current_timestamp();

        // If this is a brand new peer (since_sequence = 0), include all actors we know about
        if since_sequence == 0 {
            // Include all local actors as additions
            for (name, location) in local_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    vector_clock: location.vector_clock.clone(),
                });
            }

            // Include all known remote actors as additions
            for (name, location) in known_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    vector_clock: location.vector_clock.clone(),
                });
            }
        }

        // Include pending changes from current round
        changes.extend(gossip_state.pending_changes.clone());

        // Include historical changes since the requested sequence
        for delta in &gossip_state.delta_history {
            if delta.sequence > since_sequence {
                changes.extend(delta.changes.clone());
            }
        }

        // Deduplicate changes to send only the most recent change for each actor
        let deduped_changes = Self::deduplicate_changes(changes);

        Ok(RegistryDelta {
            since_sequence,
            current_sequence: gossip_state.gossip_sequence,
            changes: deduped_changes,
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            vector_clock: vector_clock.clone(),
            wall_clock_time: current_time,
        })
    }

    /// Create a full sync message from state
    async fn create_full_sync_message_from_state(
        &self,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        sequence: u64,
        vector_clock: VectorClock,
    ) -> RegistryMessage {
        RegistryMessage::FullSync {
            local_actors: local_actors.clone(),
            known_actors: known_actors.clone(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            vector_clock,
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a full sync response message from state
    pub async fn create_full_sync_response_from_state(
        &self,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        let current_vector_clock = {
            let vector_clock = self.vector_clock.lock().await;
            vector_clock.clone()
        };

        RegistryMessage::FullSyncResponse {
            local_actors: local_actors.clone(),
            known_actors: known_actors.clone(),
            sender_id: self.node_id,
            sender_addr: self.bind_addr,
            vector_clock: current_vector_clock,
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a delta response for incoming gossip
    pub async fn create_delta_response_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        since_sequence: u64,
        vector_clock: &VectorClock,
    ) -> Result<RegistryMessage> {
        let delta = self
            .create_delta_from_state(
                gossip_state,
                local_actors,
                known_actors,
                since_sequence,
                vector_clock,
            )
            .await?;
        Ok(RegistryMessage::DeltaGossipResponse { delta })
    }

    /// Prepare gossip round with separated locking and atomic vector clock increment
    pub async fn prepare_gossip_round(&self) -> Result<Vec<GossipTask>> {
        // Step 1: Atomically commit pending changes and increment vector clock
        let (current_sequence, current_vector_clock, has_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            // Check if we have changes to commit
            let had_changes = !gossip_state.pending_changes.is_empty();

            // Get current vector clock and increment if we have changes
            let current_vector_clock = if had_changes {
                // Acquire vector clock lock while holding gossip state lock
                // to ensure atomic increment with sequence number
                let mut vector_clock = self.vector_clock.lock().await;

                // Increment sequence number
                gossip_state.gossip_sequence += 1;

                // Increment vector clock for this local event
                vector_clock.increment(self.node_id);
                let current_vc = vector_clock.clone();

                // Commit pending changes to history
                let delta = HistoricalDelta {
                    sequence: gossip_state.gossip_sequence,
                    changes: std::mem::take(&mut gossip_state.pending_changes),
                    wall_clock_time: current_timestamp(),
                };

                gossip_state.delta_history.push(delta);

                // Trim history if needed
                if gossip_state.delta_history.len() > self.config.max_delta_history {
                    gossip_state.delta_history.remove(0);
                }

                // Vector clock lock is automatically released here
                current_vc
            } else {
                // No changes, just get current vector clock
                let vector_clock = self.vector_clock.lock().await;
                vector_clock.clone()
            };

            (
                gossip_state.gossip_sequence,
                current_vector_clock,
                !gossip_state.peers.is_empty(),
            )
            // gossip_state lock is automatically released here
        };

        if !has_peers {
            return Ok(Vec::new());
        }

        // Step 2: Get actor state for message creation
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.clone(),
                actor_state.known_actors.clone(),
            )
        };

        // Step 3: Select peers and create messages
        let tasks = {
            let gossip_state = self.gossip_state.lock().await;

            let current_time = current_timestamp();
            let available_peers: Vec<SocketAddr> = gossip_state
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
                return Ok(Vec::new());
            }

            // Select peers using adaptive fanout
            let adaptive_fanout = std::cmp::min(
                std::cmp::max(3, (available_peers.len() as f64).log2().ceil() as usize),
                self.config.max_gossip_peers,
            );

            let selected_peers: Vec<SocketAddr> = {
                let mut rng = rand::rng();
                let mut peers = available_peers;
                peers.shuffle(&mut rng);
                peers.into_iter().take(adaptive_fanout).collect()
            };

            let mut tasks = Vec::new();
            for peer_addr in selected_peers {
                let peer_info = gossip_state
                    .peers
                    .get(&peer_addr)
                    .cloned()
                    .unwrap_or(PeerInfo {
                        address: peer_addr,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
                        last_vector_clock: VectorClock::new(),
                    });

                let use_delta = self.should_use_delta_state(&gossip_state, &peer_info);

                let message = if use_delta {
                    match self
                        .create_delta_from_state(
                            &gossip_state,
                            &local_actors,
                            &known_actors,
                            peer_info.last_sequence,
                            &current_vector_clock,
                        )
                        .await
                    {
                        Ok(delta) => RegistryMessage::DeltaGossip { delta },
                        Err(err) => {
                            debug!(
                                peer = %peer_addr,
                                error = %err,
                                "failed to create delta, falling back to full sync"
                            );
                            self.create_full_sync_message_from_state(
                                &local_actors,
                                &known_actors,
                                current_sequence,
                                current_vector_clock.clone(),
                            )
                            .await
                        }
                    }
                } else {
                    self.create_full_sync_message_from_state(
                        &local_actors,
                        &known_actors,
                        current_sequence,
                        current_vector_clock.clone(),
                    )
                    .await
                };

                tasks.push(GossipTask {
                    peer_addr,
                    message,
                    current_sequence,
                });
            }

            tasks
        };

        debug!(
            task_count = tasks.len(),
            current_sequence = current_sequence,
            "prepared gossip round with atomic sequence/vector clock increment"
        );

        Ok(tasks)
    }

    /// Apply results from gossip tasks
    pub async fn apply_gossip_results(&self, results: Vec<GossipResult>) {
        let current_time = current_timestamp();

        for result in results {
            match result.outcome {
                Ok(response_opt) => {
                    // Success case
                    {
                        let mut gossip_state = self.gossip_state.lock().await;
                        if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                            peer_info.failures = 0;
                            peer_info.last_success = current_time;
                            peer_info.last_attempt = current_time;
                            peer_info.last_sent_sequence = result.sent_sequence;
                        }
                    }

                    // Record that this peer is active (we successfully communicated with it)
                    // We need to find the node_id for this peer address
                    self.record_peer_activity(result.peer_addr).await;

                    // Process response if we got one
                    if let Some(response) = response_opt {
                        if let Err(err) = self
                            .handle_gossip_response(result.peer_addr, response)
                            .await
                        {
                            warn!(peer = %result.peer_addr, error = %err, "failed to handle gossip response");
                        }
                    }
                }
                Err(err) => {
                    // Failure case
                    warn!(peer = %result.peer_addr, error = %err, "failed to gossip to peer");
                    let mut gossip_state = self.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                        peer_info.failures += 1;
                        peer_info.last_attempt = current_time;
                    }
                }
            }
        }
    }

    /// Handle gossip response with vector clock updates
    async fn handle_gossip_response(
        &self,
        addr: SocketAddr,
        response: RegistryMessage,
    ) -> Result<()> {
        match response {
            RegistryMessage::DeltaGossipResponse { delta } => {
                debug!(
                    peer = %addr,
                    remote_node = %delta.sender_id,
                    changes = delta.changes.len(),
                    "received delta gossip response with vector clock"
                );

                let delta_sequence = delta.current_sequence;
                let delta_vector_clock = delta.vector_clock.clone();

                self.apply_delta(delta).await?;
                self.add_peer(addr).await;

                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.last_sequence = delta_sequence;
                    peer_info.consecutive_deltas += 1;
                    peer_info.last_vector_clock = delta_vector_clock;
                }
                gossip_state.delta_exchanges += 1;
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_id,
                sender_addr,
                vector_clock,
                sequence,
                wall_clock_time,
            } => {
                debug!(
                    peer = %addr,
                    remote_node = %sender_id,
                    sequence = sequence,
                    "received full sync response with vector clock"
                );

                self.merge_full_sync(
                    local_actors,
                    known_actors,
                    sender_addr,
                    vector_clock,
                    sequence,
                    wall_clock_time,
                )
                .await;

                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.consecutive_deltas = 0;
                    peer_info.last_sequence = sequence;
                }
                gossip_state.full_sync_exchanges += 1;
            }
            _ => {
                warn!(peer = %addr, "received unexpected message type in response");
            }
        }

        Ok(())
    }

    /// Merge incoming full sync data with vector clock-based conflict resolution
    pub async fn merge_full_sync(
        &self,
        remote_local: HashMap<String, ActorLocation>,
        remote_known: HashMap<String, ActorLocation>,
        sender_addr: SocketAddr,
        sender_vector_clock: VectorClock,
        sequence: u64,
        _wall_clock_time: u64,
    ) {
        // Add the sender as a peer first
        self.add_peer(sender_addr).await;

        // Record comprehensive node activity using helper methods
        self.record_vector_clock_activity(&sender_vector_clock)
            .await;
        self.record_actor_node_activity(&remote_local).await;
        self.record_actor_node_activity(&remote_known).await;

        // Record node activity from the sender's vector clock
        for &node_id in sender_vector_clock.get_nodes().iter() {
            self.record_node_activity(node_id).await;
        }

        // Record activity from all actor locations
        for location in remote_local.values().chain(remote_known.values()) {
            self.record_node_activity(location.node_id).await;
            for &node_id in location.vector_clock.get_nodes().iter() {
                self.record_node_activity(node_id).await;
            }
        }

        // Add the sender as a peer first
        self.add_peer(sender_addr).await;

        // Update our vector clock by merging with sender's clock
        {
            let mut our_vector_clock = self.vector_clock.lock().await;
            our_vector_clock.merge(&sender_vector_clock);
        }

        // Check if we've already processed this or a newer sequence from this peer
        {
            let gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get(&sender_addr) {
                if sequence < peer_info.last_sequence {
                    debug!(
                        last_sequence = peer_info.last_sequence,
                        received_sequence = sequence,
                        "ignoring old gossip message"
                    );
                    return;
                }
            }
        }

        // Update peer sequence and vector clock
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                peer_info.last_sequence = std::cmp::max(peer_info.last_sequence, sequence);
                peer_info.last_vector_clock = sender_vector_clock;
            }
        }

        let mut new_actors = 0;
        let mut updated_actors = 0;

        // Merge using vector clock-based conflict resolution
        {
            let mut actor_state = self.actor_state.write().await;

            // Merge remote local actors
            for (name, location) in remote_local {
                if !actor_state.local_actors.contains_key(&name) {
                    match actor_state.known_actors.get(&name) {
                        Some(existing_location) => {
                            match location
                                .vector_clock
                                .compare(&existing_location.vector_clock)
                            {
                                ClockOrdering::After => {
                                    actor_state.known_actors.insert(name.clone(), location);
                                    updated_actors += 1;
                                }
                                ClockOrdering::Concurrent => {
                                    // Use node ID as tiebreaker
                                    if location.node_id > existing_location.node_id {
                                        actor_state.known_actors.insert(name.clone(), location);
                                        updated_actors += 1;
                                    }
                                }
                                _ => {} // Keep existing
                            }
                        }
                        None => {
                            actor_state.known_actors.insert(name.clone(), location);
                            new_actors += 1;
                        }
                    }
                }
            }

            // Merge remote known actors
            for (name, location) in remote_known {
                if actor_state.local_actors.contains_key(&name) {
                    continue; // Don't override local actors
                }

                match actor_state.known_actors.get(&name) {
                    Some(existing_location) => {
                        match location
                            .vector_clock
                            .compare(&existing_location.vector_clock)
                        {
                            ClockOrdering::After => {
                                actor_state.known_actors.insert(name, location);
                                updated_actors += 1;
                            }
                            ClockOrdering::Concurrent => {
                                // Use node ID as tiebreaker
                                if location.node_id > existing_location.node_id {
                                    actor_state.known_actors.insert(name, location);
                                    updated_actors += 1;
                                }
                            }
                            _ => {} // Keep existing
                        }
                    }
                    None => {
                        actor_state.known_actors.insert(name, location);
                        new_actors += 1;
                    }
                }
            }
        }

        debug!(
            new_actors = new_actors,
            updated_actors = updated_actors,
            "merged gossip data using vector clock conflict resolution"
        );
    }

    /// Clean up stale actor entries (using wall clock for TTL)
    pub async fn cleanup_stale_actors(&self) {
        let now = current_timestamp();
        let ttl_secs = self.config.actor_ttl.as_secs();

        // Clean up stale known actors (using wall clock time for TTL)
        {
            let mut actor_state = self.actor_state.write().await;
            let before_count = actor_state.known_actors.len();
            actor_state
                .known_actors
                .retain(|_, location| now - location.wall_clock_time < ttl_secs);

            let removed = before_count - actor_state.known_actors.len();
            if removed > 0 {
                info!(removed_count = removed, "cleaned up stale actor entries");
            }
        }

        // Clean up old delta history (using wall clock)
        {
            let mut gossip_state = self.gossip_state.lock().await;
            let history_ttl = self.config.actor_ttl.as_secs() * 2;
            gossip_state
                .delta_history
                .retain(|delta| now - delta.wall_clock_time < history_ttl);
        }

        // Clean up connection pool
        {
            let mut connection_pool = self.connection_pool.lock().await;
            connection_pool.cleanup_stale_connections();
        }
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        info!("shutting down gossip registry");
        let mut gossip_state = self.gossip_state.lock().await;
        gossip_state.shutdown = true;
    }

    pub async fn is_shutdown(&self) -> bool {
        let gossip_state = self.gossip_state.lock().await;
        gossip_state.shutdown
    }

    /// Record activity from a vector clock (all nodes mentioned are active)
    pub async fn record_vector_clock_activity(&self, vector_clock: &VectorClock) {
        for &node_id in vector_clock.get_nodes().iter() {
            self.record_node_activity(node_id).await;
        }
    }

    /// Record that a peer (by address) is active
    /// This requires finding the node_id associated with the peer address
    pub async fn record_peer_activity(&self, peer_addr: SocketAddr) {
        let gossip_state = self.gossip_state.lock().await;

        // Try to find the node_id for this peer from recent gossip exchanges
        if let Some(peer_info) = gossip_state.peers.get(&peer_addr) {
            // Get the most recent node_id we've seen from this peer
            let recent_nodes: Vec<NodeId> = peer_info
                .last_vector_clock
                .get_nodes()
                .into_iter()
                .collect();
            drop(gossip_state); // Release the lock before calling record_node_activity

            for node_id in recent_nodes {
                self.record_node_activity(node_id).await;
            }
        }
    }

    /// Enhanced record_node_activity with debug logging
    pub async fn record_node_activity(&self, node_id: NodeId) {
        let mut gossip_state = self.gossip_state.lock().await;
        let current_time = current_timestamp();

        let was_new = !gossip_state.node_last_seen.contains_key(&node_id);
        gossip_state.node_last_seen.insert(node_id, current_time);

        if was_new {
            debug!(node_id = %node_id, "recorded activity for new node");
        }
    }

    /// Record activity when we learn about actors (they indicate their nodes are active)
    async fn record_actor_node_activity(&self, actors: &HashMap<String, ActorLocation>) {
        for location in actors.values() {
            self.record_node_activity(location.node_id).await;
            self.record_vector_clock_activity(&location.vector_clock)
                .await;
        }
    }

    /// Deduplicate changes, keeping only the most recent change for each actor
    fn deduplicate_changes(changes: Vec<RegistryChange>) -> Vec<RegistryChange> {
        let mut actor_changes: HashMap<String, RegistryChange> = HashMap::new();

        for change in changes {
            let actor_name = Self::get_change_actor_name(&change);

            match actor_changes.get(&actor_name) {
                Some(existing_change) => {
                    // Keep the change with the newer vector clock
                    if Self::should_replace_change(existing_change, &change) {
                        actor_changes.insert(actor_name, change);
                    }
                    // Otherwise keep the existing change
                }
                None => {
                    // First change for this actor
                    actor_changes.insert(actor_name, change);
                }
            }
        }

        actor_changes.into_values().collect()
    }

    /// Extract the actor name from a registry change
    fn get_change_actor_name(change: &RegistryChange) -> String {
        match change {
            RegistryChange::ActorAdded { name, .. } | RegistryChange::ActorRemoved { name, .. } => {
                name.clone()
            }
        }
    }

    /// Extract the vector clock from a registry change
    fn get_change_vector_clock(change: &RegistryChange) -> &VectorClock {
        match change {
            RegistryChange::ActorAdded { vector_clock, .. }
            | RegistryChange::ActorRemoved { vector_clock, .. } => vector_clock,
        }
    }

    /// Extract the originating node ID from a registry change
    fn get_change_originating_node(change: &RegistryChange) -> Option<NodeId> {
        match change {
            RegistryChange::ActorAdded { location, .. } => Some(location.node_id),
            RegistryChange::ActorRemoved { vector_clock, .. } => {
                // For removals, use the node with the highest clock value as the likely originator
                vector_clock
                    .clocks
                    .iter()
                    .max_by_key(|(_, &clock_value)| clock_value)
                    .map(|(&node_id, _)| node_id)
            }
        }
    }

    /// Determine if we should replace an existing change with a new one
    fn should_replace_change(existing: &RegistryChange, new: &RegistryChange) -> bool {
        let existing_clock = Self::get_change_vector_clock(existing);
        let new_clock = Self::get_change_vector_clock(new);

        match new_clock.compare(existing_clock) {
            ClockOrdering::After => {
                // New change is definitely newer
                true
            }
            ClockOrdering::Before => {
                // New change is older
                false
            }
            ClockOrdering::Equal => {
                // Identical changes - prefer the new one to ensure we're sending the latest version
                true
            }
            ClockOrdering::Concurrent => {
                // For concurrent changes, use a consistent tiebreaker
                Self::resolve_concurrent_change_conflict(existing, new)
            }
        }
    }

    /// Resolve conflicts between concurrent changes using consistent tiebreakers
    fn resolve_concurrent_change_conflict(existing: &RegistryChange, new: &RegistryChange) -> bool {
        // Tiebreaker 1: Prefer additions over removals (bias toward keeping actors)
        match (existing, new) {
            (RegistryChange::ActorRemoved { .. }, RegistryChange::ActorAdded { .. }) => {
                debug!("concurrent change conflict: preferring addition over removal");
                return true;
            }
            (RegistryChange::ActorAdded { .. }, RegistryChange::ActorRemoved { .. }) => {
                debug!("concurrent change conflict: keeping addition over removal");
                return false;
            }
            _ => {
                // Both are the same type (both additions or both removals)
            }
        }

        // Tiebreaker 2: Use originating node ID
        let existing_node = Self::get_change_originating_node(existing);
        let new_node = Self::get_change_originating_node(new);

        match (existing_node, new_node) {
            (Some(existing_id), Some(new_id)) => {
                let should_replace = new_id > existing_id;
                debug!(
                    existing_node = %existing_id,
                    new_node = %new_id,
                    replacing = should_replace,
                    "concurrent change conflict: resolved using node ID"
                );
                should_replace
            }
            (None, Some(_)) => {
                debug!("concurrent change conflict: preferring change with known originating node");
                true
            }
            (Some(_), None) => {
                debug!("concurrent change conflict: keeping change with known originating node");
                false
            }
            (None, None) => {
                // Fallback: use vector clock string representation for deterministic ordering
                let existing_clock = Self::get_change_vector_clock(existing);
                let new_clock = Self::get_change_vector_clock(new);
                let should_replace = format!("{new_clock:?}") > format!("{existing_clock:?}");
                debug!(
                replacing = should_replace,
                "concurrent change conflict: resolved using vector clock string comparison (fallback)"
            );
                should_replace
            }
        }
    }
}
