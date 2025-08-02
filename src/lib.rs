pub mod remote_actor_location;
pub mod config;
pub mod connection_pool;
mod handle;
mod handle_builder;
pub mod priority;
pub mod registry;
pub mod reply_to;
pub mod stream_writer;

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::error;

pub use remote_actor_location::RemoteActorLocation;
pub use config::GossipConfig;
pub use handle::GossipRegistryHandle;
pub use handle_builder::GossipRegistryBuilder;
pub use priority::{ConsistencyLevel, RegistrationPriority};
pub use connection_pool::{DelegatedReplySender, LockFreeStreamHandle, StreamFrameType, ChannelId};
pub use reply_to::{ReplyTo, TimeoutReplyTo};

/// Key pair for node identity (using strings for now, will be cryptographic keys later)
#[derive(Debug, Clone)]
pub struct KeyPair {
    /// Public key - shared with other nodes
    pub public_key: String,
    /// Private key - kept secret by the node
    pub private_key: String,
}

impl KeyPair {
    /// Create a new key pair
    pub fn new(public_key: impl Into<String>, private_key: impl Into<String>) -> Self {
        Self {
            public_key: public_key.into(),
            private_key: private_key.into(),
        }
    }
    
    /// For testing - create a key pair where private key = public key
    pub fn new_for_testing(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            public_key: id.clone(),
            private_key: id,
        }
    }
}

/// Peer identifier - just the public key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PeerId(String);

impl PeerId {
    /// Create a new peer ID from a public key
    pub fn new(public_key: impl Into<String>) -> Self {
        Self(public_key.into())
    }
    
    /// Get the public key as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for PeerId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for PeerId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Handle to a configured peer
#[derive(Clone)]
pub struct Peer {
    peer_id: PeerId,
    registry: std::sync::Arc<registry::GossipRegistry>,
}

impl Peer {
    /// Connect to this peer at the specified address
    pub async fn connect(&self, addr: &SocketAddr) -> Result<()> {
        // Validate the address
        if addr.port() == 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid port 0 for peer {}", self.peer_id),
            )));
        }

        // First configure the address for this peer
        {
            let pool = self.registry.connection_pool.lock().await;
            pool.peer_id_to_addr.insert(self.peer_id.clone(), *addr);
        }
        
        // Add the peer to gossip state so it can be selected for gossip rounds
        {
            tracing::debug!("🎯 Peer::connect - About to add peer {} to gossip state", self.peer_id);
            let mut gossip_state = self.registry.gossip_state.lock().await;
            let current_time = crate::current_timestamp();
            let peers_before = gossip_state.peers.len();
            gossip_state.peers.insert(
                *addr,
                crate::registry::PeerInfo {
                    address: *addr,
                    peer_address: None,
                    failures: 0,  // Start with 0 failures
                    last_attempt: current_time,  // Set last_attempt to now
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                },
            );
            let peers_after = gossip_state.peers.len();
            tracing::debug!(
                peer_id = %self.peer_id,
                addr = %addr,
                peers_before = peers_before,
                peers_after = peers_after,
                "🎯 Added peer to gossip state for gossip rounds"
            );
        }
        
        // Then attempt to connect with enhanced error context
        match self.registry.connect_to_peer(self.peer_id.as_str()).await {
            Ok(()) => {
                tracing::info!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Successfully connected to peer"
                );
                // Trigger an immediate gossip round to sync
                self.registry.trigger_immediate_gossip().await;
                Ok(())
            }
            Err(GossipError::Network(io_err)) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %io_err,
                    "Network error connecting to peer"
                );
                
                // Update peer failure state in gossip state
                {
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(addr) {
                        peer_info.failures = self.registry.config.max_peer_failures;
                        peer_info.last_failure_time = Some(crate::current_timestamp());
                        tracing::debug!(
                            peer_id = %self.peer_id,
                            addr = %addr,
                            failures = peer_info.failures,
                            "Updated peer failure state after connection error"
                        );
                    }
                }
                
                Err(GossipError::Network(std::io::Error::new(
                    io_err.kind(),
                    format!("Failed to connect to peer {} at {}: {}", self.peer_id, addr, io_err),
                )))
            }
            Err(GossipError::Timeout) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Connection timeout when connecting to peer"
                );
                
                // Update peer failure state in gossip state
                {
                    let mut gossip_state = self.registry.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(addr) {
                        peer_info.failures = self.registry.config.max_peer_failures;
                        peer_info.last_failure_time = Some(crate::current_timestamp());
                    }
                }
                
                Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to peer {} at {}", self.peer_id, addr),
                )))
            }
            Err(GossipError::ConnectionExists) => {
                tracing::debug!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Connection already exists to peer"
                );
                // This is not really an error - connection already exists
                Ok(())
            }
            Err(GossipError::Shutdown) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Registry is shutting down, cannot connect to peer"
                );
                Err(GossipError::Shutdown)
            }
            Err(other_err) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %other_err,
                    "Unexpected error connecting to peer"
                );
                Err(other_err)
            }
        }
    }
    
    /// Connect to this peer with retry attempts
    pub async fn connect_with_retry(
        &self, 
        addr: &SocketAddr, 
        max_retries: u32, 
        retry_delay: std::time::Duration
    ) -> Result<()> {
        let mut last_error = None;
        
        for attempt in 0..=max_retries {
            match self.connect(addr).await {
                Ok(()) => return Ok(()),
                Err(GossipError::Shutdown) => {
                    // Don't retry if registry is shutting down
                    return Err(GossipError::Shutdown);
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt < max_retries {
                        tracing::warn!(
                            peer_id = %self.peer_id,
                            addr = %addr,
                            attempt = attempt + 1,
                            max_retries = max_retries,
                            "Connection attempt failed, retrying in {:?}",
                            retry_delay
                        );
                        tokio::time::sleep(retry_delay).await;
                    }
                }
            }
        }
        
        // All retries failed
        let final_error = last_error.unwrap_or_else(|| GossipError::Network(
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unknown error during connection attempts"
            )
        ));
        
        tracing::error!(
            peer_id = %self.peer_id,
            addr = %addr,
            max_retries = max_retries,
            "All connection attempts failed"
        );
        
        Err(final_error)
    }
    
    /// Check if this peer is currently connected
    pub async fn is_connected(&self) -> bool {
        let pool = self.registry.connection_pool.lock().await;
        
        // Check if we have a connection by peer ID
        if let Some(conn) = pool.get_connection_by_node_id(self.peer_id.as_str()) {
            conn.is_connected()
        } else {
            false
        }
    }
    
    /// Disconnect from this peer
    pub async fn disconnect(&self) -> Result<()> {
        let mut pool = self.registry.connection_pool.lock().await;
        
        if let Some(conn) = pool.get_connection_by_node_id(self.peer_id.as_str()) {
            // Mark connection as disconnected
            conn.set_state(crate::connection_pool::ConnectionState::Disconnected);
            
            // Get the peer address for mark_disconnected
            let peer_addr = pool.peer_id_to_addr.get(&self.peer_id).map(|addr| *addr.value());
            if let Some(addr) = peer_addr {
                pool.mark_disconnected(addr);
            }
            
            tracing::info!(
                peer_id = %self.peer_id,
                "Disconnected from peer"
            );
            Ok(())
        } else {
            tracing::debug!(
                peer_id = %self.peer_id,
                "No connection found to disconnect"
            );
            Ok(()) // Not an error if no connection exists
        }
    }
    
    /// Get the peer ID
    pub fn id(&self) -> &PeerId {
        &self.peer_id
    }
    
    /// Wait for the initial sync with this peer to complete
    /// 
    /// This waits for:
    /// 1. The connection to be established
    /// 2. The initial FullSync to be exchanged
    /// 3. The actor registry to be updated
    pub async fn wait_for_sync(&self, timeout: Duration) -> Result<()> {
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;
        
        // Wait for the connection to be established
        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(GossipError::Timeout);
            }
            
            // Check if we have a connection to this peer
            {
                let pool = self.registry.connection_pool.lock().await;
                if pool.get_connection_by_node_id(self.peer_id.as_str()).is_some() {
                    break;
                }
            }
            
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Wait for gossip sync to complete by checking if we've received actors
        let mut last_actor_count = 0;
        let mut stable_iterations = 0;
        
        loop {
            if tokio::time::Instant::now() > deadline {
                return Err(GossipError::Timeout);
            }
            
            // Get current actor count
            let current_count = self.registry.get_actor_count().await;
            
            // If the count is stable for 3 iterations (30ms), we're synced
            if current_count == last_actor_count && current_count > 0 {
                stable_iterations += 1;
                if stable_iterations >= 3 {
                    tracing::info!(
                        peer_id = %self.peer_id,
                        actor_count = current_count,
                        elapsed = ?start.elapsed(),
                        "Initial sync completed"
                    );
                    return Ok(());
                }
            } else {
                stable_iterations = 0;
                last_actor_count = current_count;
            }
            
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Message types for the request-response protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Gossip protocol message (registry sync, actor registrations, etc.)
    Gossip = 0,
    /// Request expecting a response (ask)
    Ask = 1,
    /// Response to an ask request
    Response = 2,
    /// Direct actor tell message (no wrapping)
    ActorTell = 3,
    /// Direct actor ask message (no wrapping)
    ActorAsk = 4,
    /// Start of a streaming transfer
    StreamStart = 0x10,
    /// Streaming data chunk
    StreamData = 0x11,
    /// End of streaming transfer
    StreamEnd = 0x12,
}

impl MessageType {
    /// Parse message type from byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(MessageType::Gossip),
            1 => Some(MessageType::Ask),
            2 => Some(MessageType::Response),
            3 => Some(MessageType::ActorTell),
            4 => Some(MessageType::ActorAsk),
            0x10 => Some(MessageType::StreamStart),
            0x11 => Some(MessageType::StreamData),
            0x12 => Some(MessageType::StreamEnd),
            _ => None,
        }
    }
}

/// Header for streaming protocol messages
#[derive(Debug, Clone, Copy)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StreamHeader {
    /// Unique stream identifier
    pub stream_id: u64,
    /// Total size of the complete message
    pub total_size: u64,
    /// Size of this chunk (0 for start/end markers)
    pub chunk_size: u32,
    /// Chunk sequence number
    pub chunk_index: u32,
    /// Message type hash
    pub type_hash: u32,
    /// Target actor ID
    pub actor_id: u64,
}

impl StreamHeader {
    /// Size of the serialized header
    pub const SERIALIZED_SIZE: usize = 8 + 8 + 4 + 4 + 4 + 8; // 36 bytes
    
    /// Serialize header to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_SIZE);
        bytes.extend_from_slice(&self.stream_id.to_be_bytes());
        bytes.extend_from_slice(&self.total_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());
        bytes.extend_from_slice(&self.type_hash.to_be_bytes());
        bytes.extend_from_slice(&self.actor_id.to_be_bytes());
        bytes
    }
    
    /// Parse header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SERIALIZED_SIZE {
            return None;
        }
        
        Some(Self {
            stream_id: u64::from_be_bytes(bytes[0..8].try_into().ok()?),
            total_size: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
            chunk_size: u32::from_be_bytes(bytes[16..20].try_into().ok()?),
            chunk_index: u32::from_be_bytes(bytes[20..24].try_into().ok()?),
            type_hash: u32::from_be_bytes(bytes[24..28].try_into().ok()?),
            actor_id: u64::from_be_bytes(bytes[28..36].try_into().ok()?),
        })
    }
}

/// Errors that can occur in the gossip registry
#[derive(Error, Debug)]
pub enum GossipError {
    #[error("network error: {0}")]
    Network(#[from] io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] rkyv::rancor::Error),

    #[error("message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("connection timeout")]
    Timeout,

    #[error("peer not found: {0}")]
    PeerNotFound(SocketAddr),

    #[error("actor not found: {0}")]
    ActorNotFound(String),

    #[error("registry shutdown")]
    Shutdown,

    #[error("delta too old: requested {requested}, oldest available {oldest}")]
    DeltaTooOld { requested: u64, oldest: u64 },

    #[error("full sync required")]
    FullSyncRequired,

    #[error("connection already exists")]
    ConnectionExists,

    #[error("actor '{0}' already exists")]
    ActorAlreadyExists(String),
}

pub type Result<T> = std::result::Result<T, GossipError>;

/// Get current timestamp in seconds (still used for TTL)
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

/// Get current timestamp in nanoseconds for high precision timing
pub fn current_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

/// Get high resolution instant for precise timing measurements
pub fn current_instant() -> std::time::Instant {
    std::time::Instant::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_timestamp() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timestamp = current_timestamp();

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(timestamp >= before);
        assert!(timestamp <= after);
    }

    #[test]
    fn test_gossip_error_display() {
        let err = GossipError::Network(io::Error::new(io::ErrorKind::Other, "test error"));
        assert_eq!(err.to_string(), "network error: test error");

        let err = GossipError::MessageTooLarge {
            size: 1000,
            max: 500,
        };
        assert_eq!(err.to_string(), "message too large: 1000 bytes (max: 500)");

        let err = GossipError::Timeout;
        assert_eq!(err.to_string(), "connection timeout");

        let err = GossipError::PeerNotFound("127.0.0.1:8080".parse().unwrap());
        assert_eq!(err.to_string(), "peer not found: 127.0.0.1:8080");

        let err = GossipError::ActorNotFound("test_actor".to_string());
        assert_eq!(err.to_string(), "actor not found: test_actor");

        let err = GossipError::Shutdown;
        assert_eq!(err.to_string(), "registry shutdown");

        let err = GossipError::DeltaTooOld {
            requested: 10,
            oldest: 20,
        };
        assert_eq!(
            err.to_string(),
            "delta too old: requested 10, oldest available 20"
        );

        let err = GossipError::FullSyncRequired;
        assert_eq!(err.to_string(), "full sync required");

        let err = GossipError::ConnectionExists;
        assert_eq!(err.to_string(), "connection already exists");

        let err = GossipError::ActorAlreadyExists("test_actor".to_string());
        assert_eq!(err.to_string(), "actor 'test_actor' already exists");
    }

    #[test]
    fn test_error_conversions() {
        // Test From<io::Error>
        let io_err = io::Error::new(io::ErrorKind::Other, "io error");
        let gossip_err: GossipError = io_err.into();
        match gossip_err {
            GossipError::Network(_) => (),
            _ => panic!("Expected Network error"),
        }

        // Test that error variants work correctly - using a different approach
        let timeout_err = GossipError::Timeout;
        match timeout_err {
            GossipError::Timeout => (),
            _ => panic!("Expected Timeout error"),
        }
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert_eq!(ok_result.unwrap(), 42);

        let err_result: Result<i32> = Err(GossipError::Timeout);
        assert!(err_result.is_err());
    }
}
