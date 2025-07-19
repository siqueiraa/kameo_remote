pub mod remote_actor_location;
pub mod config;
pub mod connection_pool;
mod handle;
pub mod priority;
pub mod registry;
pub mod reply_to;

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::error;

pub use remote_actor_location::RemoteActorLocation;
pub use config::GossipConfig;
pub use handle::GossipRegistryHandle;
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
        
        // Then attempt to connect with enhanced error context
        match self.registry.connect_to_peer(self.peer_id.as_str()).await {
            Ok(()) => {
                tracing::info!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    "Successfully connected to peer"
                );
                Ok(())
            }
            Err(GossipError::Network(io_err)) => {
                tracing::error!(
                    peer_id = %self.peer_id,
                    addr = %addr,
                    error = %io_err,
                    "Network error connecting to peer"
                );
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
}

/// Message types for the request-response protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Fire-and-forget message (tell)
    Tell = 0,
    /// Request expecting a response (ask)
    Ask = 1,
    /// Response to an ask request
    Response = 2,
}

impl MessageType {
    /// Parse message type from byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(MessageType::Tell),
            1 => Some(MessageType::Ask),
            2 => Some(MessageType::Response),
            _ => None,
        }
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
