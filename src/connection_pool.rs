use std::{collections::HashMap, net::SocketAddr, time::Duration};

use tokio::net::TcpStream;
use tracing::debug;

use crate::{current_timestamp, GossipError, Result};

/// Connection pool for reusing TCP connections across gossip rounds
pub struct ConnectionPool {
    connections: HashMap<SocketAddr, PooledConnection>,
    max_connections: usize,
    connection_timeout: Duration,
    connection_idle_timeout: Duration,
    checkout_timeout: Duration,
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
}

impl ConnectionPool {
    pub fn new(
        max_connections: usize,
        connection_timeout: Duration,
        connection_idle_timeout: Duration,
        checkout_timeout: Duration,
    ) -> Self {
        Self {
            connections: HashMap::new(),
            max_connections,
            connection_timeout,
            connection_idle_timeout,
            checkout_timeout,
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

        let addrs_to_remove: Vec<_> = self
            .connections
            .iter()
            .filter(|(_, pooled)| {
                // Remove if failed, checked out too long, or unused for too long
                pooled.failure_count > 0
                    || (current_time - pooled.last_used) > stale_threshold
                    || (pooled.checked_out
                        && (current_time - pooled.last_used) > self.checkout_timeout.as_secs())
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
}
