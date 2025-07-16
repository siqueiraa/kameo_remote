use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{interval, Instant},
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    current_timestamp,
    registry::{GossipRegistry, GossipResult, GossipTask, RegistryMessage, RegistryStats},
    ActorLocation, GossipConfig, GossipError, NodeId, Result, VectorClock,
};

/// Main API for the gossip registry with vector clocks and separated locks
pub struct GossipRegistryHandle {
    registry: Arc<GossipRegistry>,
    _server_handle: tokio::task::JoinHandle<()>,
    _timer_handle: tokio::task::JoinHandle<()>,
}

impl GossipRegistryHandle {
    /// Create and start a new gossip registry with vector clocks
    #[instrument(skip(peers, config))]
    pub async fn new(
        node_id: NodeId,
        bind_addr: SocketAddr,
        peers: Vec<SocketAddr>,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let config = config.unwrap_or_default();
        let registry = GossipRegistry::new(node_id, bind_addr, config.clone());
        registry.add_bootstrap_peers(peers).await;

        let registry = Arc::new(registry);

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
        // Bootstrap with readiness check
        let bootstrap_registry = registry.clone();
        let bootstrap_config = config.clone();
        tokio::spawn(async move {
            if let Err(err) =
                wait_for_server_readiness_and_bootstrap(bootstrap_registry, bootstrap_config).await
            {
                error!(error = %err, "bootstrap failed after readiness check");
            }
        });

        info!(node_id = %node_id, bind_addr = %bind_addr, "vector clock gossip registry started");

        Ok(Self {
            registry,
            _server_handle: server_handle,
            _timer_handle: timer_handle,
        })
    }

    /// Register a local actor (vector clock will be created automatically)
    pub async fn register(&self, name: String, node_id: NodeId, address: SocketAddr) -> Result<()> {
        // Let the registry create the full ActorLocation internally
        let location = ActorLocation::new(node_id, address, VectorClock::new());
        self.registry.register_actor(name, location).await
    }

    /// Unregister a local actor
    pub async fn unregister(&self, name: &str) -> Result<Option<ActorLocation>> {
        self.registry.unregister_actor(name).await
    }

    /// Lookup an actor (now much faster - read-only lock)
    pub async fn lookup(&self, name: &str) -> Option<ActorLocation> {
        self.registry.lookup_actor(name).await
    }

    /// Get registry statistics including vector clock metrics
    pub async fn stats(&self) -> RegistryStats {
        self.registry.get_stats().await
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        self.registry.shutdown().await;
    }
}

/// Start the gossip registry server with vector clock support
#[instrument(skip(registry))]
async fn start_gossip_server(registry: Arc<GossipRegistry>) -> Result<()> {
    let bind_addr = registry.bind_addr;
    let node_id = registry.node_id;

    let listener = TcpListener::bind(bind_addr).await?;
    info!(bind_addr = %bind_addr, node_id = %node_id, "gossip server started with vector clock support");

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

/// Start the gossip timer with vector clock support
#[instrument(skip(registry))]
async fn start_gossip_timer(registry: Arc<GossipRegistry>) {
    let gossip_interval = registry.config.gossip_interval;
    let cleanup_interval = registry.config.cleanup_interval;
    let gc_interval = registry.config.vector_clock_gc_frequency;

    let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
    let mut next_gossip_tick = Instant::now() + gossip_interval + jitter;
    let mut cleanup_timer = interval(cleanup_interval);
    let mut gc_timer = interval(gc_interval);

    info!(
        gossip_interval_ms = gossip_interval.as_millis(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
        "gossip timer started with vector clock and non-blocking I/O"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_gossip_tick) => {
                let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
                next_gossip_tick += gossip_interval + jitter;

                // Step 1: Prepare gossip tasks while holding the lock briefly
                let tasks = {
                    if registry.is_shutdown().await {
                        break;
                    }
                    match registry.prepare_gossip_round().await {
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
                    if !registry.is_shutdown().await {
                        registry.apply_gossip_results(results).await;
                    }
                }
            }
            _ = cleanup_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                registry.cleanup_stale_actors().await;
            }
            _ = gc_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                registry.gc_vector_clocks().await;
            }
        }
    }

    info!("gossip timer stopped");
}

/// Handle incoming TCP connections with vector clock support
#[instrument(skip(stream, registry), fields(peer = %peer_addr))]
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    debug!("handling new connection with vector clock support");

    let max_message_size = registry.config.max_message_size;

    match read_message(&mut stream, max_message_size).await {
        Ok(msg) => {
            let sender_node_id = match &msg {
                RegistryMessage::DeltaGossip { delta } => Some(delta.sender_id),
                RegistryMessage::FullSync { sender_id, .. } => Some(*sender_id),
                RegistryMessage::FullSyncRequest { sender_id, .. } => Some(*sender_id),
                RegistryMessage::DeltaGossipResponse { delta } => Some(delta.sender_id),
                RegistryMessage::FullSyncResponse { sender_id, .. } => Some(*sender_id),
            };

            if let Some(node_id) = sender_node_id {
                registry.record_node_activity(node_id).await;
            }

            match &msg {
                RegistryMessage::DeltaGossip { delta } => {
                    registry
                        .record_vector_clock_activity(&delta.vector_clock)
                        .await;
                }
                RegistryMessage::FullSync { vector_clock, .. }
                | RegistryMessage::FullSyncRequest { vector_clock, .. }
                | RegistryMessage::FullSyncResponse { vector_clock, .. } => {
                    registry.record_vector_clock_activity(vector_clock).await;
                }
                RegistryMessage::DeltaGossipResponse { delta } => {
                    registry
                        .record_vector_clock_activity(&delta.vector_clock)
                        .await;
                }
            }

            match msg {
                RegistryMessage::DeltaGossip { delta } => {
                    debug!(
                        sender = %delta.sender_id,
                        since_sequence = delta.since_sequence,
                        changes = delta.changes.len(),
                        "received delta gossip message with vector clock"
                    );

                    let response = {
                        // Apply the incoming delta
                        if let Err(err) = registry.apply_delta(delta.clone()).await {
                            warn!(error = %err, "failed to apply delta, sending full sync response");

                            let (local_actors, known_actors) = {
                                let actor_state = registry.actor_state.read().await;
                                (
                                    actor_state.local_actors.clone(),
                                    actor_state.known_actors.clone(),
                                )
                            };

                            let sequence = {
                                let gossip_state = registry.gossip_state.lock().await;
                                gossip_state.gossip_sequence
                            };

                            registry
                                .create_full_sync_response_from_state(
                                    &local_actors,
                                    &known_actors,
                                    sequence,
                                )
                                .await
                        } else {
                            // Update peer information
                            registry.add_peer(delta.sender_addr).await;

                            {
                                let mut gossip_state = registry.gossip_state.lock().await;
                                if let Some(peer_info) =
                                    gossip_state.peers.get_mut(&delta.sender_addr)
                                {
                                    peer_info.last_sequence = std::cmp::max(
                                        peer_info.last_sequence,
                                        delta.current_sequence,
                                    );
                                    peer_info.consecutive_deltas += 1;
                                    peer_info.last_vector_clock = delta.vector_clock.clone();
                                }
                                gossip_state.delta_exchanges += 1;
                            }

                            // Create delta response
                            let (local_actors, known_actors) = {
                                let actor_state = registry.actor_state.read().await;
                                (
                                    actor_state.local_actors.clone(),
                                    actor_state.known_actors.clone(),
                                )
                            };

                            let gossip_state = registry.gossip_state.lock().await;
                            match registry
                                .create_delta_response_from_state(
                                    &gossip_state,
                                    &local_actors,
                                    &known_actors,
                                    delta.current_sequence,
                                    &delta.vector_clock,
                                )
                                .await
                            {
                                Ok(delta_response) => delta_response,
                                Err(_) => {
                                    warn!("failed to create delta response, falling back to full sync");
                                    registry
                                        .create_full_sync_response_from_state(
                                            &local_actors,
                                            &known_actors,
                                            gossip_state.gossip_sequence,
                                        )
                                        .await
                                }
                            }
                        }
                    };

                    if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                        warn!(error = %err, "failed to send delta response");
                    } else {
                        debug!("delta exchange completed with vector clock");
                    }
                }
                RegistryMessage::FullSync {
                    local_actors,
                    known_actors,
                    sender_id,
                    sender_addr,
                    vector_clock,
                    sequence,
                    wall_clock_time,
                } => {
                    debug!(
                        sender = %sender_id,
                        sequence = sequence,
                        local_actors = local_actors.len(),
                        known_actors = known_actors.len(),
                        "received full sync message with vector clock"
                    );

                    registry
                        .merge_full_sync(
                            local_actors,
                            known_actors,
                            sender_addr,
                            vector_clock,
                            sequence,
                            wall_clock_time,
                        )
                        .await;

                    // Reset delta counter for this peer
                    {
                        let mut gossip_state = registry.gossip_state.lock().await;
                        if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                            peer_info.consecutive_deltas = 0;
                        }
                        gossip_state.full_sync_exchanges += 1;
                    }

                    let response = {
                        let (local_actors, known_actors) = {
                            let actor_state = registry.actor_state.read().await;
                            (
                                actor_state.local_actors.clone(),
                                actor_state.known_actors.clone(),
                            )
                        };

                        let sequence = {
                            let gossip_state = registry.gossip_state.lock().await;
                            gossip_state.gossip_sequence
                        };

                        registry
                            .create_full_sync_response_from_state(
                                &local_actors,
                                &known_actors,
                                sequence,
                            )
                            .await
                    };

                    if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                        warn!(error = %err, "failed to send full sync response");
                    } else {
                        debug!("full sync exchange completed with vector clock");
                    }
                }
                RegistryMessage::FullSyncRequest {
                    sender_id,
                    sender_addr,
                    vector_clock: _,
                    sequence: _,
                    wall_clock_time: _,
                } => {
                    debug!(
                        sender = %sender_id,
                        "received full sync request with vector clock"
                    );

                    registry.add_peer(sender_addr).await;

                    {
                        let mut gossip_state = registry.gossip_state.lock().await;
                        gossip_state.full_sync_exchanges += 1;
                    }

                    let response = {
                        let (local_actors, known_actors) = {
                            let actor_state = registry.actor_state.read().await;
                            (
                                actor_state.local_actors.clone(),
                                actor_state.known_actors.clone(),
                            )
                        };

                        let sequence = {
                            let gossip_state = registry.gossip_state.lock().await;
                            gossip_state.gossip_sequence
                        };

                        registry
                            .create_full_sync_response_from_state(
                                &local_actors,
                                &known_actors,
                                sequence,
                            )
                            .await
                    };

                    if let Err(err) = send_message(&mut stream, &response, max_message_size).await {
                        warn!(error = %err, "failed to send full sync response");
                    } else {
                        debug!("full sync request handled with vector clock");
                    }
                }
                // Handle response messages (shouldn't normally happen in server, but handle gracefully)
                RegistryMessage::DeltaGossipResponse { delta } => {
                    debug!(
                        sender = %delta.sender_id,
                        changes = delta.changes.len(),
                        "received delta gossip response in server mode with vector clock"
                    );

                    if let Err(err) = registry.apply_delta(delta).await {
                        warn!(error = %err, "failed to apply delta from response");
                    } else {
                        let mut gossip_state = registry.gossip_state.lock().await;
                        gossip_state.delta_exchanges += 1;
                    }
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
                        sender = %sender_id,
                        "received full sync response in server mode with vector clock"
                    );

                    registry
                        .merge_full_sync(
                            local_actors,
                            known_actors,
                            sender_addr,
                            vector_clock,
                            sequence,
                            wall_clock_time,
                        )
                        .await;

                    let mut gossip_state = registry.gossip_state.lock().await;
                    gossip_state.full_sync_exchanges += 1;
                }
            }
        }
        Err(err) => {
            warn!(error = %err, "failed to read message");
        }
    }

    debug!("connection closed");
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

/// Bootstrap by connecting to known peers and doing initial gossip with vector clocks
#[instrument(skip(registry))]
async fn bootstrap_peers(registry: Arc<GossipRegistry>) -> Result<()> {
    let node_id = registry.node_id;
    let bind_addr = registry.bind_addr;
    let config = registry.config.clone();

    let peers = {
        let gossip_state = registry.gossip_state.lock().await;
        gossip_state.peers.keys().cloned().collect::<Vec<_>>()
    };

    info!(
        peer_count = peers.len(),
        "starting bootstrap with vector clock support"
    );

    for peer in peers {
        // Always use full sync for bootstrap
        let gossip_msg = {
            let (local_actors, known_actors) = {
                let actor_state = registry.actor_state.read().await;
                (
                    actor_state.local_actors.clone(),
                    actor_state.known_actors.clone(),
                )
            };

            let sequence = {
                let gossip_state = registry.gossip_state.lock().await;
                gossip_state.gossip_sequence
            };

            let vector_clock = {
                let vector_clock = registry.vector_clock.lock().await;
                vector_clock.clone()
            };

            RegistryMessage::FullSync {
                local_actors,
                known_actors,
                sender_id: node_id,
                sender_addr: bind_addr,
                vector_clock,
                sequence,
                wall_clock_time: current_timestamp(),
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
                        vector_clock,
                        sequence,
                        wall_clock_time,
                    })) => {
                        debug!(
                            peer = %peer,
                            remote_node = %response_sender_id,
                            "received bootstrap response with vector clock"
                        );

                        // Record that this node is active
                        registry.record_node_activity(response_sender_id).await;
                        registry.record_vector_clock_activity(&vector_clock).await;

                        // Also record activity from all actors we learned about
                        for location in local_actors.values().chain(known_actors.values()) {
                            registry.record_node_activity(location.node_id).await;
                            registry
                                .record_vector_clock_activity(&location.vector_clock)
                                .await;
                        }

                        registry
                            .merge_full_sync(
                                local_actors,
                                known_actors,
                                response_sender_addr,
                                vector_clock,
                                sequence,
                                wall_clock_time,
                            )
                            .await;

                        let mut gossip_state = registry.gossip_state.lock().await;
                        gossip_state.full_sync_exchanges += 1;
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

    info!("bootstrap completed with vector clock support");
    Ok(())
}

/// Execute a single gossip task with registry access for connection pool
async fn execute_gossip_task_with_registry(
    task: GossipTask,
    registry: Arc<GossipRegistry>,
) -> GossipResult {
    let result = async {
        // Get connection and config (brief lock)
        let mut conn = {
            let mut connection_pool = registry.connection_pool.lock().await;
            connection_pool.checkout_connection(task.peer_addr).await?
        };

        let max_message_size = registry.config.max_message_size;
        let response_timeout = registry.config.response_timeout;

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
                    let mut connection_pool = registry.connection_pool.lock().await;
                    connection_pool.checkin_success(conn);
                }
                Ok(Some(response))
            }
            Ok(Err(err)) => {
                // Error - mark connection as failed (brief lock)
                {
                    let mut connection_pool = registry.connection_pool.lock().await;
                    connection_pool.checkin_failure(conn);
                }
                Err(err)
            }
            Err(_) => {
                // Timeout - not necessarily an error for gossip (brief lock)
                debug!(peer = %task.peer_addr, "gossip response timeout");
                {
                    let mut connection_pool = registry.connection_pool.lock().await;
                    connection_pool.checkin_failure(conn);
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

/// Wait for the server to be ready, then bootstrap
async fn wait_for_server_readiness_and_bootstrap(
    registry: Arc<GossipRegistry>,
    config: GossipConfig,
) -> Result<()> {
    let bind_addr = registry.bind_addr;

    // Wait for server to become ready
    let readiness_timeout = tokio::time::timeout(
        config.bootstrap_readiness_timeout,
        wait_for_server_ready(bind_addr, config.bootstrap_readiness_check_interval),
    );

    match readiness_timeout.await {
        Ok(Ok(())) => {
            info!("server is ready, starting bootstrap");
        }
        Ok(Err(err)) => {
            warn!(error = %err, "readiness check failed, proceeding with bootstrap anyway");
        }
        Err(_) => {
            warn!("readiness check timeout, proceeding with bootstrap anyway");
        }
    }

    // Attempt bootstrap with retries
    for attempt in 1..=config.bootstrap_max_retries {
        match bootstrap_peers(registry.clone()).await {
            Ok(()) => {
                info!(attempt = attempt, "bootstrap completed successfully");
                return Ok(());
            }
            Err(err) => {
                if attempt < config.bootstrap_max_retries {
                    warn!(
                        attempt = attempt,
                        max_attempts = config.bootstrap_max_retries,
                        error = %err,
                        "bootstrap attempt failed, retrying"
                    );
                    tokio::time::sleep(config.bootstrap_retry_delay).await;
                } else {
                    error!(
                        attempt = attempt,
                        error = %err,
                        "bootstrap failed after all retry attempts"
                    );
                    return Err(err);
                }
            }
        }
    }

    Ok(())
}

/// Check if the server is ready by attempting to connect to our own bind address
async fn wait_for_server_ready(bind_addr: SocketAddr, check_interval: Duration) -> Result<()> {
    loop {
        match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(bind_addr)).await
        {
            Ok(Ok(_stream)) => {
                debug!(bind_addr = %bind_addr, "server readiness confirmed");
                return Ok(());
            }
            Ok(Err(_)) | Err(_) => {
                // Server not ready yet, wait and retry
                tokio::time::sleep(check_interval).await;
            }
        }
    }
}
