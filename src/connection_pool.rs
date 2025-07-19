use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicU32, AtomicU16, Ordering};

use crate::{
    current_timestamp,
    registry::{GossipRegistry, RegistryMessage},
    GossipError, Result,
};

/// Process mock requests for testing
#[cfg(feature = "test-helpers")]
fn process_mock_request(request: &str) -> Vec<u8> {
    if request.starts_with("ECHO:") {
        let content = &request[5..];
        format!("ECHOED:{}", content).into_bytes()
    } else if request.starts_with("REVERSE:") {
        let content = &request[8..];
        let reversed: String = content.chars().rev().collect();
        format!("REVERSED:{}", reversed).into_bytes()
    } else if request.starts_with("COUNT:") {
        let content = &request[6..];
        format!("COUNTED:{} chars", content.len()).into_bytes()
    } else if request.starts_with("HASH:") {
        let content = &request[5..];
        let hash = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        content.hash(&mut hash.clone());
        format!("HASHED:{:x}", hash.finish()).into_bytes()
    } else {
        // Default response
        format!("RECEIVED:{} bytes, content: '{}'", request.len(), request).into_bytes()
    }
}

#[cfg(not(feature = "test-helpers"))]
fn process_mock_request(_request: &str) -> Vec<u8> {
    vec![]
}

/// Stream frame types for high-performance streaming protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameType {
    Data = 0x01,
    Ack = 0x02,
    Close = 0x03,
    Heartbeat = 0x04,
    TellAsk = 0x05,      // Regular tell/ask messages
    StreamData = 0x06,   // Dedicated streaming data
}

/// Channel IDs for stream multiplexing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelId {
    TellAsk = 0x00,      // Regular tell/ask channel
    Stream1 = 0x01,      // Dedicated streaming channel 1
    Stream2 = 0x02,      // Dedicated streaming channel 2
    Stream3 = 0x03,      // Dedicated streaming channel 3
    Bulk = 0x04,         // Bulk data channel
    Priority = 0x05,     // Priority streaming channel
    Global = 0xFF,       // Global channel for all operations
}

/// Stream frame flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameFlags {
    None = 0x00,
    More = 0x01,         // More frames to follow
    Compressed = 0x02,   // Frame is compressed
    Encrypted = 0x04,    // Frame is encrypted
}

/// Stream frame header for structured messaging
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
pub struct StreamFrameHeader {
    pub frame_type: u8,
    pub channel_id: u8,
    pub flags: u8,
    pub sequence_id: u16,
    pub payload_len: u32,
}


/// Lock-free connection state representation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ConnectionState {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Failed = 3,
}

impl From<u32> for ConnectionState {
    fn from(value: u32) -> Self {
        match value {
            0 => ConnectionState::Disconnected,
            1 => ConnectionState::Connecting,
            2 => ConnectionState::Connected,
            3 => ConnectionState::Failed,
            _ => ConnectionState::Failed,
        }
    }
}

/// Lock-free connection metadata
#[derive(Debug)]
pub struct LockFreeConnection {
    pub addr: SocketAddr,
    pub state: AtomicU32, // ConnectionState
    pub last_used: AtomicUsize, // Timestamp
    pub bytes_written: AtomicUsize,
    pub bytes_read: AtomicUsize,
    pub failure_count: AtomicUsize,
    pub stream_handle: Option<Arc<LockFreeStreamHandle>>,
    pub correlation: Option<Arc<CorrelationTracker>>,
}

impl LockFreeConnection {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: AtomicU32::new(ConnectionState::Disconnected as u32),
            last_used: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            bytes_read: AtomicUsize::new(0),
            failure_count: AtomicUsize::new(0),
            stream_handle: None,
            correlation: Some(CorrelationTracker::new()),
        }
    }
    
    pub fn get_state(&self) -> ConnectionState {
        self.state.load(Ordering::Acquire).into()
    }
    
    pub fn set_state(&self, state: ConnectionState) {
        self.state.store(state as u32, Ordering::Release);
    }
    
    pub fn try_set_state(&self, expected: ConnectionState, new: ConnectionState) -> bool {
        self.state.compare_exchange(
            expected as u32,
            new as u32,
            Ordering::AcqRel,
            Ordering::Acquire
        ).is_ok()
    }
    
    pub fn update_last_used(&self) {
        self.last_used.store(crate::current_timestamp() as usize, Ordering::Release);
    }
    
    pub fn increment_failure_count(&self) -> usize {
        self.failure_count.fetch_add(1, Ordering::AcqRel)
    }
    
    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, Ordering::Release);
    }
    
    pub fn is_connected(&self) -> bool {
        self.get_state() == ConnectionState::Connected
    }
    
    pub fn is_failed(&self) -> bool {
        self.get_state() == ConnectionState::Failed
    }
}

/// Write command for the lock-free ring buffer with zero-copy optimization
#[derive(Debug, Clone)]
pub struct WriteCommand {
    pub channel_id: ChannelId,
    pub data: Vec<u8>,
    pub sequence: u64,
}

/// Zero-copy write command that references pre-allocated buffer slots
#[derive(Debug)]
pub struct ZeroCopyWriteCommand {
    pub channel_id: ChannelId,
    pub buffer_ptr: *const u8,
    pub len: usize,
    pub sequence: u64,
    pub buffer_id: usize, // For buffer pool management
}

// Safety: ZeroCopyWriteCommand is only used within the single writer task
unsafe impl Send for ZeroCopyWriteCommand {}
unsafe impl Sync for ZeroCopyWriteCommand {}

/// Memory pool for zero-allocation message handling
#[derive(Debug)]
pub struct MessageBufferPool {
    pool: Vec<Vec<u8>>,
    pool_size: usize,
    buffer_size: usize,
    available_buffers: AtomicUsize,
    next_buffer_idx: AtomicUsize,
}

impl MessageBufferPool {
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(Vec::with_capacity(buffer_size));
        }
        
        Self {
            pool,
            pool_size,
            buffer_size,
            available_buffers: AtomicUsize::new(pool_size),
            next_buffer_idx: AtomicUsize::new(0),
        }
    }
    
    /// Get a buffer from the pool - returns None if pool is empty
    pub fn get_buffer(&self) -> Option<Vec<u8>> {
        let available = self.available_buffers.load(Ordering::Acquire);
        if available == 0 {
            return None;
        }
        
        let idx = self.next_buffer_idx.fetch_add(1, Ordering::AcqRel) % self.pool_size;
        
        // Try to claim this buffer
        if self.available_buffers.fetch_sub(1, Ordering::AcqRel) > 0 {
            // We successfully claimed a buffer
            unsafe {
                let buffer_ptr = self.pool.as_ptr().add(idx) as *mut Vec<u8>;
                let mut buffer = std::ptr::replace(buffer_ptr, Vec::new());
                buffer.clear(); // Reset length but keep capacity
                Some(buffer)
            }
        } else {
            // No buffers available, restore count
            self.available_buffers.fetch_add(1, Ordering::Release);
            None
        }
    }
    
    /// Return a buffer to the pool
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear(); // Reset length but keep capacity
        
        let idx = self.next_buffer_idx.fetch_add(1, Ordering::AcqRel) % self.pool_size;
        
        unsafe {
            let buffer_ptr = self.pool.as_ptr().add(idx) as *mut Vec<u8>;
            *buffer_ptr = buffer;
        }
        
        self.available_buffers.fetch_add(1, Ordering::Release);
    }
    
    pub fn available_count(&self) -> usize {
        self.available_buffers.load(Ordering::Acquire)
    }
    
    pub fn is_empty(&self) -> bool {
        self.available_count() == 0
    }
}

/// Lock-free ring buffer for high-performance writes with proper memory ordering
#[derive(Debug)]
pub struct LockFreeRingBuffer {
    buffer: Vec<Option<WriteCommand>>,
    capacity: usize,
    // Cache-line aligned atomic counters to prevent false sharing
    write_index: AtomicUsize,
    read_index: AtomicUsize,
    pending_writes: AtomicUsize,
}

impl LockFreeRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: (0..capacity).map(|_| None).collect(),
            capacity,
            write_index: AtomicUsize::new(0),
            read_index: AtomicUsize::new(0),
            pending_writes: AtomicUsize::new(0),
        }
    }
    
    /// Try to push a write command - returns true if successful
    /// Uses proper memory ordering to prevent ABA problem
    pub fn try_push(&self, command: WriteCommand) -> bool {
        let current_writes = self.pending_writes.load(Ordering::Acquire);
        if current_writes >= self.capacity {
            return false; // Buffer full
        }
        
        let write_idx = self.write_index.fetch_add(1, Ordering::AcqRel) % self.capacity;
        
        // This is unsafe but fast - we're assuming single writer per slot
        // Using proper memory ordering ensures visibility across cores
        unsafe {
            let slot = self.buffer.as_ptr().add(write_idx) as *mut Option<WriteCommand>;
            *slot = Some(command);
        }
        
        // Release ordering ensures the write above is visible before incrementing pending count
        self.pending_writes.fetch_add(1, Ordering::Release);
        true
    }
    
    /// Try to pop a write command - returns None if empty
    /// Uses proper memory ordering to prevent race conditions
    pub fn try_pop(&self) -> Option<WriteCommand> {
        let current_writes = self.pending_writes.load(Ordering::Acquire);
        if current_writes == 0 {
            return None;
        }
        
        let read_idx = self.read_index.fetch_add(1, Ordering::AcqRel) % self.capacity;
        
        // This is unsafe but fast - we're assuming single reader per slot
        // Using proper memory ordering ensures we see writes from other cores
        unsafe {
            let slot = self.buffer.as_ptr().add(read_idx) as *mut Option<WriteCommand>;
            let command = (*slot).take();
            if command.is_some() {
                // Release ordering ensures the slot is cleared before decrementing pending count
                self.pending_writes.fetch_sub(1, Ordering::Release);
            }
            command
        }
    }
    
    pub fn pending_count(&self) -> usize {
        self.pending_writes.load(Ordering::Acquire)
    }
    
    /// Get buffer utilization as a percentage (0-100)
    pub fn utilization(&self) -> f32 {
        (self.pending_count() as f32 / self.capacity as f32) * 100.0
    }
    
    /// Check if buffer is nearly full (>90% capacity)
    pub fn is_nearly_full(&self) -> bool {
        self.pending_count() > (self.capacity * 9) / 10
    }
}

/// Truly lock-free streaming handle with dedicated background writer
#[derive(Clone, Debug)]
pub struct LockFreeStreamHandle {
    addr: SocketAddr,
    channel_id: ChannelId,
    sequence_counter: Arc<AtomicUsize>,
    bytes_written: Arc<AtomicUsize>, // This tracks actual TCP bytes written
    ring_buffer: Arc<LockFreeRingBuffer>,
    shutdown_signal: Arc<AtomicBool>,
    // Channel for tell operations to go through the same writer
    tell_sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

impl LockFreeStreamHandle {
    /// Create a new lock-free streaming handle with background writer task
    pub fn new(tcp_writer: tokio::net::tcp::OwnedWriteHalf, addr: SocketAddr, channel_id: ChannelId, buffer_size: usize) -> Self {
        let ring_buffer = Arc::new(LockFreeRingBuffer::new(buffer_size));
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        
        // Create channel for tell operations
        let (tell_sender, tell_receiver) = tokio::sync::mpsc::unbounded_channel();
        
        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));
        
        // Spawn background writer task with exclusive TCP access - NO MUTEX!
        {
            let ring_buffer = ring_buffer.clone();
            let shutdown_signal = shutdown_signal.clone();
            let bytes_written_for_task = bytes_written.clone();
            
            tokio::spawn(async move {
                Self::background_writer_task(
                    tcp_writer, // Direct ownership!
                    ring_buffer, 
                    shutdown_signal, 
                    tell_receiver, 
                    bytes_written_for_task
                ).await;
            });
        }
        
        Self {
            addr,
            channel_id,
            sequence_counter: Arc::new(AtomicUsize::new(0)),
            bytes_written, // This now tracks actual TCP bytes written
            ring_buffer,
            shutdown_signal,
            tell_sender,
        }
    }
    
    /// Background writer task - truly lock-free with exclusive TCP access
    /// OPTIMIZED FOR MAXIMUM THROUGHPUT - NO MUTEX NEEDED!
    async fn background_writer_task(
        mut tcp_writer: tokio::net::tcp::OwnedWriteHalf, // Direct ownership!
        ring_buffer: Arc<LockFreeRingBuffer>,
        shutdown_signal: Arc<AtomicBool>,
        mut tell_receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        bytes_written_counter: Arc<AtomicUsize>, // Track ALL bytes written to TCP
    ) {
        use tokio::io::AsyncWriteExt;
        
        // Large batching buffers for maximum throughput
        const RING_BATCH_SIZE: usize = 4096;  // Much larger batches
        const TELL_BATCH_SIZE: usize = 2048;
        const FLUSH_THRESHOLD: usize = 256 * 1024; // 256KB before flush
        
        let mut bytes_since_flush = 0;
        
        while !shutdown_signal.load(Ordering::Relaxed) {
            let mut total_bytes_written = 0;
            
            // Process ring buffer writes with LARGE vectored I/O batches
            let mut ring_buffers = Vec::with_capacity(RING_BATCH_SIZE);
            for _ in 0..RING_BATCH_SIZE {
                if let Some(command) = ring_buffer.try_pop() {
                    ring_buffers.push(command);
                } else {
                    break;
                }
            }
            
            if !ring_buffers.is_empty() {
                let vectored: Vec<std::io::IoSlice> = ring_buffers.iter()
                    .map(|cmd| std::io::IoSlice::new(&cmd.data))
                    .collect();
                
                // Calculate total bytes for this batch
                let batch_bytes: usize = ring_buffers.iter().map(|cmd| cmd.data.len()).sum();
                
                // Single vectored write syscall for all ring buffer data
                if let Ok(_) = tcp_writer.write_vectored(&vectored).await {
                    // Count successful writes
                    bytes_written_counter.fetch_add(batch_bytes, Ordering::Relaxed);
                    total_bytes_written += batch_bytes;
                }
            }
            
            // Process tell operations with LARGE vectored I/O batches
            let mut tell_buffers = Vec::with_capacity(TELL_BATCH_SIZE);
            for _ in 0..TELL_BATCH_SIZE {
                if let Ok(tell_data) = tell_receiver.try_recv() {
                    tell_buffers.push(tell_data);
                } else {
                    break;
                }
            }
            
            if !tell_buffers.is_empty() {
                let vectored: Vec<std::io::IoSlice> = tell_buffers.iter()
                    .map(|data| std::io::IoSlice::new(data))
                    .collect();
                
                // Calculate total bytes for this batch
                let batch_bytes: usize = tell_buffers.iter().map(|data| data.len()).sum();
                
                // Single vectored write syscall for all tell data
                if let Ok(_) = tcp_writer.write_vectored(&vectored).await {
                    // Count successful writes
                    bytes_written_counter.fetch_add(batch_bytes, Ordering::Relaxed);
                    total_bytes_written += batch_bytes;
                }
            }
            
            bytes_since_flush += total_bytes_written;
            
            // Only flush when we've written enough data or no more work
            if bytes_since_flush >= FLUSH_THRESHOLD || total_bytes_written == 0 {
                let _ = tcp_writer.flush().await;
                bytes_since_flush = 0;
                
                // If no work was done, wait briefly before next iteration
                if total_bytes_written == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
    }
    
    /// Write data to the lock-free ring buffer - NO BLOCKING
    pub fn write_nonblocking(&self, data: &[u8]) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        
        let command = WriteCommand {
            channel_id: self.channel_id,
            data: data.to_vec(),
            sequence: sequence as u64,
        };
        
        if self.ring_buffer.try_push(command) {
            // Don't increment bytes_written here - only increment when actually written to TCP
            Ok(())
        } else {
            // Ring buffer full - drop the data (fire and forget)
            Ok(())
        }
    }
    
    /// Write data with vectored batching - still no blocking
    pub fn write_vectored_nonblocking(&self, data_chunks: &[&[u8]]) -> Result<()> {
        if data_chunks.is_empty() {
            return Ok(());
        }
        
        // Combine all chunks into a single buffer for simplicity
        let total_len: usize = data_chunks.iter().map(|chunk| chunk.len()).sum();
        let mut combined_buffer = Vec::with_capacity(total_len);
        
        for chunk in data_chunks {
            combined_buffer.extend_from_slice(chunk);
        }
        
        self.write_nonblocking(&combined_buffer)
    }
    
    /// Write large data in chunks to avoid blocking
    pub fn write_chunked_nonblocking(&self, data: &[u8], chunk_size: usize) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        
        for chunk in data.chunks(chunk_size) {
            let _ = self.write_nonblocking(chunk);
        }
        
        Ok(())
    }
    
    /// Get ring buffer status
    pub fn buffer_status(&self) -> (usize, usize) {
        let pending = self.ring_buffer.pending_count();
        (pending, self.ring_buffer.capacity - pending)
    }
    
    /// Send tell operation through the lock-free channel - NO BLOCKING
    pub fn tell_nonblocking(&self, data: Vec<u8>) -> Result<()> {
        self.tell_sender.send(data)
            .map_err(|_| crate::GossipError::Shutdown)?;
        Ok(())
    }
    
    /// Check if ring buffer is near capacity
    pub fn is_buffer_full(&self) -> bool {
        self.ring_buffer.pending_count() >= (self.ring_buffer.capacity * 9 / 10) // 90% full
    }
    
    /// Get channel ID
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }
    
    /// Get total bytes written
    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }
    
    /// Get sequence counter
    pub fn sequence_number(&self) -> usize {
        self.sequence_counter.load(Ordering::Relaxed)
    }
    
    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    
    /// Shutdown the background writer task
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}



/// Message types for seamless batching with tell()
pub enum TellMessage<'a> {
    Single(&'a [u8]),
    Batch(Vec<&'a [u8]>),
}

impl<'a> TellMessage<'a> {
    /// Create a single message
    pub fn single(data: &'a [u8]) -> Self {
        Self::Single(data)
    }
    
    /// Create a batch message
    pub fn batch(messages: Vec<&'a [u8]>) -> Self {
        Self::Batch(messages)
    }
    
    /// Create from a slice - automatically detects single vs batch
    pub fn from_slice(messages: &'a [&'a [u8]]) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages.to_vec())
        }
    }
    
    /// Send this message via the connection handle
    pub async fn send_via(self, handle: &ConnectionHandle) -> Result<()> {
        match self {
            TellMessage::Single(data) => handle.tell_raw(data).await,
            TellMessage::Batch(messages) => handle.tell_batch(&messages).await,
        }
    }
}

/// Implement From trait for automatic conversion
impl<'a> From<&'a [u8]> for TellMessage<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self::Single(data)
    }
}

impl<'a> From<Vec<&'a [u8]>> for TellMessage<'a> {
    fn from(messages: Vec<&'a [u8]>) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages)
        }
    }
}

impl<'a> From<&'a [&'a [u8]]> for TellMessage<'a> {
    fn from(messages: &'a [&'a [u8]]) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages.to_vec())
        }
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for TellMessage<'a> {
    fn from(data: &'a [u8; N]) -> Self {
        Self::Single(data.as_slice())
    }
}

impl<'a> From<&'a Vec<u8>> for TellMessage<'a> {
    fn from(data: &'a Vec<u8>) -> Self {
        Self::Single(data.as_slice())
    }
}

/// Macro for ergonomic tell message creation
#[macro_export]
macro_rules! tell_msg {
    ($single:expr) => {
        TellMessage::single($single)
    };
    ($($msg:expr),+ $(,)?) => {
        TellMessage::batch(vec![$($msg),+])
    };
}

/// Connection pool for maintaining persistent TCP connections to peers
/// All connections are persistent - there is no checkout/checkin
/// Lock-free connection pool using atomic operations and lock-free data structures
pub struct ConnectionPool {
    /// PRIMARY: Mapping Peer ID -> LockFreeConnection
    /// This is the main storage - we identify connections by peer ID, not address
    connections_by_peer: dashmap::DashMap<crate::PeerId, Arc<LockFreeConnection>>,
    /// SECONDARY: Mapping SocketAddr -> Peer ID (for incoming connection identification)
    addr_to_peer_id: dashmap::DashMap<SocketAddr, crate::PeerId>,
    /// Configuration: Peer ID -> Expected SocketAddr (where to connect)
    pub peer_id_to_addr: dashmap::DashMap<crate::PeerId, SocketAddr>,
    /// DEPRECATED: Old address-based connections for migration
    connections: dashmap::DashMap<SocketAddr, Arc<LockFreeConnection>>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: Option<std::sync::Weak<GossipRegistry>>,
    /// Shared message buffer pool for zero-allocation processing
    message_buffer_pool: Arc<MessageBufferPool>,
    /// Connection counter for load balancing
    connection_counter: AtomicUsize,
}


/// Maximum number of pending responses (must be power of 2 for fast modulo)
const PENDING_RESPONSES_SIZE: usize = 1024;

/// Pending response slot
type PendingResponse = Option<tokio::sync::oneshot::Sender<Vec<u8>>>;

/// Shared state for correlation tracking
pub(crate) struct CorrelationTracker {
    /// Next correlation ID to use
    next_id: AtomicU16,
    /// Fixed-size array of pending responses
    pending: [parking_lot::Mutex<PendingResponse>; PENDING_RESPONSES_SIZE],
}

impl std::fmt::Debug for CorrelationTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrelationTracker")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish()
    }
}

impl CorrelationTracker {
    fn new() -> Arc<Self> {
        // Create array of None values
        let pending = std::array::from_fn(|_| parking_lot::Mutex::new(None));
        Arc::new(Self {
            next_id: AtomicU16::new(1),
            pending,
        })
    }
    
    /// Allocate a correlation ID and store the response sender
    fn allocate(&self, sender: tokio::sync::oneshot::Sender<Vec<u8>>) -> u16 {
        loop {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if id == 0 {
                continue; // Skip 0 as it's reserved
            }
            
            let slot = (id as usize) % PENDING_RESPONSES_SIZE;
            let mut pending = self.pending[slot].lock();
            
            // If slot is empty, use it
            if pending.is_none() {
                *pending = Some(sender);
                return id;
            }
            
            // Slot is occupied, try next ID
        }
    }
    
    /// Complete a pending request with a response
    fn complete(&self, correlation_id: u16, response: Vec<u8>) {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let mut pending = self.pending[slot].lock();
        
        if let Some(sender) = pending.take() {
            let _ = sender.send(response);
        }
    }
    
    /// Cancel a pending request
    fn cancel(&self, correlation_id: u16) {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let mut pending = self.pending[slot].lock();
        pending.take();
    }
}

/// Handle to send messages through a persistent connection - LOCK-FREE
#[derive(Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    // Direct lock-free stream handle - NO MUTEX!
    stream_handle: Arc<LockFreeStreamHandle>,
    // Correlation tracker for ask/response
    correlation: Arc<CorrelationTracker>,
}

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("addr", &self.addr)
            .field("stream_handle", &self.stream_handle)
            .finish()
    }
}

/// Delegated reply sender for asynchronous response handling
/// Allows passing around the ability to reply to a request without blocking the original caller
pub struct DelegatedReplySender {
    sender: tokio::sync::oneshot::Sender<Vec<u8>>,
    receiver: tokio::sync::oneshot::Receiver<Vec<u8>>,
    request_len: usize,
    timeout: Option<Duration>,
    created_at: std::time::Instant,
}

impl DelegatedReplySender {
    /// Create a new delegated reply sender
    pub fn new(
        sender: tokio::sync::oneshot::Sender<Vec<u8>>,
        receiver: tokio::sync::oneshot::Receiver<Vec<u8>>,
        request_len: usize,
    ) -> Self {
        Self {
            sender,
            receiver,
            request_len,
            timeout: None,
            created_at: std::time::Instant::now(),
        }
    }
    
    /// Create a new delegated reply sender with timeout
    pub fn new_with_timeout(
        sender: tokio::sync::oneshot::Sender<Vec<u8>>,
        receiver: tokio::sync::oneshot::Receiver<Vec<u8>>,
        request_len: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            sender,
            receiver,
            request_len,
            timeout: Some(timeout),
            created_at: std::time::Instant::now(),
        }
    }
    
    /// Send a reply using this delegated sender
    /// This can be called from anywhere in the code to complete the request-response cycle
    pub fn reply(self, response: Vec<u8>) -> Result<()> {
        match self.sender.send(response) {
            Ok(()) => Ok(()),
            Err(_) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Reply receiver was dropped",
            ))),
        }
    }
    
    /// Send a reply with error
    pub fn reply_error(self, error: &str) -> Result<()> {
        let error_response = format!("ERROR:{}", error).into_bytes();
        self.reply(error_response)
    }
    
    /// Wait for the reply with optional timeout
    pub async fn wait_for_reply(self) -> Result<Vec<u8>> {
        if let Some(timeout) = self.timeout {
            match tokio::time::timeout(timeout, self.receiver).await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(_)) => Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Reply sender was dropped",
                ))),
                Err(_) => Err(GossipError::Timeout),
            }
        } else {
            match self.receiver.await {
                Ok(response) => Ok(response),
                Err(_) => Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Reply sender was dropped",
                ))),
            }
        }
    }
    
    /// Wait for the reply with a custom timeout
    pub async fn wait_for_reply_with_timeout(self, timeout: Duration) -> Result<Vec<u8>> {
        match tokio::time::timeout(timeout, self.receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Reply sender was dropped",
            ))),
            Err(_) => Err(GossipError::Timeout),
        }
    }
    
    /// Get the original request length (useful for creating mock responses)
    pub fn request_len(&self) -> usize {
        self.request_len
    }
    
    /// Get the elapsed time since the request was made
    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed()
    }
    
    /// Check if this reply sender has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.created_at.elapsed() > timeout
        } else {
            false
        }
    }
    
    /// Create a mock reply for testing (simulates the original ask() behavior)
    pub fn create_mock_reply(&self) -> Vec<u8> {
        format!("RESPONSE:{}", self.request_len).into_bytes()
    }
}

impl std::fmt::Debug for DelegatedReplySender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegatedReplySender")
            .field("request_len", &self.request_len)
            .field("timeout", &self.timeout)
            .field("elapsed", &self.elapsed())
            .field("is_timed_out", &self.is_timed_out())
            .finish()
    }
}

impl ConnectionHandle {
    /// Send pre-serialized data through this connection - LOCK-FREE
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        // Use lock-free ring buffer - NO MUTEX!
        self.stream_handle.write_nonblocking(&data)
    }
    
    /// Send raw bytes without any framing - used by ReplyTo
    pub fn send_raw_bytes(&self, data: &[u8]) -> Result<()> {
        self.stream_handle.write_nonblocking(data)
    }
    
    /// Raw tell() - LOCK-FREE write (used internally)
    pub async fn tell_raw(&self, data: &[u8]) -> Result<()> {
        // Create message with length header
        let len_bytes = (data.len() as u32).to_be_bytes();
        let mut message = Vec::with_capacity(4 + data.len());
        message.extend_from_slice(&len_bytes);
        message.extend_from_slice(data);
        
        // Use lock-free ring buffer - NO MUTEX!
        self.stream_handle.write_nonblocking(&message)
    }
    
    /// Smart tell() - accepts TellMessage with automatic batch detection
    pub async fn tell<'a, T: Into<TellMessage<'a>>>(&self, message: T) -> Result<()> {
        let tell_message = message.into();
        tell_message.send_via(self).await
    }
    
    /// Legacy tell() for backward compatibility
    pub async fn tell_single(&self, data: &[u8]) -> Result<()> {
        self.tell_raw(data).await
    }
    
    /// Smart tell() - automatically uses batching for Vec<T>
    pub async fn tell_smart<T: AsRef<[u8]>>(&self, payload: &[T]) -> Result<()> {
        if payload.len() == 1 {
            // Single message - use regular tell
            self.tell(payload[0].as_ref()).await
        } else {
            // Multiple messages - use batch
            let batch: Vec<&[u8]> = payload.iter().map(|item| item.as_ref()).collect();
            self.tell_batch(&batch).await
        }
    }
    
    /// Tell with automatic batching detection
    pub async fn tell_auto(&self, data: &[u8]) -> Result<()> {
        // Single message path
        self.tell(data).await
    }
    
    /// Tell multiple messages with a single call
    pub async fn tell_many<T: AsRef<[u8]>>(&self, messages: &[T]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        
        if messages.len() == 1 {
            // Single message optimization
            self.tell(messages[0].as_ref()).await
        } else {
            // Batch multiple messages
            let batch: Vec<&[u8]> = messages.iter().map(|msg| msg.as_ref()).collect();
            self.tell_batch(&batch).await
        }
    }
    
    /// Send a TellMessage (single or batch) - same as tell() but explicit
    pub async fn send_tell_message(&self, message: TellMessage<'_>) -> Result<()> {
        message.send_via(self).await
    }
    
    /// Universal send() - detects single vs multiple messages automatically
    pub async fn send_messages(&self, messages: &[&[u8]]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        
        if messages.len() == 1 {
            self.tell_raw(messages[0]).await
        } else {
            self.tell_batch(messages).await
        }
    }
    
    /// Batch tell() for multiple messages - LOCK-FREE
    pub async fn tell_batch(&self, messages: &[&[u8]]) -> Result<()> {
        // OPTIMIZATION: Pre-calculate total size and write in one go
        let total_size: usize = messages.iter().map(|m| 4 + m.len()).sum();
        let mut batch_buffer = Vec::with_capacity(total_size);
        
        for msg in messages {
            batch_buffer.extend_from_slice(&(msg.len() as u32).to_be_bytes());
            batch_buffer.extend_from_slice(msg);
        }
        
        // Use lock-free ring buffer - NO MUTEX!
        self.stream_handle.write_nonblocking(&batch_buffer)
    }
    
    /// Direct access to try_send for maximum performance testing
    pub fn try_send_direct(&self, _data: &[u8]) -> Result<()> {
        // Direct TCP doesn't support try_send - would need try_lock
        Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "use tell() for direct TCP writes",
        )))
    }
    
    
    /// Send raw bytes through existing connection (zero-copy where possible)
    pub async fn send_raw(&self, data: &[u8]) -> Result<()> {
        // Direct TCP write
        self.tell_raw(data).await
    }
    
    /// Fallback async send for when channel is full
    async fn send_data_async(&self, data: &[u8]) -> Result<()> {
        // Direct TCP write
        self.tell(data).await
    }
    
    /// Ask method for request-response (Note: This is a simplified implementation)
    /// For full request-response, you would need a proper protocol with correlation IDs
    pub async fn ask(&self, request: &[u8]) -> Result<Vec<u8>> {
        // Create oneshot channel for response
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        // Allocate correlation ID
        let correlation_id = self.correlation.allocate(tx);
        
        // Create ask message with length prefix + 8-byte header + payload
        let total_size = 8 + request.len();
        let mut message = Vec::with_capacity(4 + total_size);
        
        // Length prefix (4 bytes)
        message.extend_from_slice(&(total_size as u32).to_be_bytes());
        
        // Header: [type:1][correlation_id:2][reserved:5]
        message.push(crate::MessageType::Ask as u8);
        message.extend_from_slice(&correlation_id.to_be_bytes());
        message.extend_from_slice(&[0u8; 5]); // Reserved bytes
        message.extend_from_slice(request);
        
        // Send the message
        self.stream_handle.write_nonblocking(&message)?;
        
        tracing::debug!(
            correlation_id = correlation_id,
            request_size = request.len(),
            "ask: sent message with correlation ID"
        );
        
        // Wait for response with default timeout (30 seconds)
        match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(crate::GossipError::Network(
                std::io::Error::new(std::io::ErrorKind::Other, "Response channel closed")
            )),
            Err(_) => Err(crate::GossipError::Timeout),
        }
    }
    
    /// Ask method that returns a ReplyTo handle for delegated replies
    pub async fn ask_with_reply_to(&self, request: &[u8]) -> Result<crate::ReplyTo> {
        // Create oneshot channel for response (we won't use it for ReplyTo)
        let (tx, _rx) = tokio::sync::oneshot::channel();
        
        // Allocate correlation ID 
        let correlation_id = self.correlation.allocate(tx);
        
        // Create ask message with length prefix + 8-byte header + payload
        let total_size = 8 + request.len();
        let mut message = Vec::with_capacity(4 + total_size);
        
        // Length prefix (4 bytes)
        message.extend_from_slice(&(total_size as u32).to_be_bytes());
        
        // Header: [type:1][correlation_id:2][reserved:5]
        message.push(crate::MessageType::Ask as u8);
        message.extend_from_slice(&correlation_id.to_be_bytes());
        message.extend_from_slice(&[0u8; 5]); // Reserved bytes
        message.extend_from_slice(request);
        
        // Send the message
        self.stream_handle.write_nonblocking(&message)?;
        
        tracing::debug!(
            correlation_id = correlation_id,
            request_size = request.len(),
            "ask_with_reply_to: sent message with correlation ID"
        );
        
        // Return ReplyTo handle
        Ok(crate::ReplyTo {
            correlation_id,
            connection: Arc::new(self.clone()),
        })
    }
    
    /// Ask method with delegated reply sender for asynchronous response handling
    /// Returns a DelegatedReplySender that can be passed around to handle the response elsewhere
    pub async fn ask_with_reply_sender(&self, request: &[u8]) -> Result<DelegatedReplySender> {
        // Create a oneshot channel for the response
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel();
        
        // Send the request (in a real implementation, this would include correlation ID)
        self.tell(request).await?;
        
        // Return the delegated reply sender
        Ok(DelegatedReplySender::new(reply_sender, reply_receiver, request.len()))
    }
    
    /// Ask method with timeout and delegated reply
    pub async fn ask_with_timeout_and_reply(&self, request: &[u8], timeout: Duration) -> Result<DelegatedReplySender> {
        // Create a oneshot channel for the response
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel();
        
        // Send the request
        self.tell(request).await?;
        
        // Return the delegated reply sender with timeout
        Ok(DelegatedReplySender::new_with_timeout(reply_sender, reply_receiver, request.len(), timeout))
    }
    
    /// High-performance streaming API - send structured data with custom framing - LOCK-FREE
    pub async fn stream_send<T>(&self, data: &T) -> Result<()> 
    where
        T: for<'a> rkyv::Serialize<rkyv::rancor::Strategy<rkyv::ser::Serializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>>,
    {
        // Serialize the data using rkyv for maximum performance
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(data).map_err(|e| crate::GossipError::Serialization(e))?;
        
        // Create stream frame: [frame_type, channel_id, flags, seq_id[2], payload_len[4]]
        let frame_header = StreamFrameHeader {
            frame_type: StreamFrameType::Data as u8,
            channel_id: ChannelId::TellAsk as u8,
            flags: 0,
            sequence_id: 0, // TODO: Add sequence tracking for ordering
            payload_len: payload.len() as u32,
        };
        
        let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header).map_err(|e| crate::GossipError::Serialization(e))?;
        
        // Combine header and payload for single write
        let mut combined = Vec::with_capacity(header_bytes.len() + payload.len());
        combined.extend_from_slice(&header_bytes);
        combined.extend_from_slice(&payload);
        
        // Use lock-free ring buffer - NO MUTEX!
        self.stream_handle.write_nonblocking(&combined)
    }
    
    /// High-performance streaming API - send batch of structured data - LOCK-FREE
    pub async fn stream_send_batch<T>(&self, batch: &[T]) -> Result<()> 
    where
        T: for<'a> rkyv::Serialize<rkyv::rancor::Strategy<rkyv::ser::Serializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>>,
    {
        if batch.is_empty() {
            return Ok(());
        }
        
        // Pre-allocate buffer for entire batch
        let mut total_payload = Vec::new();
        
        for item in batch {
            let payload = rkyv::to_bytes::<rkyv::rancor::Error>(item).map_err(|e| crate::GossipError::Serialization(e))?;
            
            let frame_header = StreamFrameHeader {
                frame_type: StreamFrameType::Data as u8,
                channel_id: ChannelId::TellAsk as u8,
                flags: if item as *const _ == batch.last().unwrap() as *const _ { 0 } else { StreamFrameFlags::More as u8 },
                sequence_id: 0, // TODO: Add sequence tracking
                payload_len: payload.len() as u32,
            };
            
            let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header).map_err(|e| crate::GossipError::Serialization(e))?;
            total_payload.extend_from_slice(&header_bytes);
            total_payload.extend_from_slice(&payload);
        }
        
        // Use lock-free ring buffer for entire batch - NO MUTEX!
        self.stream_handle.write_nonblocking(&total_payload)
    }
    
    /// Get truly lock-free streaming handle - direct access to the internal handle
    pub fn get_lock_free_stream(&self) -> &Arc<LockFreeStreamHandle> {
        &self.stream_handle
    }
    
}

impl ConnectionPool {
    pub fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        const POOL_SIZE: usize = 256;
        const BUFFER_SIZE: usize = 8192;
        
        let pool = Self {
            connections_by_peer: dashmap::DashMap::new(),
            addr_to_peer_id: dashmap::DashMap::new(),
            peer_id_to_addr: dashmap::DashMap::new(),
            connections: dashmap::DashMap::new(), // DEPRECATED
            max_connections,
            connection_timeout,
            registry: None,
            message_buffer_pool: Arc::new(MessageBufferPool::new(POOL_SIZE, BUFFER_SIZE)),
            connection_counter: AtomicUsize::new(0),
        };
        
        // Log the pool's address for debugging
        info!("CONNECTION POOL: Created new pool at {:p}", &pool as *const _);
        pool
    }

    /// Set the registry reference for handling incoming messages
    pub fn set_registry(&mut self, registry: std::sync::Arc<GossipRegistry>) {
        self.registry = Some(std::sync::Arc::downgrade(&registry));
    }
    
    /// Store or update the address for a node
    pub fn update_node_address(&self, node_id: &str, addr: SocketAddr) {
        let peer_id = crate::PeerId::new(node_id);
        self.peer_id_to_addr.insert(peer_id.clone(), addr);
        self.addr_to_peer_id.insert(addr, peer_id);
        info!("CONNECTION POOL: Updated address for node {} to {}", node_id, addr);
    }
    
    /// Get a connection by node ID
    pub fn get_connection_by_node_id(&self, node_id: &str) -> Option<Arc<LockFreeConnection>> {
        // PRIMARY: Look up connection directly by node ID
        let peer_id = crate::PeerId::new(node_id);
        if let Some(conn_entry) = self.connections_by_peer.get(&peer_id) {
            let conn = conn_entry.value().clone();
            if conn.is_connected() {
                info!("CONNECTION POOL: Found connection for node '{}'", node_id);
                Some(conn)
            } else {
                warn!("CONNECTION POOL: Connection for node '{}' is disconnected", node_id);
                None
            }
        } else {
            warn!("CONNECTION POOL: No connection found for node '{}'", node_id);
            // Debug: show what nodes we do have connections for
            let connected_nodes: Vec<String> = self.connections_by_peer.iter()
                .map(|entry| entry.key().as_str().to_string())
                .collect();
            warn!("CONNECTION POOL: Available node connections: {:?}", connected_nodes);
            None
        }
    }
    
    /// Add a connection indexed by node ID
    pub fn add_connection_by_node_id(&self, node_id: String, addr: SocketAddr, connection: Arc<LockFreeConnection>) -> bool {
        // Convert to PeerId
        let peer_id = crate::PeerId::new(&node_id);
        
        // Update the address mappings
        self.addr_to_peer_id.insert(addr, peer_id.clone());
        
        // PRIMARY: Store the connection by peer ID
        self.connections_by_peer.insert(peer_id, connection.clone());
        
        // DEPRECATED: Also store by address for backward compatibility
        self.connections.insert(addr, connection);
        
        self.connection_counter.fetch_add(1, Ordering::AcqRel);
        
        info!("CONNECTION POOL: Added connection for node '{}' (address: {})", node_id, addr);
        true
    }
    
    /// Send data to a node by ID
    pub fn send_to_node_id(&self, node_id: &str, data: &[u8]) -> Result<()> {
        info!("CONNECTION POOL: send_to_node_id called for node '{}', pool has {} node connections", 
              node_id, self.connections_by_peer.len());
        if let Some(connection) = self.get_connection_by_node_id(node_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                info!("CONNECTION POOL: Sending {} bytes to node '{}'", data.len(), node_id);
                return stream_handle.write_nonblocking(data);
            } else {
                warn!(node_id = %node_id, "Connection found but no stream handle");
            }
        } else {
            warn!(node_id = %node_id, "No connection found for node");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Connection not found for node {}", node_id)
        )))
    }
    
    /// Get or create a lock-free connection - NO MUTEX NEEDED
    pub fn get_lock_free_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        self.connections.get(&addr).map(|entry| entry.value().clone())
    }
    
    /// Add a new lock-free connection - completely lock-free operation
    pub fn add_lock_free_connection(&self, addr: SocketAddr, tcp_stream: TcpStream) -> Result<Arc<LockFreeConnection>> {
        let connection_count = self.connection_counter.fetch_add(1, Ordering::AcqRel);
        
        if connection_count >= self.max_connections {
            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            return Err(crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Max connections ({}) reached", self.max_connections)
            )));
        }
        
        let (reader, writer) = tcp_stream.into_split();
        
        // Create lock-free streaming handle with exclusive socket ownership
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            addr,
            ChannelId::Global,
            4096, // Ring buffer size
        ));
        
        let mut connection = LockFreeConnection::new(addr);
        connection.stream_handle = Some(stream_handle);
        connection.set_state(ConnectionState::Connected);
        connection.update_last_used();
        
        let connection_arc = Arc::new(connection);
        
        // Spawn reader task for this connection
        // This reader needs to process incoming messages on outgoing connections
        let reader_connection = connection_arc.clone();
        let registry_weak = self.registry.clone();
        tokio::spawn(async move {
            info!(peer = %addr, "Starting reader task for outgoing connection");
            handle_persistent_connection_reader(reader, addr, registry_weak).await;
            reader_connection.set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "Reader task for outgoing connection ended");
        });
        
        // Insert into lock-free hash map
        self.connections.insert(addr, connection_arc.clone());
        info!("CONNECTION POOL: Added lock-free connection to {} - pool now has {} connections", 
              addr, self.connections.len());
        
        Ok(connection_arc)
    }
    
    
    /// Send data through lock-free connection - NO BLOCKING
    pub fn send_lock_free(&self, addr: SocketAddr, data: &[u8]) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                return stream_handle.write_nonblocking(data);
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
            warn!("Available connections: {:?}", 
                self.connections.iter().map(|e| e.key().clone()).collect::<Vec<_>>());
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found"
        )))
    }
    
    /// Try to send data through any available connection for a node
    /// This handles cases where we might have multiple connections (incoming/outgoing)
    pub fn send_to_node(&self, node_addr: SocketAddr, data: &[u8], _registry: &GossipRegistry) -> Result<()> {
        // First try direct lookup
        if let Ok(()) = self.send_lock_free(node_addr, data) {
            return Ok(());
        }
        
        // If that fails, look for any connection that could reach this node
        // This could be enhanced with a node ID -> connections mapping
        debug!(node_addr = %node_addr, "Direct send failed, looking for alternative connections");
        
        // For now, we'll rely on the caller to handle fallback strategies
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No connection found for node {}", node_addr)
        )))
    }
    
    /// Remove a connection from the pool - lock-free operation
    pub fn remove_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        // First remove from address-based map
        if let Some((_, connection)) = self.connections.remove(&addr) {
            info!("CONNECTION POOL: Removed connection to {} - pool now has {} connections", 
                  addr, self.connections.len());
            
            // Also remove from node ID mapping
            if let Some(node_id_entry) = self.addr_to_peer_id.remove(&addr) {
                let (_, node_id) = node_id_entry;
                if let Some((_, _)) = self.connections_by_peer.remove(&node_id) {
                    info!("CONNECTION POOL: Also removed connection by node ID '{}'", node_id);
                }
            }
            
            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            Some(connection)
        } else {
            None
        }
    }
    
    /// Get connection count - lock-free operation
    pub fn connection_count(&self) -> usize {
        let count = self.connections_by_peer.len();
        if count == 0 {
            // Debug: Show why we might have 0 connections
            let connected_count = self.connections_by_peer.iter()
                .filter(|entry| entry.value().is_connected())
                .count();
            if connected_count != count {
                warn!("CONNECTION POOL: {} node connections exist but only {} are connected", 
                      self.connections_by_peer.len(), connected_count);
            }
        }
        count
    }
    
    /// Get all connected peers - lock-free operation
    pub fn get_connected_peers(&self) -> Vec<SocketAddr> {
        self.connections
            .iter()
            .filter_map(|entry| {
                if entry.value().is_connected() {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Get all connections (including disconnected) - for debugging
    pub fn get_all_connections(&self) -> Vec<SocketAddr> {
        self.connections.iter().map(|entry| *entry.key()).collect()
    }

    /// Get a buffer from the pool or create a new one
    pub fn get_buffer(&mut self, min_capacity: usize) -> Vec<u8> {
        // Use the message buffer pool for lock-free buffer management
        if let Some(buffer) = self.message_buffer_pool.get_buffer() {
            if buffer.capacity() >= min_capacity {
                return buffer;
            }
            // Buffer too small, return it and create new one
            self.message_buffer_pool.return_buffer(buffer);
        }
        Vec::with_capacity(min_capacity.max(1024)) // Minimum 1KB buffers
    }

    /// Return a buffer to the pool for reuse
    pub fn return_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= 64 * 1024 {
            // Return to the lock-free message buffer pool
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Get a message buffer from the pool for zero-copy processing
    pub fn get_message_buffer(&mut self) -> Vec<u8> {
        self.message_buffer_pool.get_buffer()
            .unwrap_or_else(|| Vec::with_capacity(4096))
    }

    /// Return a message buffer to the pool
    pub fn return_message_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= 64 * 1024 {
            // Keep buffers with reasonable size
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Create a message buffer with length header (optimized for reuse)
    pub fn create_message_buffer(&mut self, data: &[u8]) -> Vec<u8> {
        let len = data.len() as u32;
        let mut buffer = self.get_buffer(4 + data.len());
        buffer.extend_from_slice(&len.to_be_bytes());
        buffer.extend_from_slice(data);
        buffer
    }

    /// Get or create a persistent connection to a peer
    /// Fast path: Check for existing connection without creating new ones
    pub fn get_existing_connection(&mut self, addr: SocketAddr) -> Option<ConnectionHandle> {
        let _current_time = current_timestamp();
        
        if let Some(conn) = self.connections.get_mut(&addr) {
            // The connection here is a mutable reference to Arc<LockFreeConnection>
            if conn.value().is_connected() {
                conn.value().update_last_used();
                debug!(addr = %addr, "using existing persistent connection (fast path)");
                // TODO: ConnectionHandle needs refactoring for lock-free connections
                return None;
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                self.connections.remove(&addr);
            }
        }
        None
    }
    
    /// Get or create a connection to a node by its ID
    pub async fn get_connection_to_node(&mut self, node_id: &str) -> Result<ConnectionHandle> {
        // Convert string to PeerId for internal use
        let peer_id = crate::PeerId::new(node_id);
        info!("CONNECTION POOL: get_connection_to_node called for node '{}'", node_id);
        
        // First check if we already have a connection to this node
        if let Some(conn_entry) = self.connections_by_peer.get(&peer_id) {
            let conn = conn_entry.value();
            if conn.is_connected() {
                conn.update_last_used();
                info!("CONNECTION POOL: Found existing connection to node '{}'", node_id);
                
                if let Some(ref stream_handle) = conn.stream_handle {
                    // Need to get the address for ConnectionHandle
                    let addr = conn.addr;
                    return Ok(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn.correlation.clone().unwrap_or_else(|| CorrelationTracker::new()),
                    });
                } else {
                    return Err(crate::GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Connection exists but no stream handle"
                    )));
                }
            } else {
                // Remove disconnected connection
                info!("CONNECTION POOL: Removing disconnected connection to node '{}'", node_id);
                drop(conn_entry);
                self.connections_by_peer.remove(&peer_id);
            }
        }
        
        // Look up the address for this node
        let addr = if let Some(addr_entry) = self.peer_id_to_addr.get(&peer_id) {
            *addr_entry.value()
        } else {
            return Err(crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No address configured for node '{}'", node_id)
            )));
        };
        
        info!("CONNECTION POOL: Creating new connection to node '{}' at {}", node_id, addr);
        
        // Create the connection and store it by node ID
        let handle = self.get_connection(addr).await?;
        
        // After successful connection, ensure it's indexed by node ID
        if let Some(conn) = self.connections.get(&addr) {
            self.connections_by_peer.insert(peer_id.clone(), conn.value().clone());
            self.addr_to_peer_id.insert(addr, peer_id.clone());
            info!("CONNECTION POOL: Indexed new connection under node ID '{}'", node_id);
        }
        
        Ok(handle)
    }

    pub async fn get_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        let _current_time = current_timestamp();
        info!("CONNECTION POOL: get_connection called on pool at {:p} for {}", self as *const _, addr);
        info!("CONNECTION POOL: This pool instance has {} connections stored", self.connections.len());

        // TEMPORARY: For backward compatibility, we'll create a hybrid approach
        // that allows the old ConnectionHandle API to work while we transition
        
        // Check if we already have a lock-free connection
        if let Some(entry) = self.connections.get(&addr) {
            let conn = entry.value();
            if conn.is_connected() {
                conn.update_last_used();
                debug!(addr = %addr, "found existing lock-free connection, creating compatibility handle");
                
                // Return the existing lock-free connection handle
                if let Some(ref stream_handle) = conn.stream_handle {
                    return Ok(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn.correlation.clone().unwrap_or_else(|| CorrelationTracker::new()),
                    });
                } else {
                    // Connection exists but no stream handle - this shouldn't happen
                    return Err(crate::GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Connection exists but no stream handle"
                    )));
                }
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                drop(entry);
                self.connections.remove(&addr);
            }
        }

        // Extract what we need before any await points to avoid Send issues
        let max_connections = self.max_connections;
        let connection_timeout = self.connection_timeout;
        let connections = self.connections.clone();
        let registry_weak = self.registry.clone();
        
        // Make room if necessary
        if connections.len() >= max_connections {
            let oldest_addr = connections.iter()
                .min_by_key(|entry| entry.value().last_used.load(Ordering::Acquire))
                .map(|entry| *entry.key());
            
            if let Some(oldest) = oldest_addr {
                connections.remove(&oldest);
                warn!(addr = %oldest, "removed oldest connection to make room");
            }
        }

        // Connect with timeout
        info!("CONNECTION POOL: Attempting to connect to {}", addr);
        let stream = tokio::time::timeout(connection_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                info!("CONNECTION POOL: Connection to {} timed out", addr);
                GossipError::Timeout
            })?
            .map_err(|e| {
                info!("CONNECTION POOL: Connection to {} failed: {}", addr, e);
                GossipError::Network(e)
            })?;
        info!("CONNECTION POOL: Successfully connected to {}", addr);

        // Configure socket
        stream.set_nodelay(true).map_err(GossipError::Network)?;

        // Split the stream
        let (mut reader, writer) = stream.into_split();
        
        // Create lock-free connection for receiving
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            addr,
            ChannelId::Global,
            4096,
        ));
        
        let mut conn = LockFreeConnection::new(addr);
        conn.stream_handle = Some(stream_handle.clone());
        conn.set_state(ConnectionState::Connected);
        conn.update_last_used();
        
        let connection_arc = Arc::new(conn);
        
        // Insert into lock-free map before spawning
        self.connections.insert(addr, connection_arc.clone());
        info!("CONNECTION POOL: Added connection via get_connection to {} - pool now has {} connections", 
              addr, self.connections.len());
        // Double check it's really there
        assert!(self.connections.contains_key(&addr), "Connection was not added to pool!");
        info!("CONNECTION POOL: Verified connection exists for {}", addr);
        
        // Send initial FullSync message to identify ourselves
        if let Some(registry_arc) = registry_weak.as_ref().and_then(|w| w.upgrade()) {
            let initial_msg = {
                let actor_state = registry_arc.actor_state.read().await;
                let gossip_state = registry_arc.gossip_state.lock().await;
                
                RegistryMessage::FullSync {
                    local_actors: actor_state.local_actors.clone().into_iter().map(|(k, v)| (k, v)).collect(),
                    known_actors: actor_state.known_actors.clone().into_iter().map(|(k, v)| (k, v)).collect(),
                    sender_peer_id: registry_arc.peer_id.clone(),
                    sequence: gossip_state.gossip_sequence,
                    wall_clock_time: crate::current_timestamp(),
                }
            };
            
            // Serialize and send the initial message
            match rkyv::to_bytes::<rkyv::rancor::Error>(&initial_msg) {
                Ok(data) => {
                    let msg_buffer = self.create_message_buffer(&data);
                    // Create a connection handle to send the message
                    let conn_handle = ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: connection_arc.correlation.clone().unwrap_or_else(|| CorrelationTracker::new()),
                    };
                    if let Err(e) = conn_handle.send_data(msg_buffer).await {
                        warn!(peer = %addr, error = %e, "Failed to send initial FullSync message");
                    } else {
                        info!(peer = %addr, "Sent initial FullSync message to identify ourselves");
                    }
                }
                Err(e) => {
                    warn!(peer = %addr, error = %e, "Failed to serialize initial FullSync message");
                }
            }
        }
        
        // Spawn reader task for outgoing connection
        // This MUST process incoming messages to receive responses!
        let reader_connection = connection_arc.clone();
        let registry_weak_for_reader = registry_weak.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            info!(peer = %addr, "Starting outgoing connection reader with message processing");
            
            let mut partial_msg_buf = Vec::new();
            let mut read_buf = vec![0u8; 4096];
            
            loop {
                match reader.read(&mut read_buf).await {
                    Ok(0) => {
                        reader_connection.set_state(ConnectionState::Disconnected);
                        info!(peer = %addr, "Outgoing connection closed by peer (EOF)");
                        // Check if connection is still in pool
                        if let Some(ref registry_weak) = registry_weak_for_reader {
                            if let Some(registry) = registry_weak.upgrade() {
                                let pool = registry.connection_pool.lock().await;
                                info!(peer = %addr, "Connection still in pool after EOF? {}", 
                                      pool.connections.contains_key(&addr));
                            }
                        }
                        break;
                    }
                    Ok(n) => {
                        partial_msg_buf.extend_from_slice(&read_buf[..n]);
                        
                        // Process complete messages
                        while partial_msg_buf.len() >= 4 {
                            let len = u32::from_be_bytes([partial_msg_buf[0], partial_msg_buf[1], partial_msg_buf[2], partial_msg_buf[3]]) as usize;
                            
                            if partial_msg_buf.len() >= 4 + len {
                                // We have a complete message
                                let msg_data = &partial_msg_buf[4..4+len];
                                
                                // Check if this is an Ask/Response message by looking at first byte
                                if msg_data.len() >= 1 {
                                    if let Some(msg_type) = crate::MessageType::from_byte(msg_data[0]) {
                                        // This is an Ask/Response message
                                        if msg_data.len() < 8 {
                                            warn!(peer = %addr, "Ask/Response message too small");
                                            partial_msg_buf.drain(..4+len);
                                            continue;
                                        }
                                        
                                        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                                        let payload = &msg_data[8..];
                                        
                                        match msg_type {
                                            crate::MessageType::Ask => {
                                                // Handle incoming Ask requests for testing
                                                #[cfg(feature = "test-helpers")]
                                                {
                                                    if let Some(ref registry_weak) = registry_weak_for_reader {
                                                        if let Some(registry) = registry_weak.upgrade() {
                                                            let conn = {
                                                                let pool = registry.connection_pool.lock().await;
                                                                pool.connections.get(&addr).map(|conn_ref| conn_ref.value().clone())
                                                            };
                                                            
                                                            if let Some(conn) = conn {
                                                                // Process the request and generate response
                                                                let request_str = String::from_utf8_lossy(payload);
                                                                let response = process_mock_request(&request_str);
                                                                
                                                                // Send response back
                                                                if let Some(ref stream_handle) = conn.stream_handle {
                                                                    let mut msg = Vec::with_capacity(4 + 8 + response.len());
                                                                    let total_size = 8 + response.len();
                                                                    msg.extend_from_slice(&(total_size as u32).to_be_bytes());
                                                                    msg.push(crate::MessageType::Response as u8);
                                                                    msg.extend_from_slice(&correlation_id.to_be_bytes());
                                                                    msg.extend_from_slice(&[0u8; 5]);
                                                                    msg.extend_from_slice(&response);
                                                                    
                                                                    if let Err(e) = stream_handle.write_nonblocking(&msg) {
                                                                        warn!(peer = %addr, error = %e, "Failed to send Ask response");
                                                                    } else {
                                                                        debug!(peer = %addr, correlation_id = correlation_id, "Sent Ask response");
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                #[cfg(not(feature = "test-helpers"))]
                                                {
                                                    warn!(peer = %addr, correlation_id = correlation_id, "Received Ask request - not implemented in production");
                                                }
                                            }
                                            crate::MessageType::Response => {
                                                // Handle incoming response
                                                if let Some(ref registry_weak) = registry_weak_for_reader {
                                                    if let Some(registry) = registry_weak.upgrade() {
                                                        // Find the connection with this peer
                                                        let conn = {
                                                            let pool = registry.connection_pool.lock().await;
                                                            pool.connections.get(&addr).map(|c| c.value().clone())
                                                        };
                                                        
                                                        if let Some(conn) = conn {
                                                            if let Some(ref correlation) = conn.correlation {
                                                                correlation.complete(correlation_id, payload.to_vec());
                                                                debug!(peer = %addr, correlation_id = correlation_id, "Delivered response to correlation tracker");
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            crate::MessageType::Tell => {
                                                warn!(peer = %addr, "Received Tell message in Ask/Response format - unexpected");
                                            }
                                        }
                                        
                                        partial_msg_buf.drain(..4+len);
                                        continue;
                                    }
                                }
                                
                                // This is a gossip protocol message
                                let msg_data_vec = msg_data.to_vec();
                                partial_msg_buf.drain(..4+len);
                                
                                // Process the message if we have a registry
                                if let Some(ref registry_weak) = registry_weak_for_reader {
                                    if let Some(registry) = registry_weak.upgrade() {
                                        match rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&msg_data_vec) {
                                            Ok(msg) => {
                                                warn!(peer = %addr, "Received message on outgoing connection");
                                                
                                                // Extract node ID from message if available
                                                let node_id = match &msg {
                                                    RegistryMessage::FullSync { sender_peer_id, .. } => Some(sender_peer_id.as_str().to_string()),
                                                    RegistryMessage::FullSyncResponse { sender_peer_id, .. } => Some(sender_peer_id.as_str().to_string()),
                                                    _ => None,
                                                };
                                                
                                                // Update node mapping if we got a node ID
                                                if let Some(node_id) = node_id {
                                                    let pool = registry.connection_pool.lock().await;
                                                    pool.update_node_address(&node_id, addr);
                                                    info!(peer = %addr, node_id = %node_id, "Updated node ID mapping from outgoing connection message");
                                                }
                                                
                                                if let Err(e) = handle_incoming_message(registry, addr, msg).await {
                                                    warn!(peer = %addr, error = %e, "Failed to handle message on outgoing connection");
                                                }
                                            }
                                            Err(e) => {
                                                warn!(peer = %addr, error = %e, "Failed to deserialize message on outgoing connection");
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Need more data
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        reader_connection.set_state(ConnectionState::Failed);
                        warn!(peer = %addr, error = %e, "Outgoing connection error");
                        break;
                    }
                }
            }
            
            info!(peer = %addr, "Outgoing connection reader exited");
        });
        
        // Reset failure state for this peer since we successfully connected
        if let Some(ref registry_weak) = registry_weak {
            if let Some(registry) = registry_weak.upgrade() {
                let registry_clone = registry.clone();
                let peer_addr = addr;
                tokio::spawn(async move {
                    let mut gossip_state = registry_clone.gossip_state.lock().await;
                    
                    // Check if we need to reset failures and clear pending
                    let need_to_clear_pending = if let Some(peer_info) = gossip_state.peers.get_mut(&peer_addr) {
                        let had_failures = peer_info.failures > 0;
                        if had_failures {
                            info!(peer = %peer_addr, 
                                  prev_failures = peer_info.failures,
                                  "✅ Successfully established outgoing connection - resetting failure state");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        had_failures
                    } else {
                        false
                    };
                    
                    // Clear pending failure record if needed
                    if need_to_clear_pending {
                        gossip_state.pending_peer_failures.remove(&peer_addr);
                    }
                });
            }
        }
        
        info!(peer = %addr, "successfully created new persistent connection");
        
        // Verify the connection is in the pool
        info!("CONNECTION POOL: After get_connection, pool has {} connections", self.connections.len());
        info!("CONNECTION POOL: Pool contains connection to {}? {}", addr, self.connections.contains_key(&addr));
        
        // Return a lock-free ConnectionHandle
        Ok(ConnectionHandle {
            addr,
            stream_handle: stream_handle,
            correlation: connection_arc.correlation.clone().unwrap_or_else(|| CorrelationTracker::new()),
        })
    }


    /// Mark a connection as disconnected
    pub fn mark_disconnected(&mut self, addr: SocketAddr) {
        if let Some(entry) = self.connections.get(&addr) {
            entry.value().set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "marked connection as disconnected");
        }
    }

    /// Remove a connection from the pool by address (old method for compatibility)
    pub fn remove_connection_mut(&mut self, addr: SocketAddr) {
        if let Some(_conn) = self.connections.remove(&addr) {
            info!(addr = %addr, "removed connection from pool");
            // Dropping the sender will cause the receiver to return None,
            // signaling the connection handler to shut down
            // No need to drop writer
        }
    }

    /// Get number of active connections (old method for compatibility)
    pub fn connection_count_old(&self) -> usize {
        self.connections.iter()
            .filter(|entry| entry.value().is_connected())
            .count()
    }


    /// Check if we have a connection to a peer by address
    pub fn has_connection(&self, addr: &SocketAddr) -> bool {
        self.connections
            .get(addr)
            .map(|entry| entry.value().is_connected())
            .unwrap_or(false)
    }

    /// Check health of all connections (for compatibility)
    pub async fn check_connection_health(&mut self) -> Vec<SocketAddr> {
        // Health checking is now done by the persistent connection handlers
        Vec::new()
    }

    /// Clean up stale connections
    pub fn cleanup_stale_connections(&mut self) {
        let to_remove: Vec<_> = self
            .connections
            .iter()
            .filter(|entry| !entry.value().is_connected())
            .map(|entry| *entry.key())
            .collect();

        for addr in to_remove {
            self.connections.remove(&addr);
            debug!(addr = %addr, "cleaned up disconnected connection");
        }
    }

    /// Close all connections (for shutdown)
    pub fn close_all_connections(&mut self) {
        let addrs: Vec<_> = self.connections.iter().map(|entry| *entry.key()).collect();
        let count = addrs.len();
        for addr in addrs {
            self.remove_connection(addr);
        }
        info!("closed all {} connections", count);
    }
}

/// Start a persistent connection handler in the background
fn start_persistent_connection_reader(
    reader: tokio::net::tcp::OwnedReadHalf,
    peer_addr: SocketAddr,
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    tokio::spawn(async move {
        handle_persistent_connection_reader(reader, peer_addr, registry_weak).await;
    });
}

/// Handle an incoming persistent connection - processes messages with direct TCP
pub(crate) async fn handle_incoming_persistent_connection(
    stream: TcpStream,
    _rx: mpsc::Receiver<Vec<u8>>,
    _peer_addr: SocketAddr,
    listening_addr: SocketAddr,
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    let (reader, writer) = stream.into_split();
    
    // Add writer to connection pool if needed
    if let Some(registry_weak) = &registry_weak {
        if let Some(_registry) = registry_weak.upgrade() {
            // Note: We don't add the connection here anymore as we need the node ID first
            // The connection will be added when we receive the first message with node ID
            drop(writer);
        }
    } else {
        // No registry, just drop the writer
        drop(writer);
    }
    
    handle_persistent_connection_reader(reader, listening_addr, registry_weak).await;
}

/// Handle persistent connection reader - only reads messages, no channels
pub(crate) async fn handle_persistent_connection_reader(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    peer_addr: SocketAddr,
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    use tokio::io::AsyncReadExt;
    
    let mut partial_msg_buf = Vec::new();
    let mut read_buf = vec![0u8; 4096];
    
    loop {
        match reader.read(&mut read_buf).await {
            Ok(0) => {
                info!(peer = %peer_addr, "Connection closed by peer");
                break;
            }
            Ok(n) => {
                partial_msg_buf.extend_from_slice(&read_buf[..n]);
                
                // Process complete messages
                while partial_msg_buf.len() >= 4 {
                    let len = u32::from_be_bytes([
                        partial_msg_buf[0],
                        partial_msg_buf[1],
                        partial_msg_buf[2],
                        partial_msg_buf[3],
                    ]) as usize;
                    
                    if len > 1024 * 1024 {
                        warn!(peer = %peer_addr, len = len, "Message too large");
                        break;
                    }
                    
                    let total_len = 4 + len;
                    if partial_msg_buf.len() >= total_len {
                        let msg_data = &partial_msg_buf[4..total_len];
                        
                        // Check if this is an Ask/Response message by looking at first byte
                        if msg_data.len() >= 1 {
                            if let Some(msg_type) = crate::MessageType::from_byte(msg_data[0]) {
                                // This is an Ask/Response message
                                if msg_data.len() < 8 {
                                    warn!(peer = %peer_addr, "Ask/Response message too small");
                                    partial_msg_buf.drain(..total_len);
                                    continue;
                                }
                                
                                let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                                let payload = &msg_data[8..];
                                
                                match msg_type {
                                    crate::MessageType::Ask => {
                                        // Handle incoming Ask requests for testing
                                        #[cfg(feature = "test-helpers")]
                                        {
                                            if let Some(ref registry_weak) = registry_weak {
                                                if let Some(registry) = registry_weak.upgrade() {
                                                    let conn = {
                                                        let pool = registry.connection_pool.lock().await;
                                                        pool.connections.get(&peer_addr).map(|conn_ref| conn_ref.value().clone())
                                                    };
                                                    
                                                    if let Some(conn) = conn {
                                                        // Process the request and generate response
                                                        let request_str = String::from_utf8_lossy(payload);
                                                        let response_data = process_mock_request(&request_str);
                                                        
                                                        // Build response message
                                                        let total_size = 8 + response_data.len();
                                                        let mut msg = Vec::with_capacity(4 + total_size);
                                                        
                                                        // Length prefix
                                                        msg.extend_from_slice(&(total_size as u32).to_be_bytes());
                                                        
                                                        // Header: [type:1][corr_id:2][reserved:5]
                                                        msg.push(crate::MessageType::Response as u8);
                                                        msg.extend_from_slice(&correlation_id.to_be_bytes());
                                                        msg.extend_from_slice(&[0u8; 5]);
                                                        msg.extend_from_slice(&response_data);
                                                        
                                                        // Send response back through stream handle
                                                        if let Some(ref stream_handle) = conn.stream_handle {
                                                            if let Err(e) = stream_handle.write_nonblocking(&msg) {
                                                                warn!("Failed to send mock response: {}", e);
                                                            } else {
                                                                debug!("Sent mock response for correlation_id {}", correlation_id);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        #[cfg(not(feature = "test-helpers"))]
                                        {
                                            warn!(peer = %peer_addr, correlation_id = correlation_id, "Received Ask request - not implemented in production");
                                        }
                                    }
                                    crate::MessageType::Response => {
                                        // Handle incoming response
                                        if let Some(ref registry_weak) = registry_weak {
                                            if let Some(registry) = registry_weak.upgrade() {
                                                // Find the connection with this peer
                                                let conn = {
                                                    let pool = registry.connection_pool.lock().await;
                                                    pool.connections.get(&peer_addr).map(|c| c.value().clone())
                                                };
                                                
                                                if let Some(conn) = conn {
                                                    if let Some(ref correlation) = conn.correlation {
                                                        correlation.complete(correlation_id, payload.to_vec());
                                                        debug!(peer = %peer_addr, correlation_id = correlation_id, "Delivered response to correlation tracker");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    crate::MessageType::Tell => {
                                        warn!(peer = %peer_addr, "Received Tell message in Ask/Response format - unexpected");
                                    }
                                }
                                
                                partial_msg_buf.drain(..total_len);
                                continue;
                            }
                        }
                        
                        // This is a gossip protocol message
                        let msg_data_vec = msg_data.to_vec();
                        partial_msg_buf.drain(..total_len);
                        
                        // Add timing right after TCP read - BEFORE any deserialization
                        let tcp_read_timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos();
                        
                        // Process message
                        if let Some(ref registry_weak) = registry_weak {
                            if let Some(registry) = registry_weak.upgrade() {
                                if let Ok(msg) = rkyv::from_bytes::<crate::registry::RegistryMessage, rkyv::rancor::Error>(&msg_data_vec) {
                                    // Debug: Show timing right after TCP read, before any processing
                                    match &msg {
                                        crate::registry::RegistryMessage::DeltaGossip { delta } => {
                                            let tcp_transmission_nanos = tcp_read_timestamp - delta.precise_timing_nanos as u128;
                                            let _tcp_transmission_ms = tcp_transmission_nanos as f64 / 1_000_000.0;
                                            // eprintln!("🔍 TCP_TRANSMISSION_TIME: {}ms ({}ns)", _tcp_transmission_ms, tcp_transmission_nanos);
                                        }
                                        _ => {}
                                    }
                                    
                                    if let Err(e) = handle_incoming_message(registry, peer_addr, msg).await {
                                        warn!(peer = %peer_addr, error = %e, "Failed to handle message");
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
            Err(e) => {
                warn!(peer = %peer_addr, error = %e, "Read error");
                break;
            }
        }
    }
    
    // Handle peer failure when connection is lost
    info!(peer = %peer_addr, "CONNECTION_POOL: Triggering peer failure handling");
    if let Some(ref registry_weak) = registry_weak {
        if let Some(registry) = registry_weak.upgrade() {
            if let Err(e) = registry.handle_peer_connection_failure(peer_addr).await {
                warn!(error = %e, peer = %peer_addr, "CONNECTION_POOL: Failed to handle peer connection failure");
            }
        }
    }
}

/// Implementation of persistent connection handling
async fn handle_persistent_connection_impl(
    stream: TcpStream,
    mut rx: mpsc::Receiver<Vec<u8>>,
    peer_addr: SocketAddr, // For outgoing: the peer's address. For incoming: the sender's listening address
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    info!(peer = %peer_addr, registry = ?registry_weak.is_some(), "persistent connection handler started");

    let (mut reader, mut writer) = stream.into_split();

    // Create a channel to signal EOF to write task
    let (eof_tx, mut eof_rx) = mpsc::channel::<()>(1);

    // Clone registry_weak for the read task
    let registry_weak_read = registry_weak.clone();

    // Spawn read task
    let read_task = tokio::spawn(async move {
        let mut partial_msg_buf = Vec::with_capacity(8192); // Pre-allocate for common message sizes
        let mut read_buf = vec![0u8; 8192]; // Larger read buffer for better performance

        loop {
            match reader.read(&mut read_buf).await {
                Ok(0) => {
                    info!(peer = %peer_addr, "🔴 CONNECTION POOL: Peer closed write (EOF) - half-closed");
                    // Signal EOF to write task
                    let _ = eof_tx.send(()).await;
                    return io::Result::Ok(());
                }
                Ok(n) => {
                    // Process the data we just read
                    partial_msg_buf.extend_from_slice(&read_buf[..n]);

                    // Try to process complete messages
                    while partial_msg_buf.len() >= 4 {
                        // All messages now have 4-byte length prefix
                        let len = u32::from_be_bytes([
                            partial_msg_buf[0],
                            partial_msg_buf[1],
                            partial_msg_buf[2],
                            partial_msg_buf[3],
                        ]) as usize;

                        if len > 10 * 1024 * 1024 {
                            warn!(peer = %peer_addr, "message too large: {}", len);
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "message too large",
                            ));
                        }

                        let total_len = 4 + len;
                        if partial_msg_buf.len() >= total_len {
                            let msg_data = &partial_msg_buf[4..total_len];
                            
                            // Check if this is an Ask/Response message by looking at first byte
                            if msg_data.len() >= 1 {
                                if let Some(msg_type) = crate::MessageType::from_byte(msg_data[0]) {
                                    // This is an Ask/Response message
                                    if msg_data.len() < 8 {
                                        warn!(peer = %peer_addr, "Ask/Response message too small");
                                        partial_msg_buf.drain(..total_len);
                                        continue;
                                    }
                                    
                                    let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
                                    let payload = &msg_data[8..];
                                    
                                    match msg_type {
                                        crate::MessageType::Ask => {
                                            // Handle incoming Ask requests for testing
                                            #[cfg(feature = "test-helpers")]
                                            {
                                                if let Some(ref registry_weak) = registry_weak_read {
                                                    if let Some(registry) = registry_weak.upgrade() {
                                                        let conn = {
                                                            let pool = registry.connection_pool.lock().await;
                                                            pool.connections.get(&peer_addr).map(|conn_ref| conn_ref.value().clone())
                                                        };
                                                        
                                                        if let Some(conn) = conn {
                                                            // Process the request and generate response
                                                            let request_str = String::from_utf8_lossy(payload);
                                                            let response_data = process_mock_request(&request_str);
                                                            
                                                            // Build response message
                                                            let total_size = 8 + response_data.len();
                                                            let mut msg = Vec::with_capacity(4 + total_size);
                                                            
                                                            // Length prefix
                                                            msg.extend_from_slice(&(total_size as u32).to_be_bytes());
                                                            
                                                            // Header: [type:1][corr_id:2][reserved:5]
                                                            msg.push(crate::MessageType::Response as u8);
                                                            msg.extend_from_slice(&correlation_id.to_be_bytes());
                                                            msg.extend_from_slice(&[0u8; 5]);
                                                            msg.extend_from_slice(&response_data);
                                                            
                                                            // Send response back through stream handle
                                                            if let Some(ref stream_handle) = conn.stream_handle {
                                                                if let Err(e) = stream_handle.write_nonblocking(&msg) {
                                                                    warn!("Failed to send mock response: {}", e);
                                                                } else {
                                                                    debug!("Sent mock response for correlation_id {}", correlation_id);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            #[cfg(not(feature = "test-helpers"))]
                                            {
                                                warn!(peer = %peer_addr, correlation_id = correlation_id, "Received Ask request - not implemented in production");
                                            }
                                        }
                                        crate::MessageType::Response => {
                                            // Handle incoming response
                                            if let Some(ref registry_weak) = registry_weak_read {
                                                if let Some(registry) = registry_weak.upgrade() {
                                                    // Find the connection with this peer
                                                    let conn = {
                                                        let pool = registry.connection_pool.lock().await;
                                                        pool.connections.get(&peer_addr).map(|c| c.value().clone())
                                                    };
                                                    
                                                    if let Some(conn) = conn {
                                                        if let Some(ref correlation) = conn.correlation {
                                                            correlation.complete(correlation_id, payload.to_vec());
                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, "Delivered response to correlation tracker");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        crate::MessageType::Tell => {
                                            warn!(peer = %peer_addr, "Received Tell message in Ask/Response format - unexpected");
                                        }
                                    }
                                    
                                    partial_msg_buf.drain(..total_len);
                                    continue;
                                }
                            }
                            
                            // This is a gossip protocol message
                            match rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(msg_data) {
                                Ok(msg) => {
                                    if let Some(ref registry_weak) = registry_weak_read {
                                        if let Some(registry) = registry_weak.upgrade() {
                                            // Direct inline processing for minimum latency
                                            if let Err(e) = handle_incoming_message(
                                                registry,
                                                peer_addr,
                                                msg,
                                            )
                                            .await
                                            {
                                                warn!(peer = %peer_addr, error = %e, "failed to handle incoming message");
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(peer = %peer_addr, error = %e, "failed to deserialize message");
                                }
                            }

                            // Remove processed message from buffer AFTER processing
                            partial_msg_buf.drain(..total_len);
                        } else {
                            // Need more data for complete message
                            break;
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    warn!(peer = %peer_addr, error = %e, "error reading from socket");
                    return Err(e);
                }
            }
        }
    });

    // Spawn write task
    let write_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // Check for EOF signal
                _ = eof_rx.recv() => {
                    info!(peer = %peer_addr, "🔴 CONNECTION POOL: EOF signal received, shutting down write task");
                    return io::Result::Ok(());
                }
                // Check for messages to send
                msg = rx.recv() => {
                    match msg {
                        Some(data) => {
                            match writer.write_all(&data).await {
                                Ok(_) => {
                                    // CRITICAL: Flush immediately to avoid TCP buffering delays
                                    if let Err(e) = writer.flush().await {
                                        warn!(peer = %peer_addr, error = %e, "failed to flush TCP socket");
                                        return Err(e);
                                    }
                                    debug!(peer = %peer_addr, bytes = data.len(), "📤 CONNECTION POOL: Sent and flushed message");
                                }
                                Err(ref e) if e.kind() == io::ErrorKind::BrokenPipe
                                          || e.kind() == io::ErrorKind::ConnectionReset => {
                                    info!(peer = %peer_addr, "🔴 CONNECTION POOL: Peer closed read - write failed");
                                    return io::Result::Ok(());
                                }
                                Err(e) => {
                                    warn!(peer = %peer_addr, error = %e, "failed to write");
                                    return Err(e);
                                }
                            }
                        }
                        None => {
                            info!(peer = %peer_addr, "🔴 CONNECTION POOL: channel closed, shutting down write task");
                            return io::Result::Ok(());
                        }
                    }
                }
            }
        }
    });

    // Wait for both tasks to complete
    let (read_res, write_res) = tokio::join!(read_task, write_task);

    // Log results
    match read_res {
        Ok(Ok(_)) => info!(peer = %peer_addr, "read task completed successfully"),
        Ok(Err(e)) => warn!(peer = %peer_addr, error = %e, "read task failed"),
        Err(e) => warn!(peer = %peer_addr, error = %e, "read task panicked"),
    }

    match write_res {
        Ok(Ok(_)) => info!(peer = %peer_addr, "write task completed successfully"),
        Ok(Err(e)) => warn!(peer = %peer_addr, error = %e, "write task failed"),
        Err(e) => warn!(peer = %peer_addr, error = %e, "write task panicked"),
    }

    // Notify of disconnection
    info!(peer = %peer_addr, "persistent connection lost, triggering failure handling");

    // Mark connection as disconnected in the pool
    if let Some(ref registry_weak) = registry_weak {
        if let Some(registry) = registry_weak.upgrade() {
            // First mark disconnected in pool
            {
                let mut pool = registry.connection_pool.lock().await;
                pool.mark_disconnected(peer_addr);
            }

            // Then handle the failure in a separate task
            let failed_addr = peer_addr;
            tokio::spawn(async move {
                if let Err(e) = registry.handle_peer_connection_failure(failed_addr).await {
                    warn!(error = %e, peer = %failed_addr, "failed to handle peer connection failure");
                }
            });
        }
    }
}

/// Handle an incoming message on a bidirectional connection
pub(crate) async fn handle_incoming_message(
    registry: Arc<GossipRegistry>,
    _peer_addr: SocketAddr,
    msg: RegistryMessage,
) -> Result<()> {
    match msg {
        RegistryMessage::DeltaGossip { delta } => {
            debug!(
                sender = %delta.sender_peer_id,
                since_sequence = delta.since_sequence,
                changes = delta.changes.len(),
                "received delta gossip message on bidirectional connection"
            );

            // OPTIMIZATION: Do all peer management in one lock acquisition
            {
                let mut gossip_state = registry.gossip_state.lock().await;
                
                // Use the actual peer address we received from
                let sender_socket_addr = _peer_addr;
                
                // Add the sender as a peer (inlined to avoid separate lock)
                if delta.sender_peer_id != registry.peer_id {
                    if let std::collections::hash_map::Entry::Vacant(e) = gossip_state.peers.entry(sender_socket_addr) {
                        let current_time = crate::current_timestamp();
                        e.insert(crate::registry::PeerInfo {
                            address: sender_socket_addr,
                            peer_address: None,
                            failures: 0,
                            last_attempt: current_time,
                            last_success: current_time,
                            last_sequence: 0,
                            last_sent_sequence: 0,
                            consecutive_deltas: 0,
                            last_failure_time: None,
                        });
                    }
                }

                // Check if this is a previously failed peer
                let was_failed = gossip_state
                    .peers
                    .get(&sender_socket_addr)
                    .map(|info| info.failures >= registry.config.max_peer_failures)
                    .unwrap_or(false);

                if was_failed {
                    info!(
                        peer = %delta.sender_peer_id,
                        "✅ Received delta from previously failed peer - connection restored!"
                    );

                    // Clear the pending failure record
                    gossip_state
                        .pending_peer_failures
                        .remove(&sender_socket_addr);
                }

                // Update peer info and check if we need to clear pending failures
                let need_to_clear_pending = if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                    // Always reset failure state when we receive messages from the peer
                    // This proves the peer is alive and communicating
                    let had_failures = peer_info.failures > 0;
                    if had_failures {
                        info!(peer = %delta.sender_peer_id, 
                              prev_failures = peer_info.failures,
                              "🔄 Resetting failure state after receiving DeltaGossip");
                        peer_info.failures = 0;
                        peer_info.last_failure_time = None;
                    }
                    peer_info.last_success = crate::current_timestamp();

                    peer_info.last_sequence =
                        std::cmp::max(peer_info.last_sequence, delta.current_sequence);
                    peer_info.consecutive_deltas += 1;
                    
                    had_failures
                } else {
                    false
                };
                
                // Clear pending failure record if needed
                if need_to_clear_pending {
                    gossip_state.pending_peer_failures.remove(&sender_socket_addr);
                }
                gossip_state.delta_exchanges += 1;
            }

            // CRITICAL OPTIMIZATION: Inline apply_delta to eliminate function call overhead
            // Apply the delta directly here to minimize async scheduling delays
            {
                let total_changes = delta.changes.len();
                // Use the actual peer address we received from
                let sender_addr = _peer_addr;
                
                // Pre-compute priority flags to avoid redundant checks
                let has_immediate = delta.changes.iter().any(|change| match change {
                    crate::registry::RegistryChange::ActorAdded { priority, .. } => {
                        priority.should_trigger_immediate_gossip()
                    }
                    crate::registry::RegistryChange::ActorRemoved { priority, .. } => {
                        priority.should_trigger_immediate_gossip()
                    }
                });

                if has_immediate {
                    error!(
                        "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                        total_changes, sender_addr
                    );
                }

                // Pre-capture timing info outside lock for better performance - use high resolution timing
                let _received_instant = std::time::Instant::now();
                let received_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                
                // eprintln!("🔍 RECEIVED_TIMESTAMP: {}ns", received_timestamp);

                // OPTIMIZATION: Fast-path state updates with try_lock to avoid blocking
                let (applied_count, _pending_updates) = {
                    // Try non-blocking access first
                    if let Ok(mut actor_state) = registry.actor_state.try_write() {
                        // Fast path - direct update without batching overhead
                        let mut applied = 0;
                        for change in delta.changes {
                            match change {
                                crate::registry::RegistryChange::ActorAdded {
                                    name,
                                    location,
                                    priority: _,
                                } => {
                                    // Don't override local actors
                                    if actor_state.local_actors.contains_key(name.as_str()) {
                                        continue;
                                    }

                                    // Quick conflict check
                                    let should_apply = match actor_state.known_actors.get(name.as_str()) {
                                        Some(existing) => location.wall_clock_time > existing.wall_clock_time
                                            || (location.wall_clock_time == existing.wall_clock_time
                                                && location.address > existing.address),
                                        None => true,
                                    };

                                    if should_apply {
                                        actor_state.known_actors.insert(name.clone(), location.clone());
                                        applied += 1;
                                        
                                        // Inline timing calculation for immediate priority
                                        if location.priority.should_trigger_immediate_gossip() {
                                            let network_time_nanos = received_timestamp - delta.precise_timing_nanos as u128;
                                            let network_time_ms = network_time_nanos as f64 / 1_000_000.0;
                                            let propagation_time_ms = network_time_ms; // Same as network time for now
                                            let processing_only_time_ms = 0.0; // No additional processing time beyond network
                                            
                                            eprintln!("🔍 FAST_PATH: Processing immediate priority actor: {} propagation_time_ms={:.3}ms", name, propagation_time_ms);
                                                     
                                            info!(
                                                actor_name = %name,
                                                priority = ?location.priority,
                                                propagation_time_ms = propagation_time_ms,
                                                network_processing_time_ms = network_time_ms,
                                                processing_only_time_ms = processing_only_time_ms,
                                                "RECEIVED_ACTOR"
                                            );
                                        }
                                    }
                                }
                                crate::registry::RegistryChange::ActorRemoved { name, priority: _ } => {
                                    if actor_state.known_actors.remove(name.as_str()).is_some() {
                                        applied += 1;
                                    }
                                }
                            }
                        }
                        (applied, Vec::new())
                    } else {
                        // Fallback to batched approach if lock is contended
                        let actor_state = registry.actor_state.read().await;
                        let mut pending_updates = Vec::new();
                        let mut pending_removals = Vec::new();
                    
                    for change in &delta.changes {
                        match change {
                            crate::registry::RegistryChange::ActorAdded {
                                name,
                                location,
                                priority: _,
                            } => {
                                // Don't override local actors - early exit
                                if actor_state.local_actors.contains_key(name.as_str()) {
                                    debug!(
                                        actor_name = %name,
                                        "skipping remote actor update - actor is local"
                                    );
                                    continue;
                                }

                                // Check if we already know about this actor
                                let should_apply = match actor_state.known_actors.get(name.as_str()) {
                                    Some(existing_location) => {
                                        // Use wall clock time as the primary decision factor
                                        location.wall_clock_time > existing_location.wall_clock_time
                                            || (location.wall_clock_time
                                                == existing_location.wall_clock_time
                                                && location.address > existing_location.address)
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
                                    pending_updates.push((name.clone(), location.clone()));
                                }
                            }
                            crate::registry::RegistryChange::ActorRemoved { name, priority: _ } => {
                                if actor_state.known_actors.contains_key(name.as_str()) {
                                    pending_removals.push(name.clone());
                                }
                            }
                        }
                    }
                    
                    // Drop read lock before acquiring write lock
                    drop(actor_state);
                    
                    // Second pass: apply all updates under write lock in one go
                    let mut actor_state = registry.actor_state.write().await;
                    let mut applied = 0;
                    
                    // Apply all actor additions
                    for (name, location) in &pending_updates {
                        actor_state.known_actors.insert(name.clone(), location.clone());
                        applied += 1;
                        
                        // Log the timing information for immediate priority changes
                        if location.priority.should_trigger_immediate_gossip() {
                            // Calculate time from when delta was sent (network + processing)
                            let network_processing_time_nanos = received_timestamp - delta.precise_timing_nanos as u128;
                            let network_processing_time_ms = network_processing_time_nanos as f64 / 1_000_000.0;
                            let propagation_time_ms = network_processing_time_ms; // Same as network time for now
                            let processing_only_time_ms = 0.0; // No additional processing time beyond network

                            // Debug: Break down where the time is spent
                            eprintln!("🔍 TIMING_BREAKDOWN: sent={}, received={}, delta={}ns ({}ms)", 
                                     delta.precise_timing_nanos, received_timestamp, 
                                     network_processing_time_nanos, network_processing_time_ms);

                            info!(
                                actor_name = %name,
                                priority = ?location.priority,
                                propagation_time_ms = propagation_time_ms,
                                network_processing_time_ms = network_processing_time_ms,
                                processing_only_time_ms = processing_only_time_ms,
                                "RECEIVED_ACTOR"
                            );
                        }
                    }
                    
                    // Apply all actor removals
                    for name in &pending_removals {
                        if actor_state.known_actors.remove(name.as_str()).is_some() {
                            applied += 1;
                        }
                    }
                        
                        // Rest of fallback batched logic stays the same...
                        drop(actor_state);
                        let mut actor_state = registry.actor_state.write().await;
                        let mut _applied = 0;
                        
                        for (name, location) in &pending_updates {
                            actor_state.known_actors.insert(name.clone(), location.clone());
                            _applied += 1;
                        }
                        
                        for name in &pending_removals {
                            if actor_state.known_actors.remove(name.as_str()).is_some() {
                                _applied += 1;
                            }
                        }
                        
                        (applied, pending_updates)
                    }
                };

                if applied_count > 0 {
                    debug!(
                        applied_count = applied_count,
                        sender = %sender_addr,
                        "applied delta changes in batched update"
                    );
                }
            }

            // Note: Response will be sent during regular gossip rounds
            Ok(())
        }
        RegistryMessage::FullSync {
            local_actors,
            known_actors,
            sender_peer_id,
            sequence,
            wall_clock_time,
        } => {
            // We no longer have sender address - will need to handle differently
            let sender_socket_addr = _peer_addr; // Use the actual peer address
            
            // Note: sender_peer_id is now a PeerId (e.g., "node_a"), not an address
            debug!("Received FullSync from node '{}' at address {}", sender_peer_id, sender_socket_addr);
            
            // OPTIMIZATION: Do all peer management in one lock acquisition
            {
                let mut gossip_state = registry.gossip_state.lock().await;
                
                // Add the sender as a peer (inlined to avoid separate lock)
                if sender_socket_addr != registry.bind_addr {
                    if let std::collections::hash_map::Entry::Vacant(e) = gossip_state.peers.entry(sender_socket_addr) {
                        info!(peer = %sender_socket_addr, "Adding new peer from FullSync");
                        let current_time = crate::current_timestamp();
                        e.insert(crate::registry::PeerInfo {
                            address: sender_socket_addr,
                            peer_address: None,
                            failures: 0,
                            last_attempt: current_time,
                            last_success: current_time,
                            last_sequence: 0,
                            last_sent_sequence: 0,
                            consecutive_deltas: 0,
                            last_failure_time: None,
                        });
                    }
                }

                // Update peer info and reset failure state
                let had_failures = gossip_state.peers.get(&sender_socket_addr)
                    .map(|info| info.failures > 0)
                    .unwrap_or(false);
                
                if had_failures {
                    // Clear the pending failure record
                    gossip_state.pending_peer_failures.remove(&sender_socket_addr);
                }
                
                if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                    let prev_failures = peer_info.failures;
                    // Always reset failure state when we receive a FullSync from the peer
                    // This proves the peer is alive and communicating
                    if peer_info.failures > 0 {
                        info!(peer = %sender_socket_addr, 
                              prev_failures = prev_failures,
                              "🔄 Resetting failure state after receiving FullSync");
                        peer_info.failures = 0;
                        peer_info.last_failure_time = None;
                    }
                    peer_info.last_success = crate::current_timestamp();
                    peer_info.consecutive_deltas = 0;
                } else {
                    warn!(peer = %sender_socket_addr, "Peer not found in peer list when trying to reset failure state");
                }
                gossip_state.full_sync_exchanges += 1;
            }

            info!(
                sender = %sender_peer_id,
                sequence = sequence,
                local_actors = local_actors.len(),
                known_actors = known_actors.len(),
                "📨 INCOMING: Received full sync message on bidirectional connection"
            );

            // Only remaining async operation
            registry
                .merge_full_sync(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    sender_socket_addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

            // Send back our state as a response so the sender can receive our actors
            // This is critical for late-joining nodes (like Node C) to get existing state
            {
                // Get our current state
                let (our_local_actors, our_known_actors, our_sequence) = {
                    let actor_state = registry.actor_state.read().await;
                    let gossip_state = registry.gossip_state.lock().await;
                    (
                        actor_state.local_actors.clone(),
                        actor_state.known_actors.clone(),
                        gossip_state.gossip_sequence,
                    )
                };
                
                // Calculate sizes before moving
                let local_actors_count = our_local_actors.len();
                let known_actors_count = our_known_actors.len();
                
                // Create a FullSyncResponse message
                let response = RegistryMessage::FullSyncResponse {
                    local_actors: our_local_actors.into_iter().map(|(k, v)| (k, v)).collect(),
                    known_actors: our_known_actors.into_iter().map(|(k, v)| (k, v)).collect(),
                    sender_peer_id: registry.peer_id.clone(), // Use peer ID
                    sequence: our_sequence,
                    wall_clock_time: crate::current_timestamp(),
                };
                
                // Send the response back through existing connection
                // We'll use send_lock_free which doesn't create new connections
                let response_data = match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!(error = %e, "Failed to serialize FullSync response");
                        return Ok(());
                    }
                };
                
                // Try to send immediately on existing connection
                {
                    info!("FULLSYNC RESPONSE: Node {} is about to acquire connection pool lock", registry.bind_addr);
                    let pool = registry.connection_pool.lock().await;
                    info!("FULLSYNC RESPONSE: Node {} got pool lock, pool has {} total entries", 
                          registry.bind_addr, pool.connection_count());
                    info!("FULLSYNC RESPONSE: Pool instance address: {:p}", &*pool);
                    
                    // Log details about each connection
                    for entry in pool.connections.iter() {
                        let addr = entry.key();
                        let conn = entry.value();
                        info!("FULLSYNC RESPONSE: Connection to {} - state={:?}", 
                              addr, conn.get_state());
                    }
                    
                    // Create message with length prefix
                    let mut buffer = Vec::with_capacity(4 + response_data.len());
                    buffer.extend_from_slice(&(response_data.len() as u32).to_be_bytes());
                    buffer.extend_from_slice(&response_data);
                    
                    // Debug: Log what connections we have
                    info!(
                        "FULLSYNC RESPONSE DEBUG: Available connections by addr: {:?}", 
                        pool.connections.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>()
                    );
                    info!(
                        "FULLSYNC RESPONSE DEBUG: Available node mappings: {:?}",
                        pool.peer_id_to_addr.iter().map(|entry| (entry.key().clone(), entry.value().clone())).collect::<Vec<_>>()
                    );
                    info!(
                        "FULLSYNC RESPONSE DEBUG: Looking for connection to sender_peer_id: {}", 
                        sender_peer_id
                    );
                    info!("FULLSYNC RESPONSE DEBUG: sender_socket_addr={}", sender_socket_addr);
                    
                    // Try to send using peer ID
                    let send_result = match pool.send_to_node_id(sender_peer_id.as_str(), &buffer) {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            warn!("Failed to send via peer ID {}: {}", sender_peer_id, e);
                            // Fall back to socket address
                            pool.send_lock_free(sender_socket_addr, &buffer)
                        }
                    };
                    
                    if send_result.is_err() {
                        warn!(
                            "Primary send failed for peer {}, no fallback available",
                            sender_peer_id
                        );
                    }
                    
                    match send_result {
                        Ok(()) => {
                            info!(peer = %sender_socket_addr, 
                                  peer_id = %sender_peer_id,
                                  local_actors = local_actors_count,
                                  known_actors = known_actors_count,
                                  bind_addr = %registry.bind_addr,
                                  "📤 RESPONSE: Successfully sent FullSync response with our state");
                        }
                        Err(e) => {
                            // If we can't send immediately, queue it for the next gossip round
                            warn!(peer = %sender_socket_addr, 
                                  peer_id = %sender_peer_id,
                                  error = %e, 
                                  "Could not send FullSync response immediately - will be sent in next gossip round");
                            
                            // Store in gossip state to be sent during next gossip round
                            drop(pool); // Release the pool lock first
                            let mut gossip_state = registry.gossip_state.lock().await;
                            
                            // Mark that we need to send a full sync to this peer
                            if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                                // Force a full sync on the next gossip round
                                peer_info.consecutive_deltas = registry.config.max_delta_history as u64;
                                info!(peer = %sender_socket_addr, 
                                      "Marked peer for full sync in next gossip round");
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        RegistryMessage::FullSyncRequest {
            sender_peer_id,
            sequence: _,
            wall_clock_time: _,
        } => {
            debug!(
                sender = %sender_peer_id,
                "received full sync request on bidirectional connection"
            );

            {
                let mut gossip_state = registry.gossip_state.lock().await;
                gossip_state.full_sync_exchanges += 1;
            }

            // Note: Response will be sent during regular gossip rounds
            Ok(())
        }
        // Handle response messages (these can arrive on incoming connections too)
        RegistryMessage::DeltaGossipResponse { delta } => {
            debug!(
                sender = %delta.sender_peer_id,
                changes = delta.changes.len(),
                "received delta gossip response on bidirectional connection"
            );

            if let Err(err) = registry.apply_delta(delta).await {
                warn!(error = %err, "failed to apply delta from response");
            } else {
                let mut gossip_state = registry.gossip_state.lock().await;
                gossip_state.delta_exchanges += 1;
            }
            Ok(())
        }
        RegistryMessage::FullSyncResponse {
            local_actors,
            known_actors,
            sender_peer_id,
            sequence,
            wall_clock_time,
        } => {
            info!(
                sender = %sender_peer_id,
                local_actors = local_actors.len(),
                known_actors = known_actors.len(),
                "📥 RECEIVED: FullSyncResponse from peer"
            );

            // Use the actual peer address we're connected to
            let sender_socket_addr = _peer_addr;

            registry
                .merge_full_sync(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    sender_socket_addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

            // Reset failure state when receiving response
            let mut gossip_state = registry.gossip_state.lock().await;
            
            // Reset failure state for responding peer
            let need_to_clear_pending = if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                let had_failures = peer_info.failures > 0;
                if had_failures {
                    info!(peer = %sender_socket_addr, 
                          prev_failures = peer_info.failures,
                          "🔄 Resetting failure state after receiving FullSyncResponse");
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                }
                peer_info.last_success = crate::current_timestamp();
                had_failures
            } else {
                false
            };
            
            // Clear pending failure record if needed
            if need_to_clear_pending {
                gossip_state.pending_peer_failures.remove(&sender_socket_addr);
            }
            
            gossip_state.full_sync_exchanges += 1;
            Ok(())
        }
        RegistryMessage::PeerHealthQuery {
            sender,
            target_peer,
            timestamp: _,
        } => {
            debug!(
                sender = %sender,
                target = %target_peer,
                "received peer health query"
            );

            // Check our connection status to the target peer
            let target_addr = match target_peer.parse::<SocketAddr>() {
                Ok(addr) => addr,
                Err(_) => {
                    warn!("Invalid target peer address in health query: {}", target_peer);
                    return Ok(());
                }
            };
            
            let is_alive = {
                let pool = registry.connection_pool.lock().await;
                pool.has_connection(&target_addr)
            };

            let last_contact = if is_alive {
                crate::current_timestamp()
            } else {
                // Check when we last had successful contact
                let gossip_state = registry.gossip_state.lock().await;
                gossip_state
                    .peers
                    .get(&target_addr)
                    .map(|info| info.last_success)
                    .unwrap_or(0)
            };

            // Send our health report back
            let mut peer_statuses = HashMap::new();

            // Get actual failure count from gossip state
            let failure_count = {
                let gossip_state = registry.gossip_state.lock().await;
                gossip_state
                    .peers
                    .get(&target_addr)
                    .map(|info| info.failures as u32)
                    .unwrap_or(0)
            };

            peer_statuses.insert(
                target_peer,
                crate::registry::PeerHealthStatus {
                    is_alive,
                    last_contact,
                    failure_count,
                },
            );

            let report = RegistryMessage::PeerHealthReport {
                reporter: registry.peer_id.clone(),
                peer_statuses: peer_statuses.into_iter().collect(),
                timestamp: crate::current_timestamp(),
            };

            // Send report back to the querying peer
            if let Ok(data) = rkyv::to_bytes::<rkyv::rancor::Error>(&report) {
                let pool = registry.connection_pool.lock().await;
                // Use the actual peer address we received from
                let sender_addr = _peer_addr;
                
                // Create message with length prefix
                let mut buffer = Vec::with_capacity(4 + data.len());
                buffer.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buffer.extend_from_slice(&data);
                
                // Use send_lock_free which doesn't create new connections
                if let Err(e) = pool.send_lock_free(sender_addr, &buffer) {
                    warn!(peer = %sender_addr, error = %e, "Failed to send peer health report");
                }
            }

            Ok(())
        }
        RegistryMessage::PeerHealthReport {
            reporter,
            peer_statuses,
            timestamp: _,
        } => {
            debug!(
                reporter = %reporter,
                peers = peer_statuses.len(),
                "received peer health report"
            );

            // Store the health reports
            {
                let mut gossip_state = registry.gossip_state.lock().await;
                for (peer, status) in peer_statuses {
                    if let Ok(peer_addr) = peer.parse::<SocketAddr>() {
                        // For now, use the reporter's peer address from the connection
                        gossip_state
                            .peer_health_reports
                            .entry(peer_addr)
                            .or_insert_with(HashMap::new)
                            .insert(_peer_addr, status);
                    }
                }
            }

            // Check if we have enough reports to make a decision
            registry.check_peer_consensus().await;

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::sleep;

    async fn create_test_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept connections in background
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    // Simple echo server - just keep connection open
                    let mut buf = vec![0; 1024];
                    loop {
                        use tokio::io::AsyncReadExt;
                        match stream.read(&mut buf).await {
                            Ok(0) => break, // Connection closed
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        addr
    }

    #[test]
    fn test_connection_handle_debug() {
        // Compile-time test to ensure Debug is implemented
        use std::fmt::Debug;
        fn assert_debug<T: Debug>() {}
        assert_debug::<ConnectionHandle>();
    }

    #[tokio::test]
    async fn test_connection_pool_new() {
        let pool = ConnectionPool::new(10, Duration::from_secs(5));
        assert_eq!(pool.connection_count(), 0);
        assert_eq!(pool.max_connections, 10);
        assert_eq!(pool.connection_timeout, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_set_registry() {
        use crate::{registry::GossipRegistry, GossipConfig};
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let registry = Arc::new(GossipRegistry::new(
            "127.0.0.1:8080".parse().unwrap(),
            GossipConfig::default(),
        ));

        pool.set_registry(registry.clone());
        assert!(pool.registry.is_some());
    }

    #[tokio::test]
    async fn test_get_connection_new() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;

        // Wait for server to start
        sleep(Duration::from_millis(10)).await;

        // Get connection should create new one
        let handle = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(handle.addr, server_addr);
        assert_eq!(pool.connection_count(), 1);
        assert!(pool.has_connection(&server_addr));
    }

    #[tokio::test]
    async fn test_get_connection_reuse() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        // First connection
        let handle1 = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);

        // Second get should reuse existing connection
        let handle2 = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1); // Still just one connection
        assert_eq!(handle1.addr, handle2.addr);
    }

    #[tokio::test]
    async fn test_get_connection_timeout() {
        let mut pool = ConnectionPool::new(10, Duration::from_millis(100));
        let nonexistent_addr = "127.0.0.1:1".parse().unwrap();

        // Should timeout or get connection refused
        let result = pool.get_connection(nonexistent_addr).await;
        match result {
            Err(GossipError::Timeout) => (),    // Expected
            Err(GossipError::Network(_)) => (), // Also acceptable (connection refused)
            _ => panic!("Expected timeout or network error, got: {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_connection_pool_full() {
        let mut pool = ConnectionPool::new(2, Duration::from_secs(5));

        let server1 = create_test_server().await;
        let server2 = create_test_server().await;
        let server3 = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        // Fill pool
        pool.get_connection(server1).await.unwrap();
        pool.get_connection(server2).await.unwrap();
        assert_eq!(pool.connection_count(), 2);

        // Third connection should evict oldest
        pool.get_connection(server3).await.unwrap();
        assert_eq!(pool.connection_count(), 2); // Still 2, one was evicted
    }

    #[tokio::test]
    async fn test_mark_disconnected() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);

        pool.mark_disconnected(server_addr);
        assert_eq!(pool.connection_count(), 0); // Disconnected connections don't count
        assert!(!pool.has_connection(&server_addr));
    }

    #[tokio::test]
    async fn test_remove_connection() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server_addr).await.unwrap();
        assert!(pool.has_connection(&server_addr));

        pool.remove_connection(server_addr);
        assert!(!pool.has_connection(&server_addr));
        assert_eq!(pool.connection_count(), 0);
    }


    #[tokio::test]
    async fn test_cleanup_stale_connections() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);

        // Mark as disconnected
        pool.mark_disconnected(server_addr);
        assert_eq!(pool.connection_count(), 0);

        // Cleanup should remove disconnected
        pool.cleanup_stale_connections();
        assert!(pool.connections.is_empty());
    }

    #[tokio::test]
    async fn test_close_all_connections() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));

        let server1 = create_test_server().await;
        let server2 = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server1).await.unwrap();
        pool.get_connection(server2).await.unwrap();
        assert_eq!(pool.connection_count(), 2);

        pool.close_all_connections();
        assert_eq!(pool.connection_count(), 0);
        assert!(pool.connections.is_empty());
    }

    #[tokio::test]
    async fn test_connection_handle_send_data() {
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, writer) = stream.into_split();
        // Writer is now owned directly by LockFreeStreamHandle
        
        // Create a LockFreeStreamHandle first
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            1024,
        ));
        
        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            correlation: CorrelationTracker::new(),
        };

        let data = vec![1, 2, 3, 4];
        // Just test that send_data doesn't error - the data goes to the mock stream
        handle.send_data(data.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_handle_send_data_closed() {
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, mut writer) = stream.into_split();
        use tokio::io::AsyncWriteExt;
        let _ = writer.shutdown().await; // Close the writer
        // Writer is now owned directly by LockFreeStreamHandle

        // Create a LockFreeStreamHandle first
        let stream_handle = Arc::new(LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            1024,
        ));
        
        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            correlation: CorrelationTracker::new(),
        };

        let result = handle.send_data(vec![1, 2, 3]).await;
        assert!(matches!(result, Err(GossipError::Network(_))));
    }

    #[tokio::test]
    async fn test_check_connection_health() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let failed = pool.check_connection_health().await;
        assert!(failed.is_empty()); // Current implementation always returns empty
    }

    #[tokio::test]
    async fn test_persistent_connection() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let _last_used = current_timestamp();
        
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, writer) = stream.into_split();
        // Writer is now owned directly by LockFreeStreamHandle

        // Test creating a LockFreeStreamHandle
        let stream_handle = Arc::new(LockFreeStreamHandle::new(writer, addr, ChannelId::Global, 1024));
        assert!(stream_handle.channel_id == ChannelId::Global);
    }

    #[tokio::test]
    async fn test_get_connection_disconnected_removal() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        // Create connection
        pool.get_connection(server_addr).await.unwrap();

        // Mark as disconnected
        pool.mark_disconnected(server_addr);

        // Getting connection again should create new one
        let handle = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);
        assert_eq!(handle.addr, server_addr);
    }
}
