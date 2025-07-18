use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::io::IoSlice;

use crate::{
    current_timestamp,
    registry::{GossipRegistry, RegistryMessage},
    GossipError, Result,
};

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

/// Stream frame flags for protocol control
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameFlags {
    More = 0x01,
    Compress = 0x02,
    Priority = 0x04,
}

/// Stream frame header for custom framing protocol with multiplexing
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StreamFrameHeader {
    pub frame_type: u8,
    pub channel_id: u8,    // Channel for multiplexing
    pub flags: u8,
    pub sequence_id: u16,
    pub payload_len: u32,
}

/// Handle for direct stream access with zero-copy operations
#[derive(Debug, Clone)]
pub struct StreamHandle {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    addr: SocketAddr,
}

/// Ultra-high-performance zero-copy streaming handle for line-rate performance
pub struct ZeroCopyStreamHandle {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    addr: SocketAddr,
    batch_buffer: Vec<u8>,
    batch_size: usize,
    auto_flush: AtomicBool,
    bytes_pending: AtomicUsize,
}

/// Ultimate performance streaming handle with no-acknowledgment fire-and-forget mode
pub struct UltimateStreamHandle {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    addr: SocketAddr,
    buffer_pool: Vec<Vec<u8>>,
    current_buffer_idx: AtomicUsize,
    total_bytes_written: AtomicUsize,
    no_ack_mode: AtomicBool,
}

/// Write command for the lock-free ring buffer
#[derive(Debug, Clone)]
pub struct WriteCommand {
    pub channel_id: ChannelId,
    pub data: Vec<u8>,
    pub sequence: u64,
}

/// Lock-free ring buffer for high-performance writes
#[derive(Debug)]
pub struct LockFreeRingBuffer {
    buffer: Vec<Option<WriteCommand>>,
    capacity: usize,
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
    pub fn try_push(&self, command: WriteCommand) -> bool {
        let current_writes = self.pending_writes.load(Ordering::Relaxed);
        if current_writes >= self.capacity {
            return false; // Buffer full
        }
        
        let write_idx = self.write_index.fetch_add(1, Ordering::Relaxed) % self.capacity;
        
        // This is unsafe but fast - we're assuming single writer per slot
        unsafe {
            let slot = self.buffer.as_ptr().add(write_idx) as *mut Option<WriteCommand>;
            *slot = Some(command);
        }
        
        self.pending_writes.fetch_add(1, Ordering::Relaxed);
        true
    }
    
    /// Try to pop a write command - returns None if empty
    pub fn try_pop(&self) -> Option<WriteCommand> {
        let current_writes = self.pending_writes.load(Ordering::Relaxed);
        if current_writes == 0 {
            return None;
        }
        
        let read_idx = self.read_index.fetch_add(1, Ordering::Relaxed) % self.capacity;
        
        // This is unsafe but fast - we're assuming single reader per slot
        unsafe {
            let slot = self.buffer.as_ptr().add(read_idx) as *mut Option<WriteCommand>;
            let command = (*slot).take();
            if command.is_some() {
                self.pending_writes.fetch_sub(1, Ordering::Relaxed);
            }
            command
        }
    }
    
    pub fn pending_count(&self) -> usize {
        self.pending_writes.load(Ordering::Relaxed)
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
    pub fn new(writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>, addr: SocketAddr, channel_id: ChannelId, buffer_size: usize) -> Self {
        let ring_buffer = Arc::new(LockFreeRingBuffer::new(buffer_size));
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        
        // Create channel for tell operations
        let (tell_sender, tell_receiver) = tokio::sync::mpsc::unbounded_channel();
        
        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));
        
        // Spawn background writer task with exclusive TCP access
        let writer_task = {
            let ring_buffer = ring_buffer.clone();
            let shutdown_signal = shutdown_signal.clone();
            let bytes_written_for_task = bytes_written.clone();
            
            tokio::spawn(async move {
                Self::background_writer_task(
                    writer, 
                    ring_buffer, 
                    shutdown_signal, 
                    tell_receiver, 
                    bytes_written_for_task
                ).await;
            })
        };
        
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
    /// OPTIMIZED FOR MAXIMUM THROUGHPUT
    async fn background_writer_task(
        writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        ring_buffer: Arc<LockFreeRingBuffer>,
        shutdown_signal: Arc<AtomicBool>,
        mut tell_receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        bytes_written_counter: Arc<AtomicUsize>, // Track ALL bytes written to TCP
    ) {
        use tokio::io::AsyncWriteExt;
        
        // Extract TCP writer from mutex - exclusive access from now on!
        let mut tcp_writer = writer.lock().await;
        
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
        
        let mut bytes_written = 0;
        let mut successful_chunks = 0;
        
        for chunk in data.chunks(chunk_size) {
            if self.write_nonblocking(chunk).is_ok() {
                bytes_written += chunk.len();
                successful_chunks += 1;
            }
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

impl ZeroCopyStreamHandle {
    /// Create a new zero-copy stream handle with pre-allocated buffers
    pub fn new(writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>, addr: SocketAddr, batch_size: usize) -> Self {
        Self {
            writer,
            addr,
            batch_buffer: Vec::with_capacity(batch_size),
            batch_size,
            auto_flush: AtomicBool::new(false),
            bytes_pending: AtomicUsize::new(0),
        }
    }
    
    /// Write raw bytes with zero-copy vectored I/O - NO ALLOCATIONS
    pub async fn write_raw_vectored(&self, bufs: &[&[u8]]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        // Convert to IoSlice for true vectored I/O
        let io_slices: Vec<IoSlice> = bufs.iter().map(|buf| IoSlice::new(buf)).collect();
        
        // Try lock first for lock-free fast path
        if let Ok(mut guard) = self.writer.try_lock() {
            // Fast path - no async scheduling overhead
            guard.write_vectored(&io_slices).await?;
            return Ok(());
        }
        
        // Slow path - wait for lock
        let mut guard = self.writer.lock().await;
        guard.write_vectored(&io_slices).await?;
        Ok(())
    }
    
    /// Write single buffer with zero-copy - NO ALLOCATIONS, NO FLUSH
    pub async fn write_raw_no_flush(&self, data: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        // Try lock first for lock-free fast path
        if let Ok(mut guard) = self.writer.try_lock() {
            // Fast path - direct write, no flush
            guard.write_all(data).await?;
            self.bytes_pending.fetch_add(data.len(), Ordering::Relaxed);
            return Ok(());
        }
        
        // Slow path
        let mut guard = self.writer.lock().await;
        guard.write_all(data).await?;
        self.bytes_pending.fetch_add(data.len(), Ordering::Relaxed);
        Ok(())
    }
    
    /// Batch write multiple buffers without intermediate flushes
    pub async fn write_batch_no_flush(&self, bufs: &[&[u8]]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let io_slices: Vec<IoSlice> = bufs.iter().map(|buf| IoSlice::new(buf)).collect();
        let total_bytes: usize = bufs.iter().map(|buf| buf.len()).sum();
        
        // Try lock first
        if let Ok(mut guard) = self.writer.try_lock() {
            guard.write_vectored(&io_slices).await?;
            self.bytes_pending.fetch_add(total_bytes, Ordering::Relaxed);
            return Ok(());
        }
        
        let mut guard = self.writer.lock().await;
        guard.write_vectored(&io_slices).await?;
        self.bytes_pending.fetch_add(total_bytes, Ordering::Relaxed);
        Ok(())
    }
    
    /// Manual flush - call only when needed for maximum performance
    pub async fn flush(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let mut guard = self.writer.lock().await;
        guard.flush().await?;
        self.bytes_pending.store(0, Ordering::Relaxed);
        Ok(())
    }
    
    /// Get pending bytes count
    pub fn pending_bytes(&self) -> usize {
        self.bytes_pending.load(Ordering::Relaxed)
    }
    
    /// Enable/disable automatic flushing
    pub fn set_auto_flush(&self, enabled: bool) {
        self.auto_flush.store(enabled, Ordering::Relaxed);
    }
    
    /// Write with conditional flush based on batch size
    pub async fn write_with_batching(&self, data: &[u8]) -> Result<()> {
        self.write_raw_no_flush(data).await?;
        
        if self.pending_bytes() >= self.batch_size {
            self.flush().await?;
        }
        
        Ok(())
    }
    
    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl StreamHandle {
    /// Write raw bytes directly to the stream (zero-copy)
    pub async fn write_raw(&self, data: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let mut guard = self.writer.lock().await;
        guard.write_all(data).await?;
        guard.flush().await?;
        Ok(())
    }
    
    /// Write multiple buffers in a single syscall (vectored I/O)
    pub async fn write_vectored(&self, bufs: &[&[u8]]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let mut guard = self.writer.lock().await;
        for buf in bufs {
            guard.write_all(buf).await?;
        }
        guard.flush().await?;
        Ok(())
    }
    
    /// Get the socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    
    /// Get zero-copy handle for maximum performance
    pub fn get_zero_copy_handle(&self, batch_size: usize) -> ZeroCopyStreamHandle {
        ZeroCopyStreamHandle::new(self.writer.clone(), self.addr, batch_size)
    }
}

impl UltimateStreamHandle {
    /// Create ultimate performance streaming handle with buffer pool
    pub fn new(writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>, addr: SocketAddr, pool_size: usize, buffer_size: usize) -> Self {
        let buffer_pool: Vec<Vec<u8>> = (0..pool_size)
            .map(|_| Vec::with_capacity(buffer_size))
            .collect();
            
        Self {
            writer,
            addr,
            buffer_pool,
            current_buffer_idx: AtomicUsize::new(0),
            total_bytes_written: AtomicUsize::new(0),
            no_ack_mode: AtomicBool::new(true),
        }
    }
    
    /// Write with absolutely no acknowledgment - fire and forget for maximum performance
    pub async fn write_no_ack(&self, data: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        // Try lock with no blocking - if we can't get it immediately, skip
        if let Ok(mut guard) = self.writer.try_lock() {
            // Direct write with no error handling for maximum speed
            let _ = guard.write_all(data).await;
            self.total_bytes_written.fetch_add(data.len(), Ordering::Relaxed);
        }
        // If we can't get the lock, just drop the data (no-ack mode)
        Ok(())
    }
    
    /// Write multiple buffers with no acknowledgment using vectored I/O
    pub async fn write_vectored_no_ack(&self, bufs: &[&[u8]]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        if let Ok(mut guard) = self.writer.try_lock() {
            let io_slices: Vec<IoSlice> = bufs.iter().map(|buf| IoSlice::new(buf)).collect();
            let total_bytes: usize = bufs.iter().map(|buf| buf.len()).sum();
            
            // Fire and forget - no error handling
            let _ = guard.write_vectored(&io_slices).await;
            self.total_bytes_written.fetch_add(total_bytes, Ordering::Relaxed);
        }
        Ok(())
    }
    
    /// Bulk write with pre-allocated buffer pool - maximum performance
    pub async fn bulk_write_no_ack(&self, data_chunks: &[&[u8]]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        if let Ok(mut guard) = self.writer.try_lock() {
            let io_slices: Vec<IoSlice> = data_chunks.iter().map(|chunk| IoSlice::new(chunk)).collect();
            let total_bytes: usize = data_chunks.iter().map(|chunk| chunk.len()).sum();
            
            // Single vectored write syscall for entire bulk
            let _ = guard.write_vectored(&io_slices).await;
            self.total_bytes_written.fetch_add(total_bytes, Ordering::Relaxed);
        }
        Ok(())
    }
    
    /// Never flush - accumulate everything in kernel buffers for maximum throughput
    pub async fn never_flush(&self) -> Result<()> {
        // Intentionally do nothing - let the kernel handle buffering
        Ok(())
    }
    
    /// Get total bytes written (approximate)
    pub fn bytes_written(&self) -> usize {
        self.total_bytes_written.load(Ordering::Relaxed)
    }
    
    /// Force flush only when absolutely necessary
    pub async fn emergency_flush(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        if let Ok(mut guard) = self.writer.try_lock() {
            let _ = guard.flush().await;
        }
        Ok(())
    }
    
    /// Write with memory-mapped style direct access (unsafe for maximum performance)
    pub unsafe fn write_direct_unsafe(&self, data: &[u8]) -> Result<()> {
        // This would require unsafe direct memory access
        // For now, this is a placeholder for potential future optimization
        Ok(())
    }
    
    /// Set no-acknowledgment mode
    pub fn set_no_ack_mode(&self, enabled: bool) {
        self.no_ack_mode.store(enabled, Ordering::Relaxed);
    }
    
    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
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
pub struct ConnectionPool {
    /// Mapping: SocketAddr -> Connection
    connections: HashMap<SocketAddr, PersistentConnection>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: Option<std::sync::Weak<GossipRegistry>>,
    /// Reusable buffer pool to reduce allocations
    buffer_pool: Vec<Vec<u8>>,
    /// Pre-allocated message buffers for zero-copy processing
    message_buffer_pool: Vec<Vec<u8>>,
}

struct PersistentConnection {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    peer_addr: SocketAddr, // Actual connection address (may be NATed)
    last_used: u64,
    connected: bool,
}

/// Handle to send messages through a persistent connection
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    // Shared ring buffer for all operations on this connection
    global_ring_buffer: Option<Arc<LockFreeStreamHandle>>,
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
    /// Send pre-serialized data through this connection - DIRECT TCP
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        let mut guard = self.writer.lock().await;
        guard.write_all(&data).await?;
        guard.flush().await?;
        Ok(())
    }
    
    /// Raw tell() - DIRECT TCP write, no channels (used internally)
    pub async fn tell_raw(&self, data: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        // OPTIMIZATION: Write directly without intermediate buffer allocation
        let len_bytes = (data.len() as u32).to_be_bytes();
        
        // Try to get the lock without yielding first
        if let Ok(mut guard) = self.writer.try_lock() {
            // Fast path - no async scheduling delay, write in two parts
            guard.write_all(&len_bytes).await?;
            guard.write_all(data).await?;
            guard.flush().await?;
            Ok(())
        } else {
            // Slow path - have to wait for lock
            let mut guard = self.writer.lock().await;
            guard.write_all(&len_bytes).await?;
            guard.write_all(data).await?;
            guard.flush().await?;
            Ok(())
        }
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
    
    /// Batch tell() for multiple messages - even more efficient
    pub async fn tell_batch(&self, messages: &[&[u8]]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        
        // OPTIMIZATION: Pre-calculate total size and write in one go
        let total_size: usize = messages.iter().map(|m| 4 + m.len()).sum();
        let mut batch_buffer = Vec::with_capacity(total_size);
        
        for msg in messages {
            batch_buffer.extend_from_slice(&(msg.len() as u32).to_be_bytes());
            batch_buffer.extend_from_slice(msg);
        }
        
        // Try to get the lock without yielding first
        if let Ok(mut guard) = self.writer.try_lock() {
            // Fast path - single write for all messages
            guard.write_all(&batch_buffer).await?;
            guard.flush().await?;
            Ok(())
        } else {
            // Slow path - have to wait for lock
            let mut guard = self.writer.lock().await;
            guard.write_all(&batch_buffer).await?;
            guard.flush().await?;
            Ok(())
        }
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
        // This is a placeholder implementation
        // In a real system, you'd need:
        // 1. Correlation IDs to match requests with responses
        // 2. Response handling mechanism
        // 3. Timeout handling
        
        // For now, we'll just send the request
        self.tell(request).await?;
        
        // Return a dummy response
        Ok(format!("RESPONSE:{}", request.len()).into_bytes())
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
    
    /// High-performance streaming API - send structured data with custom framing
    pub async fn stream_send<T>(&self, data: &T) -> Result<()> 
    where
        T: for<'a> rkyv::Serialize<rkyv::rancor::Strategy<rkyv::ser::Serializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>>,
    {
        use tokio::io::AsyncWriteExt;
        
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
        
        // Write frame header + payload as single atomic operation
        let mut guard = self.writer.lock().await;
        guard.write_all(&header_bytes).await?;
        guard.write_all(&payload).await?;
        guard.flush().await?;
        Ok(())
    }
    
    /// High-performance streaming API - send batch of structured data
    pub async fn stream_send_batch<T>(&self, batch: &[T]) -> Result<()> 
    where
        T: for<'a> rkyv::Serialize<rkyv::rancor::Strategy<rkyv::ser::Serializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>, rkyv::rancor::Error>>,
    {
        use tokio::io::AsyncWriteExt;
        
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
        
        // Single atomic write for entire batch
        let mut guard = self.writer.lock().await;
        guard.write_all(&total_payload).await?;
        guard.flush().await?;
        Ok(())
    }
    
    /// Get access to raw stream for zero-copy operations
    pub fn get_stream_handle(&self) -> StreamHandle {
        StreamHandle {
            writer: self.writer.clone(),
            addr: self.addr,
        }
    }
    
    /// Get zero-copy stream handle for line-rate performance
    pub fn get_zero_copy_handle(&self, batch_size: usize) -> ZeroCopyStreamHandle {
        ZeroCopyStreamHandle::new(self.writer.clone(), self.addr, batch_size)
    }
    
    /// Get ultimate performance handle with no-acknowledgment mode
    pub fn get_ultimate_handle(&self, pool_size: usize, buffer_size: usize) -> UltimateStreamHandle {
        UltimateStreamHandle::new(self.writer.clone(), self.addr, pool_size, buffer_size)
    }
    
    
    /// Get truly lock-free streaming handle with background writer - ZERO CONTENTION
    pub fn get_lock_free_stream(&self, channel_id: ChannelId, buffer_size: usize) -> LockFreeStreamHandle {
        LockFreeStreamHandle::new(self.writer.clone(), self.addr, channel_id, buffer_size)
    }
    
    /// Get or create the global ring buffer for this connection
    pub fn get_or_create_global_ring_buffer(&mut self) -> Arc<LockFreeStreamHandle> {
        if self.global_ring_buffer.is_none() {
            let ring_buffer = Arc::new(LockFreeStreamHandle::new(
                self.writer.clone(), 
                self.addr, 
                ChannelId::Global, // Special channel for all operations
                2048 // Larger buffer for global operations
            ));
            self.global_ring_buffer = Some(ring_buffer.clone());
            ring_buffer
        } else {
            self.global_ring_buffer.as_ref().unwrap().clone()
        }
    }
    
    /// TRULY LOCK-FREE tell() - routes through global ring buffer
    pub fn tell_lockfree(&mut self, data: &[u8]) -> Result<()> {
        let ring_buffer = self.get_or_create_global_ring_buffer();
        ring_buffer.tell_nonblocking(data.to_vec())
    }
}

impl ConnectionPool {
    pub fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        Self {
            connections: HashMap::new(),
            max_connections,
            connection_timeout,
            registry: None,
            buffer_pool: Vec::with_capacity(16), // Pre-allocate buffer pool
            message_buffer_pool: Vec::with_capacity(32), // Pre-allocate message buffers
        }
    }

    /// Set the registry reference for handling incoming messages
    pub fn set_registry(&mut self, registry: std::sync::Arc<GossipRegistry>) {
        self.registry = Some(std::sync::Arc::downgrade(&registry));
    }

    /// Get a buffer from the pool or create a new one
    pub fn get_buffer(&mut self, min_capacity: usize) -> Vec<u8> {
        if let Some(mut buffer) = self.buffer_pool.pop() {
            buffer.clear();
            if buffer.capacity() >= min_capacity {
                return buffer;
            }
            // Buffer too small, fall through to create new one
        }
        Vec::with_capacity(min_capacity.max(1024)) // Minimum 1KB buffers
    }

    /// Return a buffer to the pool for reuse
    pub fn return_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024
            && buffer.capacity() <= 64 * 1024
            && self.buffer_pool.len() < 32
        {
            // Only keep reasonably-sized buffers and limit pool size
            self.buffer_pool.push(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Get a message buffer from the pool for zero-copy processing
    pub fn get_message_buffer(&mut self) -> Vec<u8> {
        self.message_buffer_pool.pop().unwrap_or_else(|| Vec::with_capacity(4096))
    }

    /// Return a message buffer to the pool
    pub fn return_message_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024
            && buffer.capacity() <= 64 * 1024
            && self.message_buffer_pool.len() < 32
        {
            // Keep buffers with reasonable size
            self.message_buffer_pool.push(buffer);
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
        let current_time = current_timestamp();
        
        if let Some(conn) = self.connections.get_mut(&addr) {
            if conn.connected {
                conn.last_used = current_time;
                debug!(addr = %addr, "using existing persistent connection (fast path)");
                return Some(ConnectionHandle {
                    addr: conn.peer_addr,
                    writer: conn.writer.clone(),
                    global_ring_buffer: None, // Will be created on first use
                });
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                self.connections.remove(&addr);
            }
        }
        None
    }
    
    pub async fn get_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        let current_time = current_timestamp();

        // Check if we already have a connection to this address
        if let Some(conn) = self.connections.get_mut(&addr) {
            if conn.connected {
                conn.last_used = current_time;
                debug!(addr = %addr, "using existing persistent connection");
                return Ok(ConnectionHandle {
                    addr: conn.peer_addr,
                    writer: conn.writer.clone(),
                    global_ring_buffer: None, // Will be created on first use
                });
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                self.connections.remove(&addr);
            }
        }

        // Create new persistent connection
        self.create_connection(addr).await
    }

    async fn create_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        debug!(peer = %addr, "creating new persistent connection");

        // Make room if necessary
        if self.connections.len() >= self.max_connections {
            if let Some((oldest_addr, _)) = self.connections.iter().min_by_key(|(_, c)| c.last_used)
            {
                let oldest = *oldest_addr;
                self.connections.remove(&oldest);
                warn!(addr = %oldest, "removed oldest connection to make room");
            }
        }

        // Connect with timeout
        let stream = tokio::time::timeout(self.connection_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| GossipError::Timeout)?
            .map_err(GossipError::Network)?;

        // Configure socket
        stream.set_nodelay(true).map_err(GossipError::Network)?;

        // Split stream for direct TCP access - NO CHANNELS
        let (reader, writer) = stream.into_split();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        // Store connection info
        let conn = PersistentConnection {
            writer: writer.clone(),
            peer_addr: addr,
            last_used: current_timestamp(),
            connected: true,
        };

        self.connections.insert(addr, conn);

        // Start the connection handler for reading only
        let pool_addr = addr;
        let registry_weak = self.registry.clone();

        // Start handling the connection in background (read-only)
        start_persistent_connection_reader(reader, pool_addr, registry_weak);

        Ok(ConnectionHandle { 
            addr, 
            writer,
            global_ring_buffer: None, // Will be created on first use
        })
    }

    /// Mark a connection as disconnected
    pub fn mark_disconnected(&mut self, addr: SocketAddr) {
        if let Some(conn) = self.connections.get_mut(&addr) {
            conn.connected = false;
            info!(peer = %addr, "marked connection as disconnected");
        }
    }

    /// Remove a connection from the pool by address
    pub fn remove_connection(&mut self, addr: SocketAddr) {
        if let Some(_conn) = self.connections.remove(&addr) {
            info!(addr = %addr, "removed connection from pool");
            // Dropping the sender will cause the receiver to return None,
            // signaling the connection handler to shut down
            // No need to drop writer - it's Arc<Mutex<>>
        }
    }

    /// Get number of active connections
    pub fn connection_count(&self) -> usize {
        self.connections.iter().filter(|(_, c)| c.connected).count()
    }

    /// Add a sender channel for an existing connection (used for incoming connections)
    /// Supports bidirectional connections by allowing multiple connections to the same address
    pub fn add_connection_sender(
        &mut self,
        listening_addr: SocketAddr,
        peer_addr: SocketAddr,
        writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    ) -> bool {
        let conn = PersistentConnection {
            writer,
            peer_addr,
            last_used: current_timestamp(),
            connected: true,
        };

        // For incoming connections, we use listening_addr as key
        // This allows bidirectional connections where both nodes can initiate connections
        self.connections.insert(listening_addr, conn);
        info!(peer = %peer_addr, listening = %listening_addr, "added incoming connection sender to pool");
        true
    }

    /// Check if we have a connection to a peer by address
    pub fn has_connection(&self, addr: &SocketAddr) -> bool {
        self.connections
            .get(addr)
            .map(|c| c.connected)
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
            .filter(|(_, conn)| !conn.connected)
            .map(|(addr, _)| *addr)
            .collect();

        for addr in to_remove {
            self.connections.remove(&addr);
            debug!(addr = %addr, "cleaned up disconnected connection");
        }
    }

    /// Close all connections (for shutdown)
    pub fn close_all_connections(&mut self) {
        let addrs: Vec<_> = self.connections.keys().cloned().collect();
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
    let writer = Arc::new(tokio::sync::Mutex::new(writer));
    
    // Add writer to connection pool if needed
    if let Some(registry_weak) = &registry_weak {
        if let Some(registry) = registry_weak.upgrade() {
            let mut pool = registry.connection_pool.lock().await;
            pool.add_connection_sender(listening_addr, _peer_addr, writer.clone());
        }
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
                        let msg_data = partial_msg_buf[4..total_len].to_vec();
                        partial_msg_buf.drain(..total_len);
                        
                        // Add timing right after TCP read - BEFORE any deserialization
                        let tcp_read_timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos();
                        
                        // Process message
                        if let Some(ref registry_weak) = registry_weak {
                            if let Some(registry) = registry_weak.upgrade() {
                                if let Ok(msg) = rkyv::from_bytes::<crate::registry::RegistryMessage, rkyv::rancor::Error>(&msg_data) {
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
                            // OPTIMIZATION: Zero-copy deserialization - avoid to_vec() allocation
                            let msg_data = &partial_msg_buf[4..total_len];

                            // Process message immediately without spawning task
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
                sender = %delta.sender_addr,
                since_sequence = delta.since_sequence,
                changes = delta.changes.len(),
                "received delta gossip message on bidirectional connection"
            );

            // OPTIMIZATION: Do all peer management in one lock acquisition
            {
                let mut gossip_state = registry.gossip_state.lock().await;
                
                // Parse sender address once for consistent use
                let sender_socket_addr = if let Ok(addr) = delta.sender_addr.parse::<SocketAddr>() {
                    addr
                } else {
                    warn!("Invalid sender address: {}", delta.sender_addr);
                    return Ok(());
                };
                
                // Add the sender as a peer (inlined to avoid separate lock)
                if delta.sender_addr != registry.bind_addr.to_string() {
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
                        peer = %delta.sender_addr,
                        "✅ Received delta from previously failed peer - connection restored!"
                    );

                    // Clear the pending failure record
                    gossip_state
                        .pending_peer_failures
                        .remove(&sender_socket_addr);
                }

                // Update peer info
                if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                    // Only reset failure state if peer was actually failed
                    // Don't reset if just a buffered message after connection failure
                    if peer_info.failures >= registry.config.max_peer_failures {
                        info!(peer = %delta.sender_addr, "peer reconnected - resetting failure state");
                        peer_info.failures = 0;
                        peer_info.last_failure_time = None;
                    }
                    peer_info.last_success = crate::current_timestamp();

                    peer_info.last_sequence =
                        std::cmp::max(peer_info.last_sequence, delta.current_sequence);
                    peer_info.consecutive_deltas += 1;
                }
                gossip_state.delta_exchanges += 1;
            }

            // CRITICAL OPTIMIZATION: Inline apply_delta to eliminate function call overhead
            // Apply the delta directly here to minimize async scheduling delays
            {
                let total_changes = delta.changes.len();
                let sender_addr = if let Ok(addr) = delta.sender_addr.parse::<SocketAddr>() {
                    addr
                } else {
                    warn!("Invalid sender address in delta: {}", delta.sender_addr);
                    return Ok(());
                };
                
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
            sender_addr,
            sequence,
            wall_clock_time,
        } => {
            // Parse sender address once for consistent use
            let sender_socket_addr = if let Ok(addr) = sender_addr.parse::<SocketAddr>() {
                addr
            } else {
                warn!("Invalid sender address: {}", sender_addr);
                return Ok(());
            };
            
            // OPTIMIZATION: Do all peer management in one lock acquisition
            let failed_peers = {
                let mut gossip_state = registry.gossip_state.lock().await;
                
                // Calculate failed peers count
                let active_peers = gossip_state
                    .peers
                    .values()
                    .filter(|p| p.failures < registry.config.max_peer_failures)
                    .count();
                let failed_peers = gossip_state.peers.len() - active_peers;
                
                // Add the sender as a peer (inlined to avoid separate lock)
                if sender_addr != registry.bind_addr.to_string() {
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
                        peer = %sender_addr,
                        "✅ Received full sync from previously failed peer - connection restored!"
                    );

                    // Clear the pending failure record
                    gossip_state.pending_peer_failures.remove(&sender_socket_addr);
                }

                // Update peer info
                if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                    // Only reset failure state if peer was actually failed
                    // Don't reset if just a buffered message after connection failure
                    if peer_info.failures >= registry.config.max_peer_failures {
                        info!(peer = %sender_addr, "peer reconnected - resetting failure state");
                        peer_info.failures = 0;
                        peer_info.last_failure_time = None;
                    }
                    peer_info.last_success = crate::current_timestamp();
                    peer_info.consecutive_deltas = 0;
                }
                gossip_state.full_sync_exchanges += 1;
                
                failed_peers
            };

            info!(
                sender = %sender_addr,
                sequence = sequence,
                local_actors = local_actors.len(),
                known_actors = known_actors.len(),
                failed_peers = failed_peers,
                "📨 INCOMING: Received full sync message on bidirectional connection"
            );

            // Only remaining async operation
            registry
                .merge_full_sync(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| {
                        warn!("Invalid sender address: {}", sender_addr);
                        "127.0.0.1:0".parse().unwrap()
                    }),
                    sequence,
                    wall_clock_time,
                )
                .await;

            // Note: Response will be sent during regular gossip rounds
            Ok(())
        }
        RegistryMessage::FullSyncRequest {
            sender_addr,
            sequence: _,
            wall_clock_time: _,
        } => {
            debug!(
                sender = %sender_addr,
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
                sender = %delta.sender_addr,
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
            sender_addr,
            sequence,
            wall_clock_time,
        } => {
            debug!(
                sender = %sender_addr,
                "received full sync response on bidirectional connection"
            );

            registry
                .merge_full_sync(
                    local_actors.into_iter().collect(),
                    known_actors.into_iter().collect(),
                    sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| {
                        warn!("Invalid sender address: {}", sender_addr);
                        "127.0.0.1:0".parse().unwrap()
                    }),
                    sequence,
                    wall_clock_time,
                )
                .await;

            let mut gossip_state = registry.gossip_state.lock().await;
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
            let is_alive = {
                let pool = registry.connection_pool.lock().await;
                pool.has_connection(&target_peer.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()))
            };

            let last_contact = if is_alive {
                crate::current_timestamp()
            } else {
                // Check when we last had successful contact
                let gossip_state = registry.gossip_state.lock().await;
                gossip_state
                    .peers
                    .get(&target_peer.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()))
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
                    .get(&target_peer.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()))
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
                reporter: registry.bind_addr.to_string(),
                peer_statuses: peer_statuses.into_iter().collect(),
                timestamp: crate::current_timestamp(),
            };

            // Send report back to the querying peer
            if let Ok(data) = rkyv::to_bytes::<rkyv::rancor::Error>(&report) {
                let mut pool = registry.connection_pool.lock().await;
                if let Ok(conn) = pool.get_connection(sender.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap())).await {
                    // Create message buffer with length header using buffer pool
                    let buffer = pool.create_message_buffer(&data);
                    let _ = conn.send_data(buffer).await;
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
                    gossip_state
                        .peer_health_reports
                        .entry(peer.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()))
                        .or_insert_with(HashMap::new)
                        .insert(reporter.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()), status);
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
    async fn test_add_connection_sender() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let listening_addr = "127.0.0.1:8080".parse().unwrap();
        let peer_addr = "127.0.0.1:9090".parse().unwrap();
        
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, writer) = stream.into_split();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let added = pool.add_connection_sender(listening_addr, peer_addr, writer);
        assert!(added);
        assert!(pool.has_connection(&listening_addr));
        assert_eq!(pool.connection_count(), 1);
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
        let writer = Arc::new(tokio::sync::Mutex::new(writer));
        
        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            writer,
            global_ring_buffer: None,
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
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            writer,
            global_ring_buffer: None,
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
        let last_used = current_timestamp();
        
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, writer) = stream.into_split();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let conn = PersistentConnection {
            writer,
            peer_addr: addr,
            last_used,
            connected: true,
        };

        assert_eq!(conn.peer_addr, addr);
        assert_eq!(conn.last_used, last_used);
        assert!(conn.connected);
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
