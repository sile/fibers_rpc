//! RPC channel related components.
use crate::packet::MAX_PACKET_LEN;
use std::time::Duration;

/// Options for a RPC channel.
#[derive(Debug, Clone)]
pub struct ChannelOptions {
    /// The byte size of the application level read buffer.
    pub read_buffer_size: usize,

    /// The byte size of the application level write buffer.
    pub write_buffer_size: usize,

    /// The maximum length of the transmit queue.
    ///
    /// If the queue exceeds this value, the RPC channel (i.e., TCP connection) will be disconnected.
    pub max_transmit_queue_len: usize,

    /// Maximum number of iterations in a `Future::poll()` call.
    ///
    /// If it exceeds this value, it will break the loop by calling `fibers::fiber::yield_poll()`.
    pub yield_threshold: usize,

    /// TCP connect timeout duration.
    pub tcp_connect_timeout: Duration,

    /// Timeout duration of a write operation.
    pub tcp_write_timeout: Duration,
}
impl ChannelOptions {
    /// The default value of `read_buffer_size` field.
    pub const DEFAULT_READ_BUFFER_SIZE: usize = MAX_PACKET_LEN * 2;

    /// The default value of `write_buffer_size` field.
    pub const DEFAULT_WRITE_BUFFER_SIZE: usize = MAX_PACKET_LEN * 2;

    /// The default value of `max_transmit_queue_len` field.
    pub const DEFAULT_MAX_TRANSMIT_QUEUE_LEN: usize = 10_000;

    /// The default value of `yield_threshold` field.
    pub const DEFAULT_YIELD_THRESHOLD: usize = 128;

    /// The default duration of `tcp_connect_timeout` field.
    pub const DEFAULT_TCP_CONNECT_TIMEOUT_SECONDS: u64 = 5;

    /// The default duration of `tcp_write_timeout` field.
    pub const DEFAULT_TCP_WRITE_TIMEOUT_SECONDS: u64 = 5;
}
impl Default for ChannelOptions {
    fn default() -> Self {
        ChannelOptions {
            read_buffer_size: Self::DEFAULT_READ_BUFFER_SIZE,
            write_buffer_size: Self::DEFAULT_WRITE_BUFFER_SIZE,
            max_transmit_queue_len: Self::DEFAULT_MAX_TRANSMIT_QUEUE_LEN,
            yield_threshold: Self::DEFAULT_YIELD_THRESHOLD,
            tcp_connect_timeout: Duration::from_secs(Self::DEFAULT_TCP_CONNECT_TIMEOUT_SECONDS),
            tcp_write_timeout: Duration::from_secs(Self::DEFAULT_TCP_WRITE_TIMEOUT_SECONDS),
        }
    }
}
