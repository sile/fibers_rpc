use std::fmt;
use std::net::SocketAddr;
use std::sync::mpsc::RecvError;
use std::time::Duration;
use fibers;
use fibers::net::TcpStream;
use fibers::net::futures::Connect;
use fibers::time::timer::{self, Timeout};
use futures::{Async, Future, Poll, Stream};
use slog::Logger;
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use client_side_handlers::{BoxResponseHandler, IncomingFrameHandler};
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::{MessageSeqNo, OutgoingMessage};
use message_stream::{MessageStream, MessageStreamEvent};
use metrics::{ChannelMetrics, ClientMetrics};

pub const DEFAULT_KEEP_ALIVE_TIMEOUT_SECS: u64 = 60 * 10;

#[derive(Debug)]
pub struct ClientSideChannel {
    logger: Logger,
    server: SocketAddr,
    keep_alive: KeepAlive,
    next_seqno: MessageSeqNo,
    message_stream: MessageStreamState,
    exponential_backoff: ExponentialBackoff,
    metrics: ClientMetrics,
}
impl ClientSideChannel {
    pub fn new(logger: Logger, server: SocketAddr, metrics: ClientMetrics) -> Self {
        ClientSideChannel {
            logger,
            server,
            keep_alive: KeepAlive::new(Duration::from_secs(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS)),
            next_seqno: MessageSeqNo::new_client_side_seqno(),
            message_stream: MessageStreamState::new(server),
            exponential_backoff: ExponentialBackoff::new(),
            metrics,
        }
    }

    pub fn set_keep_alive_timeout(&mut self, duration: Duration) {
        self.keep_alive = KeepAlive::new(duration);
    }

    pub fn send_message(
        &mut self,
        message: OutgoingMessage,
        response_handler: Option<BoxResponseHandler>,
    ) {
        let seqno = self.next_seqno.next();
        self.message_stream
            .send_message(seqno, message, response_handler);
    }

    pub fn force_wakeup(&mut self) {
        if let MessageStreamState::Wait { .. } = self.message_stream {
            info!(self.logger, "Waked up");
            self.exponential_backoff.next();
            let next = MessageStreamState::Connecting {
                buffer: Vec::new(),
                future: TcpStream::connect(self.server),
            };
            self.message_stream = next;
        }
    }

    fn wait_or_reconnect(
        server: SocketAddr,
        backoff: &mut ExponentialBackoff,
        metrics: &ClientMetrics,
    ) -> MessageStreamState {
        metrics.channels().remove_channel_metrics(server);
        if let Some(timeout) = backoff.timeout() {
            MessageStreamState::Wait { timeout }
        } else {
            backoff.next();
            MessageStreamState::Connecting {
                buffer: Vec::new(),
                future: TcpStream::connect(server),
            }
        }
    }

    fn poll_message_stream(&mut self) -> Result<Async<Option<MessageStreamState>>> {
        match self.message_stream {
            MessageStreamState::Wait { ref mut timeout } => {
                let is_expired = track!(timeout.poll().map_err(from_timeout_error))?.is_ready();
                if is_expired {
                    info!(
                        self.logger,
                        "Reconnecting timeout expired; starts reconnecting"
                    );
                    self.exponential_backoff.next();
                    let next = MessageStreamState::Connecting {
                        buffer: Vec::new(),
                        future: TcpStream::connect(self.server),
                    };
                    Ok(Async::Ready(Some(next)))
                } else {
                    Ok(Async::NotReady)
                }
            }
            MessageStreamState::Connecting {
                ref mut future,
                ref mut buffer,
            } => match track!(future.poll().map_err(Error::from)) {
                Err(e) => {
                    error!(self.logger, "Failed to TCP connect: {}", e);
                    self.metrics
                        .discarded_outgoing_messages
                        .add_u64(buffer.len() as u64);
                    let next = Self::wait_or_reconnect(
                        self.server,
                        &mut self.exponential_backoff,
                        &self.metrics,
                    );
                    Ok(Async::Ready(Some(next)))
                }
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Ok(Async::Ready(stream)) => {
                    info!(
                        self.logger,
                        "TCP connected: stream={:?}, buffered_messages={}",
                        stream,
                        buffer.len()
                    );
                    let stream = MessageStream::new(
                        FrameStream::new(stream),
                        IncomingFrameHandler::new(),
                        self.metrics.channels().create_channel_metrics(self.server),
                    );
                    let mut connected = MessageStreamState::Connected { stream };
                    for m in buffer.drain(..) {
                        connected.send_message(m.seqno, m.message, m.handler);
                    }
                    Ok(Async::Ready(Some(connected)))
                }
            },
            MessageStreamState::Connected { ref mut stream } => match track!(stream.poll()) {
                Err(e) => {
                    error!(self.logger, "Message stream aborted: {}", e);
                    let next = Self::wait_or_reconnect(
                        self.server,
                        &mut self.exponential_backoff,
                        &self.metrics,
                    );
                    Ok(Async::Ready(Some(next)))
                }
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    warn!(self.logger, "Message stream terminated");
                    let next = Self::wait_or_reconnect(
                        self.server,
                        &mut self.exponential_backoff,
                        &self.metrics,
                    );
                    Ok(Async::Ready(Some(next)))
                }
                Ok(Async::Ready(Some(event))) => {
                    if event.is_ok() {
                        self.exponential_backoff.reset();
                        self.keep_alive.extend_period();
                    }
                    match event {
                        MessageStreamEvent::Sent {
                            seqno,
                            result: Err(e),
                        } => {
                            error!(self.logger, "Cannot send message({:?}): {}", seqno, e);
                            stream.incoming_frame_handler_mut().handle_error(seqno, e);
                        }
                        MessageStreamEvent::Received {
                            seqno,
                            result: Err(e),
                        } => {
                            error!(self.logger, "Cannot receive message({:?}): {}", seqno, e);
                            stream.incoming_frame_handler_mut().handle_error(seqno, e);
                        }
                        _ => {}
                    }
                    Ok(Async::Ready(None))
                }
            },
        }
    }
}
impl Future for ClientSideChannel {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let is_timeout = track!(self.keep_alive.poll())?.is_ready();
        if is_timeout {
            return Ok(Async::Ready(()));
        }

        let mut count = 0;
        while let Async::Ready(next) = track!(self.poll_message_stream())? {
            if let Some(next) = next {
                self.message_stream = next;
            }

            // FIXME: parameterize
            count += 1;
            if count > 64 {
                self.message_stream
                    .metrics()
                    .map(|m| m.fiber_yielded.increment());
                return fibers::fiber::yield_poll();
            }
        }
        Ok(Async::NotReady)
    }
}
impl Drop for ClientSideChannel {
    fn drop(&mut self) {
        self.metrics.channels().remove_channel_metrics(self.server);
    }
}

#[cfg_attr(feature = "cargo-clippy", allow(large_enum_variant))]
#[derive(Debug)]
enum MessageStreamState {
    Wait {
        timeout: Timeout,
    },
    Connecting {
        buffer: Vec<BufferedMessage>,
        future: Connect,
    },
    Connected {
        stream: MessageStream<IncomingFrameHandler>,
    },
}
impl MessageStreamState {
    fn new(addr: SocketAddr) -> Self {
        MessageStreamState::Connecting {
            buffer: Vec::new(),
            future: TcpStream::connect(addr),
        }
    }

    fn metrics(&self) -> Option<&ChannelMetrics> {
        match *self {
            MessageStreamState::Wait { .. } | MessageStreamState::Connecting { .. } => None,
            MessageStreamState::Connected { ref stream } => Some(stream.metrics()),
        }
    }

    fn send_message(
        &mut self,
        seqno: MessageSeqNo,
        message: OutgoingMessage,
        handler: Option<BoxResponseHandler>,
    ) {
        match *self {
            MessageStreamState::Wait { .. } => {
                if let Some(mut handler) = handler {
                    let e = ErrorKind::Unavailable
                        .cause("TCP stream disconnected (waiting for reconnecting)");
                    handler.handle_error(seqno, track!(e).into());
                }
            }
            MessageStreamState::Connecting { ref mut buffer, .. } => {
                buffer.push(BufferedMessage {
                    seqno,
                    message,
                    handler,
                });
            }
            MessageStreamState::Connected { ref mut stream } => {
                stream.send_message(seqno, message);
                if let Some(handler) = handler {
                    stream
                        .incoming_frame_handler_mut()
                        .register_response_handler(seqno, handler);
                }
            }
        }
    }
}

struct BufferedMessage {
    seqno: MessageSeqNo,
    message: OutgoingMessage,
    handler: Option<BoxResponseHandler>,
}
impl fmt::Debug for BufferedMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BufferedMessage {{ seqno: {:?}, .. }}", self.seqno)
    }
}

#[derive(Debug)]
struct KeepAlive {
    future: Timeout,
    timeout: Duration,
    extend_period: bool,
}
impl KeepAlive {
    fn new(timeout: Duration) -> Self {
        KeepAlive {
            future: timer::timeout(timeout),
            timeout,
            extend_period: false,
        }
    }

    fn extend_period(&mut self) {
        self.extend_period = true;
    }

    fn poll_timeout(&mut self) -> Result<bool> {
        let result = track!(self.future.poll().map_err(from_timeout_error))?;
        Ok(result.is_ready())
    }
}
impl Future for KeepAlive {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while track!(self.poll_timeout())? {
            if self.extend_period {
                self.future = timer::timeout(self.timeout);
                self.extend_period = false;
            } else {
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct ExponentialBackoff {
    retried_count: usize,
}
impl ExponentialBackoff {
    fn new() -> Self {
        ExponentialBackoff { retried_count: 0 }
    }
    fn next(&mut self) {
        self.retried_count += 1;
    }
    fn timeout(&self) -> Option<Timeout> {
        if self.retried_count == 0 {
            None
        } else {
            let duration = Duration::from_secs(2u64.pow(self.retried_count as u32 - 1));
            Some(timer::timeout(duration))
        }
    }
    fn reset(&mut self) {
        self.retried_count = 0;
    }
}

fn from_timeout_error(_: RecvError) -> Error {
    ErrorKind::Other.cause("Broken timer").into()
}
