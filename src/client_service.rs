use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use slog::{Discard, Logger};
use atomic_immut::AtomicImmut;
use fibers::{BoxSpawn, Spawn};
use fibers::sync::mpsc;
use futures::{Async, Future, Poll, Stream};
use prometrics::metrics::MetricBuilder;

use Error;
use client_side_channel::{ClientSideChannel, DEFAULT_KEEP_ALIVE_TIMEOUT_SECS};
use client_side_handlers::BoxResponseHandler;
use message::OutgoingMessage;
use metrics::ClientMetrics;

/// `ClientService` builder.
#[derive(Debug)]
pub struct ClientServiceBuilder {
    logger: Logger,
    keep_alive_timeout: Duration,
    metrics: MetricBuilder,
}
impl ClientServiceBuilder {
    /// Makes a new `ClientServiceBuilder` instance.
    pub fn new() -> Self {
        ClientServiceBuilder {
            logger: Logger::root(Discard, o!()),
            keep_alive_timeout: Duration::from_secs(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS),
            metrics: MetricBuilder::new(),
        }
    }

    /// Sets the logger of the service.
    ///
    /// The default value is `Logger::root(Discard, o!())`.
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
        self
    }

    /// Sets the keep-alive timeout for each RPC connection of the service.
    ///
    /// If a connection do not send or receive any messages duration the timeout period,
    /// it will be disconnected.
    ///
    /// The default value is `Duration::from_secs(60 * 10)`.
    pub fn keep_alive_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.keep_alive_timeout = timeout;
        self
    }

    /// Sets `MetricBuilder` used by the service.
    ///
    /// The default value is `MetricBuilder::new()`.
    pub fn metrics(&mut self, builder: MetricBuilder) -> &mut Self {
        self.metrics = builder;
        self
    }

    /// Builds a new `ClientService` instance.
    pub fn finish<S>(&self, spawner: S) -> ClientService
    where
        S: Spawn + Send + 'static,
    {
        let (command_tx, command_rx) = mpsc::channel();
        let channels = Arc::new(AtomicImmut::default());
        let metrics = ClientMetrics::new(self.metrics.clone());
        ClientService {
            logger: self.logger.clone(),
            spawner: spawner.boxed(),
            command_rx,
            command_tx,
            channels: channels.clone(),
            keep_alive_timeout: self.keep_alive_timeout,
            metrics,
        }
    }
}
impl Default for ClientServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Client side RPC service.
///
/// This managements TCP connections between clients and servers.
#[derive(Debug)]
pub struct ClientService {
    logger: Logger,
    spawner: BoxSpawn,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    channels: Arc<AtomicImmut<HashMap<SocketAddr, ChannelHandle>>>,
    keep_alive_timeout: Duration,
    metrics: ClientMetrics,
}
impl ClientService {
    /// Returns a handle of the service.
    pub fn handle(&self) -> ClientServiceHandle {
        ClientServiceHandle {
            command_tx: self.command_tx.clone(),
            channels: Arc::clone(&self.channels),
            metrics: Arc::new(self.metrics.clone()),
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::CreateChannel { server, message } => {
                if !self.channels.load().contains_key(&server) {
                    self.channels.update(|channels| {
                        let logger = self.logger.new(o!("peer" => server.to_string()));

                        info!(logger, "New client-side RPC channel is created");
                        let command_tx = self.command_tx.clone();
                        let mut channels = channels.clone();
                        let (mut channel, handle) =
                            Channel::new(logger.clone(), server, self.metrics.clone());
                        channel
                            .inner
                            .set_keep_alive_timeout(self.keep_alive_timeout);

                        self.spawner.spawn(channel.then(move |result| {
                            if let Err(e) = result {
                                error!(logger, "A client-side RPC channel aborted: {}", e);
                            } else {
                                info!(logger, "A client-side RPC channel was closed");
                            }
                            let command = Command::RemoveChannel { server };
                            let _ = command_tx.send(command);
                            Ok(())
                        }));;
                        channels.insert(server, handle);
                        channels
                    });
                }
                if let Some(message) = message {
                    self.channels.load()[&server].send_message(message);
                }
            }
            Command::RemoveChannel { server } => {
                self.channels.update(|channels| {
                    info!(self.logger, "A client-side RPC channel was deleted"; "peer" => server.to_string());
                    let mut channels = channels.clone();
                    channels.remove(&server);
                    channels
                });
            }
        }
    }
}
impl Future for ClientService {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(command) = self.command_rx.poll().expect("Never fails") {
            let command = command.expect("Infinite stream");
            self.handle_command(command);
        }
        Ok(Async::NotReady)
    }
}

/// Handle of `ClientService`.
#[derive(Debug, Clone)]
pub struct ClientServiceHandle {
    command_tx: mpsc::Sender<Command>,
    channels: Arc<AtomicImmut<HashMap<SocketAddr, ChannelHandle>>>,
    pub(crate) metrics: Arc<ClientMetrics>,
}
impl ClientServiceHandle {
    /// Returns the metrics of the client service.
    pub fn metrics(&self) -> &ClientMetrics {
        &self.metrics
    }

    pub(crate) fn send_message(&self, server: SocketAddr, message: Message) {
        if let Some(channel) = self.channels.load().get(&server) {
            channel.send_message(message);
        } else {
            let command = Command::CreateChannel {
                server,
                message: Some(message),
            };
            if self.command_tx.send(command).is_err() {
                self.metrics.discarded_outgoing_messages.increment();
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    CreateChannel {
        server: SocketAddr,
        message: Option<Message>,
    },
    RemoveChannel {
        server: SocketAddr,
    },
}

#[derive(Debug)]
struct Channel {
    inner: ClientSideChannel,
    message_rx: mpsc::Receiver<Message>,
}
impl Channel {
    fn new(logger: Logger, server: SocketAddr, metrics: ClientMetrics) -> (Self, ChannelHandle) {
        let (message_tx, message_rx) = mpsc::channel();
        let inner = ClientSideChannel::new(logger, server, metrics);
        let channel = Channel { inner, message_rx };
        let handle = ChannelHandle { message_tx };
        (channel, handle)
    }
}
impl Future for Channel {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(message) = self.message_rx.poll().expect("Never fails") {
            if let Some(m) = message {
                if m.force_wakeup {
                    self.inner.force_wakeup();
                }
                self.inner.send_message(m.message, m.response_handler);
            } else {
                return Ok(Async::Ready(()));
            }
        }
        track!(self.inner.poll())
    }
}

#[derive(Debug, Clone)]
struct ChannelHandle {
    message_tx: mpsc::Sender<Message>,
}
impl ChannelHandle {
    pub fn send_message(&self, message: Message) {
        let _ = self.message_tx.send(message);
    }
}

pub struct Message {
    pub message: OutgoingMessage,
    pub response_handler: Option<BoxResponseHandler>,
    pub force_wakeup: bool,
}
impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Message {{ .. }}")
    }
}
