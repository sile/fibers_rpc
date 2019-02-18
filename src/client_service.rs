use atomic_immut::AtomicImmut;
use fibers::sync::mpsc;
use fibers::{BoxSpawn, Spawn};
use futures::{Async, Future, Poll, Stream};
use prometrics::metrics::MetricBuilder;
use slog::{Discard, Logger};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;

use channel::ChannelOptions;
use client_side_channel::{ClientSideChannel, DEFAULT_KEEP_ALIVE_TIMEOUT_SECS};
use client_side_handlers::BoxResponseHandler;
use message::OutgoingMessage;
use metrics::ClientMetrics;
use Error;

/// `ClientService` builder.
#[derive(Debug)]
pub struct ClientServiceBuilder {
    logger: Logger,
    keep_alive_timeout: Duration,
    channel_options: ChannelOptions,
    metrics: MetricBuilder,
}
impl ClientServiceBuilder {
    /// Makes a new `ClientServiceBuilder` instance.
    pub fn new() -> Self {
        ClientServiceBuilder {
            logger: Logger::root(Discard, o!()),
            keep_alive_timeout: Duration::from_secs(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS),
            channel_options: ChannelOptions::default(),
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

    /// Sets `ChannelOptions` used by the service.
    ///
    /// The default value is `ChannelOptions::default()`.
    pub fn channel_options(&mut self, options: ChannelOptions) -> &mut Self {
        self.channel_options = options;
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
    pub fn finish<S>(self, spawner: S) -> ClientService
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
            channel_options: self.channel_options.clone(),
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
#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
pub struct ClientService {
    logger: Logger,
    spawner: BoxSpawn,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    channels: Arc<AtomicImmut<HashMap<SocketAddr, ChannelHandle>>>,
    keep_alive_timeout: Duration,
    channel_options: ChannelOptions,
    metrics: ClientMetrics,
}
impl ClientService {
    /// Makes a new `ClientService` with the default settings.
    ///
    /// If you want to customize some settings, please use `ClientServiceBuilder` instead of this.
    pub fn new<S>(spawner: S) -> Self
    where
        S: Spawn + Send + 'static,
    {
        ClientServiceBuilder::new().finish(spawner)
    }

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
                        let logger = self.logger.new(o!("server" => server.to_string()));

                        info!(logger, "New client-side RPC channel is created");
                        let command_tx = self.command_tx.clone();
                        let mut channels = channels.clone();
                        let (mut channel, handle) = Channel::new(
                            logger.clone(),
                            server,
                            self.channel_options.clone(),
                            self.metrics.clone(),
                        );
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
                    info!(self.logger, "A client-side RPC channel was deleted";
                          "peer" => server.to_string());
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

    pub(crate) fn send_message(&self, server: SocketAddr, message: Message) -> bool {
        if let Some(channel) = self.channels.load().get(&server) {
            channel.send_message(message)
        } else {
            let command = Command::CreateChannel {
                server,
                message: Some(message),
            };
            self.command_tx.send(command).is_ok()
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
    fn new(
        logger: Logger,
        server: SocketAddr,
        options: ChannelOptions,
        metrics: ClientMetrics,
    ) -> (Self, ChannelHandle) {
        let (message_tx, message_rx) = mpsc::channel();
        let is_server_down = Arc::new(AtomicBool::new(false));
        let inner = ClientSideChannel::new(
            logger,
            server,
            Arc::clone(&is_server_down),
            options,
            metrics,
        );
        let channel = Channel { inner, message_rx };
        let handle = ChannelHandle {
            message_tx,
            is_server_down,
        };
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
    is_server_down: Arc<AtomicBool>,
}
impl ChannelHandle {
    pub fn send_message(&self, message: Message) -> bool {
        if !message.force_wakeup && self.is_server_down.load(atomic::Ordering::SeqCst) {
            false
        } else {
            self.message_tx.send(message).is_ok()
        }
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
