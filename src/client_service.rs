use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use slog::{Discard, Logger};
use atomic_immut::AtomicImmut;
use fibers::{BoxSpawn, Spawn};
use fibers::sync::mpsc;
use futures::{Async, Future, Poll, Stream};

use Error;
use client::RpcClient;
use client_side_channel::ClientSideChannel;
use client_side_handlers::BoxResponseHandler;
use traits::Encodable;

#[derive(Debug)]
pub struct RpcClientServiceBuilder {
    logger: Logger,
}
impl RpcClientServiceBuilder {
    pub fn new() -> Self {
        RpcClientServiceBuilder {
            logger: Logger::root(Discard, o!()),
        }
    }
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
        self
    }
    pub fn finish<S>(&self, spawner: S) -> RpcClientService
    where
        S: Spawn + Send + 'static,
    {
        let (command_tx, command_rx) = mpsc::channel();
        let channels = Arc::new(AtomicImmut::default());
        RpcClientService {
            logger: self.logger.clone(),
            spawner: spawner.boxed(),
            command_rx,
            command_tx,
            channels: channels.clone(),
        }
    }
}

#[derive(Debug)]
pub struct RpcClientService {
    logger: Logger,
    spawner: BoxSpawn,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    channels: Arc<AtomicImmut<HashMap<SocketAddr, RpcChannelHandle>>>,
}
impl RpcClientService {
    pub fn client(&self) -> RpcClient {
        let handle = RpcClientServiceHandle {
            command_tx: self.command_tx.clone(),
            channels: Arc::clone(&self.channels),
        };
        RpcClient { service: handle }
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
                        let (channel, handle) = RpcChannel::new(logger.clone(), server);
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
impl Future for RpcClientService {
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

#[derive(Debug, Clone)]
pub struct RpcClientServiceHandle {
    command_tx: mpsc::Sender<Command>,
    channels: Arc<AtomicImmut<HashMap<SocketAddr, RpcChannelHandle>>>,
}
impl RpcClientServiceHandle {
    pub fn send_message(&self, server: SocketAddr, message: Message) {
        if let Some(channel) = self.channels.load().get(&server) {
            channel.send_message(message);
        } else {
            let command = Command::CreateChannel {
                server,
                message: Some(message),
            };
            let _ = self.command_tx.send(command);
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
struct RpcChannel {
    inner: ClientSideChannel,
    message_rx: mpsc::Receiver<Message>,
}
impl RpcChannel {
    fn new(logger: Logger, server: SocketAddr) -> (Self, RpcChannelHandle) {
        let (message_tx, message_rx) = mpsc::channel();
        let inner = ClientSideChannel::new(logger, server);
        let channel = RpcChannel { inner, message_rx };
        let handle = RpcChannelHandle { message_tx };
        (channel, handle)
    }
}
impl Future for RpcChannel {
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
struct RpcChannelHandle {
    message_tx: mpsc::Sender<Message>,
}
impl RpcChannelHandle {
    pub fn send_message(&self, message: Message) {
        let _ = self.message_tx.send(message);
    }
}

pub struct Message {
    pub message: Encodable,
    pub response_handler: Option<BoxResponseHandler>,
    pub force_wakeup: bool,
}
impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Message {{ .. }}")
    }
}
