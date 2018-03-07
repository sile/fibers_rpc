use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use slog::{Discard, Logger};
use atomic_immut::AtomicImmut;
use fibers::{BoxSpawn, Spawn};
use fibers::sync::mpsc;
use futures::{Async, Future, Poll, Stream};

use Error;
use channel::{RpcChannel, RpcChannelHandle};
use message::OutgoingMessage;
use super::RpcClient;

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
                        self.spawner
                            .spawn(channel.for_each(|m| Ok(())).then(move |result| {
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
    pub fn send_message(&self, server: SocketAddr, message: OutgoingMessage) {
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
        message: Option<OutgoingMessage>,
    },
    RemoveChannel {
        server: SocketAddr,
    },
}
