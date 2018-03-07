use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use slog::{Discard, Logger};
use byteorder::{BigEndian, ByteOrder};
use fibers::{BoxSpawn, Spawn};
use fibers::net::{TcpListener, TcpStream};
use fibers::net::futures::{Connected, TcpListenerBind};
use fibers::net::streams::Incoming;
use futures::{Async, Future, Poll, Stream};

use {Error, ErrorKind, ProcedureId, Result};
use channel::{RpcChannel, RpcChannelHandle};
use traits::{Cast, HandleCast};

#[derive(Debug, Clone)]
pub struct RpcServerBuilder {
    bind_addr: SocketAddr,
    logger: Logger,

    // TODO: name
    factory: IncomingMessageFactory,
}
impl RpcServerBuilder {
    pub fn new(bind_addr: SocketAddr) -> Self {
        RpcServerBuilder {
            bind_addr,
            logger: Logger::root(Discard, o!()),
            factory: IncomingMessageFactory::new(),
        }
    }
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
        self
    }

    pub fn register_cast_handler<T: Cast, H: HandleCast<T>>(&mut self, handler: H) -> &mut Self {
        // TODO: check duplication
        // T::PROCEDURE
        self
    }

    pub fn finish<S>(&self, spawner: S) -> RpcServer
    where
        S: Spawn + Send + 'static,
    {
        let logger = self.logger.new(o!("server" => self.bind_addr.to_string()));
        info!(logger, "Starts RPC server");
        RpcServer {
            listener: Listener::Binding(TcpListener::bind(self.bind_addr)),
            logger,
            spawner: spawner.boxed(),
            factory: self.factory.clone(),
        }
    }
}

#[derive(Debug)]
pub struct RpcServer {
    listener: Listener,
    logger: Logger,
    spawner: BoxSpawn,
    factory: IncomingMessageFactory,
}
impl Future for RpcServer {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(item) = track!(self.listener.poll())? {
            if let Some((client, addr)) = item {
                let logger = self.logger.new(o!("client" => addr.to_string()));
                info!(logger, "New TCP client");

                let exit_logger = logger.clone();
                let factory = self.factory.clone();
                self.spawner.spawn(
                    client
                        .map_err(|e| track!(Error::from(e)))
                        .and_then(move |stream| TcpStreamHandler::new(logger, stream, factory))
                        .then(move |result| {
                            if let Err(e) = result {
                                error!(exit_logger, "TCP connection aborted: {}", e);
                            } else {
                                info!(exit_logger, "TCP connection was closed");
                            }
                            Ok(())
                        }),
                );
            } else {
                info!(self.logger, "RPC server stopped");
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
struct MessageHandlerFactory;
impl MessageHandlerFactory {
    fn create(&self) -> MessageHandler {
        MessageHandler
    }
}

#[derive(Debug)]
struct MessageHandler;
impl ::traits::IncrementalDeserialize for MessageHandler {
    fn incremental_deserialize(&mut self, buf: &[u8]) -> Result<()> {
        // TODO: consume buf
        unimplemented!()
    }

    fn finish(&mut self) -> Result<()> {
        // TODO: spawn handler
        unimplemented!()
    }
}

#[derive(Debug)]
struct IncomingMessageDispatcher {
    handlers: Arc<HashMap<ProcedureId, MessageHandlerFactory>>,
    handler: Option<MessageHandler>,
}
impl IncomingMessageDispatcher {
    fn new(handlers: Arc<HashMap<ProcedureId, MessageHandlerFactory>>) -> Self {
        IncomingMessageDispatcher {
            handlers,
            handler: None,
        }
    }
}
impl ::traits::IncrementalDeserialize for IncomingMessageDispatcher {
    fn incremental_deserialize(&mut self, buf: &[u8]) -> Result<()> {
        if let Some(ref mut handler) = self.handler {
            track!(handler.incremental_deserialize(buf))
        } else {
            track_assert!(buf.len() >= 4, ErrorKind::InvalidInput);
            let procedure = BigEndian::read_u32(buf);
            if let Some(handler_factory) = self.handlers.get(&procedure) {
                let mut handler = handler_factory.create();
                track!(handler.incremental_deserialize(&buf[4..]))?;
                self.handler = Some(handler);
                Ok(())
            } else {
                track_panic!(ErrorKind::InvalidInput, "Unknown procedure: {}", procedure);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct IncomingMessageFactory {
    handlers: Arc<HashMap<ProcedureId, MessageHandlerFactory>>,
}
impl IncomingMessageFactory {
    fn new() -> Self {
        IncomingMessageFactory {
            handlers: Arc::new(HashMap::new()),
        }
    }
}
impl ::traits::Factory for IncomingMessageFactory {
    fn create_instance(&mut self) -> ::message::IncomingMessage {
        let dispatcher = IncomingMessageDispatcher::new(self.handlers.clone());
        ::message::IncomingMessage {
            data: Box::new(dispatcher),
        }
    }
}

#[derive(Debug)]
struct TcpStreamHandler {
    channel: RpcChannel,
    channel_handle: RpcChannelHandle,
}
impl TcpStreamHandler {
    fn new(logger: Logger, stream: TcpStream, factory: IncomingMessageFactory) -> Self {
        let factory = ::traits::BoxFactory(Box::new(factory));
        let (channel, channel_handle) = RpcChannel::with_stream(logger, stream, factory);
        TcpStreamHandler {
            channel,
            channel_handle,
        }
    }
}
impl Future for TcpStreamHandler {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(_todo) = track!(self.channel.poll())? {
            unimplemented!()
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
enum Listener {
    Binding(TcpListenerBind),
    Listening(Incoming),
}
impl Stream for Listener {
    type Item = (Connected, SocketAddr);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let next = match *self {
                Listener::Binding(ref mut f) => {
                    if let Async::Ready(listener) = track!(f.poll().map_err(Error::from))? {
                        Listener::Listening(listener.incoming())
                    } else {
                        break;
                    }
                }
                Listener::Listening(ref mut s) => return track!(s.poll().map_err(Error::from)),
            };
            *self = next;
        }
        Ok(Async::NotReady)
    }
}
