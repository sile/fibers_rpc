use std::net::SocketAddr;
use slog::{Discard, Logger};
use fibers::{BoxSpawn, Spawn};
use fibers::net::{TcpListener, TcpStream};
use fibers::net::futures::{Connected, TcpListenerBind};
use fibers::net::streams::Incoming;
use futures::{Async, Future, Poll, Stream};

use Error;
use channel::{RpcChannel, RpcChannelHandle};
use traits::{Cast, HandleCast};

#[derive(Debug, Clone)]
pub struct RpcServerBuilder {
    bind_addr: SocketAddr,
    logger: Logger,
}
impl RpcServerBuilder {
    pub fn new(bind_addr: SocketAddr) -> Self {
        RpcServerBuilder {
            bind_addr,
            logger: Logger::root(Discard, o!()),
        }
    }
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
        self
    }

    pub fn register_cast_handler<T: Cast, H: HandleCast<T>>(&mut self, handler: H) -> &mut Self {
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
        }
    }
}

#[derive(Debug)]
pub struct RpcServer {
    listener: Listener,
    logger: Logger,
    spawner: BoxSpawn,
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
                self.spawner.spawn(
                    client
                        .map_err(|e| track!(Error::from(e)))
                        .and_then(move |stream| TcpStreamHandler::new(logger, stream))
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
struct TcpStreamHandler {
    channel: RpcChannel,
    channel_handle: RpcChannelHandle,
}
impl TcpStreamHandler {
    fn new(logger: Logger, stream: TcpStream) -> Self {
        let (channel, channel_handle) = RpcChannel::with_stream(logger, stream);
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
