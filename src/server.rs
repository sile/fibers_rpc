use std::collections::HashMap;
use std::net::SocketAddr;
use slog::{Discard, Logger};
use fibers::{BoxSpawn, Spawn};
use fibers::net::TcpListener;
use fibers::net::futures::{Connected, TcpListenerBind};
use fibers::net::streams::Incoming;
use fibers::sync::mpsc;
use futures::{Async, Future, Poll, Stream};

use Error;
use codec::{DefaultDecoderMaker, IntoEncoderMaker};
use message::MessageSeqNo;
use server_side_channel::ServerSideChannel;
use server_side_handlers::{Action, CallHandlerFactory, CastHandlerFactory, IncomingFrameHandler,
                           MessageHandlers};
use traits::{Call, Cast, Encodable, HandleCall, HandleCast};

pub struct RpcServerBuilder {
    bind_addr: SocketAddr,
    logger: Logger,
    handlers: MessageHandlers,
}
impl RpcServerBuilder {
    pub fn new(bind_addr: SocketAddr) -> Self {
        RpcServerBuilder {
            bind_addr,
            logger: Logger::root(Discard, o!()),
            handlers: HashMap::new(),
        }
    }
    pub fn logger(mut self, logger: Logger) -> Self {
        self.logger = logger;
        self
    }

    pub fn call_handler<T: Call, H: HandleCall<T>>(mut self, handler: H) -> Self
    where
        T::RequestDecoder: Default,
        T::Response: Into<T::ResponseEncoder>,
    {
        assert!(!self.handlers.contains_key(&T::PROCEDURE));

        let handler =
            CallHandlerFactory::new(handler, DefaultDecoderMaker::new(), IntoEncoderMaker::new());
        self.handlers.insert(T::PROCEDURE, Box::new(handler));
        self
    }

    pub fn cast_handler<T: Cast, H: HandleCast<T>>(mut self, handler: H) -> Self
    where
        T::Decoder: Default,
    {
        assert!(!self.handlers.contains_key(&T::PROCEDURE));

        let handler = CastHandlerFactory::new(handler, DefaultDecoderMaker::new());
        self.handlers.insert(T::PROCEDURE, Box::new(handler));
        self
    }

    pub fn finish<S>(self, spawner: S) -> RpcServer<S>
    where
        S: Clone + Spawn + Send + 'static,
    {
        let logger = self.logger.new(o!("server" => self.bind_addr.to_string()));
        info!(logger, "Starts RPC server");
        RpcServer {
            listener: Listener::Binding(TcpListener::bind(self.bind_addr)),
            logger,
            spawner,
            incoming_frame_handler: IncomingFrameHandler::new(self.handlers),
        }
    }
}

pub struct RpcServer<S> {
    listener: Listener,
    logger: Logger,
    spawner: S,
    incoming_frame_handler: IncomingFrameHandler,
}
impl<S> Future for RpcServer<S>
where
    S: Clone + Spawn + Send + 'static,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(item) = track!(self.listener.poll())? {
            if let Some((client, addr)) = item {
                let logger = self.logger.new(o!("client" => addr.to_string()));
                info!(logger, "New TCP client");

                let exit_logger = logger.clone();
                let spawner = self.spawner.clone().boxed();
                let incoming_frame_handler = self.incoming_frame_handler.clone();
                let future = client
                    .map_err(|e| track!(Error::from(e)))
                    .and_then(move |stream| {
                        let channel =
                            ServerSideChannel::new(logger, stream, incoming_frame_handler.clone());
                        ChannelHandler::new(spawner, channel)
                    });
                self.spawner.spawn(future.then(move |result| {
                    if let Err(e) = result {
                        error!(exit_logger, "TCP connection aborted: {}", e);
                    } else {
                        info!(exit_logger, "TCP connection was closed");
                    }
                    Ok(())
                }));
            } else {
                info!(self.logger, "RPC server stopped");
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}

struct ChannelHandler {
    spawner: BoxSpawn,
    channel: ServerSideChannel,
    reply_tx: mpsc::Sender<(MessageSeqNo, Encodable)>,
    reply_rx: mpsc::Receiver<(MessageSeqNo, Encodable)>,
}
impl ChannelHandler {
    fn new(spawner: BoxSpawn, channel: ServerSideChannel) -> Self {
        let (reply_tx, reply_rx) = mpsc::channel();
        ChannelHandler {
            spawner,
            channel,
            reply_tx,
            reply_rx,
        }
    }
}
impl Future for ChannelHandler {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(action) = track!(self.channel.poll())? {
            if let Some(action) = action {
                match action {
                    Action::NoReply(noreply) => {
                        if !noreply.is_done() {
                            self.spawner.spawn(noreply);
                        }
                    }
                    Action::Reply(mut reply) => {
                        if let Some((seqno, message)) = reply.try_take() {
                            self.channel.reply(seqno, message);
                        } else {
                            let reply_tx = self.reply_tx.clone();
                            let future = reply.map(move |(seqno, message)| {
                                let _ = reply_tx.send((seqno, message));
                            });
                            self.spawner.spawn(future);
                        }
                    }
                }
            } else {
                return Ok(Async::Ready(()));
            }
        }
        while let Async::Ready(item) = self.reply_rx.poll().expect("Never fails") {
            let (seqno, message) = item.expect("Never fails");
            self.channel.reply(seqno, message);
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
