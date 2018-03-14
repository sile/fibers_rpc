use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;
use slog::{Discard, Logger};
use fibers::{BoxSpawn, Spawn};
use fibers::net::TcpListener;
use fibers::net::futures::{Connected, TcpListenerBind};
use fibers::net::streams::Incoming;
use fibers::sync::mpsc;
use futures::{Async, Future, Poll, Stream};

use {Call, Cast, Error};
use codec::{DefaultDecoderMaker, IntoEncoderMaker, MakeDecoder, MakeEncoder};
use message::{MessageSeqNo, OutgoingMessage};
use server_side_channel::ServerSideChannel;
use server_side_handlers::{Action, CallHandlerFactory, CastHandlerFactory, HandleCall, HandleCast,
                           IncomingFrameHandler, MessageHandlers, Never};

/// RPC server builder.
pub struct ServerBuilder {
    bind_addr: SocketAddr,
    logger: Logger,
    handlers: MessageHandlers,
}
impl ServerBuilder {
    /// Makes a new `ServerBuilder` instance.
    pub fn new(bind_addr: SocketAddr) -> Self {
        ServerBuilder {
            bind_addr,
            logger: Logger::root(Discard, o!()),
            handlers: HashMap::new(),
        }
    }

    /// Sets the logger of the server.
    ///
    /// The default value is `Logger::root(Discard, o!())`.
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
        self
    }

    /// Registers a handler for the request/response RPC.
    ///
    /// This equivalent to
    /// `call_handler_with_codec(handler, DefaultDecoderMaker::new(), IntoEncoderMaker::new())`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn call_handler<T, H>(&mut self, handler: H) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        T::ReqDecoder: Default,
        T::Res: Into<T::ResEncoder>,
    {
        self.call_handler_with_codec(handler, DefaultDecoderMaker::new(), IntoEncoderMaker::new())
    }

    /// Registers a handler (with the given decoder maker) for the request/response RPC.
    ///
    /// This equivalent to `call_handler_with_codec(handler, decoder_maker, IntoEncoderMaker::new())`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn call_handler_with_decoder<T, H, D>(&mut self, handler: H, decoder_maker: D) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        D: MakeDecoder<T::ReqDecoder>,
        T::Res: Into<T::ResEncoder>,
    {
        self.call_handler_with_codec(handler, decoder_maker, IntoEncoderMaker::new())
    }

    /// Registers a handler (with the given encoder maker) for the request/response RPC.
    ///
    /// This equivalent to `call_handler_with_codec(handler, DefaultDecoderMaker::new(), encoder_maker)`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn call_handler_with_encoder<T, H, E>(&mut self, handler: H, encoder_maker: E) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        E: MakeEncoder<T::ResEncoder>,
        T::ReqDecoder: Default,
    {
        self.call_handler_with_codec(handler, DefaultDecoderMaker::new(), encoder_maker)
    }

    /// Registers a handler (with the given decoder/encoder makers) for the request/response RPC.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn call_handler_with_codec<T, H, D, E>(
        &mut self,
        handler: H,
        decoder_maker: D,
        encoder_maker: E,
    ) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        D: MakeDecoder<T::ReqDecoder>,
        E: MakeEncoder<T::ResEncoder>,
    {
        assert!(
            !self.handlers.contains_key(&T::ID),
            "RPC registration conflicts: procedure={:?}, name={:?}",
            T::ID,
            T::NAME
        );

        let handler = CallHandlerFactory::new(handler, decoder_maker, encoder_maker);
        self.handlers.insert(T::ID, Box::new(handler));
        self
    }

    /// Registers a handler for the notification RPC.
    ///
    /// This equivalent to `cast_handler_with_encoder(handler, DefaultDecoderMaker::new())`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn cast_handler<T, H>(&mut self, handler: H) -> &mut Self
    where
        T: Cast,
        H: HandleCast<T>,
        T::Decoder: Default,
    {
        self.cast_handler_with_decoder(handler, DefaultDecoderMaker::new())
    }

    /// Registers a handler (with the given decoder maker) for the notification RPC.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn cast_handler_with_decoder<T, H, D>(&mut self, handler: H, decoder_maker: D) -> &mut Self
    where
        T: Cast,
        H: HandleCast<T>,
        D: MakeDecoder<T::Decoder>,
    {
        assert!(
            !self.handlers.contains_key(&T::ID),
            "RPC registration conflicts: procedure={:?}, name={:?}",
            T::ID,
            T::NAME
        );

        let handler = CastHandlerFactory::new(handler, decoder_maker);
        self.handlers.insert(T::ID, Box::new(handler));
        self
    }

    /// Returns the resulting RPC server.
    ///
    /// The invocation of this method consumes all registered handlers.
    pub fn finish<S>(&mut self, spawner: S) -> Server<S>
    where
        S: Clone + Spawn + Send + 'static,
    {
        let logger = self.logger.new(o!("server" => self.bind_addr.to_string()));
        info!(logger, "Starts RPC server");
        let handlers = mem::replace(&mut self.handlers, HashMap::new());
        Server {
            listener: Listener::Binding(TcpListener::bind(self.bind_addr)),
            logger,
            spawner,
            incoming_frame_handler: IncomingFrameHandler::new(handlers),
        }
    }
}

/// RPC server.
pub struct Server<S> {
    listener: Listener,
    logger: Logger,
    spawner: S,
    incoming_frame_handler: IncomingFrameHandler,
}
impl<S> Future for Server<S>
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
    reply_tx: mpsc::Sender<(MessageSeqNo, OutgoingMessage)>,
    reply_rx: mpsc::Receiver<(MessageSeqNo, OutgoingMessage)>,
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
        loop {
            while let Async::Ready(action) = track!(self.channel.poll())? {
                if let Some(action) = action {
                    match action {
                        Action::NoReply(noreply) => {
                            if let Some(future) = noreply.into_future() {
                                self.spawner.spawn(future.map_err(|_: Never| ()));
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
                                self.spawner.spawn(future.map_err(|_: Never| ()));
                            }
                        }
                    }
                } else {
                    return Ok(Async::Ready(()));
                }
            }

            let mut do_break = true;
            while let Async::Ready(item) = self.reply_rx.poll().expect("Never fails") {
                let (seqno, message) = item.expect("Never fails");
                self.channel.reply(seqno, message);
                do_break = false;
            }
            if do_break {
                break;
            }
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
