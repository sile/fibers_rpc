use bytecodec::marker::Never;
use factory::{DefaultFactory, Factory};
use fibers::net::futures::{Connected, TcpListenerBind};
use fibers::net::streams::Incoming;
use fibers::net::TcpListener;
use fibers::sync::mpsc;
use fibers::{BoxSpawn, Spawn};
use futures::{Async, Future, Poll, Stream};
use prometrics::metrics::MetricBuilder;
use slog::{Discard, Logger};
use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;

use channel::ChannelOptions;
use message::OutgoingMessage;
use metrics::{HandlerMetrics, ServerMetrics};
use server_side_channel::ServerSideChannel;
use server_side_handlers::{
    Action, Assigner, CallHandlerFactory, CastHandlerFactory, HandleCall, HandleCast,
    MessageHandlers,
};
use {Call, Cast, Error, ProcedureId};

/// RPC server builder.
pub struct ServerBuilder {
    bind_addr: SocketAddr,
    logger: Logger,
    handlers: MessageHandlers,
    channel_options: ChannelOptions,
    metrics: MetricBuilder,
    handlers_metrics: HashMap<ProcedureId, HandlerMetrics>,
}
impl ServerBuilder {
    /// Makes a new `ServerBuilder` instance.
    pub fn new(bind_addr: SocketAddr) -> Self {
        ServerBuilder {
            bind_addr,
            logger: Logger::root(Discard, o!()),
            handlers: HashMap::new(),
            channel_options: ChannelOptions::default(),
            metrics: MetricBuilder::new(),
            handlers_metrics: HashMap::new(),
        }
    }

    /// Sets the logger of the server.
    ///
    /// The default value is `Logger::root(Discard, o!())`.
    pub fn logger(&mut self, logger: Logger) -> &mut Self {
        self.logger = logger;
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

    /// Registers a handler for the request/response RPC.
    ///
    /// This equivalent to
    /// `add_call_handler_with_codec(handler, DefaultFactory::new(), DefaultFactory::new())`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn add_call_handler<T, H>(&mut self, handler: H) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        T::ReqDecoder: Default,
        T::ResEncoder: Default,
    {
        self.add_call_handler_with_codec(handler, DefaultFactory::new(), DefaultFactory::new())
    }

    /// Registers a handler (with the given decoder maker) for the request/response RPC.
    ///
    /// This equivalent to `add_call_handler_with_codec(handler, decoder_factory, DefaultFactory::new())`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn add_call_handler_with_decoder<T, H, D>(
        &mut self,
        handler: H,
        decoder_factory: D,
    ) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        D: Factory<Item = T::ReqDecoder> + Send + Sync + 'static,
        T::ResEncoder: Default,
    {
        self.add_call_handler_with_codec(handler, decoder_factory, DefaultFactory::new())
    }

    /// Registers a handler (with the given encoder maker) for the request/response RPC.
    ///
    /// This equivalent to
    /// `add_call_handler_with_codec(handler, DefaultFactory::new(), encoder_factory)`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered,
    /// the calling thread will panic.
    pub fn add_call_handler_with_encoder<T, H, E>(
        &mut self,
        handler: H,
        encoder_factory: E,
    ) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        E: Factory<Item = T::ResEncoder> + Send + Sync + 'static,
        T::ReqDecoder: Default,
    {
        self.add_call_handler_with_codec(handler, DefaultFactory::new(), encoder_factory)
    }

    /// Registers a handler (with the given decoder/encoder makers) for the request/response RPC.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn add_call_handler_with_codec<T, H, D, E>(
        &mut self,
        handler: H,
        decoder_factory: D,
        encoder_factory: E,
    ) -> &mut Self
    where
        T: Call,
        H: HandleCall<T>,
        D: Factory<Item = T::ReqDecoder> + Send + Sync + 'static,
        E: Factory<Item = T::ResEncoder> + Send + Sync + 'static,
    {
        assert!(
            !self.handlers.contains_key(&T::ID),
            "RPC registration conflicts: procedure={:?}, name={:?}",
            T::ID,
            T::NAME
        );

        let metrics = HandlerMetrics::new(self.metrics.clone(), T::ID, T::NAME, "call");
        self.handlers_metrics.insert(T::ID, metrics.clone());

        let handler = CallHandlerFactory::new(handler, decoder_factory, encoder_factory, metrics);
        self.handlers.insert(T::ID, Box::new(handler));
        self
    }

    /// Registers a handler for the notification RPC.
    ///
    /// This equivalent to `add_cast_handler_with_encoder(handler, DefaultFactory::new())`.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn add_cast_handler<T, H>(&mut self, handler: H) -> &mut Self
    where
        T: Cast,
        H: HandleCast<T>,
        T::Decoder: Default,
    {
        self.add_cast_handler_with_decoder(handler, DefaultFactory::new())
    }

    /// Registers a handler (with the given decoder maker) for the notification RPC.
    ///
    /// # Panices
    ///
    /// If a procedure which has `T::ID` already have been registered, the calling thread will panic.
    pub fn add_cast_handler_with_decoder<T, H, D>(
        &mut self,
        handler: H,
        decoder_factory: D,
    ) -> &mut Self
    where
        T: Cast,
        H: HandleCast<T>,
        D: Factory<Item = T::Decoder> + Send + Sync + 'static,
    {
        assert!(
            !self.handlers.contains_key(&T::ID),
            "RPC registration conflicts: procedure={:?}, name={:?}",
            T::ID,
            T::NAME
        );

        let metrics = HandlerMetrics::new(self.metrics.clone(), T::ID, T::NAME, "cast");
        self.handlers_metrics.insert(T::ID, metrics.clone());

        let handler = CastHandlerFactory::new(handler, decoder_factory, metrics);
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
            assigner: Assigner::new(handlers),
            channel_options: self.channel_options.clone(),
            metrics: ServerMetrics::new(self.metrics.clone(), self.handlers_metrics.clone()),
        }
    }
}

/// RPC server.
#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
pub struct Server<S> {
    listener: Listener,
    logger: Logger,
    spawner: S,
    assigner: Assigner,
    channel_options: ChannelOptions,
    metrics: ServerMetrics,
}
impl<S> Server<S> {
    /// Returns the metrics of the server.
    pub fn metrics(&self) -> &ServerMetrics {
        &self.metrics
    }
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

                let options = self.channel_options.clone();
                let metrics = self.metrics.channels().create_channel_metrics(addr);
                let channels = self.metrics.channels().clone();
                let exit_logger = logger.clone();
                let spawner = self.spawner.clone().boxed();
                let assigner = self.assigner.clone();
                let future = client
                    .map_err(|e| track!(Error::from(e)))
                    .and_then(move |stream| {
                        let channel = ServerSideChannel::new(
                            logger,
                            stream,
                            assigner.clone(),
                            options,
                            metrics,
                        );
                        ChannelHandler::new(spawner, channel)
                    });
                self.spawner.spawn(future.then(move |result| {
                    channels.remove_channel_metrics(addr);
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
    reply_tx: mpsc::Sender<OutgoingMessage>,
    reply_rx: mpsc::Receiver<OutgoingMessage>,
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
                            if let Some(message) = reply.try_take() {
                                self.channel.reply(message);
                            } else {
                                let reply_tx = self.reply_tx.clone();
                                let future = reply.map(move |message| {
                                    let _ = reply_tx.send(message);
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
                let message = item.expect("Never fails");
                self.channel.reply(message);
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
