use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use bytecodec::{self, ByteCount, Decode, Eos};
use bytecodec::marker::Never;
use factory::Factory;
use futures::{Async, Future, Poll};
use futures::future::Either;

use {Call, Cast, ErrorKind, ProcedureId, Result};
use message::{AssignIncomingMessageHandler, MessageHeader, OutgoingMessage, OutgoingMessagePayload};
use metrics::HandlerMetrics;

pub type MessageHandlers = HashMap<ProcedureId, Box<MessageHandlerFactory>>;

/// This trait allows for handling notification RPC.
pub trait HandleCast<T: Cast>: Send + Sync + 'static {
    /// Handles a notification.
    fn handle_cast(&self, notification: T::Notification) -> NoReply;
}

/// This trait allows for handling request/response RPC.
pub trait HandleCall<T: Call>: Send + Sync + 'static {
    /// Handles a request.
    fn handle_call(&self, request: T::Req) -> Reply<T>;
}

type BoxResponseFuture<T> = Box<Future<Item = T, Error = Never> + Send + 'static>;

/// This represents a reply from a RPC server.
pub struct Reply<T: Call> {
    either: Either<BoxResponseFuture<T::Res>, Option<T::Res>>,
}
impl<T: Call> Reply<T> {
    /// Makes a `Reply` instance which will execute `future`
    /// then reply the resulting item as the response.
    pub fn future<F>(future: F) -> Self
    where
        F: Future<Item = T::Res, Error = Never> + Send + 'static,
    {
        Reply {
            either: Either::A(Box::new(future)),
        }
    }

    /// Makes a `Reply` instance which replies the response immediately.
    pub fn done(response: T::Res) -> Self {
        Reply {
            either: Either::B(Some(response)),
        }
    }

    fn boxed<F>(self, f: F) -> BoxReply
    where
        F: FnOnce(T::Res) -> OutgoingMessage + Send + 'static,
    {
        match self.either {
            Either::A(v) => BoxReply {
                either: Either::A(Box::new(v.map(f))),
            },
            Either::B(v) => BoxReply {
                either: Either::B(v.map(f)),
            },
        }
    }
}
impl<T: Call> fmt::Debug for Reply<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Reply {{ .. }}")
    }
}

pub struct BoxReply {
    either: Either<
        Box<Future<Item = OutgoingMessage, Error = Never> + Send + 'static>,
        Option<OutgoingMessage>,
    >,
}
impl BoxReply {
    pub fn try_take(&mut self) -> Option<OutgoingMessage> {
        if let Either::B(ref mut v) = self.either {
            v.take().map(|v| v)
        } else {
            None
        }
    }
}
impl Future for BoxReply {
    type Item = OutgoingMessage;
    type Error = Never;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(match self.either {
            Either::A(ref mut f) => f.poll()?.map(|v| v),
            Either::B(ref mut v) => Async::Ready(v.take().expect("Cannot poll BoxReply twice")),
        })
    }
}
impl fmt::Debug for BoxReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoxReply {{ .. }}")
    }
}

/// This represents a task for handling an RPC notification.
pub struct NoReply {
    future: Option<Box<Future<Item = (), Error = Never> + Send + 'static>>,
}
impl NoReply {
    /// Makes a `NoReply` instance which will execute `future` for handling the notification.
    pub fn future<F>(f: F) -> Self
    where
        F: Future<Item = (), Error = Never> + Send + 'static,
    {
        NoReply {
            future: Some(Box::new(f)),
        }
    }

    /// Makes a `NoReply` instance which has no task.
    pub fn done() -> Self {
        NoReply { future: None }
    }

    pub(crate) fn into_future(
        self,
    ) -> Option<Box<Future<Item = (), Error = Never> + Send + 'static>> {
        self.future
    }
}
impl fmt::Debug for NoReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.future.is_none() {
            write!(f, "NoReply {{ future: None }}")
        } else {
            write!(f, "NoReply {{ future: Some(_) }}")
        }
    }
}

#[derive(Debug)]
pub enum Action {
    Reply(BoxReply),
    NoReply(NoReply),
}

pub struct Assigner {
    handlers: Arc<MessageHandlers>,
}
impl Assigner {
    pub fn new(mut handlers: MessageHandlers) -> Self {
        handlers.shrink_to_fit();
        Assigner {
            handlers: Arc::new(handlers),
        }
    }
}
impl AssignIncomingMessageHandler for Assigner {
    type Handler = Box<Decode<Item = Action> + Send + 'static>;

    fn assign_incoming_message_handler(&mut self, header: &MessageHeader) -> Result<Self::Handler> {
        let factory = track_assert_some!(
            self.handlers.get(&header.procedure),
            ErrorKind::InvalidInput,
            "Unregistered RPC: {:?}",
            header.procedure,
        );
        let handler = factory.create_message_handler(header);
        Ok(handler)
    }
}
impl fmt::Debug for Assigner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Assigner {{ handlers.len: {} }}", self.handlers.len(),)
    }
}
impl Clone for Assigner {
    fn clone(&self) -> Self {
        Assigner {
            handlers: Arc::clone(&self.handlers),
        }
    }
}

pub trait MessageHandlerFactory: Send + Sync + 'static {
    fn create_message_handler(
        &self,
        header: &MessageHeader,
    ) -> Box<Decode<Item = Action> + Send + 'static>;
}

pub struct CastHandlerFactory<T, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder_maker: D,
    metrics: HandlerMetrics,
}
impl<T, H, D> CastHandlerFactory<T, H, D>
where
    T: Cast,
    H: HandleCast<T>,
    D: Factory<Item = T::Decoder>,
{
    pub fn new(handler: H, decoder_maker: D, metrics: HandlerMetrics) -> Self {
        CastHandlerFactory {
            _rpc: PhantomData,
            handler: Arc::new(handler),
            decoder_maker,
            metrics,
        }
    }
}
impl<T, H, D> MessageHandlerFactory for CastHandlerFactory<T, H, D>
where
    T: Cast,
    H: HandleCast<T>,
    D: Factory<Item = T::Decoder> + Send + Sync + 'static,
{
    fn create_message_handler(
        &self,
        _header: &MessageHeader,
    ) -> Box<Decode<Item = Action> + Send + 'static> {
        let decoder = self.decoder_maker.create();
        let handler = CastHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
        };
        self.metrics.rpc_count.increment();
        Box::new(handler)
    }
}

struct CastHandler<T: Cast, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
}
impl<T, H> Decode for CastHandler<T, H, T::Decoder>
where
    T: Cast,
    H: HandleCast<T>,
{
    type Item = Action;

    fn decode(&mut self, buf: &[u8], eos: Eos) -> bytecodec::Result<(usize, Option<Self::Item>)> {
        let (size, item) = track!(self.decoder.decode(buf, eos))?;
        if let Some(notification) = item {
            let noreply = self.handler.handle_cast(notification);
            Ok((size, Some(Action::NoReply(noreply))))
        } else {
            Ok((size, None))
        }
    }

    fn has_terminated(&self) -> bool {
        self.decoder.has_terminated()
    }

    fn requiring_bytes(&self) -> ByteCount {
        self.decoder.requiring_bytes()
    }
}

pub struct CallHandlerFactory<T, H, D, E> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder_maker: D,
    encoder_maker: Arc<E>,
    metrics: HandlerMetrics,
}
impl<T, H, D, E> CallHandlerFactory<T, H, D, E>
where
    T: Call,
    H: HandleCall<T>,
    D: Factory<Item = T::ReqDecoder>,
    E: Factory<Item = T::ResEncoder>,
{
    pub fn new(handler: H, decoder_maker: D, encoder_maker: E, metrics: HandlerMetrics) -> Self {
        CallHandlerFactory {
            _rpc: PhantomData,
            handler: Arc::new(handler),
            decoder_maker,
            encoder_maker: Arc::new(encoder_maker),
            metrics,
        }
    }
}
impl<T, H, D, E> MessageHandlerFactory for CallHandlerFactory<T, H, D, E>
where
    T: Call,
    H: HandleCall<T>,
    D: Factory<Item = T::ReqDecoder> + Send + Sync + 'static,
    E: Factory<Item = T::ResEncoder> + Send + Sync + 'static,
{
    fn create_message_handler(
        &self,
        header: &MessageHeader,
    ) -> Box<Decode<Item = Action> + Send + 'static> {
        let decoder = self.decoder_maker.create();
        let handler = CallHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
            encoder: Some(self.encoder_maker.create()),
            header: header.clone(),
        };
        self.metrics.rpc_count.increment();
        Box::new(handler)
    }
}

struct CallHandler<T: Call, H, D, E> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
    encoder: Option<E>,
    header: MessageHeader,
}
impl<T, H> Decode for CallHandler<T, H, T::ReqDecoder, T::ResEncoder>
where
    T: Call,
    H: HandleCall<T>,
{
    type Item = Action;

    fn decode(&mut self, buf: &[u8], eos: Eos) -> bytecodec::Result<(usize, Option<Self::Item>)> {
        let (size, item) = track!(self.decoder.decode(buf, eos))?;
        if let Some(request) = item {
            let encoder = track_assert_some!(self.encoder.take(), bytecodec::ErrorKind::Other);
            let header = self.header.clone();
            let reply = self.handler
                .handle_call(request)
                .boxed(move |v| OutgoingMessage {
                    header,
                    payload: OutgoingMessagePayload::with_item(encoder, v),
                });
            Ok((size, Some(Action::Reply(reply))))
        } else {
            Ok((size, None))
        }
    }

    fn has_terminated(&self) -> bool {
        self.decoder.has_terminated() || self.encoder.is_none()
    }

    fn requiring_bytes(&self) -> ByteCount {
        if self.encoder.is_none() {
            ByteCount::Finite(0)
        } else {
            self.decoder.requiring_bytes()
        }
    }
}
