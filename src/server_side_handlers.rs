use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use bytecodec::{Decode, Encode, Eos};
use byteorder::{BigEndian, ByteOrder};
use factory::Factory;
use futures::{Async, Future, Poll};
use futures::future::Either;

use {Call, Cast, Error, ErrorKind, ProcedureId, Result};
use frame::{Frame, HandleFrame};
use message::{MessageId, OutgoingMessage};
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

/// A marker which represents never instantiated type.
pub struct Never(());

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

    fn boxed<F>(self, message_id: MessageId, f: F) -> BoxReply
    where
        F: FnOnce(T::Res) -> OutgoingMessage + Send + 'static,
    {
        match self.either {
            Either::A(v) => BoxReply {
                message_id,
                either: Either::A(Box::new(v.map(f))),
            },
            Either::B(v) => BoxReply {
                message_id,
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
    message_id: MessageId,
    either: Either<
        Box<Future<Item = OutgoingMessage, Error = Never> + Send + 'static>,
        Option<OutgoingMessage>,
    >,
}
impl BoxReply {
    pub fn try_take(&mut self) -> Option<(MessageId, OutgoingMessage)> {
        let message_id = self.message_id;
        if let Either::B(ref mut v) = self.either {
            v.take().map(|v| (message_id, v))
        } else {
            None
        }
    }
}
impl Future for BoxReply {
    type Item = (MessageId, OutgoingMessage);
    type Error = Never;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let message_id = self.message_id;
        Ok(match self.either {
            Either::A(ref mut f) => f.poll()?.map(|v| (message_id, v)),
            Either::B(ref mut v) => {
                Async::Ready((message_id, v.take().expect("Cannot poll BoxReply twice")))
            }
        })
    }
}
impl fmt::Debug for BoxReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoxReply {{ message_id: {:?}, .. }}", self.message_id)
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

pub struct IncomingFrameHandler {
    handlers: Arc<MessageHandlers>,
    runnings: HashMap<MessageId, Box<HandleMessage>>,
}
impl IncomingFrameHandler {
    pub fn new(mut handlers: MessageHandlers) -> Self {
        handlers.shrink_to_fit();
        IncomingFrameHandler {
            handlers: Arc::new(handlers),
            runnings: HashMap::new(),
        }
    }
}
impl HandleFrame for IncomingFrameHandler {
    type Item = Action;

    #[cfg_attr(feature = "cargo-clippy", allow(map_entry))]
    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>> {
        debug_assert!(!frame.is_error());

        let mut offset = 0;
        if !self.runnings.contains_key(&frame.message_id()) {
            debug_assert!(frame.data().len() >= 4);
            let procedure = ProcedureId(BigEndian::read_u32(frame.data()));
            offset = 4;

            let factory = track_assert_some!(
                self.handlers.get(&procedure),
                ErrorKind::InvalidInput,
                "Unregistered RPC: {:?}",
                procedure,
            );
            let handler = factory.create_message_handler(frame.message_id());
            self.runnings.insert(frame.message_id(), handler);
        }

        let mut handler = self.runnings
            .remove(&frame.message_id())
            .expect("Never fails");
        track!(handler.handle_message(&frame.data()[offset..], frame.is_end_of_message()))?;
        if frame.is_end_of_message() {
            let action = track!(handler.finish(frame.priority()))?;
            Ok(Some(action))
        } else {
            self.runnings.insert(frame.message_id(), handler);
            Ok(None)
        }
    }

    fn handle_error(&mut self, message_id: MessageId, _error: Error) {
        self.runnings.remove(&message_id);
    }
}
impl fmt::Debug for IncomingFrameHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingFrameHandler {{ handlers.len: {}, runnings.len: {} }}",
            self.handlers.len(),
            self.runnings.len()
        )
    }
}
impl Clone for IncomingFrameHandler {
    fn clone(&self) -> Self {
        IncomingFrameHandler {
            handlers: Arc::clone(&self.handlers),
            runnings: HashMap::new(),
        }
    }
}

pub trait MessageHandlerFactory: Send + Sync + 'static {
    fn create_message_handler(&self, message_id: MessageId) -> Box<HandleMessage>;
}

// TODO:
pub trait HandleMessage: Send + 'static {
    fn handle_message(&mut self, data: &[u8], eos: bool) -> Result<()>;
    fn finish(&mut self, priority: u8) -> Result<Action>;
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
    T::Notification: Send, // TODO: delete
    H: HandleCast<T>,
    D: Factory<Item = T::Decoder> + Send + Sync + 'static,
{
    fn create_message_handler(&self, _message_id: MessageId) -> Box<HandleMessage> {
        let decoder = self.decoder_maker.create();
        let handler = CastHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
            notification: None,
        };
        self.metrics.rpc_count.increment();
        Box::new(handler)
    }
}

struct CastHandler<T: Cast, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
    notification: Option<T::Notification>,
}
impl<T, H> HandleMessage for CastHandler<T, H, T::Decoder>
where
    T: Cast,
    T::Notification: Send, // TODO: delete
    H: HandleCast<T>,
{
    fn handle_message(&mut self, data: &[u8], eos: bool) -> Result<()> {
        let eos = Eos::new(eos);
        let (_size, item) = track!(self.decoder.decode(data, eos))?;
        if let Some(notification) = item {
            // TODO: validate eos and _size
            self.notification = Some(notification);
        }
        Ok(())
    }
    fn finish(&mut self, _priority: u8) -> Result<Action> {
        let notification = track_assert_some!(self.notification.take(), ErrorKind::Other);
        let noreply = self.handler.handle_cast(notification);
        Ok(Action::NoReply(noreply))
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
    T::Req: Send, // TODO: delete
    H: HandleCall<T>,
    D: Factory<Item = T::ReqDecoder> + Send + Sync + 'static,
    E: Factory<Item = T::ResEncoder> + Send + Sync + 'static,
{
    fn create_message_handler(&self, message_id: MessageId) -> Box<HandleMessage> {
        let decoder = self.decoder_maker.create();
        let handler = CallHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
            encoder_maker: Arc::clone(&self.encoder_maker),
            message_id,
            request: None,
        };
        self.metrics.rpc_count.increment();
        Box::new(handler)
    }
}

struct CallHandler<T: Call, H, D, E> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
    encoder_maker: Arc<E>, // TODO: s/encoder_maker/encoder/
    message_id: MessageId,
    request: Option<T::Req>,
}
impl<T, H, E> HandleMessage for CallHandler<T, H, T::ReqDecoder, E>
where
    T: Call,
    T::Req: Send, // TODO: delete
    H: HandleCall<T>,
    E: Factory<Item = T::ResEncoder> + Send + Sync + 'static,
{
    fn handle_message(&mut self, data: &[u8], eos: bool) -> Result<()> {
        let eos = Eos::new(eos);
        let (_size, item) = track!(self.decoder.decode(data, eos))?;
        if let Some(request) = item {
            // TODO: validate eos and _size
            self.request = Some(request);
        }
        Ok(())
    }
    fn finish(&mut self, priority: u8) -> Result<Action> {
        let request = track_assert_some!(self.request.take(), ErrorKind::Other);
        let encoder_maker = Arc::clone(&self.encoder_maker);
        let reply = self.handler
            .handle_call(request)
            .boxed(self.message_id, move |v| {
                let mut encoder = encoder_maker.create();
                encoder.start_encoding(v).expect("TODO");
                OutgoingMessage::new(None, priority, encoder)
            });
        Ok(Action::Reply(reply))
    }
}
