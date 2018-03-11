use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};
use futures::{Async, Future, Poll};
use futures::future::Either;

use {Call, Cast, Error, ErrorKind, ProcedureId, Result};
use codec::{Decode, MakeDecoder, MakeEncoder};
use frame::{Frame, HandleFrame};
use message::{MessageSeqNo, OutgoingMessage};

pub type MessageHandlers = HashMap<ProcedureId, Box<MessageHandlerFactory>>;

/// This trait allows for handling notification RPC.
pub trait HandleCast<T: Cast>: Send + Sync + 'static {
    /// Handles a notification.
    fn handle_cast(&self, notification: T::Notification) -> NoReply;
}

/// This trait allows for handling request/response RPC.
pub trait HandleCall<T: Call>: Send + Sync + 'static {
    /// Handles a request.
    fn handle_call(&self, request: T::Request) -> Reply<T::Response>;
}

/// A marker which represents never instantiated type.
pub struct Never(());

/// `Future` that represents a reply from a RPC server.
pub struct Reply<T> {
    seqno: MessageSeqNo,
    either: Either<Box<Future<Item = T, Error = Never> + Send + 'static>, Option<T>>,
}
impl<T> Reply<T> {
    /// Makes a `Reply` instance which will execute `future` then reply the resulting item as the response.
    pub fn future<F>(future: F) -> Self
    where
        F: Future<Item = T, Error = Never> + Send + 'static,
    {
        Reply {
            seqno: MessageSeqNo::from_u64(0), // dummy initial value
            either: Either::A(Box::new(future)),
        }
    }

    /// Makes a `Reply` instance which replies the response immediately.
    pub fn done(response: T) -> Self {
        Reply {
            seqno: MessageSeqNo::from_u64(0), // dummy initial value
            either: Either::B(Some(response)),
        }
    }

    pub(crate) fn try_take(&mut self) -> Option<(MessageSeqNo, T)> {
        let seqno = self.seqno;
        if let Either::B(ref mut v) = self.either {
            v.take().map(|v| (seqno, v))
        } else {
            None
        }
    }

    fn into_encodable<F>(self, f: F) -> Reply<OutgoingMessage>
    where
        F: FnOnce(T) -> OutgoingMessage + Send + 'static,
        T: 'static,
    {
        match self.either {
            Either::A(v) => Reply {
                seqno: self.seqno,
                either: Either::A(Box::new(v.map(f))),
            },
            Either::B(v) => Reply {
                seqno: self.seqno,
                either: Either::B(v.map(f)),
            },
        }
    }
}
impl<T> Future for Reply<T> {
    type Item = (MessageSeqNo, T);
    type Error = Never;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let seqno = self.seqno;
        Ok(match self.either {
            Either::A(ref mut f) => f.poll()?.map(|v| (seqno, v)),
            Either::B(ref mut v) => {
                Async::Ready((seqno, v.take().expect("Cannot poll Reply twice")))
            }
        })
    }
}
impl<T> fmt::Debug for Reply<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Reply {{ seqno: {:?}, .. }}", self.seqno)
    }
}

/// `Future` that represents a task for handling an RPC notification.
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

    pub(crate) fn is_done(&self) -> bool {
        self.future.is_none()
    }
}
impl Future for NoReply {
    type Item = ();
    type Error = Never;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.future.poll().map(|x| x.map(|_| ()))
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
    Reply(Reply<OutgoingMessage>),
    NoReply(NoReply),
}
impl Future for Action {
    type Item = Option<(MessageSeqNo, OutgoingMessage)>;
    type Error = Never;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(match *self {
            Action::Reply(ref mut f) => f.poll()?.map(Some),
            Action::NoReply(ref mut f) => f.poll()?.map(|_| None),
        })
    }
}

pub struct IncomingFrameHandler {
    handlers: Arc<MessageHandlers>,
    runnings: HashMap<MessageSeqNo, Box<HandleMessage>>,
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

    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>> {
        debug_assert!(!frame.is_error());

        let mut offset = 0;
        if !self.runnings.contains_key(&frame.seqno()) {
            debug_assert!(frame.data().len() >= 4);
            let procedure = ProcedureId(BigEndian::read_u32(frame.data()));
            offset = 4;

            let factory = track_assert_some!(
                self.handlers.get(&procedure),
                ErrorKind::InvalidInput,
                "Unregistered RPC: {:?}",
                procedure,
            );
            let handler = factory.create_message_handler(frame.seqno());
            self.runnings.insert(frame.seqno(), handler);
        }

        let mut handler = self.runnings.remove(&frame.seqno()).expect("Never fails");
        track!(handler.handle_message(&frame.data()[offset..], frame.is_end_of_message()))?;
        if frame.is_end_of_message() {
            let action = track!(handler.finish())?;
            Ok(Some(action))
        } else {
            self.runnings.insert(frame.seqno(), handler);
            Ok(None)
        }
    }

    fn handle_error(&mut self, seqno: MessageSeqNo, _error: Error) {
        self.runnings.remove(&seqno);
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
    fn create_message_handler(&self, seqno: MessageSeqNo) -> Box<HandleMessage>;
}

pub trait HandleMessage: Send + 'static {
    fn handle_message(&mut self, data: &[u8], eos: bool) -> Result<()>;
    fn finish(&mut self) -> Result<Action>;
}

pub struct CastHandlerFactory<T, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder_maker: D,
}
impl<T, H, D> CastHandlerFactory<T, H, D>
where
    T: Cast,
    H: HandleCast<T>,
    D: MakeDecoder<T::Decoder>,
{
    pub fn new(handler: H, decoder_maker: D) -> Self {
        CastHandlerFactory {
            _rpc: PhantomData,
            handler: Arc::new(handler),
            decoder_maker,
        }
    }
}
impl<T, H, D> MessageHandlerFactory for CastHandlerFactory<T, H, D>
where
    T: Cast,
    H: HandleCast<T>,
    D: MakeDecoder<T::Decoder>,
{
    fn create_message_handler(&self, _seqno: MessageSeqNo) -> Box<HandleMessage> {
        let decoder = self.decoder_maker.make_decoder();
        let handler = CastHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
        };
        Box::new(handler)
    }
}

struct CastHandler<T, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
}
impl<T, H> HandleMessage for CastHandler<T, H, T::Decoder>
where
    T: Cast,
    H: HandleCast<T>,
{
    fn handle_message(&mut self, data: &[u8], eos: bool) -> Result<()> {
        track!(self.decoder.decode(data, eos))
    }
    fn finish(&mut self) -> Result<Action> {
        let notification = track!(self.decoder.finish())?;
        let noreply = self.handler.handle_cast(notification);
        Ok(Action::NoReply(noreply))
    }
}

pub struct CallHandlerFactory<T, H, D, E> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder_maker: D,
    encoder_maker: Arc<E>,
}
impl<T, H, D, E> CallHandlerFactory<T, H, D, E>
where
    T: Call,
    H: HandleCall<T>,
    D: MakeDecoder<T::RequestDecoder>,
    E: MakeEncoder<T::Response, T::ResponseEncoder>,
{
    pub fn new(handler: H, decoder_maker: D, encoder_maker: E) -> Self {
        CallHandlerFactory {
            _rpc: PhantomData,
            handler: Arc::new(handler),
            decoder_maker,
            encoder_maker: Arc::new(encoder_maker),
        }
    }
}
impl<T, H, D, E> MessageHandlerFactory for CallHandlerFactory<T, H, D, E>
where
    T: Call,
    H: HandleCall<T>,
    D: MakeDecoder<T::RequestDecoder>,
    E: MakeEncoder<T::Response, T::ResponseEncoder>,
{
    fn create_message_handler(&self, seqno: MessageSeqNo) -> Box<HandleMessage> {
        let decoder = self.decoder_maker.make_decoder();
        let handler = CallHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
            encoder_maker: Arc::clone(&self.encoder_maker),
            seqno,
        };
        Box::new(handler)
    }
}

struct CallHandler<T, H, D, E> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
    encoder_maker: Arc<E>,
    seqno: MessageSeqNo,
}
impl<T, H, E> HandleMessage for CallHandler<T, H, T::RequestDecoder, E>
where
    T: Call,
    H: HandleCall<T>,
    E: MakeEncoder<T::Response, T::ResponseEncoder>,
{
    fn handle_message(&mut self, data: &[u8], eos: bool) -> Result<()> {
        track!(self.decoder.decode(data, eos))
    }
    fn finish(&mut self) -> Result<Action> {
        let request = track!(self.decoder.finish())?;
        let mut reply = self.handler.handle_call(request);
        reply.seqno = self.seqno;

        let encoder_maker = Arc::clone(&self.encoder_maker);
        Ok(Action::Reply(reply.into_encodable(move |v| {
            OutgoingMessage::new(None, encoder_maker.make_encoder(v))
        })))
    }
}
