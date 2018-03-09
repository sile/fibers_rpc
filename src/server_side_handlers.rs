use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};
use futures::{Async, Future, Poll};
use futures::future::Either;

use {ErrorKind, ProcedureId, Result};
use frame::{Frame, HandleFrame};
use message::MessageSeqNo;
use traits::{Call, Cast, Decode, DecoderFactory, Encodable, Encode, EncoderFactory, HandleCall,
             HandleCast};

pub type MessageHandlers = HashMap<ProcedureId, Box<MessageHandlerFactory>>;

#[derive(Debug)]
pub enum Action {
    Reply(Reply<Encodable>),
    NoReply(NoReply),
}
impl Action {
    pub fn is_done(&self) -> bool {
        match *self {
            Action::Reply(ref x) => x.is_done(),
            Action::NoReply(ref x) => x.future.is_none(),
        }
    }
}
impl Future for Action {
    type Item = Option<(MessageSeqNo, Encodable)>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(match *self {
            Action::Reply(ref mut f) => f.poll()?.map(Some),
            Action::NoReply(ref mut f) => f.poll()?.map(|_| None),
        })
    }
}

pub struct Reply<T> {
    seqno: MessageSeqNo,
    either: Either<Box<Future<Item = T, Error = ()> + Send + 'static>, Option<T>>,
}
impl<T> Reply<T> {
    pub fn done(response: T) -> Self {
        Reply {
            seqno: 0,
            either: Either::B(Some(response)),
        }
    }
    pub fn future<F>(future: F) -> Self
    where
        F: Future<Item = T, Error = ()> + Send + 'static,
    {
        Reply {
            seqno: 0,
            either: Either::A(Box::new(future)),
        }
    }

    fn is_done(&self) -> bool {
        if let Either::B(_) = self.either {
            true
        } else {
            false
        }
    }
    fn into_encodable<F>(self, f: F) -> Reply<Encodable>
    where
        F: FnOnce(T) -> Encodable + Send + 'static,
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
    type Error = ();

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
        write!(f, "Reply {{ seqno: {}, .. }}", self.seqno)
    }
}

pub struct NoReply {
    future: Option<Box<Future<Item = (), Error = ()> + Send + 'static>>,
}
impl NoReply {
    pub fn done() -> Self {
        NoReply { future: None }
    }
    pub fn future<F>(f: F) -> Self
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        NoReply {
            future: Some(Box::new(f)),
        }
    }
}
impl Future for NoReply {
    type Item = ();
    type Error = (); // TODO: Never

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

pub struct IncomingFramesHandler {
    handlers: Arc<MessageHandlers>,
    runnings: HashMap<MessageSeqNo, Box<HandleMessage>>,
}
impl IncomingFramesHandler {
    pub fn new(handlers: MessageHandlers) -> Self {
        IncomingFramesHandler {
            handlers: Arc::new(handlers),
            runnings: HashMap::new(),
        }
    }

    pub fn handle_error(&mut self, seqno: MessageSeqNo) {
        self.runnings.remove(&seqno);
    }
}
impl HandleFrame for IncomingFramesHandler {
    type Future = Action;

    fn handle_frame(&mut self, frame: Frame) -> Result<Option<Self::Future>> {
        debug_assert!(!frame.is_error());

        let mut offset = 0;
        if !self.runnings.contains_key(&frame.seqno) {
            debug_assert!(frame.data.len() >= 4);
            let procedure = BigEndian::read_u32(&frame.data);
            offset = 4;

            let factory = track_assert_some!(
                self.handlers.get(&procedure),
                ErrorKind::InvalidInput,
                "Unregistered RPC: {}",
                procedure
            );
            let handler = factory.create_message_handler(frame.seqno);
            self.runnings.insert(frame.seqno, handler);
        }

        let mut handler = self.runnings.remove(&frame.seqno).expect("Never fails");
        track!(handler.handle_message(&frame.data[offset..]))?;
        if frame.is_end_of_message() {
            let action = track!(handler.finish())?;
            Ok(Some(action))
        } else {
            self.runnings.insert(frame.seqno, handler);
            Ok(None)
        }
    }
}
impl fmt::Debug for IncomingFramesHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingFramesHandler {{ handlers.len: {}, runnings.len: {} }}",
            self.handlers.len(),
            self.runnings.len()
        )
    }
}
impl Clone for IncomingFramesHandler {
    fn clone(&self) -> Self {
        IncomingFramesHandler {
            handlers: Arc::clone(&self.handlers),
            runnings: HashMap::new(),
        }
    }
}

pub trait MessageHandlerFactory: Send + Sync + 'static {
    fn create_message_handler(&self, seqno: MessageSeqNo) -> Box<HandleMessage>;
}

pub trait HandleMessage: Send + 'static {
    fn handle_message(&mut self, data: &[u8]) -> Result<()>;
    fn finish(&mut self) -> Result<Action>;
}

pub struct CastHandlerFactory<T, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder_factory: D,
}
impl<T, H, D> CastHandlerFactory<T, H, D>
where
    T: Cast,
    H: HandleCast<T>,
    D: DecoderFactory<T::Decoder>,
{
    pub fn new(handler: H, decoder_factory: D) -> Self {
        CastHandlerFactory {
            _rpc: PhantomData,
            handler: Arc::new(handler),
            decoder_factory,
        }
    }
}
impl<T, H, D> MessageHandlerFactory for CastHandlerFactory<T, H, D>
where
    T: Cast,
    H: HandleCast<T>,
    D: DecoderFactory<T::Decoder>,
{
    fn create_message_handler(&self, _seqno: MessageSeqNo) -> Box<HandleMessage> {
        let decoder = self.decoder_factory.create_decoder();
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
    fn handle_message(&mut self, data: &[u8]) -> Result<()> {
        track!(self.decoder.decode(data))
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
    decoder_factory: D,
    encoder_factory: Arc<E>,
}
impl<T, H, D, E> CallHandlerFactory<T, H, D, E>
where
    T: Call,
    H: HandleCall<T>,
    D: DecoderFactory<T::RequestDecoder>,
    E: EncoderFactory<T::ResponseEncoder>,
{
    pub fn new(handler: H, decoder_factory: D, encoder_factory: E) -> Self {
        CallHandlerFactory {
            _rpc: PhantomData,
            handler: Arc::new(handler),
            decoder_factory,
            encoder_factory: Arc::new(encoder_factory),
        }
    }
}
impl<T, H, D, E> MessageHandlerFactory for CallHandlerFactory<T, H, D, E>
where
    T: Call,
    H: HandleCall<T>,
    D: DecoderFactory<T::RequestDecoder>,
    E: EncoderFactory<T::ResponseEncoder>,
{
    fn create_message_handler(&self, seqno: MessageSeqNo) -> Box<HandleMessage> {
        let decoder = self.decoder_factory.create_decoder();
        let handler = CallHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
            encoder_factory: Arc::clone(&self.encoder_factory),
            seqno,
        };
        Box::new(handler)
    }
}

struct CallHandler<T, H, D, E> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
    encoder_factory: Arc<E>,
    seqno: MessageSeqNo,
}
impl<T, H, E> HandleMessage for CallHandler<T, H, T::RequestDecoder, E>
where
    T: Call,
    H: HandleCall<T>,
    E: EncoderFactory<T::ResponseEncoder>,
{
    fn handle_message(&mut self, data: &[u8]) -> Result<()> {
        track!(self.decoder.decode(data))
    }
    fn finish(&mut self) -> Result<Action> {
        let request = track!(self.decoder.finish())?;
        let mut reply = self.handler.handle_call(request);
        reply.seqno = self.seqno;

        let encoder_factory = Arc::clone(&self.encoder_factory);
        Ok(Action::Reply(reply.into_encodable(move |v| {
            encoder_factory.create_encoder(v).into_encodable()
        })))
    }
}
