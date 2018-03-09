use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};

use {ErrorKind, ProcedureId, Result};
use frame::{Frame, HandleFrame};
use message::MessageSeqNo;
use traits::{Call, Cast, Decode, DecoderFactory, HandleCall, HandleCast};

pub type MessageHandlers = HashMap<ProcedureId, Box<MessageHandlerFactory>>;

#[derive(Debug)]
pub enum Action {
    Reply,
    NoReply,
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
        let _noreply = self.handler.handle_cast(notification);
        Ok(Action::NoReply)
    }
}

pub struct CallHandlerFactory<T, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder_factory: D,
}
impl<T, H, D> MessageHandlerFactory for CallHandlerFactory<T, H, D>
where
    T: Call,
    H: HandleCall<T>,
    D: DecoderFactory<T::RequestDecoder>,
{
    fn create_message_handler(&self, seqno: MessageSeqNo) -> Box<HandleMessage> {
        let decoder = self.decoder_factory.create_decoder();
        let handler = CallHandler {
            _rpc: PhantomData,
            handler: Arc::clone(&self.handler),
            decoder,
            seqno,
        };
        Box::new(handler)
    }
}

struct CallHandler<T, H, D> {
    _rpc: PhantomData<T>,
    handler: Arc<H>,
    decoder: D,
    seqno: MessageSeqNo,
}
impl<T, H> HandleMessage for CallHandler<T, H, T::RequestDecoder>
where
    T: Call,
    H: HandleCall<T>,
{
    fn handle_message(&mut self, data: &[u8]) -> Result<()> {
        track!(self.decoder.decode(data))
    }
    fn finish(&mut self) -> Result<Action> {
        let request = track!(self.decoder.finish())?;
        let _reply = self.handler.handle_call(request);
        Ok(Action::Reply)
    }
}
