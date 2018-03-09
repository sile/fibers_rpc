use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};

use {ErrorKind, ProcedureId, Result};
use frame::{Frame, HandleFrame};
use message::MessageSeqNo;

pub type MessageHandlers = HashMap<ProcedureId, Box<MessageHandlerFactory + Send + Sync + 'static>>;

#[derive(Debug)]
pub enum Action {
    Reply,
    NoReply,
}

pub struct IncomingFramesHandler {
    handlers: Arc<MessageHandlers>,
    runnings: HashMap<MessageSeqNo, Box<HandleMessage + Send + 'static>>,
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

pub trait MessageHandlerFactory {
    fn create_message_handler(&self, seqno: MessageSeqNo) -> Box<HandleMessage + Send + 'static>;
}

pub trait HandleMessage {
    fn handle_message(&mut self, data: &[u8]) -> Result<()>;
    fn finish(&mut self) -> Result<Action>;
}
