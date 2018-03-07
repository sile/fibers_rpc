use std::fmt;
use byteorder::{BigEndian, WriteBytesExt};

use {Error, ProcedureId, Result};
use traits::IncrementalSerialize;

#[derive(Debug)]
pub enum Message<T> {
    Notification { procedure: ProcedureId, data: T },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageKind {
    Notification = 0,
}

pub struct OutgoingMessage {
    procedure: ProcedureId,
    kind: MessageKind,
    data: Box<IncrementalSerialize + Send>,

    is_header_written: bool,
}
impl OutgoingMessage {
    pub fn new<T>(message: Message<T>) -> Self
    where
        T: IncrementalSerialize + Send + 'static,
    {
        match message {
            Message::Notification { procedure, data } => OutgoingMessage {
                procedure,
                kind: MessageKind::Notification,
                data: Box::new(data),
                is_header_written: false,
            },
        }
    }
}

impl fmt::Debug for OutgoingMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OutgoingMessage {{ procedure:{}, kind:{:?}, .. }}",
            self.procedure, self.kind
        )
    }
}
impl IncrementalSerialize for OutgoingMessage {
    fn incremental_serialize(&mut self, mut buf: &mut [u8]) -> Result<usize> {
        let mut size = 0;
        if !self.is_header_written {
            track!(
                buf.write_u32::<BigEndian>(self.procedure)
                    .map_err(Error::from)
            )?;
            track!(buf.write_u8(self.kind as u8).map_err(Error::from))?;
            self.is_header_written = true;
            size = 5;
        }
        size += track!(self.data.incremental_serialize(buf))?;
        Ok(size)
    }
}
