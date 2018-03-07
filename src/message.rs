use std::fmt;
use byteorder::{BigEndian, WriteBytesExt};

use {Error, ProcedureId, Result};
use traits::{IncrementalDeserialize, IncrementalSerialize};

#[derive(Debug)]
pub struct Message<T> {
    pub procedure: ProcedureId,
    pub data: T,
}

pub struct IncomingMessage {
    pub data: Box<IncrementalDeserialize + Send>,
}
impl fmt::Debug for IncomingMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IncomingMessage {{ .. }}")
    }
}

pub struct OutgoingMessage {
    procedure: ProcedureId,
    data: Box<IncrementalSerialize + Send>,

    is_header_written: bool,
}
impl OutgoingMessage {
    pub fn new<T>(message: Message<T>) -> Self
    where
        T: IncrementalSerialize + Send + 'static,
    {
        OutgoingMessage {
            procedure: message.procedure,
            data: Box::new(message.data),
            is_header_written: false,
        }
    }
}

impl fmt::Debug for OutgoingMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutgoingMessage {{ procedure:{}, .. }}", self.procedure,)
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
            self.is_header_written = true;
            size = 4;
        }
        size += track!(self.data.incremental_serialize(buf))?;
        Ok(size)
    }
}
