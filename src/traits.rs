use std::io::{Cursor, Read, Write};

use {Error, ProcedureId, Result};

pub trait Cast {
    const PROCEDURE: ProcedureId;
    type Notification: IncrementalSerialize + IncrementalDeserialize + Send + 'static;
}

pub trait IncrementalSerialize {
    fn incremental_serialize(&mut self, buf: &mut [u8]) -> Result<usize>;
}

pub trait IncrementalDeserialize {
    fn incremental_deserialize(&mut self, buf: &[u8]) -> Result<()>;
}

pub trait Serialize: Sized {
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;
    fn into_serializable(self) -> Serializable<Self> {
        Serializable {
            object: Some(self),
            buffer: Cursor::new(Vec::new()),
        }
    }
}

pub trait Deserialize: Sized {
    fn deserialize<R: Read>(reader: R) -> Result<Self>;
}

#[derive(Debug)]
pub struct Serializable<T> {
    object: Option<T>,
    buffer: Cursor<Vec<u8>>,
}
impl<T: Serialize> IncrementalSerialize for Serializable<T> {
    fn incremental_serialize(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(object) = self.object.take() {
            let mut buffer = Vec::new();
            track!(object.serialize(&mut buffer))?;
            self.buffer = Cursor::new(buffer);
        }
        track!(self.buffer.incremental_serialize(buf))
    }
}

impl<T: AsRef<[u8]>> IncrementalSerialize for Cursor<T> {
    fn incremental_serialize(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.read(buf).map_err(Error::from)
    }
}
