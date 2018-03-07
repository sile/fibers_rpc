use std::io::{Read, Write};

use {ProcedureId, Result};

pub trait Cast {
    const PROCEDURE: ProcedureId;
    type Notification: IncrementalSerialize + IncrementalDeserialize;
}

pub trait IncrementalSerialize {
    fn incremental_serialize(&mut self, buf: &mut [u8]) -> Result<usize>;
}

pub trait IncrementalDeserialize {
    fn incremental_deserialize(&mut self, buf: &[u8]) -> Result<()>;
}

pub trait Serialize {
    fn serialize<W: Write>(&self, writer: W) -> Result<()>;
}

pub trait Deserialize: Sized {
    fn deserialize<R: Read>(reader: R) -> Result<Self>;
}
