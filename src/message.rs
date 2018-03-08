use byteorder::{BigEndian, ByteOrder};

use {ErrorKind, ProcedureId, Result};
use traits::Encode;

#[derive(Debug)]
pub struct OutgoingMessage<E> {
    procedure: Option<ProcedureId>,
    encoder: E,
}
impl<E> OutgoingMessage<E> {
    pub fn new(procedure: ProcedureId, encoder: E) -> Self {
        OutgoingMessage {
            procedure: Some(procedure),
            encoder,
        }
    }
}
impl<T, E> Encode<T> for OutgoingMessage<E>
where
    E: Encode<T>,
{
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut written_size = 0;
        if let Some(id) = self.procedure.take() {
            track_assert!(buf.len() >= 4, ErrorKind::InvalidInput);
            BigEndian::write_u32(buf, id);
            written_size = 4;
        }
        written_size += track!(self.encoder.encode(&mut buf[written_size..]))?;
        Ok(written_size)
    }
}
