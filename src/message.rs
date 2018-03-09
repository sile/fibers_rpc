use byteorder::{BigEndian, ByteOrder};

use {ErrorKind, ProcedureId, Result};
use traits::{BoxDecoder, Encodable, Encode};

// TODO: 最上位ビットが1なら応答メッセージ、とする
pub type MessageSeqNo = u32; // TODO: u64

#[derive(Debug)]
pub enum Message {
    Request(RequestMesasge),
    Notification(Encodable), // TODO
}
impl Message {
    pub fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        match *self {
            Message::Request(ref mut x) => track!(x.encode(buf)),
            Message::Notification(ref mut x) => track!(x.encode(buf)),
        }
    }
}

#[derive(Debug)]
pub struct RequestMesasge {
    pub phase: u8, // TODO:

    pub procedure: ProcedureId,
    pub request_id: u32, // TODO: remove
    pub request_data: Encodable,
    pub response_decoder: BoxDecoder<()>,
}
impl RequestMesasge {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut offset = 0;
        while self.phase < 3 {
            match self.phase {
                0 => {
                    BigEndian::write_u32(&mut buf[offset..], self.procedure);
                    offset += 4;
                    self.phase = 1;
                }
                1 => {
                    BigEndian::write_u32(&mut buf[offset..], self.procedure);
                    offset += 4;
                    self.phase = 2;
                }
                2 => {
                    let size = track!(self.request_data.encode(&mut buf[offset..]))?;
                    offset += size;
                    if size == 0 {
                        self.phase = 3;
                    }
                    break;
                }
                _ => unreachable!(),
            }
        }
        Ok(offset)
    }
}

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
