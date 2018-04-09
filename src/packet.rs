use std::cmp;
use bytecodec::{self, ByteCount, Decode, Encode, Eos};
use bytecodec::bytes::CopyableBytesDecoder;
use byteorder::{BigEndian, ByteOrder};

use message::{MessageHeader, MessageId};

pub const MIN_PACKET_LEN: usize = PacketHeader::SIZE;
const MAX_PAYLOAD_LEN: usize = 0xFFFF;

#[derive(Debug, Copy, Clone)]
pub enum PacketKind {
    MiddleOfMessage = 0,
    EndOfMessage = 1,
    Error = 2,
    Cancel = 3,
}
impl PacketKind {
    fn from_u8(n: u8) -> bytecodec::Result<Self> {
        Ok(match n {
            0 => PacketKind::MiddleOfMessage,
            1 => PacketKind::EndOfMessage,
            2 => PacketKind::Error,
            3 => track_panic!(bytecodec::ErrorKind::Other, "Unimplemented"),
            _ => track_panic!(
                bytecodec::ErrorKind::InvalidInput,
                "Unknown packet kind: {}",
                n
            ),
        })
    }
}

#[derive(Debug)]
pub struct PacketHeader {
    message: MessageHeader,
    kind: PacketKind,
    payload_len: u16,
}
impl PacketHeader {
    pub const SIZE: usize = MessageHeader::SIZE + 1 + 2;

    fn write(&self, buf: &mut [u8]) {
        self.message.write(buf);
        buf[MessageHeader::SIZE] = self.kind as u8;
        BigEndian::write_u16(&mut buf[MessageHeader::SIZE + 1..], self.payload_len);
    }

    fn read(buf: &[u8]) -> bytecodec::Result<Self> {
        let message = track!(MessageHeader::read(buf))?;
        let kind = track!(PacketKind::from_u8(buf[MessageHeader::SIZE]))?;
        let payload_len = BigEndian::read_u16(&buf[MessageHeader::SIZE + 1..]);
        Ok(PacketHeader {
            message,
            kind,
            payload_len,
        })
    }
}

#[derive(Debug, Default)]
pub struct PacketHeaderDecoder {
    bytes: CopyableBytesDecoder<[u8; 16]>,
}
impl Decode for PacketHeaderDecoder {
    type Item = PacketHeader;

    fn decode(&mut self, buf: &[u8], eos: Eos) -> bytecodec::Result<(usize, Option<Self::Item>)> {
        let (size, item) = track!(self.bytes.decode(buf, eos))?;
        if let Some(bytes) = item {
            let header = track!(PacketHeader::read(&bytes[..]))?;
            Ok((size, Some(header)))
        } else {
            Ok((size, None))
        }
    }

    fn has_terminated(&self) -> bool {
        false
    }

    fn requiring_bytes(&self) -> ByteCount {
        self.bytes.requiring_bytes()
    }
}

#[derive(Debug)]
pub struct PacketizedMessageEncoder<E> {
    message_header: MessageHeader,
    message_encoder: E,
}
impl<E: Encode> PacketizedMessageEncoder<E> {
    pub fn new(message_header: MessageHeader, message_encoder: E) -> Self {
        PacketizedMessageEncoder {
            message_header,
            message_encoder,
        }
    }
}
impl<E: Encode> Encode for PacketizedMessageEncoder<E> {
    type Item = ();

    fn encode(&mut self, buf: &mut [u8], eos: Eos) -> bytecodec::Result<usize> {
        debug_assert!(buf.len() >= PacketHeader::SIZE);

        let limit = cmp::min(buf.len() - PacketHeader::SIZE, MAX_PAYLOAD_LEN);
        let payload_len = track!(
            self.message_encoder
                .encode(&mut buf[PacketHeader::SIZE..][..limit], eos)
        )?;
        let packet_header = PacketHeader {
            message: self.message_header.clone(),
            kind: if self.message_encoder.is_idle() {
                PacketKind::EndOfMessage
            } else {
                PacketKind::MiddleOfMessage
            },
            payload_len: payload_len as u16,
        };
        packet_header.write(buf);
        Ok(PacketHeader::SIZE + payload_len)
    }

    fn start_encoding(&mut self, _item: Self::Item) -> bytecodec::Result<()> {
        unreachable!()
    }

    fn is_idle(&self) -> bool {
        self.message_encoder.is_idle()
    }

    fn requiring_bytes(&self) -> ByteCount {
        ByteCount::Unknown
    }
}
