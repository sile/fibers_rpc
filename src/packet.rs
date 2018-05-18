use bytecodec::bytes::CopyableBytesDecoder;
use bytecodec::marker::Never;
use bytecodec::{self, ByteCount, Decode, Encode, Eos};
use byteorder::{BigEndian, ByteOrder};
use std::cmp;

use message::{MessageHeader, OutgoingMessage};

pub const MIN_PACKET_LEN: usize = PacketHeader::SIZE;
pub const MAX_PACKET_LEN: usize = PacketHeader::SIZE + MAX_PAYLOAD_LEN;
pub const MAX_PAYLOAD_LEN: usize = 0xFFFF;

const FLAG_END_OF_MESSAGE: u8 = 0b0000_0001;
const FLAG_ASYNC: u8 = 0b0000_0010;

#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub message: MessageHeader,
    pub flags: u8,
    pub payload_len: u16,
}
impl PacketHeader {
    pub const SIZE: usize = MessageHeader::SIZE + 1 + 2;

    fn write(&self, buf: &mut [u8]) {
        self.message.write(buf);
        buf[MessageHeader::SIZE] = self.flags;
        BigEndian::write_u16(&mut buf[MessageHeader::SIZE + 1..], self.payload_len);
    }

    fn read(buf: &[u8]) -> Self {
        let mut message = MessageHeader::read(buf);
        let flags = buf[MessageHeader::SIZE];
        message.async = (flags & FLAG_ASYNC) != 0;
        let payload_len = BigEndian::read_u16(&buf[MessageHeader::SIZE + 1..]);
        PacketHeader {
            message,
            flags,
            payload_len,
        }
    }

    pub fn is_end_of_message(&self) -> bool {
        (self.flags & FLAG_END_OF_MESSAGE) != 0
    }

    pub fn is_async(&self) -> bool {
        (self.flags & FLAG_ASYNC) != 0
    }
}

#[derive(Debug, Default)]
pub struct PacketHeaderDecoder {
    bytes: CopyableBytesDecoder<[u8; PacketHeader::SIZE]>,
}
impl Decode for PacketHeaderDecoder {
    type Item = PacketHeader;

    fn decode(&mut self, buf: &[u8], eos: Eos) -> bytecodec::Result<usize> {
        track!(self.bytes.decode(buf, eos))
    }

    fn finish_decoding(&mut self) -> bytecodec::Result<Self::Item> {
        let bytes = track!(self.bytes.finish_decoding())?;
        let header = PacketHeader::read(&bytes[..]);
        Ok(header)
    }

    fn requiring_bytes(&self) -> ByteCount {
        self.bytes.requiring_bytes()
    }

    fn is_idle(&self) -> bool {
        self.bytes.is_idle()
    }
}

#[derive(Debug)]
pub struct PacketizedMessage {
    message: OutgoingMessage,
}
impl PacketizedMessage {
    pub fn new(message: OutgoingMessage) -> Self {
        PacketizedMessage { message }
    }

    pub fn header(&self) -> &MessageHeader {
        &self.message.header
    }
}
impl Encode for PacketizedMessage {
    type Item = Never;

    fn encode(&mut self, buf: &mut [u8], eos: Eos) -> bytecodec::Result<usize> {
        debug_assert!(buf.len() >= PacketHeader::SIZE);

        let limit = cmp::min(buf.len() - PacketHeader::SIZE, MAX_PAYLOAD_LEN);
        let payload_len = track!(
            self.message
                .payload
                .encode(&mut buf[PacketHeader::SIZE..][..limit], eos)
        )?;

        let flags = (self.message.payload.is_idle() as u8 * FLAG_END_OF_MESSAGE)
            | (self.message.header.async as u8 * FLAG_ASYNC);
        let packet_header = PacketHeader {
            message: self.message.header.clone(),
            flags,
            payload_len: payload_len as u16,
        };
        packet_header.write(buf);
        Ok(PacketHeader::SIZE + payload_len)
    }

    fn start_encoding(&mut self, _item: Self::Item) -> bytecodec::Result<()> {
        unreachable!()
    }

    fn is_idle(&self) -> bool {
        self.message.payload.is_idle()
    }

    fn requiring_bytes(&self) -> ByteCount {
        ByteCount::Unknown
    }
}
