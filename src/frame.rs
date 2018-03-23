use byteorder::{BigEndian, ByteOrder};

use {Error, Result};
use message::MessageId;

const MAX_FRAME_SIZE: usize = FRAME_HEADER_SIZE + MAX_FRAME_DATA_SIZE;
const FRAME_HEADER_SIZE: usize = 8 + 1 + 2;
const MAX_FRAME_DATA_SIZE: usize = 0xFFFF;

const RECV_BUF_SIZE: usize = MAX_FRAME_SIZE * 4;
const SEND_BUF_SIZE: usize = MAX_FRAME_SIZE * 2;

const FLAG_ERROR: u8 = 0b0000_0001;

#[derive(Debug)]
pub struct Frame<'a> {
    message_id: MessageId,
    flags: u8,
    data: &'a [u8],
}
impl<'a> Frame<'a> {
    pub fn message_id(&self) -> MessageId {
        self.message_id
    }

    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn is_error(&self) -> bool {
        (self.flags & FLAG_ERROR) != 0
    }

    pub fn is_end_of_message(&self) -> bool {
        self.data.len() < MAX_FRAME_DATA_SIZE
    }

    fn guess_frame_len(buf: &[u8]) -> usize {
        if buf.len() < FRAME_HEADER_SIZE {
            FRAME_HEADER_SIZE
        } else {
            let data_len = BigEndian::read_u16(&buf[9..]) as usize;
            FRAME_HEADER_SIZE + data_len
        }
    }

    fn from_bytes(buf: &'a [u8]) -> Self {
        let message_id = MessageId::from_u64(BigEndian::read_u64(buf));
        let flags = buf[8];
        let data_len = BigEndian::read_u16(&buf[9..]) as usize;
        let data = &buf[FRAME_HEADER_SIZE..][..data_len];
        Frame {
            message_id,
            flags,
            data,
        }
    }
}

#[derive(Debug)]
pub struct FrameMut<'a>(&'a mut FrameSendBuf);
impl<'a> FrameMut<'a> {
    pub fn data(&mut self) -> &mut [u8] {
        &mut self.0.buf[self.0.write_start + FRAME_HEADER_SIZE..][..MAX_FRAME_DATA_SIZE]
    }

    pub fn ok(mut self, message_id: MessageId, data_len: usize) -> bool {
        debug_assert!(data_len <= MAX_FRAME_DATA_SIZE);

        BigEndian::write_u64(self.header(), message_id.as_u64());
        self.header()[8] = 0;
        BigEndian::write_u16(&mut self.header()[9..], data_len as u16);

        self.0.write_start += FRAME_HEADER_SIZE + data_len;
        debug_assert!(self.0.write_start <= self.0.buf.len());

        data_len < MAX_FRAME_DATA_SIZE
    }

    pub fn err(mut self, message_id: MessageId) {
        BigEndian::write_u64(self.header(), message_id.as_u64());
        self.header()[8] = FLAG_ERROR;
        BigEndian::write_u16(&mut self.header()[9..], 0);

        self.0.write_start += FRAME_HEADER_SIZE;
        debug_assert!(self.0.write_start <= self.0.buf.len());
    }

    fn header(&mut self) -> &mut [u8] {
        &mut self.0.buf[self.0.write_start..]
    }
}

pub trait HandleFrame {
    type Item;

    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>>;
    fn handle_error(&mut self, message_id: MessageId, error: Error);
}

#[derive(Debug)]
pub struct FrameRecvBuf {
    buf: Vec<u8>,
    read_start: usize,
    write_start: usize,
}
impl FrameRecvBuf {
    pub fn new() -> Self {
        FrameRecvBuf {
            buf: vec![0; RECV_BUF_SIZE],
            read_start: 0,
            write_start: 0,
        }
    }

    pub fn is_full(&self) -> bool {
        self.write_start == self.buf.len()
    }

    pub fn writable_region(&mut self) -> &mut [u8] {
        &mut self.buf[self.write_start..]
    }

    pub fn consume_writable_region(&mut self, size: usize) {
        self.write_start += size;
        debug_assert!(self.write_start <= self.buf.len());
    }

    pub fn next_frame(&mut self) -> Option<Frame> {
        let frame_len = Frame::guess_frame_len(&self.buf[self.read_start..self.write_start]);
        let frame_end = self.read_start + frame_len;
        if self.write_start < frame_end {
            if self.buf.len() < frame_end {
                let remaining_len = self.write_start - self.read_start;
                let (left, right) = self.buf.split_at_mut(self.read_start);
                (&mut left[..remaining_len]).copy_from_slice(&right[..remaining_len]);
                self.read_start = 0;
                self.write_start = remaining_len;
            }
            None
        } else {
            let frame = Frame::from_bytes(&self.buf[self.read_start..self.write_start]);
            self.read_start += frame_len;
            debug_assert!(self.read_start <= self.write_start);

            if self.read_start == self.write_start {
                self.read_start = 0;
                self.write_start = 0;
            }
            Some(frame)
        }
    }
}

#[derive(Debug)]
pub struct FrameSendBuf {
    buf: Vec<u8>,
    read_start: usize,
    write_start: usize,
}
impl FrameSendBuf {
    pub fn new() -> Self {
        FrameSendBuf {
            buf: vec![0; SEND_BUF_SIZE],
            read_start: 0,
            write_start: 0,
        }
    }

    pub fn next_frame(&mut self) -> Option<FrameMut> {
        if !self.has_space() {
            None
        } else {
            Some(FrameMut(self))
        }
    }

    pub fn readable_region(&self) -> &[u8] {
        &self.buf[self.read_start..self.write_start]
    }

    pub fn consume_readable_region(&mut self, size: usize) {
        self.read_start += size;
        debug_assert!(self.read_start <= self.write_start);
        if self.read_start == self.write_start {
            self.read_start = 0;
            self.write_start = 0;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.read_start == self.write_start
    }

    fn has_space(&self) -> bool {
        (self.buf.len() - self.write_start) >= MAX_FRAME_SIZE
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use message::MessageId;
    use super::*;

    #[test]
    fn send_buf_ok() {
        let mut buf = FrameSendBuf::new();
        assert!(buf.is_empty());
        assert!(buf.readable_region().is_empty());

        {
            let message_id = MessageId::from_u64(0xFF);
            let mut frame = buf.next_frame().unwrap();
            (&mut frame.data()[..3]).copy_from_slice("foo".as_bytes());
            let end_of_message = frame.ok(message_id, 3);
            assert!(end_of_message);
        }
        assert_eq!(buf.readable_region().len(), FRAME_HEADER_SIZE + 3);
        assert_eq!(
            buf.readable_region(),
            &[0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0, 3, b'f', b'o', b'o'][..]
        );

        assert!(!buf.is_empty());
        buf.consume_readable_region(FRAME_HEADER_SIZE + 3);
        assert!(buf.readable_region().is_empty());
        assert!(buf.is_empty());
    }

    #[test]
    fn send_buf_error() {
        let mut buf = FrameSendBuf::new();
        {
            let message_id = MessageId::from_u64(0xFF);
            let mut frame = buf.next_frame().unwrap();
            (&mut frame.data()[..3]).copy_from_slice("bar".as_bytes());
            frame.err(message_id);
        }
        assert_eq!(buf.readable_region().len(), FRAME_HEADER_SIZE);
        assert_eq!(
            buf.readable_region(),
            &[0, 0, 0, 0, 0, 0, 0, 0xFF, FLAG_ERROR, 0, 0][..]
        );
        buf.consume_readable_region(FRAME_HEADER_SIZE);
        assert!(buf.is_empty());
    }

    #[test]
    fn send_buf_full() {
        let mut buf = FrameSendBuf::new();

        {
            let message_id = MessageId::from_u64(0xFF);
            let frame = buf.next_frame().unwrap();
            let end_of_message = frame.ok(message_id, MAX_FRAME_DATA_SIZE);
            assert!(!end_of_message);
        }
        assert_eq!(buf.readable_region().len(), MAX_FRAME_SIZE);
        assert!(!buf.is_empty());

        {
            let message_id = MessageId::from_u64(0xFF);
            let frame = buf.next_frame().unwrap();
            let end_of_message = frame.ok(message_id, MAX_FRAME_DATA_SIZE);
            assert!(!end_of_message);
        }
        assert_eq!(buf.readable_region().len(), MAX_FRAME_SIZE * 2);
        assert!(!buf.is_empty());

        assert!(buf.next_frame().is_none()); // full

        buf.consume_readable_region(MAX_FRAME_SIZE * 2 - 1);
        assert!(buf.next_frame().is_none()); // full

        buf.consume_readable_region(1);
        assert!(buf.next_frame().is_some()); // not full
    }

    #[test]
    fn recv_buf_small_message() {
        let mut buf = FrameRecvBuf::new();
        assert!(!buf.is_full());
        assert!(buf.next_frame().is_none());
        assert_eq!(buf.writable_region().len(), RECV_BUF_SIZE);

        let size = buf.writable_region()
            .write(&[0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0, 3, b'f', b'o', b'o'][..])
            .unwrap();
        buf.consume_writable_region(size);
        assert_eq!(buf.writable_region().len(), RECV_BUF_SIZE - size);
        {
            let frame = buf.next_frame().unwrap();
            assert_eq!(frame.message_id().as_u64(), 0xFF);
            assert_eq!(frame.data(), b"foo");
            assert!(!frame.is_error());
            assert!(frame.is_end_of_message());
        }
        assert!(!buf.is_full());
        assert!(buf.next_frame().is_none());
    }

    #[test]
    fn recv_buf_fragmented_message() {
        let mut buf = FrameRecvBuf::new();

        let size = buf.writable_region()
            .write(&[0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0, 3, b'f'][..])
            .unwrap();
        buf.consume_writable_region(size);
        assert!(buf.next_frame().is_none());

        let size = buf.writable_region().write(&[b'o', b'o'][..]).unwrap();
        buf.consume_writable_region(size);
        {
            let frame = buf.next_frame().unwrap();
            assert_eq!(frame.message_id().as_u64(), 0xFF);
            assert_eq!(frame.data(), b"foo");
            assert!(!frame.is_error());
            assert!(frame.is_end_of_message());
        }
        assert!(!buf.is_full());
        assert!(buf.next_frame().is_none());
    }

    #[test]
    fn recv_buf_full() {
        let mut buf = FrameRecvBuf::new();

        for _ in 0..(RECV_BUF_SIZE / MAX_FRAME_SIZE) {
            buf.writable_region()
                .write_all(&[0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0xFF, 0xFF][..])
                .unwrap();
            buf.consume_writable_region(FRAME_HEADER_SIZE);
            buf.writable_region().write_all(&[0; 0xFFFF][..]).unwrap();
            buf.consume_writable_region(0xFFFF);
        }
        assert!(buf.is_full());

        for _ in 0..(RECV_BUF_SIZE / MAX_FRAME_SIZE) - 1 {
            {
                let frame = buf.next_frame().unwrap();
                assert_eq!(frame.message_id().as_u64(), 0xFF);
                assert_eq!(frame.data().len(), 0xFFFF);
                assert!(!frame.is_error());
                assert!(!frame.is_end_of_message());
            }
            assert!(buf.is_full());
        }

        assert!(buf.next_frame().is_some());
        assert!(!buf.is_full());
        assert!(buf.next_frame().is_none());
    }

    #[test]
    fn recv_buf_shift() {
        let mut buf = FrameRecvBuf::new();

        for _ in 0..(RECV_BUF_SIZE / MAX_FRAME_SIZE) - 1 {
            buf.writable_region()
                .write_all(&[0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0xFF, 0xFF][..])
                .unwrap();
            buf.consume_writable_region(FRAME_HEADER_SIZE);
            buf.consume_writable_region(0xFFFF);
        }
        buf.writable_region()
            .write_all(&[0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0xFF, 0xF9][..])
            .unwrap();
        buf.consume_writable_region(FRAME_HEADER_SIZE);
        buf.consume_writable_region(0xFFF9);
        assert!(!buf.is_full());

        buf.writable_region()
            .write_all(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66])
            .unwrap();
        buf.consume_writable_region(6);
        assert!(buf.is_full());
        assert!(buf.writable_region().is_empty());

        while buf.next_frame().is_some() {}
        assert!(!buf.is_full());

        buf.writable_region()
            .write_all(&[0x77, 0x88, 0, 0, 3, b'f', b'o', b'o'][..])
            .unwrap();
        buf.consume_writable_region(8);

        {
            let frame = buf.next_frame().unwrap();
            assert_eq!(frame.message_id().as_u64(), 0x1122_3344_5566_7788);
            assert_eq!(frame.data(), b"foo");
            assert!(!frame.is_error());
            assert!(frame.is_end_of_message());
        }
        assert!(buf.next_frame().is_none());
    }
}
