use byteorder::{BigEndian, ByteOrder};

use Result;

const MAX_FRAME_SIZE: usize = FRAME_HEADER_SIZE + MAX_FRAME_DATA_SIZE;
const FRAME_HEADER_SIZE: usize = 4 + 1 + 2;
pub const MAX_FRAME_DATA_SIZE: usize = 0xFFFF;

pub const FLAG_ERROR: u8 = 0b0000_0001;

#[derive(Debug)]
pub struct FrameRecvBuf {
    buf: Vec<u8>,
    read_start: usize,
    write_start: usize,
}
impl FrameRecvBuf {
    pub fn new() -> Self {
        FrameRecvBuf {
            buf: vec![0; MAX_FRAME_SIZE * 4],
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
        if (self.write_start - self.read_start) < FRAME_HEADER_SIZE {
            return None;
        }
        let header_start = self.read_start;
        let data_start = header_start + FRAME_HEADER_SIZE;

        // header
        let seqno = BigEndian::read_u32(&self.buf[header_start..]);
        let flags = self.buf[header_start + 4];
        let data_len = BigEndian::read_u16(&self.buf[header_start + 5..]) as usize;
        let frame_end = data_start + data_len;

        // data
        if self.write_start < frame_end {
            if self.buf.len() < frame_end {
                let remaining_len = self.write_start - header_start;
                for i in 0..remaining_len {
                    self.buf[i] = self.buf[header_start + i];
                }
                self.read_start = 0;
                self.write_start = remaining_len;
            }
            return None;
        }

        // frame
        let frame = Frame {
            seqno,
            flags,
            data: &self.buf[data_start..frame_end],
        };

        self.read_start = frame_end;
        if self.read_start == self.write_start {
            self.read_start = 0;
            self.write_start = 0;
        }
        Some(frame)
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
            buf: vec![0; MAX_FRAME_SIZE * 2],
            read_start: 0,
            write_start: 0,
        }
    }

    pub fn peek_frame(&mut self) -> Option<FrameMut> {
        if !self.has_space() {
            None
        } else {
            let frame = FrameMut {
                frame_bytes: &mut self.buf[self.write_start..][..MAX_FRAME_SIZE],
            };
            frame.frame_bytes[4] = 0;
            Some(frame)
        }
    }

    pub fn fix_frame(&mut self, seqno: u32, result: &Result<usize>) {
        BigEndian::write_u32(&mut self.buf[self.write_start..], seqno);
        let frame_data_len = if let Ok(frame_data_len) = *result {
            debug_assert!(frame_data_len <= 0xFFFF);
            frame_data_len
        } else {
            self.buf[self.write_start + 4] = FLAG_ERROR;
            0
        };
        BigEndian::write_u16(&mut self.buf[self.write_start + 5..], frame_data_len as u16);
        self.write_start += FRAME_HEADER_SIZE + frame_data_len;
        debug_assert!(self.write_start <= self.buf.len());
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

// TODO: remove clone,copy
#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    pub seqno: u32, // TODO: u64
    flags: u8,
    pub data: &'a [u8],
}
impl<'a> Frame<'a> {
    pub fn is_error(&self) -> bool {
        (self.flags & FLAG_ERROR) != 0
    }

    pub fn is_end_of_message(&self) -> bool {
        self.data.len() < MAX_FRAME_DATA_SIZE
    }
}

#[derive(Debug)]
pub struct FrameMut<'a> {
    frame_bytes: &'a mut [u8],
}
impl<'a> FrameMut<'a> {
    pub fn data(&mut self) -> &mut [u8] {
        &mut self.frame_bytes[FRAME_HEADER_SIZE..]
    }
}

// | msg_seqno:32 | flags:8 | frame_len:16 | frame_data:* |
#[derive(Debug)]
pub struct FrameBuf {
    pub buffer: Vec<u8>,
    pub start: usize,
    pub end: usize,
}
impl FrameBuf {
    pub const HEADER_SIZE: usize = 4 + 1 + 2;
    pub const MAX_SIZE: usize = Self::HEADER_SIZE + Self::MAX_DATA_SIZE;
    pub const MAX_DATA_SIZE: usize = 0xFFFF;

    pub fn new() -> Self {
        FrameBuf {
            buffer: vec![0; Self::MAX_SIZE * 2],
            start: 0,
            end: 0,
        }
    }
    pub fn next_frame(&mut self) -> Option<Frame> {
        unimplemented!()
    }
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
    pub fn is_full(&self) -> bool {
        self.end == Self::MAX_SIZE
    }
    pub fn has_space(&self) -> bool {
        self.end <= Self::MAX_SIZE
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer[self.start..self.end]
    }
}

pub trait HandleFrame {
    // TODO: rename
    type Future;

    fn handle_frame(&mut self, frame: Frame) -> Result<Option<Self::Future>>;
}

#[derive(Debug)]
pub struct DummyFrameHandler;
impl HandleFrame for DummyFrameHandler {
    type Future = ();
    fn handle_frame(&mut self, _frame: Frame) -> Result<Option<Self::Future>> {
        panic!()
    }
}