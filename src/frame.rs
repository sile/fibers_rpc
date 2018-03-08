use byteorder::{BigEndian, ByteOrder};

use Result;

#[derive(Debug)]
pub struct Frame<'a> {
    pub seqno: u32,
    flags: u8,
    pub data: &'a [u8],
}
impl<'a> Frame<'a> {
    pub const FLAG_ERROR: u8 = 0b0000_0001;
    pub const FLAG_EOF: u8 = 0b0000_0010;
    pub const FLAG_SOF: u8 = 0b0000_0100;

    pub fn is_error(&self) -> bool {
        (self.flags & Self::FLAG_ERROR) != 0
    }

    pub fn is_eof(&self) -> bool {
        (self.flags & Self::FLAG_EOF) != 0
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
        if self.as_bytes().len() < Self::HEADER_SIZE {
            None
        } else {
            let seqno = BigEndian::read_u32(self.as_bytes());
            let mut flags = self.as_bytes()[4];
            let len = BigEndian::read_u16(&self.as_bytes()[5..]) as usize;

            if (self.as_bytes().len() - FrameBuf::HEADER_SIZE) < len {
                None
            } else {
                assert_eq!(self.as_bytes().len() - FrameBuf::HEADER_SIZE, len); // TODO
                let start = self.start + FrameBuf::HEADER_SIZE;
                let end = start + len;
                self.start = end;
                if self.start == self.end {
                    self.start = 0;
                    self.end = 0;
                }
                if self.end > Self::MAX_SIZE {
                    unimplemented!()
                }

                // TODO: set by sender
                if len < FrameBuf::MAX_DATA_SIZE {
                    flags |= Frame::FLAG_EOF;
                }
                let frame = Frame {
                    seqno,
                    flags,
                    data: &self.buffer[start..end],
                };
                Some(frame)
            }
        }
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
    type Future;

    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Future>>;
}

#[derive(Debug)]
pub struct DummyFrameHandler;
impl HandleFrame for DummyFrameHandler {
    type Future = ();
    fn handle_frame(&mut self, _frame: &Frame) -> Result<Option<Self::Future>> {
        panic!()
    }
}
