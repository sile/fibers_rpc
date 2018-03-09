use std::io::{self, Read, Write};
use fibers::net::TcpStream;
use futures::{Async, Future, Poll};

use {Error, Result};
use frame::{Frame, FrameMut, FrameRecvBuf, FrameSendBuf, MAX_FRAME_DATA_SIZE};

#[derive(Debug)]
pub struct FrameStream {
    transport_stream: TcpStream,
    send_buf: FrameSendBuf,
    recv_buf: FrameRecvBuf,
}
impl FrameStream {
    pub fn new(transport_stream: TcpStream) -> Self {
        unsafe {
            let _ = transport_stream.with_inner(|s| s.set_nodelay(true));
        }
        FrameStream {
            transport_stream,
            send_buf: FrameSendBuf::new(),
            recv_buf: FrameRecvBuf::new(),
        }
    }

    pub fn send_frame<F>(&mut self, seqno: u32, f: F) -> Result<Option<bool>>
    where
        F: FnOnce(FrameMut) -> Result<usize>,
    {
        let maybe_result = if let Some(frame) = self.send_buf.peek_frame() {
            Some(track!(f(frame)))
        } else {
            None
        };
        match maybe_result {
            None => Ok(None),
            Some(result) => {
                self.send_buf.fix_frame(seqno, &result);
                result.map(|data_len| Some(data_len < MAX_FRAME_DATA_SIZE))
            }
        }
    }

    pub fn recv_frame(&mut self) -> Option<Frame> {
        self.recv_buf.next_frame()
    }
}
impl Future for FrameStream {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while !self.send_buf.is_empty() {
            match self.transport_stream.write(self.send_buf.readable_region()) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        break;
                    }
                    return Err(track!(Error::from(e)));
                }
                Ok(0) => {
                    return Ok(Async::Ready(()));
                }
                Ok(size) => {
                    self.send_buf.consume_readable_region(size);
                }
            }
        }
        while !self.recv_buf.is_full() {
            match self.transport_stream.read(self.recv_buf.writable_region()) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        break;
                    }
                    return Err(track!(Error::from(e)));
                }
                Ok(0) => {
                    return Ok(Async::Ready(()));
                }
                Ok(size) => {
                    self.recv_buf.consume_writable_region(size);
                }
            }
        }
        Ok(Async::NotReady)
    }
}
