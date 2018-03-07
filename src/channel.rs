use std::collections::VecDeque;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Duration;
use byteorder::{BigEndian, ByteOrder};
use fibers::net::TcpStream;
use fibers::net::futures::Connect;
use fibers::sync::mpsc;
use fibers::time::timer::{self, Timeout};
use futures::{Async, Future, Poll, Stream};
use slog::Logger;
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use message::OutgoingMessage;
use traits::IncrementalSerialize;

// TODO: parameter
const RECONNECT_INTERVAL: u64 = 10_000;

#[derive(Debug)]
pub struct RpcChannel {
    connection: Connection,
    outgoing_message_rx: mpsc::Receiver<OutgoingMessage>,
    sending_messages: VecDeque<SendingMessage>,
    next_message_seqno: u32,
    frame: Frame,
}
impl RpcChannel {
    pub fn with_stream(logger: Logger, stream: TcpStream) -> (Self, RpcChannelHandle) {
        let (outgoing_message_tx, outgoing_message_rx) = mpsc::channel();
        let channel = RpcChannel {
            connection: Connection::with_stream(logger, stream),
            outgoing_message_rx,
            sending_messages: VecDeque::new(),
            next_message_seqno: 0,
            frame: Frame::new(),
        };
        let handle = RpcChannelHandle {
            outgoing_message_tx,
        };
        (channel, handle)
    }

    pub fn new(logger: Logger, peer: SocketAddr) -> (Self, RpcChannelHandle) {
        let (outgoing_message_tx, outgoing_message_rx) = mpsc::channel();
        let channel = RpcChannel {
            connection: Connection::new(logger, peer),
            outgoing_message_rx,
            sending_messages: VecDeque::new(),
            next_message_seqno: 0,
            frame: Frame::new(),
        };
        let handle = RpcChannelHandle {
            outgoing_message_tx,
        };
        (channel, handle)
    }
}
// TODO: Stream
impl Future for RpcChannel {
    type Item = (); // TODO: IncomingMessage
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(message) = self.outgoing_message_rx.poll().expect("Never fails") {
            if let Some(message) = message {
                let message = SendingMessage::new(self.next_message_seqno, message);
                self.next_message_seqno += 1;
                self.sending_messages.push_back(message); // TODO: limit
            } else {
                return Ok(Async::Ready(()));
            }
        }

        while track!(self.connection.poll())?.is_ready() {
            while self.frame.has_space() {
                if let Some(mut message) = self.sending_messages.pop_front() {
                    match track!(message.fill_frame(&mut self.frame)) {
                        Err(e) => {
                            debug!(self.connection.logger, "Cannot fill frame: {}", e);
                            message.fill_error_frame(&mut self.frame);
                        }
                        Ok(false) => {
                            self.sending_messages.push_back(message);
                        }
                        Ok(true) => {}
                    }
                } else {
                    break;
                }
            }
            if !self.connection.send_frame(&mut self.frame) {
                break;
            }
        }

        Ok(Async::NotReady)
    }
}

#[derive(Debug, Clone)]
pub struct RpcChannelHandle {
    outgoing_message_tx: mpsc::Sender<OutgoingMessage>,
}
impl RpcChannelHandle {
    pub fn send_message(&self, message: OutgoingMessage) {
        let _ = self.outgoing_message_tx.send(message); // TODO: metrics
    }
}

#[derive(Debug)]
struct SendingMessage {
    seqno: u32,
    message: OutgoingMessage,
}
impl SendingMessage {
    fn new(seqno: u32, message: OutgoingMessage) -> Self {
        SendingMessage { seqno, message }
    }
    fn fill_frame(&mut self, frame: &mut Frame) -> Result<bool> {
        let buf = &mut frame.buffer[frame.start..];

        BigEndian::write_u32(&mut buf[0..], self.seqno);

        let size = track!(self.message.incremental_serialize(&mut buf[7..]))?;
        BigEndian::write_u16(&mut buf[5..], size as u16);

        frame.end += Frame::HEADER_SIZE + size;
        let is_completed = size < Frame::MAX_DATA_SIZE;
        Ok(is_completed)
    }
    fn fill_error_frame(&self, frame: &mut Frame) {
        let buf = &mut frame.buffer[frame.start..];

        BigEndian::write_u32(&mut buf[0..], self.seqno);
        buf[4] = FLAG_ERROR;
        BigEndian::write_u16(&mut buf[5..], 0);

        frame.end += Frame::HEADER_SIZE;
    }
}

const FLAG_ERROR: u8 = 0b0000_0001;

// TODO: リトライは上のエイヤーで行う
#[derive(Debug)]
struct Connection {
    logger: Logger,
    peer: Option<SocketAddr>,
    state: ConnectionState,
}
impl Connection {
    fn with_stream(logger: Logger, stream: TcpStream) -> Self {
        unsafe {
            let _ = stream.with_inner(|s| s.set_nodelay(false));
        }
        let state = ConnectionState::Connected(stream);
        Connection {
            logger,
            peer: None,
            state,
        }
    }
    fn new(logger: Logger, peer: SocketAddr) -> Self {
        let state = ConnectionState::Wait(timer::timeout(Duration::default()));
        Connection {
            logger,
            peer: Some(peer),
            state,
        }
    }

    fn send_frame(&mut self, frame: &mut Frame) -> bool {
        if frame.is_empty() {
            return false;
        }
        let next = if let ConnectionState::Connected(ref mut stream) = self.state {
            match stream.write(frame.as_bytes()) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return false;
                    }

                    error!(self.logger, "Cannot write frame: {}", e);
                    let timeout = timer::timeout(Duration::from_millis(RECONNECT_INTERVAL));
                    Some(ConnectionState::Wait(timeout))
                }
                Ok(size) => {
                    frame.start += size;
                    if frame.start == frame.end {
                        frame.start = 0;
                        frame.end = 0;
                    }
                    None
                }
            }
        } else {
            return false;
        };
        if let Some(next) = next {
            self.state = next;
        }
        true
    }
}
impl Future for Connection {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let next = match self.state {
                ConnectionState::Wait(ref mut f) => {
                    if track!(f.poll().map_err(|e| ErrorKind::Other.cause(e)))?.is_ready() {
                        if let Some(peer) = self.peer {
                            debug!(self.logger, "Starts TCP connecting");
                            ConnectionState::Connecting(TcpStream::connect(peer))
                        } else {
                            track_panic!(ErrorKind::Other, "TODO");
                        }
                    } else {
                        break;
                    }
                }
                ConnectionState::Connecting(ref mut f) => {
                    match track!(f.poll().map_err(Error::from)) {
                        Err(e) => {
                            debug!(self.logger, "Cannot TCP connect: {}", e);
                            let timeout = timer::timeout(Duration::from_millis(RECONNECT_INTERVAL));
                            ConnectionState::Wait(timeout)
                        }
                        Ok(Async::NotReady) => {
                            break;
                        }
                        Ok(Async::Ready(stream)) => {
                            info!(self.logger, "TCP connected: stream={:?}", stream);
                            unsafe {
                                let _ = stream.with_inner(|s| s.set_nodelay(false));
                            }
                            ConnectionState::Connected(stream)
                        }
                    }
                }
                ConnectionState::Connected(_) => {
                    return Ok(Async::Ready(()));
                }
            };
            self.state = next;
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
enum ConnectionState {
    Wait(Timeout),
    Connecting(Connect),
    Connected(TcpStream),
}

// | msg_seqno:32 | flags:8 | frame_len:16 | frame_data:* |
//
// s/Frame/FrameBuffer/
#[derive(Debug)]
struct Frame {
    buffer: Vec<u8>,
    start: usize,
    end: usize,
}
impl Frame {
    const HEADER_SIZE: usize = 4 + 1 + 2;
    const MAX_SIZE: usize = Self::HEADER_SIZE + Self::MAX_DATA_SIZE;
    const MAX_DATA_SIZE: usize = 0xFFFF;

    fn new() -> Self {
        Frame {
            buffer: vec![0; Self::MAX_SIZE * 2],
            start: 0,
            end: 0,
        }
    }
    fn is_empty(&self) -> bool {
        self.start == self.end
    }
    fn has_space(&self) -> bool {
        self.end <= Self::MAX_SIZE
    }
    fn as_bytes(&self) -> &[u8] {
        &self.buffer[self.start..self.end]
    }
}
