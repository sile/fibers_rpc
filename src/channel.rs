use std::collections::VecDeque;
use std::io::{self, Read, Write};
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
use frame::{DummyFrameHandler, FrameBuf, HandleFrame, FLAG_ERROR};
use message::Message;

// TODO: parameter
const RECONNECT_INTERVAL: u64 = 10_000;

#[derive(Debug)]
pub struct RpcChannel<H> {
    connection: Connection,
    outgoing_message_rx: mpsc::Receiver<Message>,
    sending_messages: VecDeque<SendingMessage>,
    next_message_seqno: u32,
    frame: FrameBuf,
    read_frame: FrameBuf,
    frame_handler: H,
}
impl RpcChannel<DummyFrameHandler> {
    pub fn new(logger: Logger, peer: SocketAddr) -> (Self, RpcChannelHandle) {
        let (outgoing_message_tx, outgoing_message_rx) = mpsc::channel();
        let channel = RpcChannel {
            connection: Connection::new(logger, peer),
            outgoing_message_rx,
            sending_messages: VecDeque::new(),
            next_message_seqno: 0,
            frame: FrameBuf::new(),
            read_frame: FrameBuf::new(),
            frame_handler: DummyFrameHandler,
        };
        let handle = RpcChannelHandle {
            outgoing_message_tx,
        };
        (channel, handle)
    }
}
impl<H: HandleFrame> Stream for RpcChannel<H> {
    type Item = H::Future;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Async::Ready(message) = self.outgoing_message_rx.poll().expect("Never fails") {
            if let Some(message) = message {
                // TODO: 外だし
                // (この中で`Message`ではなく`Encodable`だけを意識するようにしたい and message_rxは外に)
                let message = SendingMessage::new(self.next_message_seqno, message);
                self.next_message_seqno += 1;
                self.sending_messages.push_back(message); // TODO: limit
            } else {
                return Ok(Async::Ready(None));
            }
        }

        while track!(self.connection.poll())?.is_ready() {
            self.connection.recv_frame(&mut self.read_frame);
            if let Some(frame) = self.read_frame.next_frame() {
                if let Some(future) = track!(self.frame_handler.handle_frame(frame))? {
                    return Ok(Async::Ready(Some(future)));
                }
            }

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
    outgoing_message_tx: mpsc::Sender<Message>,
}
impl RpcChannelHandle {
    pub fn send_message(&self, message: Message) {
        let _ = self.outgoing_message_tx.send(message); // TODO: metrics
    }
}

#[derive(Debug)]
struct SendingMessage {
    seqno: u32,
    message: Message,
}
impl SendingMessage {
    fn new(seqno: u32, mut message: Message) -> Self {
        if let Message::Request(ref mut r) = message {
            r.request_id = seqno; // TODO: 暫定
        }
        SendingMessage { seqno, message }
    }
    fn fill_frame(&mut self, frame: &mut FrameBuf) -> Result<bool> {
        let buf = &mut frame.buffer[frame.start..];

        BigEndian::write_u32(&mut buf[0..], self.seqno);

        let size = track!(self.message.encode(&mut buf[7..]))?;
        BigEndian::write_u16(&mut buf[5..], size as u16);

        frame.end += FrameBuf::HEADER_SIZE + size;
        let is_completed = size < FrameBuf::MAX_DATA_SIZE;
        Ok(is_completed)
    }
    fn fill_error_frame(&self, frame: &mut FrameBuf) {
        let buf = &mut frame.buffer[frame.start..];

        BigEndian::write_u32(&mut buf[0..], self.seqno);
        buf[4] = FLAG_ERROR;
        BigEndian::write_u16(&mut buf[5..], 0);

        frame.end += FrameBuf::HEADER_SIZE;
    }
}

// TODO: リトライは上のエイヤーで行う
#[derive(Debug)]
struct Connection {
    logger: Logger,
    peer: Option<SocketAddr>,
    state: ConnectionState,
}
impl Connection {
    fn new(logger: Logger, peer: SocketAddr) -> Self {
        let state = ConnectionState::Wait(timer::timeout(Duration::default()));
        Connection {
            logger,
            peer: Some(peer),
            state,
        }
    }

    fn recv_frame(&mut self, frame: &mut FrameBuf) {
        if frame.is_full() {
            return;
        }
        let next = if let ConnectionState::Connected(ref mut stream) = self.state {
            match stream.read(&mut frame.buffer[frame.end..]) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return;
                    }

                    error!(self.logger, "Cannot read frame: {}", e);
                    let timeout = timer::timeout(Duration::from_millis(RECONNECT_INTERVAL));
                    ConnectionState::Wait(timeout)
                }
                Ok(0) => {
                    info!(self.logger, "Connection closed");
                    let timeout = timer::timeout(Duration::from_millis(RECONNECT_INTERVAL));
                    ConnectionState::Wait(timeout)
                }
                Ok(size) => {
                    frame.end += size;
                    return;
                }
            }
        } else {
            return;
        };
        self.state = next;
    }

    fn send_frame(&mut self, frame: &mut FrameBuf) -> bool {
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

// #[derive(Debug)]
// struct KeepAlive {
//     future: Timeout,
//     timeout: Duration,
//     extend_period: bool,
// }
// impl KeepAlive {
//     fn new(options: &ChannelOptions) -> Self {
//         KeepAlive {
//             future: timer::timeout(options.keep_alive_timeout),
//             timeout: options.keep_alive_timeout,
//             extend_period: false,
//         }
//     }

//     fn extend_period(&mut self) {
//         self.extend_period = true;
//     }

//     fn poll_timeout(&mut self) -> Result<bool> {
//         let result = self.future
//             .poll()
//             .map_err(|_| ErrorKind::Other.cause("Broken timer"));
//         Ok(track!(result)?.is_ready())
//     }
// }
// impl Future for KeepAlive {
//     type Item = ();
//     type Error = Error;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         while track!(self.poll_timeout())? {
//             if self.extend_period {
//                 self.future = timer::timeout(self.timeout);
//                 self.extend_period = false;
//             } else {
//                 return Ok(Async::Ready(()));
//             }
//         }
//         Ok(Async::NotReady)
//     }
// }
