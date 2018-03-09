use std::collections::{HashSet, VecDeque};
use futures::{Async, Future, Poll, Stream};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::MessageSeqNo;
use traits::Encodable;

#[derive(Debug)]
pub struct MessageStream<H: HandleFrame> {
    frame_stream: FrameStream,
    outgoing_messages: VecDeque<(MessageSeqNo, Encodable)>,
    incoming_frame_handler: H,
    cancelled_incoming_messages: HashSet<MessageSeqNo>,
    event_queue: VecDeque<MessageStreamEvent<H::Future>>,
    next_seqno: MessageSeqNo,
}
impl<H: HandleFrame> MessageStream<H> {
    pub fn new(frame_stream: FrameStream, incoming_frame_handler: H) -> Self {
        MessageStream {
            frame_stream,
            outgoing_messages: VecDeque::new(),
            incoming_frame_handler,
            cancelled_incoming_messages: HashSet::new(),
            event_queue: VecDeque::new(),
            next_seqno: 0,
        }
    }

    pub fn send_message(&mut self, message: Encodable) -> MessageSeqNo {
        let seqno = self.next_seqno;
        self.outgoing_messages.push_back((seqno, message));
        self.next_seqno += 1;
        seqno
    }

    pub fn incoming_frame_handler_mut(&mut self) -> &mut H {
        &mut self.incoming_frame_handler
    }
}
impl<H: HandleFrame> Stream for MessageStream<H> {
    type Item = MessageStreamEvent<H::Future>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if track!(self.frame_stream.poll())?.is_ready() {
            return Ok(Async::Ready(None));
        }

        while let Some((seqno, mut message)) = self.outgoing_messages.pop_front() {
            let result = self.frame_stream
                .send_frame(seqno, |mut frame| track!(message.encode(frame.data())));
            match result {
                Err(e) => {
                    let event = MessageStreamEvent::Sent {
                        seqno,
                        result: Err(e),
                    };
                    self.event_queue.push_back(event);
                }
                Ok(None) => {
                    self.outgoing_messages.push_front((seqno, message));
                    break;
                }
                Ok(Some(false)) => {
                    self.outgoing_messages.push_back((seqno, message));
                }
                Ok(Some(true)) => {
                    let event = MessageStreamEvent::Sent {
                        seqno,
                        result: Ok(()),
                    };
                    self.event_queue.push_back(event);
                }
            }
        }

        while let Some(frame) = self.frame_stream.recv_frame() {
            let seqno = frame.seqno;
            if self.cancelled_incoming_messages.contains(&seqno) {
                if frame.is_end_of_message() {
                    self.cancelled_incoming_messages.remove(&seqno);
                }
                continue;
            }

            if frame.is_error() {
                let event = MessageStreamEvent::Received {
                    seqno,
                    result: Err(track!(ErrorKind::InvalidInput.error()).into()),
                };
                self.event_queue.push_back(event);
            } else {
                match track!(self.incoming_frame_handler.handle_frame(frame)) {
                    Err(e) => {
                        if !frame.is_end_of_message() {
                            self.cancelled_incoming_messages.insert(seqno);
                        }
                        let event = MessageStreamEvent::Received {
                            seqno,
                            result: Err(e),
                        };
                        self.event_queue.push_back(event);
                    }
                    Ok(None) => {}
                    Ok(Some(message)) => {
                        let event = MessageStreamEvent::Received {
                            seqno,
                            result: Ok(message),
                        };
                        self.event_queue.push_back(event);
                    }
                }
            }
        }

        if let Some(event) = self.event_queue.pop_front() {
            Ok(Async::Ready(Some(event)))
        } else {
            Ok(Async::NotReady)
        }
    }
}

#[derive(Debug)]
pub enum MessageStreamEvent<T> {
    Sent {
        seqno: MessageSeqNo,
        result: Result<()>,
    },
    Received {
        seqno: MessageSeqNo,
        result: Result<T>,
    },
}
