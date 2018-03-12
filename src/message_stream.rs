use std::collections::{HashSet, VecDeque};
use futures::{Async, Future, Poll, Stream};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::{MessageSeqNo, OutgoingMessage};

#[derive(Debug)]
pub struct MessageStream<H: HandleFrame> {
    frame_stream: FrameStream,
    outgoing_messages: VecDeque<(MessageSeqNo, OutgoingMessage)>,
    incoming_frame_handler: H,
    cancelled_incoming_messages: HashSet<MessageSeqNo>,
    event_queue: VecDeque<MessageStreamEvent<H::Item>>,
}
impl<H: HandleFrame> MessageStream<H> {
    pub fn new(frame_stream: FrameStream, incoming_frame_handler: H) -> Self {
        MessageStream {
            frame_stream,
            outgoing_messages: VecDeque::new(),
            incoming_frame_handler,
            cancelled_incoming_messages: HashSet::new(),
            event_queue: VecDeque::new(),
        }
    }

    pub fn send_message(&mut self, seqno: MessageSeqNo, message: OutgoingMessage) {
        self.outgoing_messages.push_back((seqno, message));
    }

    pub fn incoming_frame_handler_mut(&mut self) -> &mut H {
        &mut self.incoming_frame_handler
    }

    fn handle_outgoing_messages(&mut self) {
        while let Some((seqno, mut message)) = self.outgoing_messages.pop_front() {
            let result = self.frame_stream
                .send_frame(seqno, |frame| track!(message.encode(frame.data())));
            match result {
                Err(e) => {
                    let event = MessageStreamEvent::Sent {
                        seqno,
                        result: Err(e),
                    };
                    self.event_queue.push_back(event);
                }
                Ok(None) => {
                    // The sending buffer is full
                    self.outgoing_messages.push_front((seqno, message));
                    break;
                }
                Ok(Some(false)) => {
                    // A part of the message was written to the sending buffer
                    self.outgoing_messages.push_back((seqno, message));
                }
                Ok(Some(true)) => {
                    // Completed to write the message to the sending buffer
                    let event = MessageStreamEvent::Sent {
                        seqno,
                        result: Ok(()),
                    };
                    self.event_queue.push_back(event);
                }
            }
        }
    }

    fn handle_incoming_frames(&mut self) {
        while let Some(frame) = self.frame_stream.recv_frame() {
            let seqno = frame.seqno();
            if self.cancelled_incoming_messages.contains(&seqno) {
                if frame.is_end_of_message() {
                    self.cancelled_incoming_messages.remove(&seqno);
                }
                continue;
            }

            let result = if frame.is_error() {
                Err(track!(ErrorKind::InvalidInput.error()).into())
            } else {
                track!(self.incoming_frame_handler.handle_frame(&frame))
            };
            match result {
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
}
impl<H: HandleFrame> Stream for MessageStream<H> {
    type Item = MessageStreamEvent<H::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let eos = track!(self.frame_stream.poll())?.is_ready();
        if eos {
            return Ok(Async::Ready(None));
        }

        self.handle_outgoing_messages();
        self.handle_incoming_frames();

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
impl<T> MessageStreamEvent<T> {
    pub fn is_ok(&self) -> bool {
        match *self {
            MessageStreamEvent::Sent { ref result, .. } => result.is_ok(),
            MessageStreamEvent::Received { ref result, .. } => result.is_ok(),
        }
    }
}
