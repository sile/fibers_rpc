use std::cmp;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use futures::{Async, Future, Poll, Stream};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::{MessageId, OutgoingMessage};
use metrics::ChannelMetrics;

#[derive(Debug)]
pub struct MessageStream<H: HandleFrame> {
    frame_stream: FrameStream,
    outgoing_messages: BinaryHeap<SendingMessage>,
    incoming_frame_handler: H,
    cancelled_incoming_messages: HashSet<MessageId>,
    event_queue: VecDeque<MessageStreamEvent<H::Item>>,
    seqno: u64,
    metrics: ChannelMetrics,
}
impl<H: HandleFrame> MessageStream<H> {
    pub fn new(
        frame_stream: FrameStream,
        incoming_frame_handler: H,
        metrics: ChannelMetrics,
    ) -> Self {
        MessageStream {
            frame_stream,
            outgoing_messages: BinaryHeap::new(),
            incoming_frame_handler,
            cancelled_incoming_messages: HashSet::new(),
            event_queue: VecDeque::new(),
            seqno: 0,
            metrics,
        }
    }

    pub fn metrics(&self) -> &ChannelMetrics {
        &self.metrics
    }

    pub fn send_message(&mut self, message_id: MessageId, message: OutgoingMessage) {
        let message = SendingMessage {
            message_id,
            seqno: self.seqno,
            is_error: false,
            message,
        };
        self.seqno += 1;
        self.outgoing_messages.push(message);
        self.metrics.enqueued_outgoing_messages.increment();
    }

    pub fn send_error_frame(&mut self, message_id: MessageId) {
        let message = SendingMessage {
            message_id,
            seqno: self.seqno,
            is_error: true,
            message: OutgoingMessage::error(),
        };
        self.seqno += 1;
        self.outgoing_messages.push(message);
        self.metrics.enqueued_outgoing_messages.increment();
    }

    pub fn incoming_frame_handler_mut(&mut self) -> &mut H {
        &mut self.incoming_frame_handler
    }

    fn handle_outgoing_messages(&mut self) -> bool {
        let mut did_something = false;
        while let Some(mut sending) = self.outgoing_messages.pop() {
            let result = self.frame_stream.send_frame(
                sending.message_id,
                sending.message.priority(),
                |frame| track!(sending.message.encode(frame.data())),
            );
            match result {
                Err(e) => {
                    if !sending.is_error {
                        let event = MessageStreamEvent::Sent {
                            message_id: sending.message_id,
                            result: Err(e),
                        };
                        self.event_queue.push_back(event);
                        self.metrics.encode_frame_failures.increment();
                    }
                    self.metrics.dequeued_outgoing_messages.increment();
                    did_something = true;
                }
                Ok(None) => {
                    // The sending buffer is full
                    self.outgoing_messages.push(sending);
                    break;
                }
                Ok(Some(false)) => {
                    // A part of the message was written to the sending buffer
                    sending.seqno = self.seqno;
                    self.seqno += 1;
                    self.outgoing_messages.push(sending);
                    did_something = true;
                }
                Ok(Some(true)) => {
                    // Completed to write the message to the sending buffer
                    let event = MessageStreamEvent::Sent {
                        message_id: sending.message_id,
                        result: Ok(()),
                    };
                    self.event_queue.push_back(event);
                    self.metrics.dequeued_outgoing_messages.increment();
                    did_something = true;
                }
            }
        }
        did_something
    }

    fn handle_incoming_frames(&mut self) -> bool {
        let mut did_something = false;
        while let Some(frame) = self.frame_stream.recv_frame() {
            did_something = true;

            let message_id = frame.message_id();
            if self.cancelled_incoming_messages.contains(&message_id) {
                if frame.is_end_of_message() {
                    self.cancelled_incoming_messages.remove(&message_id);
                }
                continue;
            }

            let result = if frame.is_error() {
                Err(track!(ErrorKind::InvalidInput.error()).into())
            } else {
                track!(
                    self.incoming_frame_handler.handle_frame(&frame),
                    "frame.data.len={}",
                    frame.data().len()
                )
            };
            match result {
                Err(e) => {
                    if !frame.is_end_of_message() {
                        self.cancelled_incoming_messages.insert(message_id);
                    }
                    let event = MessageStreamEvent::Received {
                        message_id,
                        result: Err(e),
                    };
                    self.event_queue.push_back(event);
                    self.metrics.decode_frame_failures.increment();
                }
                Ok(None) => {}
                Ok(Some(message)) => {
                    let event = MessageStreamEvent::Received {
                        message_id,
                        result: Ok(message),
                    };
                    self.event_queue.push_back(event);
                }
            }
        }
        did_something
    }
}
impl<H: HandleFrame> Stream for MessageStream<H> {
    type Item = MessageStreamEvent<H::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let eos = track!(self.frame_stream.poll())?.is_ready();
            if eos {
                return Ok(Async::Ready(None));
            }

            let is_send_buf_updated = self.handle_outgoing_messages();
            let is_recv_buf_updated = self.handle_incoming_frames();
            if !(is_send_buf_updated || is_recv_buf_updated) {
                break;
            }
        }

        // FIXME: parameterize
        track_assert!(
            self.outgoing_messages.len() <= 10_000,
            ErrorKind::Other,
            "This stream may be overloaded"
        );

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
        message_id: MessageId,
        result: Result<()>,
    },
    Received {
        message_id: MessageId,
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

#[derive(Debug)]
struct SendingMessage {
    message_id: MessageId,
    seqno: u64,
    is_error: bool,
    message: OutgoingMessage,
}
impl PartialEq for SendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.message_id == other.message_id
    }
}
impl Eq for SendingMessage {}
impl PartialOrd for SendingMessage {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SendingMessage {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let ordering =
            (self.message.priority(), self.seqno).cmp(&(other.message.priority(), other.seqno));
        ordering.reverse()
    }
}
