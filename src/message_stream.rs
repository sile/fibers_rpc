use std::collections::{HashSet, VecDeque};
use futures::{Async, Future, Poll, Stream};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::{MessageSeqNo, OutgoingMessage};
use metrics::ChannelMetrics;

#[derive(Debug)]
pub struct MessageStream<H: HandleFrame> {
    frame_stream: FrameStream,
    outgoing_messages: VecDeque<(MessageSeqNo, OutgoingMessage)>,
    incoming_frame_handler: H,
    cancelled_incoming_messages: HashSet<MessageSeqNo>,
    event_queue: VecDeque<MessageStreamEvent<H::Item>>,
    metrics: ChannelMetrics,
}
impl<H: HandleFrame> MessageStream<H> {
    pub fn new(
        frame_stream: FrameStream,
        incoming_frame_handler: H,
        metrics: ChannelMetrics,
    ) -> Self {
        metrics.created_channels.increment();
        MessageStream {
            frame_stream,
            outgoing_messages: VecDeque::new(),
            incoming_frame_handler,
            cancelled_incoming_messages: HashSet::new(),
            event_queue: VecDeque::new(),
            metrics,
        }
    }

    pub fn send_message(&mut self, seqno: MessageSeqNo, message: OutgoingMessage) {
        self.outgoing_messages.push_back((seqno, message));
        self.metrics.enqueued_outgoing_messages.increment();
    }

    pub fn send_error_frame(&mut self, seqno: MessageSeqNo) {
        self.outgoing_messages
            .push_back((seqno, OutgoingMessage::error()));
        self.metrics.enqueued_outgoing_messages.increment();
    }

    pub fn incoming_frame_handler_mut(&mut self) -> &mut H {
        &mut self.incoming_frame_handler
    }

    fn handle_outgoing_messages(&mut self) -> bool {
        let mut did_something = false;
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
                    self.metrics.encode_frame_failures.increment();
                    self.metrics.dequeued_outgoing_messages.increment();
                    did_something = true;
                }
                Ok(None) => {
                    // The sending buffer is full
                    self.outgoing_messages.push_front((seqno, message));
                    break;
                }
                Ok(Some(false)) => {
                    // A part of the message was written to the sending buffer
                    self.outgoing_messages.push_back((seqno, message));
                    did_something = true;
                }
                Ok(Some(true)) => {
                    // Completed to write the message to the sending buffer
                    let event = MessageStreamEvent::Sent {
                        seqno,
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
                    self.metrics.encode_frame_failures.increment();
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

        if let Some(event) = self.event_queue.pop_front() {
            Ok(Async::Ready(Some(event)))
        } else {
            Ok(Async::NotReady)
        }
    }
}
impl<H: HandleFrame> Drop for MessageStream<H> {
    fn drop(&mut self) {
        self.metrics.removed_channels.increment();
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
