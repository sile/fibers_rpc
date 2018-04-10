use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use bytecodec::{Decode, DecodeExt, Encode, Eos};
use bytecodec::combinator::{Buffered, Slice};
use bytecodec::io::{IoDecodeExt, IoEncodeExt, ReadBuf, WriteBuf};
use fibers::net::TcpStream;
use futures::{Async, Poll, Stream};

use {Error, ErrorKind, Result};
use message::{AssignIncomingMessageHandler, MessageId, OutgoingMessage};
use metrics::ChannelMetrics;
use packet::{PacketHeaderDecoder, PacketizedMessage, MAX_PACKET_LEN, MIN_PACKET_LEN};

// FIXME: parameterize
const BUF_SIZE: usize = MAX_PACKET_LEN * 2;

pub struct MessageStream<A: AssignIncomingMessageHandler> {
    transport_stream: TcpStream,
    rbuf: ReadBuf<Vec<u8>>,
    wbuf: WriteBuf<Vec<u8>>,
    assigner: A,
    packet_header_decoder: Buffered<PacketHeaderDecoder>,
    receiving_messages: HashMap<MessageId, Slice<A::Handler>>,
    sending_messages: BinaryHeap<SendingMessage>,
    seqno: u64,
    metrics: ChannelMetrics,
}
impl<A: AssignIncomingMessageHandler> MessageStream<A> {
    pub fn new(transport_stream: TcpStream, assigner: A, metrics: ChannelMetrics) -> Self {
        MessageStream {
            transport_stream,
            rbuf: ReadBuf::new(vec![0; BUF_SIZE]),
            wbuf: WriteBuf::new(vec![0; BUF_SIZE]),
            sending_messages: BinaryHeap::new(),
            assigner,
            packet_header_decoder: PacketHeaderDecoder::default().buffered(),
            receiving_messages: HashMap::new(),
            seqno: 0,
            metrics,
        }
    }

    pub fn metrics(&self) -> &ChannelMetrics {
        &self.metrics
    }

    pub fn send_message(&mut self, message: OutgoingMessage) {
        let message = SendingMessage {
            seqno: self.seqno,
            message: PacketizedMessage::new(message),
        };
        self.seqno += 1;
        self.sending_messages.push(message);
        self.metrics.enqueued_outgoing_messages.increment();
    }

    pub fn assigner_mut(&mut self) -> &mut A {
        &mut self.assigner
    }

    fn handle_outgoing_messages(
        &mut self,
    ) -> Result<Option<MessageEvent<<A::Handler as Decode>::Item>>> {
        while !(self.sending_messages.is_empty() && self.wbuf.is_empty()) {
            if self.wbuf.room() >= MIN_PACKET_LEN {
                if let Some(mut sending) = self.sending_messages.pop() {
                    track!(sending.message.encode_to_write_buf(&mut self.wbuf))?;
                    if sending.message.is_idle() {
                        // Completed to write the message to the sending buffer
                        let event = MessageEvent::Sent;
                        self.metrics.dequeued_outgoing_messages.increment();
                        return Ok(Some(event));
                    } else {
                        // A part of the message was written to the sending buffer
                        sending.seqno = self.seqno;
                        self.seqno += 1;
                        self.sending_messages.push(sending);
                    }
                    continue;
                }
            }

            track!(self.wbuf.flush(&mut self.transport_stream))?;
            if !self.wbuf.stream_state().is_normal() {
                break;
            }
        }
        Ok(None)
    }

    fn handle_incoming_messages(
        &mut self,
    ) -> Result<Option<MessageEvent<<A::Handler as Decode>::Item>>> {
        loop {
            track!(self.rbuf.fill(&mut self.transport_stream))?;

            if !self.packet_header_decoder.has_item() {
                track!(
                    self.packet_header_decoder
                        .decode_from_read_buf(&mut self.rbuf)
                )?;
                if let Some(header) = self.packet_header_decoder.get_item().cloned() {
                    if !self.receiving_messages.contains_key(&header.message.id) {
                        let handler = track!(
                            self.assigner
                                .assign_incoming_message_handler(&header.message)
                        )?;
                        self.receiving_messages
                            .insert(header.message.id, handler.slice());
                    }
                    let handler = self.receiving_messages
                        .get_mut(&header.message.id)
                        .expect("Never fails");
                    handler.set_consumable_bytes(u64::from(header.payload_len));
                }
            }

            if let Some(header) = self.packet_header_decoder.get_item().cloned() {
                let mut handler = self.receiving_messages
                    .remove(&header.message.id)
                    .expect("Never fails");

                let mut item = track!(handler.decode_from_read_buf(&mut self.rbuf))?;
                if item.is_none() && handler.is_suspended() && header.is_end_of_message() {
                    item = track!(handler.decode(&[][..], Eos::new(true)); header)?.1;
                }
                if handler.is_suspended() {
                    self.packet_header_decoder.take_item();
                }
                if let Some(next_action) = item {
                    let event = MessageEvent::Received { next_action };
                    return Ok(Some(event));
                } else {
                    self.receiving_messages.insert(header.message.id, handler);
                }
            }

            if self.rbuf.is_empty() && !self.rbuf.stream_state().is_normal() {
                break;
            }
        }
        Ok(None)
    }
}
impl<A: AssignIncomingMessageHandler> Stream for MessageStream<A> {
    type Item = MessageEvent<<A::Handler as Decode>::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(event) = track!(self.handle_incoming_messages())? {
            return Ok(Async::Ready(Some(event)));
        }
        if let Some(event) = track!(self.handle_outgoing_messages())? {
            return Ok(Async::Ready(Some(event)));
        }

        // FIXME: parameterize
        track_assert!(
            self.sending_messages.len() <= 10_000,
            ErrorKind::Other,
            "This stream may be overloaded"
        );

        if self.rbuf.stream_state().is_eos() {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}
impl<A: AssignIncomingMessageHandler> fmt::Debug for MessageStream<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MessageStream {{ .. }}")
    }
}

#[derive(Debug)]
pub enum MessageEvent<T> {
    Sent,
    Received { next_action: T },
}

#[derive(Debug)]
struct SendingMessage {
    seqno: u64,
    message: PacketizedMessage,
}
impl PartialEq for SendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.message.header().id == other.message.header().id
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
        let ordering = (self.message.header().priority, self.seqno)
            .cmp(&(other.message.header().priority, other.seqno));
        ordering.reverse()
    }
}
