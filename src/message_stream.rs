use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
use bytecodec::combinator::{Buffered, MaybeEos, Slice};
use bytecodec::io::{IoDecodeExt, IoEncodeExt, ReadBuf, WriteBuf};
use bytecodec::{Decode, DecodeExt, Encode, EncodeExt, Eos};
use fibers::net::TcpStream;
use fibers::sync::mpsc;
use fibers_tasque::DefaultCpuTaskQueue;
use futures::{Async, Poll, Stream};
use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;

use channel::ChannelOptions;
use message::{AssignIncomingMessageHandler, MessageId, OutgoingMessage, OutgoingMessagePayload};
use metrics::ChannelMetrics;
use packet::{PacketHeaderDecoder, PacketizedMessage, MIN_PACKET_LEN};
use {Error, ErrorKind, Result};

pub struct MessageStream<A: AssignIncomingMessageHandler> {
    transport_stream: TcpStream,
    rbuf: ReadBuf<Vec<u8>>,
    wbuf: WriteBuf<Vec<u8>>,
    assigner: A,
    packet_header_decoder: Buffered<MaybeEos<PacketHeaderDecoder>>,
    receiving_messages: HashMap<MessageId, Slice<A::Handler>>,
    sending_messages: BinaryHeap<SendingMessage>,
    async_outgoing_tx: mpsc::Sender<Result<OutgoingMessage>>,
    async_outgoing_rx: mpsc::Receiver<Result<OutgoingMessage>>,
    async_incoming_tx: mpsc::Sender<Result<<A::Handler as Decode>::Item>>,
    async_incoming_rx: mpsc::Receiver<Result<<A::Handler as Decode>::Item>>,
    async_incomings: HashMap<MessageId, Slice<RemainingBytesDecoder>>,
    seqno: u64,
    options: ChannelOptions,
    metrics: ChannelMetrics,
}
impl<A: AssignIncomingMessageHandler> MessageStream<A>
where
    <A::Handler as Decode>::Item: Send + 'static,
{
    pub fn new(
        transport_stream: TcpStream,
        assigner: A,
        options: ChannelOptions,
        metrics: ChannelMetrics,
    ) -> Self {
        let _ = unsafe { transport_stream.with_inner(|s| s.set_nodelay(true)) };
        let (async_outgoing_tx, async_outgoing_rx) = mpsc::channel();
        let (async_incoming_tx, async_incoming_rx) = mpsc::channel();
        MessageStream {
            transport_stream,
            rbuf: ReadBuf::new(vec![0; options.read_buffer_size]),
            wbuf: WriteBuf::new(vec![0; options.write_buffer_size]),
            sending_messages: BinaryHeap::new(),
            async_outgoing_tx,
            async_outgoing_rx,
            async_incoming_tx,
            async_incoming_rx,
            async_incomings: HashMap::new(),
            assigner,
            packet_header_decoder: PacketHeaderDecoder::default().maybe_eos().buffered(),
            receiving_messages: HashMap::new(),
            seqno: 0,
            options,
            metrics,
        }
    }

    pub fn options(&self) -> &ChannelOptions {
        &self.options
    }

    pub fn metrics(&self) -> &ChannelMetrics {
        &self.metrics
    }

    pub fn send_message(&mut self, mut message: OutgoingMessage) {
        if message.header.async {
            self.metrics.async_outgoing_messages.increment();
            let tx = self.async_outgoing_tx.clone();
            DefaultCpuTaskQueue.with(|tasque| {
                tasque.enqueue(move || {
                    let f = || {
                        let header = message.header;
                        let mut payload = Vec::new();
                        track!(message.payload.encode_all(&mut payload))?;
                        let payload =
                            OutgoingMessagePayload::new(track!(BytesEncoder::with_item(payload))?);
                        Ok(OutgoingMessage { header, payload })
                    };
                    let _ = tx.send(f());
                })
            })
        } else {
            self.start_sending_message(message);
        }
    }

    pub fn assigner_mut(&mut self) -> &mut A {
        &mut self.assigner
    }

    fn start_sending_message(&mut self, message: OutgoingMessage) {
        let message = SendingMessage {
            seqno: self.seqno,
            message: PacketizedMessage::new(message),
        };
        self.seqno += 1;
        self.sending_messages.push(message);
        self.metrics.enqueued_outgoing_messages.increment();
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
                    if header.is_async() {
                        let decoder = self.async_incomings
                            .entry(header.message.id)
                            .or_insert_with(Default::default);
                        decoder.set_consumable_bytes(u64::from(header.payload_len));
                    } else {
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
            }

            if let Some(header) = self.packet_header_decoder.get_item().cloned() {
                if header.is_async() {
                    let mut decoder = self.async_incomings
                        .remove(&header.message.id)
                        .expect("Never fails");

                    let item = track!(decoder.decode_from_read_buf(&mut self.rbuf))?;
                    track_assert!(item.is_none(), ErrorKind::Other);
                    if decoder.is_suspended() {
                        self.packet_header_decoder.take_item();
                    }
                    if decoder.is_suspended() && header.is_end_of_message() {
                        let item = track!(decoder.decode(&[][..], Eos::new(true)))?.1;
                        let buf = track_assert_some!(item, ErrorKind::Other);
                        self.metrics.async_incoming_messages.increment();

                        let tx = self.async_incoming_tx.clone();
                        let mut handler = track!(
                            self.assigner
                                .assign_incoming_message_handler(&header.message)
                        )?;
                        DefaultCpuTaskQueue.with(|tasque| {
                            tasque.enqueue(move || {
                                let mut f = || {
                                    let item = track!(handler.decode(&buf[..], Eos::new(true)))?.1;
                                    let action = track_assert_some!(item, ErrorKind::Other);
                                    Ok(action)
                                };
                                let _ = tx.send(f());
                            })
                        })
                    } else {
                        self.async_incomings.insert(header.message.id, decoder);
                    }
                } else {
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
                        track_assert_eq!(handler.consumable_bytes(), 0, ErrorKind::Other; header);
                        track_assert!(header.is_end_of_message(), ErrorKind::Other; header);

                        let event = MessageEvent::Received { next_action };
                        return Ok(Some(event));
                    } else {
                        self.receiving_messages.insert(header.message.id, handler);
                    }
                }
            }

            if self.rbuf.is_empty() && !self.rbuf.stream_state().is_normal() {
                break;
            }
        }
        Ok(None)
    }
}
impl<A: AssignIncomingMessageHandler> Stream for MessageStream<A>
where
    <A::Handler as Decode>::Item: Send + 'static,
{
    type Item = MessageEvent<<A::Handler as Decode>::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Async::Ready(Some(message)) = self.async_outgoing_rx.poll().expect("Never fails")
        {
            self.start_sending_message(track!(message)?);
        }
        if let Async::Ready(Some(next)) = self.async_incoming_rx.poll().expect("Never fails") {
            let next_action = track!(next)?;
            let event = MessageEvent::Received { next_action };
            return Ok(Async::Ready(Some(event)));
        }

        if let Some(event) = track!(self.handle_incoming_messages())? {
            return Ok(Async::Ready(Some(event)));
        }
        if let Some(event) = track!(self.handle_outgoing_messages())? {
            return Ok(Async::Ready(Some(event)));
        }

        track_assert!(
            self.sending_messages.len() <= self.options.max_transmit_queue_len,
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
