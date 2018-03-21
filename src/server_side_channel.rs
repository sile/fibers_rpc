use fibers;
use fibers::net::TcpStream;
use futures::{Async, Poll, Stream};
use slog::Logger;

use Error;
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::{MessageSeqNo, OutgoingMessage};
use message_stream::{MessageStream, MessageStreamEvent};
use server_side_handlers::{Action, IncomingFrameHandler};

#[derive(Debug)]
pub struct ServerSideChannel {
    logger: Logger,
    message_stream: MessageStream<IncomingFrameHandler>,
}
impl ServerSideChannel {
    pub fn new(logger: Logger, transport_stream: TcpStream, handler: IncomingFrameHandler) -> Self {
        // TODO:
        let metrics = ::metrics::ChannelMetrics::new(&mut Default::default());
        let message_stream =
            MessageStream::new(FrameStream::new(transport_stream), handler, metrics);
        ServerSideChannel {
            logger,
            message_stream,
        }
    }

    pub fn reply(&mut self, seqno: MessageSeqNo, message: OutgoingMessage) {
        self.message_stream.send_message(seqno, message);
    }
}
impl Stream for ServerSideChannel {
    type Item = Action;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut count = 0;
        while let Async::Ready(item) = track!(self.message_stream.poll())? {
            if let Some(event) = item {
                match event {
                    MessageStreamEvent::Sent { seqno, result } => {
                        if let Err(e) = result {
                            error!(self.logger, "Failed to send message({:?}): {}", seqno, e);
                        } else {
                            debug!(self.logger, "Completed to send message({:?})", seqno);
                        }
                    }
                    MessageStreamEvent::Received { seqno, result } => match result {
                        Err(e) => {
                            error!(self.logger, "Failed to receive message({:?}): {}", seqno, e);
                            self.message_stream.send_error_frame(seqno);
                            self.message_stream
                                .incoming_frame_handler_mut()
                                .handle_error(seqno, e);
                        }
                        Ok(action) => {
                            debug!(self.logger, "Completed to receive message({:?})", seqno);
                            return Ok(Async::Ready(Some(action)));
                        }
                    },
                }

                // FIXME: parameterize
                count += 1;
                if count > 64 {
                    return fibers::fiber::yield_poll();
                }
            } else {
                return Ok(Async::Ready(None));
            }
        }
        Ok(Async::NotReady)
    }
}
