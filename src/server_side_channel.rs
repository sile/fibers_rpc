use fibers;
use fibers::net::TcpStream;
use futures::{Async, Poll, Stream};
use slog::Logger;

use Error;
use frame::HandleFrame;
use frame_stream::FrameStream;
use message::{MessageId, OutgoingMessage};
use message_stream::{MessageStream, MessageStreamEvent};
use metrics::ChannelMetrics;
use server_side_handlers::{Action, IncomingFrameHandler};

#[derive(Debug)]
pub struct ServerSideChannel {
    logger: Logger,
    message_stream: MessageStream<IncomingFrameHandler>,
}
impl ServerSideChannel {
    pub fn new(
        logger: Logger,
        transport_stream: TcpStream,
        handler: IncomingFrameHandler,
        metrics: ChannelMetrics,
    ) -> Self {
        let message_stream =
            MessageStream::new(FrameStream::new(transport_stream), handler, metrics);
        ServerSideChannel {
            logger,
            message_stream,
        }
    }

    pub fn reply(&mut self, message_id: MessageId, message: OutgoingMessage) {
        self.message_stream.send_message(message_id, message);
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
                    MessageStreamEvent::Sent { message_id, result } => {
                        if let Err(e) = result {
                            error!(
                                self.logger,
                                "Failed to send message({:?}): {}", message_id, e
                            );
                        } else {
                            debug!(self.logger, "Completed to send message({:?})", message_id);
                        }
                    }
                    MessageStreamEvent::Received { message_id, result } => match result {
                        Err(e) => {
                            error!(
                                self.logger,
                                "Failed to receive message({:?}): {}", message_id, e
                            );
                            self.message_stream.send_error_frame(message_id);
                            self.message_stream
                                .incoming_frame_handler_mut()
                                .handle_error(message_id, e);
                        }
                        Ok(action) => {
                            debug!(
                                self.logger,
                                "Completed to receive message({:?})", message_id
                            );
                            return Ok(Async::Ready(Some(action)));
                        }
                    },
                }

                // FIXME: parameterize
                count += 1;
                if count > 64 {
                    self.message_stream.metrics().fiber_yielded.increment();
                    return fibers::fiber::yield_poll();
                }
            } else {
                return Ok(Async::Ready(None));
            }
        }
        Ok(Async::NotReady)
    }
}
