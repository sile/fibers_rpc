use fibers;
use fibers::net::TcpStream;
use futures::{Async, Poll, Stream};
use slog::Logger;

use Error;
use message::OutgoingMessage;
use message_stream::{MessageEvent, MessageStream};
use metrics::ChannelMetrics;
use server_side_handlers::{Action, Assigner};

#[derive(Debug)]
pub struct ServerSideChannel {
    logger: Logger,
    message_stream: MessageStream<Assigner>,
}
impl ServerSideChannel {
    pub fn new(
        logger: Logger,
        transport_stream: TcpStream,
        assigner: Assigner,
        metrics: ChannelMetrics,
    ) -> Self {
        let message_stream = MessageStream::new(transport_stream, assigner, metrics);
        ServerSideChannel {
            logger,
            message_stream,
        }
    }

    pub fn reply(&mut self, message: OutgoingMessage) {
        self.message_stream.send_message(message);
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
                    MessageEvent::Sent => {
                        debug!(self.logger, "Completed to send a message");
                    }
                    MessageEvent::Received { next_action } => {
                        debug!(self.logger, "Completed to receive a message");
                        return Ok(Async::Ready(Some(next_action)));
                    }
                }

                // FIXME: parameterize
                count += 1;
                if count > 128 {
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
