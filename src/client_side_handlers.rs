use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use bytecodec::{Decode, Eos};
use fibers::sync::oneshot;
use fibers::time::timer::{self, Timeout};
use futures::{Async, Future, Poll};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use frame::{Frame, HandleFrame};
use message::MessageId;
use metrics::ClientMetrics;

/// `Future` that represents a response from a RPC server.
#[derive(Debug)]
pub struct Response<T> {
    reply_rx: oneshot::Monitor<T, Error>,
    timeout: Option<Timeout>,
}
impl<T> Response<T> {
    pub(crate) fn error(e: Error) -> Self {
        let (tx, rx) = oneshot::monitor();
        tx.exit(Err(e));
        Response {
            reply_rx: rx,
            timeout: None,
        }
    }
}
impl<T> Future for Response<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let item = self.reply_rx.poll().map_err(|e| {
            track!(e.unwrap_or_else(|| {
                ErrorKind::Other
                    .cause("RPC response monitoring channel disconnected")
                    .into()
            }))
        })?;
        if let Async::Ready(item) = item {
            Ok(Async::Ready(item))
        } else {
            let expired = self.timeout
                .poll()
                .map_err(|_| track!(ErrorKind::Other.cause("Broken timer")))?;
            if let Async::Ready(Some(())) = expired {
                track_panic!(ErrorKind::Timeout);
            }
            Ok(Async::NotReady)
        }
    }
}

#[derive(Default)]
pub struct IncomingFrameHandler {
    handlers: HashMap<MessageId, BoxResponseHandler>,
}
impl IncomingFrameHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_response_handler(
        &mut self,
        message_id: MessageId,
        handler: BoxResponseHandler,
    ) {
        self.handlers.insert(message_id, handler);
    }
}
impl HandleFrame for IncomingFrameHandler {
    type Item = ();
    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>> {
        let eos = if let Some(handler) = self.handlers.get_mut(&frame.message_id()) {
            track!(handler.handle_frame(frame))?.is_some()
        } else {
            false
        };
        if eos {
            self.handlers.remove(&frame.message_id());
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
    fn handle_error(&mut self, message_id: MessageId, error: Error) {
        if let Some(mut handler) = self.handlers.remove(&message_id) {
            handler.handle_error(message_id, error);
        }
    }
}
impl fmt::Debug for IncomingFrameHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncomingFrameHandler {{ handlers.len: {} }}",
            self.handlers.len()
        )
    }
}

pub type BoxResponseHandler = Box<HandleFrame<Item = ()> + Send + 'static>;

#[derive(Debug)]
pub struct ResponseHandler<D: Decode> {
    decoder: D,
    reply_tx: Option<oneshot::Monitored<D::Item, Error>>,
    metrics: Arc<ClientMetrics>,
}
impl<D: Decode> ResponseHandler<D> {
    pub fn new(
        decoder: D,
        timeout: Option<Duration>,
        metrics: Arc<ClientMetrics>,
    ) -> (Self, Response<D::Item>) {
        let (reply_tx, reply_rx) = oneshot::monitor();
        let handler = ResponseHandler {
            decoder: decoder,
            reply_tx: Some(reply_tx),
            metrics,
        };

        let timeout = timeout.map(timer::timeout);
        let response = Response { reply_rx, timeout };
        (handler, response)
    }
}
impl<D: Decode> HandleFrame for ResponseHandler<D> {
    type Item = ();
    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>> {
        let eos = Eos::new(frame.is_end_of_message());
        let (_size, item) = track!(self.decoder.decode(frame.data(), eos))?;
        if let Some(response) = item {
            // TODO: validate `_size`
            let reply_tx = self.reply_tx.take().expect("Never fails");
            reply_tx.exit(Ok(response));
            self.metrics.ok_responses.increment();
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
    fn handle_error(&mut self, _message_id: MessageId, error: Error) {
        let reply_tx = self.reply_tx.take().expect("Never fails");
        reply_tx.exit(Err(error));
        self.metrics.error_responses.increment();
    }
}
