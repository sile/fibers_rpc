use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use fibers::sync::oneshot;
use fibers::time::timer::{self, Timeout};
use futures::{Async, Future, Poll};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use codec::Decode;
use frame::{Frame, HandleFrame};
use message::MessageSeqNo;
use metrics::ClientMetrics;

/// `Future` that represents a response from a RPC server.
#[derive(Debug)]
pub struct Response<T> {
    reply_rx: oneshot::Monitor<T, Error>,
    timeout: Option<Timeout>,
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
    handlers: HashMap<MessageSeqNo, BoxResponseHandler>,
}
impl IncomingFrameHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_response_handler(&mut self, seqno: MessageSeqNo, handler: BoxResponseHandler) {
        self.handlers.insert(seqno, handler);
    }
}
impl HandleFrame for IncomingFrameHandler {
    type Item = ();
    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>> {
        let eos = if let Some(handler) = self.handlers.get_mut(&frame.seqno()) {
            track!(handler.handle_frame(frame))?.is_some()
        } else {
            false
        };
        if eos {
            self.handlers.remove(&frame.seqno());
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
    fn handle_error(&mut self, seqno: MessageSeqNo, error: Error) {
        if let Some(mut handler) = self.handlers.remove(&seqno) {
            handler.handle_error(seqno, error);
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
    decoder: Option<D>,
    reply_tx: Option<oneshot::Monitored<D::Message, Error>>,
    metrics: Arc<ClientMetrics>,
}
impl<D: Decode> ResponseHandler<D> {
    pub fn new(
        decoder: D,
        timeout: Option<Duration>,
        metrics: Arc<ClientMetrics>,
    ) -> (Self, Response<D::Message>) {
        let (reply_tx, reply_rx) = oneshot::monitor();
        let handler = ResponseHandler {
            decoder: Some(decoder),
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
        {
            let decoder = track_assert_some!(self.decoder.as_mut(), ErrorKind::Other);
            track!(decoder.decode(frame.data(), frame.is_end_of_message()))?;
        }
        if frame.is_end_of_message() {
            let decoder = track_assert_some!(self.decoder.take(), ErrorKind::Other);
            let response = track!(decoder.finish())?;
            let reply_tx = self.reply_tx.take().expect("Never fails");
            reply_tx.exit(Ok(response));
            self.metrics.ok_responses.increment();
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
    fn handle_error(&mut self, _seqno: MessageSeqNo, error: Error) {
        let reply_tx = self.reply_tx.take().expect("Never fails");
        reply_tx.exit(Err(error));
        self.metrics.error_responses.increment();
    }
}
