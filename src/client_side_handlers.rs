use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use fibers::sync::oneshot;
use fibers::time::timer::{self, Timeout};
use futures::{Async, Future, Poll};
use trackable::error::ErrorKindExt;

use {Error, ErrorKind, Result};
use codec::Decode;
use frame::{Frame, HandleFrame};
use message::MessageSeqNo;

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
        let seqno = frame.seqno();
        if let Some(mut handler) = self.handlers.remove(&seqno) {
            if track!(handler.handle_frame(frame))?.is_some() {
                Ok(Some(()))
            } else {
                self.handlers.insert(seqno, handler);
                Ok(None)
            }
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
    decoder: D,
    reply_tx: Option<oneshot::Monitored<D::Message, Error>>,
}
impl<D: Decode> ResponseHandler<D> {
    pub fn new(decoder: D, timeout: Option<Duration>) -> (Self, Response<D::Message>) {
        let (reply_tx, reply_rx) = oneshot::monitor();
        let handler = ResponseHandler {
            decoder,
            reply_tx: Some(reply_tx),
        };

        let timeout = timeout.map(|d| timer::timeout(d));
        let response = Response { reply_rx, timeout };
        (handler, response)
    }
}
impl<D: Decode> HandleFrame for ResponseHandler<D> {
    type Item = ();
    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Item>> {
        track!(self.decoder.decode(frame.data(), frame.is_end_of_message()))?;
        if frame.is_end_of_message() {
            let response = track!(self.decoder.finish())?;
            let reply_tx = self.reply_tx.take().expect("Never fails");
            let _ = reply_tx.exit(Ok(response));
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
    fn handle_error(&mut self, _seqno: MessageSeqNo, error: Error) {
        let reply_tx = self.reply_tx.take().expect("Never fails");
        let _ = reply_tx.exit(Err(error));
    }
}
