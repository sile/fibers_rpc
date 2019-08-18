use crate::message::{AssignIncomingMessageHandler, MessageHeader, MessageId};
use crate::metrics::ClientMetrics;
use crate::{Error, ErrorKind, Result};
use bytecodec::{self, ByteCount, Decode, Eos};
use fibers::sync::oneshot;
use fibers::time::timer::{self, Timeout};
use futures::{Async, Future, Poll};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use trackable::error::ErrorKindExt;

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
            track!(e.unwrap_or_else(|| ErrorKind::Other
                .cause("RPC response monitoring channel disconnected")
                .into()))
        })?;
        if let Async::Ready(item) = item {
            Ok(Async::Ready(item))
        } else {
            let expired = self
                .timeout
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
pub struct Assigner {
    handlers: HashMap<MessageId, BoxResponseHandler>,
}
impl Assigner {
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
impl AssignIncomingMessageHandler for Assigner {
    type Handler = BoxResponseHandler;

    fn assign_incoming_message_handler(&mut self, header: &MessageHeader) -> Result<Self::Handler> {
        let handler = track_assert_some!(
            self.handlers.remove(&header.id),
            ErrorKind::InvalidInput,
            "header={:?}",
            header
        );
        Ok(handler)
    }
}
impl fmt::Debug for Assigner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Assigner {{ handlers.len: {} }}", self.handlers.len())
    }
}

pub type BoxResponseHandler = Box<dyn HandleResponse<Item = ()> + Send + 'static>;

pub trait HandleResponse: Decode<Item = ()> {
    fn handle_error(&mut self, error: Error);
}

#[derive(Debug)]
pub struct ResponseHandler<D: Decode> {
    decoder: D,
    reply_tx: Option<oneshot::Monitored<D::Item, Error>>,
    metrics: Arc<ClientMetrics>,
    rpc_name: &'static str,
}
impl<D: Decode> ResponseHandler<D> {
    pub fn new(
        decoder: D,
        timeout: Option<Duration>,
        metrics: Arc<ClientMetrics>,
        rpc_name: &'static str,
    ) -> (Self, Response<D::Item>) {
        let (reply_tx, reply_rx) = oneshot::monitor();
        let handler = ResponseHandler {
            decoder,
            reply_tx: Some(reply_tx),
            metrics,
            rpc_name,
        };

        let timeout = timeout.map(timer::timeout);
        let response = Response { reply_rx, timeout };
        (handler, response)
    }
}
impl<D: Decode> Decode for ResponseHandler<D> {
    type Item = ();

    fn decode(&mut self, buf: &[u8], eos: Eos) -> bytecodec::Result<usize> {
        track!(self.decoder.decode(buf, eos); self.rpc_name)
    }

    fn finish_decoding(&mut self) -> bytecodec::Result<Self::Item> {
        let response = track!(self.decoder.finish_decoding(); self.rpc_name)?;
        let reply_tx = self.reply_tx.take().expect("Never fails");
        reply_tx.exit(Ok(response));
        self.metrics.ok_responses.increment();
        Ok(())
    }

    fn requiring_bytes(&self) -> ByteCount {
        self.decoder.requiring_bytes()
    }

    fn is_idle(&self) -> bool {
        self.decoder.is_idle()
    }
}
impl<D: Decode> HandleResponse for ResponseHandler<D> {
    fn handle_error(&mut self, error: Error) {
        let reply_tx = self.reply_tx.take().expect("Never fails");
        reply_tx.exit(Err(error));
        self.metrics.error_responses.increment();
    }
}
