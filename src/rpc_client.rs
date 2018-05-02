use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use {Call, Cast, ErrorKind, Result};
use client_service::{ClientServiceHandle, Message};
use client_side_handlers::{Response, ResponseHandler};
use message::{MessageHeader, MessageId, OutgoingMessage, OutgoingMessagePayload};
use metrics::ClientMetrics;

/// Client for notification RPC.
#[derive(Debug)]
pub struct CastClient<'a, T: Cast> {
    service: &'a ClientServiceHandle,
    encoder: T::Encoder,
    options: Options,
    _cast: PhantomData<T>,
}
impl<'a, T> CastClient<'a, T>
where
    T: Cast,
{
    pub(crate) fn new(service: &'a ClientServiceHandle, encoder: T::Encoder) -> Self {
        CastClient {
            service,
            encoder,
            options: Options::default(),
            _cast: PhantomData,
        }
    }

    /// Sends the notification message to the RPC server.
    pub fn cast(self, server: SocketAddr, notification: T::Notification) -> Result<()> {
        if !self.options
            .is_allowable_queue_len(&self.service.metrics, server)
        {
            self.service.metrics.discarded_outgoing_messages.increment();
            let e = track!(ErrorKind::Unavailable.cause("too long transmit queue"));
            return Err(e.into());
        }

        let header = MessageHeader {
            id: MessageId(0), // dummy
            procedure: T::ID,
            priority: self.options.priority,
            async: T::enable_async(&notification),
        };
        let message = Message {
            message: OutgoingMessage {
                header,
                payload: OutgoingMessagePayload::with_item(self.encoder, notification),
            },
            response_handler: None,
            force_wakeup: self.options.force_wakeup,
        };
        if !self.service.send_message(server, message) {
            self.service.metrics.discarded_outgoing_messages.increment();
            let e = track!(ErrorKind::Unavailable.cause("client service or server is unavailable"));
            return Err(e.into());
        }

        self.service.metrics.notifications.increment();
        Ok(())
    }
}
impl<'a, T: Cast> CastClient<'a, T> {
    /// Returns a reference to the RPC options of this client.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Returns a mutable reference to the RPC options of this client.
    pub fn options_mut(&mut self) -> &mut Options {
        &mut self.options
    }

    /// Returns a reference to the encoder of this client.
    pub fn encoder(&self) -> &T::Encoder {
        &self.encoder
    }

    /// Returns a mutable reference to the encoder of this client.
    pub fn encoder_mu(&mut self) -> &mut T::Encoder {
        &mut self.encoder
    }
}

/// Client for request/response RPC.
#[derive(Debug)]
pub struct CallClient<'a, T: Call> {
    service: &'a ClientServiceHandle,
    decoder: T::ResDecoder,
    encoder: T::ReqEncoder,
    options: Options,
    _call: PhantomData<T>,
}
impl<'a, T: Call> CallClient<'a, T> {
    pub(crate) fn new(
        service: &'a ClientServiceHandle,
        decoder: T::ResDecoder,
        encoder: T::ReqEncoder,
    ) -> Self {
        CallClient {
            service,
            decoder,
            encoder,
            options: Options::default(),
            _call: PhantomData,
        }
    }

    /// Sends the request message to the RPC server,
    /// and returns a future that represents the response from the server.
    pub fn call(self, server: SocketAddr, request: T::Req) -> Response<T::Res> {
        if !self.options
            .is_allowable_queue_len(&self.service.metrics, server)
        {
            self.service.metrics.discarded_outgoing_messages.increment();
            let e = track!(ErrorKind::Unavailable.cause("too long transmit queue"));
            return Response::error(e.into());
        }

        let header = MessageHeader {
            id: MessageId(0), // dummy
            procedure: T::ID,
            priority: self.options.priority,
            async: T::enable_async_request(&request),
        };

        let (handler, response) = ResponseHandler::new(
            self.decoder,
            self.options.timeout,
            Arc::clone(&self.service.metrics),
            T::NAME,
        );

        let message = Message {
            message: OutgoingMessage {
                header,
                payload: OutgoingMessagePayload::with_item(self.encoder, request),
            },
            response_handler: Some(Box::new(handler)),
            force_wakeup: self.options.force_wakeup,
        };

        if !self.service.send_message(server, message) {
            self.service.metrics.discarded_outgoing_messages.increment();
            let e = track!(ErrorKind::Unavailable.cause("client service or server is unavailable"));
            return Response::error(e.into());
        }
        self.service.metrics.requests.increment();
        response
    }
}
impl<'a, T: Call> CallClient<'a, T> {
    /// Returns a reference to the RPC options of this client.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Returns a mutable reference to the RPC options of this client.
    pub fn options_mut(&mut self) -> &mut Options {
        &mut self.options
    }

    /// Returns a reference to the decoder of this client.
    pub fn decoder(&self) -> &T::ResDecoder {
        &self.decoder
    }

    /// Returns a mutable reference to the decoder of this client.
    pub fn decoder_mut(&mut self) -> &mut T::ResDecoder {
        &mut self.decoder
    }

    /// Returns a reference to the encoder of this client.
    pub fn encoder(&self) -> &T::ReqEncoder {
        &self.encoder
    }

    /// Returns a mutable reference to the encoder of this client.
    pub fn encoder_mut(&mut self) -> &mut T::ReqEncoder {
        &mut self.encoder
    }
}

/// Options for RPC.
#[derive(Debug, Clone)]
pub struct Options {
    /// The timeout of a RPC request.
    ///
    /// The default value is `None` and it means there is no timeout.
    ///
    /// This is no effect on notification RPC.
    pub timeout: Option<Duration>,

    /// The allowable number of messages in the transmit queue.
    ///
    /// If the current queue length exceeds this value, the new message will be discarded before sending.
    ///
    /// The default value is `None` and it means there is no limitation.
    pub max_queue_len: Option<u64>,

    /// The priority of the RPC invocation.
    ///
    /// The lower the value, the higher the priority.
    ///
    /// The default value is `128`.
    pub priority: u8,

    /// If it is `true`, RPC channel waiting for reconnecting will wake up immediately.
    ///
    /// The defaul value is `false`.
    pub force_wakeup: bool,
}
impl Options {
    /// The default priority.
    pub const DEFAULT_PRIORITY: u8 = 128;

    fn is_allowable_queue_len(&self, metrics: &ClientMetrics, server: SocketAddr) -> bool {
        self.max_queue_len.map_or(true, |max| {
            let queue_len = metrics
                .channels()
                .as_map()
                .load()
                .get(&server)
                .map_or(0, |channel| channel.queue_len());
            queue_len <= max
        })
    }
}
impl Default for Options {
    fn default() -> Self {
        Options {
            timeout: None,
            max_queue_len: None,
            priority: Self::DEFAULT_PRIORITY,
            force_wakeup: false,
        }
    }
}
