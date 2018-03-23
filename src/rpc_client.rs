use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use trackable::error::ErrorKindExt;

use {Call, Cast, ErrorKind};
use client_service::{ClientServiceHandle, Message};
use client_side_handlers::{Response, ResponseHandler};
use codec::{MakeDecoder, MakeEncoder};
use message::OutgoingMessage;
use metrics::ClientMetrics;

/// Client for notification RPC.
#[derive(Debug)]
pub struct CastClient<'a, T, E> {
    service: &'a ClientServiceHandle,
    encoder_maker: E,
    options: Options,
    _cast: PhantomData<T>,
}
impl<'a, T, E> CastClient<'a, T, E>
where
    T: Cast,
    E: MakeEncoder<T::Encoder>,
{
    pub(crate) fn new(service: &'a ClientServiceHandle, encoder_maker: E) -> Self {
        CastClient {
            service,
            encoder_maker,
            options: Options::default(),
            _cast: PhantomData,
        }
    }

    /// Sends the notification message to the RPC server.
    ///
    /// If the message is discarded before sending, this method will return `false`.
    pub fn cast(&self, server: SocketAddr, notification: T::Notification) -> bool {
        if !self.options
            .is_allowable_queue_len(&self.service.metrics, server)
        {
            self.service.metrics.discarded_outgoing_messages.increment();
            return false;
        }

        let encoder = self.encoder_maker.make_encoder(notification);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), self.options.priority, encoder),
            response_handler: None,
            force_wakeup: self.options.force_wakeup,
        };
        if !self.service.send_message(server, message) {
            self.service.metrics.discarded_outgoing_messages.increment();
            return false;
        }

        self.service.metrics.notifications.increment();
        true
    }
}
impl<'a, T, E> CastClient<'a, T, E> {
    /// Returns a reference to the RPC options of this client.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Returns a mutable reference to the RPC options of this client.
    pub fn options_mut(&mut self) -> &mut Options {
        &mut self.options
    }

    /// Returns a reference to the encoder maker of this client.
    pub fn encoder_maker(&self) -> &E {
        &self.encoder_maker
    }

    /// Returns a mutable reference to the encoder maker of this client.
    pub fn encoder_maker_mut(&mut self) -> &mut E {
        &mut self.encoder_maker
    }
}

/// Client for request/response RPC.
#[derive(Debug)]
pub struct CallClient<'a, T, D, E> {
    service: &'a ClientServiceHandle,
    decoder_maker: D,
    encoder_maker: E,
    options: Options,
    _call: PhantomData<T>,
}
impl<'a, T, D, E> CallClient<'a, T, D, E>
where
    T: Call,
    D: MakeDecoder<T::ResDecoder>,
    E: MakeEncoder<T::ReqEncoder>,
{
    pub(crate) fn new(
        service: &'a ClientServiceHandle,
        decoder_maker: D,
        encoder_maker: E,
    ) -> Self {
        CallClient {
            service,
            decoder_maker,
            encoder_maker,
            options: Options::default(),
            _call: PhantomData,
        }
    }

    /// Sends the request message to the RPC server,
    /// and returns a future that represents the response from the server.
    pub fn call(&self, server: SocketAddr, request: T::Req) -> Response<T::Res> {
        if !self.options
            .is_allowable_queue_len(&self.service.metrics, server)
        {
            self.service.metrics.discarded_outgoing_messages.increment();
            let e = track!(ErrorKind::Unavailable.cause("too long transmit queue"));
            return Response::error(e.into());
        }

        let encoder = self.encoder_maker.make_encoder(request);
        let decoder = self.decoder_maker.make_decoder();
        let (handler, response) = ResponseHandler::new(
            decoder,
            self.options.timeout,
            Arc::clone(&self.service.metrics),
        );

        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), self.options.priority, encoder),
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
impl<'a, T, D, E> CallClient<'a, T, D, E> {
    /// Returns a reference to the RPC options of this client.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Returns a mutable reference to the RPC options of this client.
    pub fn options_mut(&mut self) -> &mut Options {
        &mut self.options
    }

    /// Returns a reference to the decoder maker of this client.
    pub fn decoder_maker(&self) -> &D {
        &self.decoder_maker
    }

    /// Returns a mutable reference to the decoder maker of this client.
    pub fn decoder_maker_mut(&mut self) -> &mut D {
        &mut self.decoder_maker
    }

    /// Returns a reference to the encoder maker of this client.
    pub fn encoder_maker(&self) -> &E {
        &self.encoder_maker
    }

    /// Returns a mutable reference to the encoder maker of this client.
    pub fn encoder_maker_mut(&mut self) -> &mut E {
        &mut self.encoder_maker
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
