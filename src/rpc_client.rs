use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use {Call, Cast};
use client_service::{ClientServiceHandle, Message};
use client_side_handlers::{Response, ResponseHandler};
use codec::{MakeDecoder, MakeEncoder};
use message::OutgoingMessage;

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
    pub fn cast(&self, server: SocketAddr, notification: T::Notification) {
        let encoder = self.encoder_maker.make_encoder(notification);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: None,
            force_wakeup: self.options.force_wakeup,
        };
        self.service.send_message(server, message);
        self.service.metrics.notifications.increment();
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
        let encoder = self.encoder_maker.make_encoder(request);
        let decoder = self.decoder_maker.make_decoder();
        let (handler, response) = ResponseHandler::new(
            decoder,
            self.options.timeout,
            Arc::clone(&self.service.metrics),
        );
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: Some(Box::new(handler)),
            force_wakeup: self.options.force_wakeup,
        };
        self.service.send_message(server, message);
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

    /// If it is `true`, RPC chanenl waiting for reconnecting will wake up immediately.
    ///
    /// The defaul value is `false`.
    pub force_wakeup: bool,
}
impl Default for Options {
    fn default() -> Self {
        Options {
            timeout: None,
            force_wakeup: false,
        }
    }
}
