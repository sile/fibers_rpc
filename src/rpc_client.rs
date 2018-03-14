use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use {Call, Cast};
use client_service::{Message, RpcClientServiceHandle};
use client_side_handlers::{Response, ResponseHandler};
use codec::{MakeDecoder, MakeEncoder};
use message::OutgoingMessage;

/// Client for notification RPC.
#[derive(Debug)]
pub struct RpcCastClient<'a, T, E> {
    service: &'a RpcClientServiceHandle,
    encoder_maker: E,
    options: RpcOptions,
    _cast: PhantomData<T>,
}
impl<'a, T, E> RpcCastClient<'a, T, E>
where
    T: Cast,
    E: MakeEncoder<T::Encoder>,
{
    pub(crate) fn new(service: &'a RpcClientServiceHandle, encoder_maker: E) -> Self {
        RpcCastClient {
            service,
            encoder_maker,
            options: RpcOptions::default(),
            _cast: PhantomData,
        }
    }

    /// Sends the notification message to the RPC server.
    pub fn cast(&self, server: SocketAddr, notification: T::Notification) {
        // TODO: self.max_concurrency

        let encoder = self.encoder_maker.make_encoder(notification);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: None,
            force_wakeup: self.options.force_wakeup,
        };
        self.service.send_message(server, message);
    }
}
impl<'a, T, E> RpcCastClient<'a, T, E> {
    /// Returns a reference to the RPC options of this client.
    pub fn options(&self) -> &RpcOptions {
        &self.options
    }

    /// Returns a mutable reference to the RPC options of this client.
    pub fn options_mut(&mut self) -> &mut RpcOptions {
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
pub struct RpcCallClient<'a, T, D, E> {
    service: &'a RpcClientServiceHandle,
    decoder_maker: D,
    encoder_maker: E,
    options: RpcOptions,
    _call: PhantomData<T>,
}
impl<'a, T, D, E> RpcCallClient<'a, T, D, E>
where
    T: Call,
    D: MakeDecoder<T::ResDecoder>,
    E: MakeEncoder<T::ReqEncoder>,
{
    pub(crate) fn new(
        service: &'a RpcClientServiceHandle,
        decoder_maker: D,
        encoder_maker: E,
    ) -> Self {
        RpcCallClient {
            service,
            decoder_maker,
            encoder_maker,
            options: RpcOptions::default(),
            _call: PhantomData,
        }
    }

    /// Sends the request message to the RPC server,
    /// and returns a future that represents the response from the server.
    pub fn call(&self, server: SocketAddr, request: T::Req) -> Response<T::Res> {
        // TODO: self.max_concurrency

        let encoder = self.encoder_maker.make_encoder(request);
        let decoder = self.decoder_maker.make_decoder();
        let (handler, response) = ResponseHandler::new(decoder, self.options.timeout);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: Some(Box::new(handler)),
            force_wakeup: self.options.force_wakeup,
        };
        self.service.send_message(server, message);
        response
    }
}
impl<'a, T, D, E> RpcCallClient<'a, T, D, E> {
    /// Returns a reference to the RPC options of this client.
    pub fn options(&self) -> &RpcOptions {
        &self.options
    }

    /// Returns a mutable reference to the RPC options of this client.
    pub fn options_mut(&mut self) -> &mut RpcOptions {
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
pub struct RpcOptions {
    /// The timeout of a RPC request.
    ///
    /// The default value is `None` and it means there is no timeout.
    ///
    /// This is no effect on notification RPC.
    pub timeout: Option<Duration>,

    /// The maximum concurrency of the RPC channel between the client service and the server.
    ///
    /// If the channel has ongoing messages more than `concurrency`,
    /// the RPC will fail with `ErrorKind::Unavailable` error.
    ///
    /// The default value is `DEFAULT_MAX_CONCURRENCY`.
    pub max_concurrency: usize,

    /// If it is `true`, RPC chanenl waiting for reconnecting will wake up immediately.
    ///
    /// The defaul value is `false`.
    pub force_wakeup: bool,
}
impl RpcOptions {
    /// The default value of the `max_concurrency` field.
    pub const DEFAULT_MAX_CONCURRENCY: usize = 4096;
}
impl Default for RpcOptions {
    fn default() -> Self {
        RpcOptions {
            timeout: None,
            max_concurrency: Self::DEFAULT_MAX_CONCURRENCY,
            force_wakeup: false,
        }
    }
}
