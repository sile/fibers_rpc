use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use {Call, Cast};
use client_service::{Message, RpcClientServiceHandle};
use client_side_handlers::{Response, ResponseHandler};
use codec::{MakeDecoder, MakeEncoder};
use message::OutgoingMessage;

const DEFAULT_MAX_CONCURRENCY: usize = 4096;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

/// Client for notification RPC.
#[derive(Debug)]
pub struct RpcCastClient<'a, T, E> {
    service: &'a RpcClientServiceHandle,
    encoder_maker: E,
    max_concurrency: usize,
    force_wakeup: bool,
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
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            force_wakeup: false,
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
            force_wakeup: self.force_wakeup,
        };
        self.service.send_message(server, message);
    }
}
impl<'a, T, E> RpcCastClient<'a, T, E> {
    /// Sets the maximum concurrency of the RPC channel between the client service and the server.
    ///
    /// If the channel has ongoing messages more than `concurrency`,
    /// the RPC will fail with `ErrorKind::Unavailable` error.
    ///
    /// The default value is `4096`.
    pub fn max_concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.max_concurrency = concurrency;
        self
    }

    /// If `force` is `true`, RPC chanenl waiting for reconnecting will wake up immediately.
    ///
    /// The default value is `false`.
    pub fn force_wakeup(&mut self, force: bool) -> &mut Self {
        self.force_wakeup = force;
        self
    }
}

/// Client for request/response RPC.
#[derive(Debug)]
pub struct RpcCallClient<'a, T, D, E> {
    service: &'a RpcClientServiceHandle,
    decoder_maker: D,
    encoder_maker: E,
    max_concurrency: usize,
    timeout: Option<Duration>,
    force_wakeup: bool,
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
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            timeout: Some(Duration::from_secs(DEFAULT_TIMEOUT_SECS)),
            force_wakeup: false,
            _call: PhantomData,
        }
    }

    /// Sends the request message to the RPC server,
    /// and returns a future that represents the response from the server.
    pub fn call(&self, server: SocketAddr, request: T::Req) -> Response<T::Res> {
        // TODO: self.max_concurrency

        let encoder = self.encoder_maker.make_encoder(request);
        let decoder = self.decoder_maker.make_decoder();
        let (handler, response) = ResponseHandler::new(decoder, self.timeout);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: Some(Box::new(handler)),
            force_wakeup: self.force_wakeup,
        };
        self.service.send_message(server, message);
        response
    }
}
impl<'a, T, D, E> RpcCallClient<'a, T, D, E> {
    /// Sets the maximum concurrency of the RPC channel between the client service and the server.
    ///
    /// If the channel has ongoing messages more than `concurrency`,
    /// the RPC will fail with `ErrorKind::Unavailable` error.
    ///
    /// The default value is `4096`.
    pub fn max_concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.max_concurrency = concurrency;
        self
    }

    /// Sets the timeout of the RPC request.
    ///
    /// `None` means there is no timeout.
    ///
    /// The default value is `Some(Duration::from_secs(5))`.
    pub fn timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        self.timeout = timeout;
        self
    }

    /// If `force` is `true`, RPC chanenl waiting for reconnecting will wake up immediately.
    ///
    /// The default value is `false`.
    pub fn force_wakeup(&mut self, force: bool) -> &mut Self {
        self.force_wakeup = force;
        self
    }
}
