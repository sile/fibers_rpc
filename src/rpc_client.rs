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
            _cast: PhantomData,
        }
    }

    pub fn cast(&self, server: SocketAddr, notification: T::Notification) {
        // TODO: self.max_concurrency

        let encoder = self.encoder_maker.make_encoder(notification);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: None,
            force_wakeup: false,
        };
        self.service.send_message(server, message);
    }
}
impl<'a, T, E> RpcCastClient<'a, T, E> {
    pub fn max_concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.max_concurrency = concurrency;
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
    _call: PhantomData<T>,
}
impl<'a, T, D, E> RpcCallClient<'a, T, D, E>
where
    T: Call,
    D: MakeDecoder<T::ResponseDecoder>,
    E: MakeEncoder<T::RequestEncoder>,
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
            _call: PhantomData,
        }
    }

    pub fn call(&self, server: SocketAddr, request: T::Request) -> Response<T::Response> {
        // TODO: self.max_concurrency

        let encoder = self.encoder_maker.make_encoder(request);
        let decoder = self.decoder_maker.make_decoder();
        let (handler, response) = ResponseHandler::new(decoder, self.timeout);
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), encoder),
            response_handler: Some(Box::new(handler)),
            force_wakeup: false,
        };
        self.service.send_message(server, message);
        response
    }
}
impl<'a, T, D, E> RpcCallClient<'a, T, D, E> {
    pub fn max_concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.max_concurrency = concurrency;
        self
    }

    pub fn timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        self.timeout = timeout;
        self
    }
}
