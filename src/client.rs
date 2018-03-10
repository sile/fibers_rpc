// use std::net::SocketAddr;
// use std::marker::PhantomData;
// use fibers::sync::oneshot;
// use futures::{Future, Poll};
// use trackable::error::ErrorKindExt;

// pub use self::service::{RpcClientService, RpcClientServiceBuilder};

// use {Error, ErrorKind, Result};
// use message::{Message, OutgoingMessage, RequestMesasge};
// use traits::{Call, Cast, Decode, Encode};
// use self::service::RpcClientServiceHandle;

// mod service;

use client_service::RpcClientServiceHandle;

#[derive(Debug, Clone)]
pub struct RpcClient {
    // TODO:
    pub(crate) service: RpcClientServiceHandle,
}
// impl RpcClient {
//     pub fn cast<T: Cast>(&self, server: SocketAddr, notification: T::Notification)
//     where
//         T::Encoder: From<T::Notification>,
//     {
//         self.cast_options::<T>().cast(server, notification)
//     }

//     pub fn cast_options<T: Cast>(&self) -> CastOptions<T> {
//         CastOptions::new(self)
//     }

//     pub fn call<T: Call>(&self, server: SocketAddr, request: T::Request) -> RpcCall<T>
//     where
//         T::RequestEncoder: From<T::Request>,
//         T::ResponseDecoder: Default,
//     {
//         self.call_options().call(server, request)
//     }

//     pub fn call_options<T: Call>(&self) -> CallOptions<T> {
//         CallOptions::new(self)
//     }
// }

// #[derive(Debug)]
// pub struct WithEncoder<'a, T: 'a, F> {
//     inner: &'a T,
//     into_encoder: F,
// }
// impl<'a, T: Cast, F> WithEncoder<'a, CastOptions<'a, T>, F> {
//     pub fn cast(self, server: SocketAddr, notification: T::Notification)
//     where
//         F: FnOnce(T::Notification) -> T::Encoder,
//     {
//         let encoder = (self.into_encoder)(notification);
//         self.inner.execute(server, encoder);
//     }
// }
// impl<'a, T: Call, F> WithEncoder<'a, CallOptions<'a, T>, F> {
//     pub fn call(self, server: SocketAddr, request: T::Request) -> RpcCall<T>
//     where
//         F: FnOnce(T::Request) -> T::RequestEncoder,
//         T::ResponseDecoder: Default,
//     {
//         let encoder = (self.into_encoder)(request);
//         let decoder = Default::default();
//         self.inner.execute(server, encoder, decoder)
//     }
// }

// #[derive(Debug)]
// pub struct CastOptions<'a, T> {
//     client: &'a RpcClient,
//     _rpc: PhantomData<T>,
// }
// impl<'a, T: Cast> CastOptions<'a, T> {
//     fn new(client: &'a RpcClient) -> Self {
//         CastOptions {
//             client,
//             _rpc: PhantomData,
//         }
//     }

//     pub fn cast(&self, server: SocketAddr, notification: T::Notification)
//     where
//         T::Encoder: From<T::Notification>,
//     {
//         let encoder = From::from(notification);
//         self.execute(server, encoder);
//     }

//     pub fn with_encoder<F>(&self, into_encoder: F) -> WithEncoder<Self, F>
//     where
//         F: FnOnce(T::Notification) -> T::Encoder,
//     {
//         WithEncoder {
//             inner: self,
//             into_encoder,
//         }
//     }

//     fn execute(&self, server: SocketAddr, encoder: T::Encoder) {
//         let message = OutgoingMessage::<T::Encoder>::new(T::PROCEDURE, encoder).into_encodable();
//         self.client
//             .service
//             .send_message(server, Message::Notification(message));
//     }
// }

// #[derive(Debug)]
// pub struct CallOptions<'a, T> {
//     client: &'a RpcClient,
//     _rpc: PhantomData<T>,
// }
// impl<'a, T: Call> CallOptions<'a, T> {
//     fn new(client: &'a RpcClient) -> Self {
//         CallOptions {
//             client,
//             _rpc: PhantomData,
//         }
//     }

//     pub fn with_encoder<F>(&self, into_encoder: F) -> WithEncoder<Self, F>
//     where
//         F: FnOnce(T::Request) -> T::RequestEncoder,
//     {
//         WithEncoder {
//             inner: self,
//             into_encoder,
//         }
//     }

//     pub fn call(&self, server: SocketAddr, request: T::Request) -> RpcCall<T>
//     where
//         T::RequestEncoder: From<T::Request>,
//         T::ResponseDecoder: Default,
//     {
//         let encoder = From::from(request);
//         let decoder = Default::default();
//         self.execute(server, encoder, decoder)
//     }

//     fn execute(
//         &self,
//         server: SocketAddr,
//         encoder: T::RequestEncoder,
//         decoder: T::ResponseDecoder,
//     ) -> RpcCall<T> {
//         let (response_tx, response_rx) = oneshot::monitor();
//         let decoder: ResponseDecoder<T::Response, T::ResponseDecoder> = ResponseDecoder {
//             decoder,
//             response_tx: Some(response_tx),
//         };
//         let message = RequestMesasge {
//             phase: 0,
//             procedure: T::PROCEDURE,
//             request_id: 0, // TODO: remove
//             request_data: encoder.into_encodable(),
//             response_decoder: decoder.boxed(),
//         };
//         self.client
//             .service
//             .send_message(server, Message::Request(message));
//         RpcCall(response_rx)
//     }
// }

// // TODO: rename
// #[derive(Debug)]
// pub struct RpcCall<T: Call>(oneshot::Monitor<T::Response, Error>);
// impl<T: Call> Future for RpcCall<T> {
//     type Item = T::Response;
//     type Error = Error;
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         track!(self.0.poll().map_err(|e| e.unwrap_or_else(|| {
//             ErrorKind::Other.cause("RPC channel disconnected").into()
//         })))
//     }
// }

// #[derive(Debug)]
// struct ResponseDecoder<T, D> {
//     decoder: D,
//     response_tx: Option<oneshot::Monitored<T, Error>>,
// }
// impl<T, D> Decode<()> for ResponseDecoder<T, D>
// where
//     D: Decode<T>,
// {
//     fn decode(&mut self, buf: &[u8]) -> Result<()> {
//         if let Err(e) = track!(self.decoder.decode(buf)) {
//             let response_tx = self.response_tx.take().expect("Never fails");
//             response_tx.exit(Err(e.clone()));
//             Err(e)
//         } else {
//             Ok(())
//         }
//     }
//     fn finish(&mut self) -> Result<()> {
//         let response_tx = self.response_tx.take().expect("Never fails");
//         match track!(self.decoder.finish()) {
//             Err(e) => {
//                 response_tx.exit(Err(e.clone()));
//                 Err(e)
//             }
//             Ok(response) => {
//                 response_tx.exit(Ok(response));
//                 Ok(())
//             }
//         }
//     }
// }
