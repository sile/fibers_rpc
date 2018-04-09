//! RPC library built on top of [fibers] crate.
//!
//!
//! [fibers]: https://github.com/dwango/fibers-rs
//!
//! # Examples
//!
//! Simple echo RPC server:
//!
//! ```
//! # extern crate bytecodec;
//! # extern crate fibers;
//! # extern crate fibers_rpc;
//! # extern crate futures;
//! # fn main() {
//! use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
//! use fibers::{Executor, InPlaceExecutor, Spawn};
//! use fibers_rpc::{Call, ProcedureId};
//! use fibers_rpc::client::ClientServiceBuilder;
//! use fibers_rpc::server::{HandleCall, Reply, ServerBuilder};
//! use futures::Future;
//!
//! // RPC definition
//! struct EchoRpc;
//! impl Call for EchoRpc {
//!     const ID: ProcedureId = ProcedureId(0);
//!     const NAME: &'static str = "echo";
//!
//!     type Req = Vec<u8>;
//!     type ReqEncoder = BytesEncoder<Vec<u8>>;
//!     type ReqDecoder = RemainingBytesDecoder;
//!
//!     type Res = Vec<u8>;
//!     type ResEncoder = BytesEncoder<Vec<u8>>;
//!     type ResDecoder = RemainingBytesDecoder;
//! }
//!
//! // Executor
//! let mut executor = InPlaceExecutor::new().unwrap();
//!
//! // RPC server
//! struct EchoHandler;
//! impl HandleCall<EchoRpc> for EchoHandler {
//!     fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<EchoRpc> {
//!         Reply::done(request)
//!     }
//! }
//! let server_addr = "127.0.0.1:1919".parse().unwrap();
//! let server = ServerBuilder::new(server_addr)
//!     .call_handler(EchoHandler)
//!     .finish(executor.handle());
//! executor.spawn(server.map_err(|e| panic!("{}", e)));
//!
//! // RPC client
//! let service = ClientServiceBuilder::new().finish(executor.handle());
//!
//! let request = Vec::from(&b"hello"[..]);
//! let response = EchoRpc::client(&service.handle()).call(server_addr, request.clone());
//!
//! executor.spawn(service.map_err(|e| panic!("{}", e)));
//! let result = executor.run_future(response).unwrap();
//! assert_eq!(result.ok(), Some(request));
//! # }
//! ```
#![warn(missing_docs)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate byteorder;
extern crate factory;
extern crate fibers;
extern crate futures;
extern crate prometrics;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use error::{Error, ErrorKind};

pub mod client {
    //! RPC client.

    pub use client_side_handlers::Response;
    pub use client_service::{ClientService, ClientServiceBuilder, ClientServiceHandle};
    pub use rpc_client::{CallClient, CastClient, Options};
}
pub mod metrics;
pub mod server {
    //! RPC server.

    pub use rpc_server::{Server, ServerBuilder};
    pub use server_side_handlers::{HandleCall, HandleCast, Never, NoReply, Reply};
}

use client::{CallClient, CastClient, ClientServiceHandle};

mod client_service;
mod client_side_channel;
mod client_side_handlers;
mod error;
mod message;
mod message_stream;
mod packet;
mod rpc_client;
mod rpc_server;
mod server_side_channel;
mod server_side_handlers;

/// This crate specific `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

/// The identifier of a procedure.
///
/// This must be unique among procedures registered in an RPC server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcedureId(pub u32);

/// Request/response RPC.
pub trait Call: Sized + Send + Sync + 'static {
    /// The identifier of the procedure.
    const ID: ProcedureId;

    /// The name of the procedure.
    ///
    /// This is only used for debugging purpose.
    const NAME: &'static str;

    /// Request message.
    type Req;

    /// Request message encoder.
    type ReqEncoder: bytecodec::Encode<Item = Self::Req> + Send + 'static;

    /// Request message decoder.
    type ReqDecoder: bytecodec::Decode<Item = Self::Req> + Send + 'static;

    /// Response message.
    type Res: Send + 'static;

    /// Response message encoder.
    type ResEncoder: bytecodec::Encode<Item = Self::Res> + Send + 'static;

    /// Response message decoder.
    type ResDecoder: bytecodec::Decode<Item = Self::Res> + Send + 'static;

    /// Makes a new RPC client.
    fn client(service: &ClientServiceHandle) -> CallClient<Self>
    where
        Self::ReqEncoder: Default,
        Self::ResDecoder: Default,
    {
        Self::client_with_codec(service, Default::default(), Default::default())
    }

    /// Makes a new RPC client with the given decoder maker.
    fn client_with_decoder(
        service: &ClientServiceHandle,
        decoder: Self::ResDecoder,
    ) -> CallClient<Self>
    where
        Self::ReqEncoder: Default,
    {
        Self::client_with_codec(service, decoder, Default::default())
    }

    /// Makes a new RPC client with the given encoder maker.
    fn client_with_encoder(
        service: &ClientServiceHandle,
        encoder: Self::ReqEncoder,
    ) -> CallClient<Self>
    where
        Self::ResDecoder: Default,
    {
        Self::client_with_codec(service, Default::default(), encoder)
    }

    /// Makes a new RPC client with the given decoder and encoder makers.
    fn client_with_codec(
        service: &ClientServiceHandle,
        decoder: Self::ResDecoder,
        encoder: Self::ReqEncoder,
    ) -> CallClient<Self> {
        CallClient::new(service, decoder, encoder)
    }
}

/// Notification RPC.
pub trait Cast: Sized + Sync + Send + 'static {
    /// The identifier of the procedure.
    const ID: ProcedureId;

    /// The name of the procedure.
    ///
    /// This is only used for debugging purpose.
    const NAME: &'static str;

    /// Notification message.
    type Notification;

    /// Notification message encoder.
    type Encoder: bytecodec::Encode<Item = Self::Notification> + Send + 'static;

    /// Notification message decoder.
    type Decoder: bytecodec::Decode<Item = Self::Notification> + Send + 'static;

    /// Makes a new RPC client.
    fn client(service: &ClientServiceHandle) -> CastClient<Self>
    where
        Self::Encoder: Default,
    {
        Self::client_with_encoder(service, Default::default())
    }

    /// Makes a new RPC client with the given encoder maker.
    fn client_with_encoder(
        service: &ClientServiceHandle,
        encoder: Self::Encoder,
    ) -> CastClient<Self> {
        CastClient::new(service, encoder)
    }
}

#[cfg(test)]
mod test {
    use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
    use fibers::{Executor, InPlaceExecutor, Spawn};
    use futures::Future;

    use {Call, ProcedureId};
    use client::ClientServiceBuilder;
    use server::{HandleCall, Reply, ServerBuilder};

    // RPC
    struct EchoRpc;
    impl Call for EchoRpc {
        const ID: ProcedureId = ProcedureId(0);
        const NAME: &'static str = "echo";

        type Req = Vec<u8>;
        type ReqEncoder = BytesEncoder<Vec<u8>>;
        type ReqDecoder = RemainingBytesDecoder;

        type Res = Vec<u8>;
        type ResEncoder = BytesEncoder<Vec<u8>>;
        type ResDecoder = RemainingBytesDecoder;
    }

    // Handler
    struct EchoHandler;
    impl HandleCall<EchoRpc> for EchoHandler {
        fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<EchoRpc> {
            Reply::done(request)
        }
    }

    #[test]
    fn it_works() {
        // Executor
        let mut executor = track_try_unwrap!(track_any_err!(InPlaceExecutor::new()));

        // Server
        let server_addr = "127.0.0.1:1920".parse().unwrap();
        let server = ServerBuilder::new(server_addr)
            .call_handler(EchoHandler)
            .finish(executor.handle());
        executor.spawn(server.map_err(|e| panic!("{}", e)));

        // Client
        let service = ClientServiceBuilder::new().finish(executor.handle());

        let request = Vec::from(&b"hello"[..]);
        let response = EchoRpc::client(&service.handle()).call(server_addr, request.clone());

        executor.spawn(service.map_err(|e| panic!("{}", e)));
        let result = track_try_unwrap!(track_any_err!(executor.run_future(response)));
        assert_eq!(result.ok(), Some(request));
    }

    #[test]
    fn large_message_works() {
        // Executor
        let mut executor = track_try_unwrap!(track_any_err!(InPlaceExecutor::new()));

        // Server
        let server_addr = "127.0.0.1:1921".parse().unwrap();
        let server = ServerBuilder::new(server_addr)
            .call_handler(EchoHandler)
            .finish(executor.handle());
        executor.spawn(server.map_err(|e| panic!("{}", e)));

        // Client
        let service = ClientServiceBuilder::new().finish(executor.handle());

        let request = vec![0; 10 * 1024 * 1024];
        let response = EchoRpc::client(&service.handle()).call(server_addr, request.clone());

        executor.spawn(service.map_err(|e| panic!("{}", e)));
        let result = track_try_unwrap!(track_any_err!(executor.run_future(response)));
        assert_eq!(result.ok(), Some(request));
    }
}
