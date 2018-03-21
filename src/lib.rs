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
//! # extern crate fibers;
//! # extern crate fibers_rpc;
//! # extern crate futures;
//! # fn main() {
//! use fibers::{Executor, InPlaceExecutor, Spawn};
//! use fibers_rpc::{Call, ProcedureId};
//! use fibers_rpc::client::ClientServiceBuilder;
//! use fibers_rpc::codec::BytesEncoder;
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
//!     type ReqDecoder = Vec<u8>;
//!
//!     type Res = Vec<u8>;
//!     type ResEncoder = BytesEncoder<Vec<u8>>;
//!     type ResDecoder = Vec<u8>;
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
#[cfg(feature = "bincode_codec")]
extern crate bincode;
extern crate byteorder;
extern crate fibers;
extern crate futures;
extern crate prometrics;
#[cfg(feature = "msgpack_codec")]
extern crate rmp_serde;
#[cfg(any(feature = "bincode_codec", feature = "json_codec", feature = "msgpack_codec"))]
extern crate serde;
#[cfg(feature = "json_codec")]
extern crate serde_json;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use error::{Error, ErrorKind};

pub mod codec;
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
use codec::{DefaultDecoderMaker, IntoEncoderMaker, MakeDecoder, MakeEncoder};

mod client_service;
mod client_side_channel;
mod client_side_handlers;
#[cfg(feature = "bincode_codec")]
mod codec_bincode;
#[cfg(feature = "json_codec")]
mod codec_json;
#[cfg(feature = "msgpack_codec")]
mod codec_msgpack;
mod error;
mod frame;
mod frame_stream;
mod message;
mod message_stream;
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
    type ReqEncoder: codec::Encode<Message = Self::Req> + Send + 'static;

    /// Request message decoder.
    type ReqDecoder: codec::Decode<Message = Self::Req> + Send + 'static;

    /// Response message.
    type Res: Send + 'static;

    /// Response message encoder.
    type ResEncoder: codec::Encode<Message = Self::Res> + Send + 'static;

    /// Response message decoder.
    type ResDecoder: codec::Decode<Message = Self::Res> + Send + 'static;

    /// Makes a new RPC client.
    fn client(
        service: &ClientServiceHandle,
    ) -> CallClient<Self, DefaultDecoderMaker<Self::ResDecoder>, IntoEncoderMaker<Self::ReqEncoder>>
    where
        Self::ReqEncoder: From<Self::Req>,
        Self::ResDecoder: Default,
    {
        Self::client_with_codec(service, DefaultDecoderMaker::new(), IntoEncoderMaker::new())
    }

    /// Makes a new RPC client with the given decoder maker.
    fn client_with_decoder<D>(
        service: &ClientServiceHandle,
        decoder_maker: D,
    ) -> CallClient<Self, D, IntoEncoderMaker<Self::ReqEncoder>>
    where
        Self::ReqEncoder: From<Self::Req>,
        D: MakeDecoder<Self::ResDecoder>,
    {
        Self::client_with_codec(service, decoder_maker, IntoEncoderMaker::new())
    }

    /// Makes a new RPC client with the given encoder maker.
    fn client_with_encoder<E>(
        service: &ClientServiceHandle,
        encoder_maker: E,
    ) -> CallClient<Self, DefaultDecoderMaker<Self::ResDecoder>, E>
    where
        Self::ResDecoder: Default,
        E: MakeEncoder<Self::ReqEncoder>,
    {
        Self::client_with_codec(service, DefaultDecoderMaker::new(), encoder_maker)
    }

    /// Makes a new RPC client with the given decoder and encoder makers.
    fn client_with_codec<D, E>(
        service: &ClientServiceHandle,
        decoder_maker: D,
        encoder_maker: E,
    ) -> CallClient<Self, D, E>
    where
        D: MakeDecoder<Self::ResDecoder>,
        E: MakeEncoder<Self::ReqEncoder>,
    {
        CallClient::new(service, decoder_maker, encoder_maker)
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
    type Encoder: codec::Encode<Message = Self::Notification> + Send + 'static;

    /// Notification message decoder.
    type Decoder: codec::Decode<Message = Self::Notification> + Send + 'static;

    /// Makes a new RPC client.
    fn client(service: &ClientServiceHandle) -> CastClient<Self, IntoEncoderMaker<Self::Encoder>>
    where
        Self::Encoder: From<Self::Notification>,
    {
        Self::client_with_encoder(service, IntoEncoderMaker::new())
    }

    /// Makes a new RPC client with the given encoder maker.
    fn client_with_encoder<E>(
        service: &ClientServiceHandle,
        encoder_maker: E,
    ) -> CastClient<Self, E>
    where
        E: MakeEncoder<Self::Encoder>,
    {
        CastClient::new(service, encoder_maker)
    }
}

#[cfg(test)]
mod test {
    use fibers::{Executor, InPlaceExecutor, Spawn};
    use futures::Future;

    use {Call, ProcedureId};
    use client::ClientServiceBuilder;
    use codec::BytesEncoder;
    use server::{HandleCall, Reply, ServerBuilder};

    // RPC
    struct EchoRpc;
    impl Call for EchoRpc {
        const ID: ProcedureId = ProcedureId(0);
        const NAME: &'static str = "echo";

        type Req = Vec<u8>;
        type ReqEncoder = BytesEncoder<Vec<u8>>;
        type ReqDecoder = Vec<u8>;

        type Res = Vec<u8>;
        type ResEncoder = BytesEncoder<Vec<u8>>;
        type ResDecoder = Vec<u8>;
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
