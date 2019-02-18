//! RPC library built on top of [fibers] crate.
//!
//!
//! [fibers]: https://github.com/dwango/fibers-rs
//!
//! # Features
//!
//! - Asynchronous RPC server/client using [fibers] crate
//! - Support two type of RPC:
//!   - Request/response model
//!   - Notification model
//! - Strongly typed RPC using [bytecodec] crate
//!   - You can treat arbitrarily Rust structures that support [serde] as RPC messages
//!   - It is possible to handle huge structures as RPC messages without
//!     compromising efficiency and real-time property by implementing your own encoder/decoder
//! - Multiplexing multiple RPC messages in a single TCP stream
//! - Prioritization between messages
//! - Expose [Prometheus] metrics
//!
//! [fibers]: https://github.com/dwango/fibers-rs
//! [bytecodec]: https://github.com/sile/bytecodec
//! [serde]: https://crates.io/crates/serde
//! [Prometheus]: https://prometheus.io/
//!
//! # Technical Details
//!
//! See [doc/].
//!
//! [doc/]: https://github.com/sile/fibers_rpc/tree/master/doc
//!
//! # Examples
//!
//! Simple echo RPC server:
//!
//! ```
//! # extern crate bytecodec;
//! # extern crate fibers_global;
//! # extern crate fibers_rpc;
//! # extern crate futures;
//! # extern crate trackable;
//! # fn main() -> trackable::result::MainResult {
//! use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
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
//! // RPC server
//! struct EchoHandler;
//! impl HandleCall<EchoRpc> for EchoHandler {
//!     fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<EchoRpc> {
//!         Reply::done(request)
//!     }
//! }
//! let server_addr = "127.0.0.1:1919".parse().unwrap();
//! let mut builder = ServerBuilder::new(server_addr);
//! builder.add_call_handler(EchoHandler);
//! let server = builder.finish(fibers_global::handle());
//! fibers_global::spawn(server.map_err(|e| panic!("{}", e)));
//!
//! // RPC client
//! let service = ClientServiceBuilder::new().finish(fibers_global::handle());
//! let service_handle = service.handle();
//! fibers_global::spawn(service.map_err(|e| panic!("{}", e)));
//!
//! let request = Vec::from(&b"hello"[..]);
//! let response = EchoRpc::client(&service_handle).call(server_addr, request.clone());
//! let response = fibers_global::execute(response)?;
//! assert_eq!(response, request);
//! # Ok(())
//! # }
//! ```
#![warn(missing_docs)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate byteorder;
extern crate factory;
extern crate fibers;
#[cfg(test)]
extern crate fibers_global;
extern crate fibers_tasque;
extern crate futures;
extern crate prometrics;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use error::{Error, ErrorKind};

pub mod client {
    //! RPC client.

    pub use client_service::{ClientService, ClientServiceBuilder, ClientServiceHandle};
    pub use client_side_handlers::Response;
    pub use rpc_client::{CallClient, CastClient, Options};
}
pub mod channel;
pub mod metrics;
pub mod server {
    //! RPC server.

    pub use rpc_server::{Server, ServerBuilder};
    pub use server_side_handlers::{HandleCall, HandleCast, NoReply, Reply};
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
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcedureId(pub u32);
impl std::fmt::Debug for ProcedureId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ProcedureId(0x{:08x})", self.0)
    }
}

/// Request/response RPC.
pub trait Call: Sized + Send + Sync + 'static {
    /// The identifier of the procedure.
    const ID: ProcedureId;

    /// The name of the procedure.
    ///
    /// This is only used for debugging purpose.
    const NAME: &'static str;

    /// Request message.
    type Req: Send + 'static;

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

    /// If it returns `true`, encoding/decoding request messages will be executed asynchronously.
    ///
    /// For large RPC messages, asynchronous encoding/decoding may improve real-time property
    /// (especially if messages will be encoded/decoded by using `serde`).
    ///
    /// The default implementation always return `false`.
    #[allow(unused_variables)]
    fn enable_async_request(request: &Self::Req) -> bool {
        false
    }

    /// If it returns `true`, encoding/decoding response messages will be executed asynchronously.
    ///
    /// For large RPC messages, asynchronous encoding/decoding may improve real-time property
    /// (especially if messages will be encoded/decoded by using `serde`).
    ///
    /// The default implementation always return `false`.
    #[allow(unused_variables)]
    fn enable_async_response(response: &Self::Res) -> bool {
        false
    }

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
    type Notification: Send + 'static;

    /// Notification message encoder.
    type Encoder: bytecodec::Encode<Item = Self::Notification> + Send + 'static;

    /// Notification message decoder.
    type Decoder: bytecodec::Decode<Item = Self::Notification> + Send + 'static;

    /// If it returns `true`, encoding/decoding notification messages will be executed asynchronously.
    ///
    /// For large RPC messages, asynchronous encoding/decoding may improve real-time property
    /// (especially if messages will be encoded/decoded by using `serde`).
    ///
    /// The default implementation always return `false`.
    #[allow(unused_variables)]
    fn enable_async(notification: &Self::Notification) -> bool {
        false
    }

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
mod tests {
    use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
    use fibers_global;
    use futures::Future;
    use trackable::result::TestResult;

    use client::ClientServiceBuilder;
    use server::{HandleCall, Reply, ServerBuilder};
    use {Call, ProcedureId};

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

        fn enable_async_request(x: &Self::Req) -> bool {
            x == b"async"
        }

        fn enable_async_response(x: &Self::Res) -> bool {
            x == b"async"
        }
    }

    // Handler
    struct EchoHandler;
    impl HandleCall<EchoRpc> for EchoHandler {
        fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<EchoRpc> {
            Reply::done(request)
        }
    }

    #[test]
    fn it_works() -> TestResult {
        // Server
        let mut builder = ServerBuilder::new("127.0.0.1:0".parse().unwrap());
        builder.add_call_handler(EchoHandler);
        let server = builder.finish(fibers_global::handle());
        let (server, server_addr) = track!(fibers_global::execute(server.local_addr()))?;
        fibers_global::spawn(server.map_err(|e| panic!("{}", e)));

        // Client
        let service = ClientServiceBuilder::new().finish(fibers_global::handle());
        let service_handle = service.handle();
        fibers_global::spawn(service.map_err(|e| panic!("{}", e)));

        let request = Vec::from(&b"hello"[..]);
        let response = EchoRpc::client(&service_handle).call(server_addr, request.clone());
        let response = track_any_err!(fibers_global::execute(response))?;
        assert_eq!(response, request);

        let metrics = service_handle
            .metrics()
            .channels()
            .as_map()
            .load()
            .get(&server_addr)
            .cloned()
            .unwrap();
        assert_eq!(metrics.async_outgoing_messages(), 0);
        assert_eq!(metrics.async_incoming_messages(), 0);
        Ok(())
    }

    #[test]
    fn large_message_works() -> TestResult {
        // Server
        let mut builder = ServerBuilder::new("127.0.0.1:0".parse().unwrap());
        builder.add_call_handler(EchoHandler);
        let server = builder.finish(fibers_global::handle());
        // let (server, server_addr) = track!(fibers_global::execute(server.local_addr()))?;
        let future = server.local_addr();
        let (server, server_addr) = track!(fibers_global::execute(future))?;
        fibers_global::spawn(server.map_err(|e| panic!("{}", e)));

        // Client
        let service = ClientServiceBuilder::new().finish(fibers_global::handle());
        let service_handle = service.handle();
        fibers_global::spawn(service.map_err(|e| panic!("{}", e)));

        let request = vec![0; 10 * 1024 * 1024];
        let response = EchoRpc::client(&service_handle).call(server_addr, request.clone());
        let response = track!(fibers_global::execute(response))?;
        assert_eq!(response, request);
        Ok(())
    }

    #[test]
    fn async_works() -> TestResult {
        // Server
        let mut builder = ServerBuilder::new("127.0.0.1:0".parse().unwrap());
        builder.add_call_handler(EchoHandler);
        let server = builder.finish(fibers_global::handle());
        let (server, server_addr) = track!(fibers_global::execute(server.local_addr()))?;
        fibers_global::spawn(server.map_err(|e| panic!("{}", e)));

        // Client
        let service = ClientServiceBuilder::new().finish(fibers_global::handle());
        let service_handle = service.handle();
        fibers_global::spawn(service.map_err(|e| panic!("{}", e)));

        let request = Vec::from(&b"async"[..]);
        let response = EchoRpc::client(&service_handle).call(server_addr, request.clone());
        let response = track!(fibers_global::execute(response))?;
        assert_eq!(response, request);

        let metrics = service_handle
            .metrics()
            .channels()
            .as_map()
            .load()
            .get(&server_addr)
            .cloned()
            .unwrap();
        assert_eq!(metrics.async_outgoing_messages(), 1);
        assert_eq!(metrics.async_incoming_messages(), 1);
        Ok(())
    }
}
