extern crate atomic_immut;
extern crate byteorder;
extern crate fibers;
extern crate futures;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use error::{Error, ErrorKind};

pub mod codec;
pub mod client {
    //! RPC client.

    pub use client_service::{RpcClientService, RpcClientServiceBuilder};
    pub use rpc_client::RpcClient;
}
pub mod server {
    //! RPC server.

    pub use rpc_server::{RpcServer, RpcServerBuilder};
    pub use server_side_handlers::{HandleCall, HandleCast, Never, NoReply, Reply};
}

mod client_service; // TODO:
mod client_side_channel; // TODO:
mod client_side_handlers; // TODO:
mod error;
mod frame;
mod frame_stream;
mod message;
mod message_stream;
mod rpc_client; // TODO
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
pub trait Call: Send + Sync + 'static {
    /// The identifier of the procedure.
    const ID: ProcedureId;

    /// The name of the procedure.
    ///
    /// This is only used for debugging purpose.
    const NAME: &'static str;

    /// Request message.
    type Request;

    /// Request message encoder.
    type RequestEncoder: codec::Encode<Self::Request> + Send + 'static;

    /// Request message decoder.
    type RequestDecoder: codec::Decode<Self::Request> + Send + 'static;

    /// Response message.
    type Response: Send + 'static;

    /// Response message encoder.
    type ResponseEncoder: codec::Encode<Self::Response> + Send + 'static;

    /// Response message decoder.
    type ResponseDecoder: codec::Decode<Self::Response> + Send + 'static;
}

/// Notification RPC.
pub trait Cast: Sync + Send + 'static {
    /// The identifier of the procedure.
    const ID: ProcedureId;

    /// The name of the procedure.
    ///
    /// This is only used for debugging purpose.
    const NAME: &'static str;

    /// Notification message.
    type Notification;

    /// Notification message encoder.
    type Encoder: codec::Encode<Self::Notification> + Send + 'static;

    /// Notification message decoder.
    type Decoder: codec::Decode<Self::Notification> + Send + 'static;
}
