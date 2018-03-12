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

    pub use client_service::{RpcClientService, RpcClientServiceBuilder, RpcClientServiceHandle};
    pub use rpc_client::{RpcCallClient, RpcCastClient};
}
pub mod server {
    //! RPC server.

    pub use rpc_server::{RpcServer, RpcServerBuilder};
    pub use server_side_handlers::{HandleCall, HandleCast, Never, NoReply, Reply};
}

use client::{RpcCallClient, RpcCastClient, RpcClientServiceHandle};
use codec::{DefaultDecoderMaker, IntoEncoderMaker, MakeDecoder, MakeEncoder};

mod client_service; // TODO:
mod client_side_channel; // TODO:
mod client_side_handlers; // TODO:
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

    /// Makes a new RPC client.
    fn client(
        service: &RpcClientServiceHandle,
    ) -> RpcCallClient<
        Self,
        DefaultDecoderMaker<Self::ResponseDecoder>,
        IntoEncoderMaker<Self::Request, Self::RequestEncoder>,
    >
    where
        Self::RequestEncoder: From<Self::Request>,
        Self::ResponseDecoder: Default,
    {
        Self::client_with_codec(service, DefaultDecoderMaker::new(), IntoEncoderMaker::new())
    }

    /// Makes a new RPC client with the given decoder maker.
    fn client_with_decoder<D>(
        service: &RpcClientServiceHandle,
        decoder_maker: D,
    ) -> RpcCallClient<Self, D, IntoEncoderMaker<Self::Request, Self::RequestEncoder>>
    where
        Self::RequestEncoder: From<Self::Request>,
        D: MakeDecoder<Self::ResponseDecoder>,
    {
        Self::client_with_codec(service, decoder_maker, IntoEncoderMaker::new())
    }

    /// Makes a new RPC client with the given encoder maker.
    fn client_with_encoder<E>(
        service: &RpcClientServiceHandle,
        encoder_maker: E,
    ) -> RpcCallClient<Self, DefaultDecoderMaker<Self::ResponseDecoder>, E>
    where
        Self::ResponseDecoder: Default,
        E: MakeEncoder<Self::Request, Self::RequestEncoder>,
    {
        Self::client_with_codec(service, DefaultDecoderMaker::new(), encoder_maker)
    }

    /// Makes a new RPC client with the given decoder and encoder makers.
    fn client_with_codec<D, E>(
        service: &RpcClientServiceHandle,
        decoder_maker: D,
        encoder_maker: E,
    ) -> RpcCallClient<Self, D, E>
    where
        D: MakeDecoder<Self::ResponseDecoder>,
        E: MakeEncoder<Self::Request, Self::RequestEncoder>,
    {
        RpcCallClient::new(service, decoder_maker, encoder_maker)
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
    type Encoder: codec::Encode<Self::Notification> + Send + 'static;

    /// Notification message decoder.
    type Decoder: codec::Decode<Self::Notification> + Send + 'static;

    /// Makes a new RPC client.
    fn client(
        service: &RpcClientServiceHandle,
    ) -> RpcCastClient<Self, IntoEncoderMaker<Self::Notification, Self::Encoder>>
    where
        Self::Encoder: From<Self::Notification>,
    {
        Self::client_with_encoder(service, IntoEncoderMaker::new())
    }

    /// Makes a new RPC client with the given encoder maker.
    fn client_with_encoder<E>(
        service: &RpcClientServiceHandle,
        encoder_maker: E,
    ) -> RpcCastClient<Self, E>
    where
        E: MakeEncoder<Self::Notification, Self::Encoder>,
    {
        RpcCastClient::new(service, encoder_maker)
    }
}
