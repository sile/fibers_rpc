extern crate atomic_immut;
extern crate byteorder;
extern crate fibers;
extern crate futures;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use client::RpcClient;
pub use client_service::{RpcClientService, RpcClientServiceBuilder};
pub use error::{Error, ErrorKind};
pub use server::{RpcServer, RpcServerBuilder};

pub mod codec;
pub mod traits;

mod client;
mod client_service;
mod client_side_channel;
mod client_side_handlers;
mod error;
mod frame;
mod frame_stream;
mod message;
mod message_stream;
mod server;
mod server_side_channel;
mod server_side_handlers;

/// This crate specific `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

pub type ProcedureId = u32;
