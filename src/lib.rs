extern crate atomic_immut;
extern crate byteorder;
extern crate fibers;
extern crate futures;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate trackable;

pub use error::{Error, ErrorKind};

pub mod client;
pub mod server;
pub mod traits;

mod channel;
mod error;
pub mod frame; // TODO
mod message;

/// This crate specific `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

pub type ProcedureId = u32;
