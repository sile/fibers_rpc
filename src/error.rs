use std;
#[cfg(feature = "bincode_codec")]
use bincode;
#[cfg(feature = "msgpack_codec")]
use rmp_serde;
#[cfg(feature = "json_codec")]
use serde_json;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt, Failure, TrackableError};

/// This crate specific `Error` type.
#[derive(Debug, Clone)]
pub struct Error(TrackableError<ErrorKind>);
derive_traits_for_trackable_error_newtype!(Error, ErrorKind);
impl From<Failure> for Error {
    fn from(f: Failure) -> Self {
        ErrorKind::Other.takes_over(f).into()
    }
}
impl From<std::io::Error> for Error {
    fn from(f: std::io::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
#[cfg(feature = "bincode_codec")]
impl From<Box<bincode::ErrorKind>> for Error {
    fn from(f: Box<bincode::ErrorKind>) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
#[cfg(feature = "msgpack_codec")]
impl From<rmp_serde::encode::Error> for Error {
    fn from(f: rmp_serde::encode::Error) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
#[cfg(feature = "msgpack_codec")]
impl From<rmp_serde::decode::Error> for Error {
    fn from(f: rmp_serde::decode::Error) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}
#[cfg(feature = "json_codec")]
impl From<serde_json::Error> for Error {
    fn from(f: serde_json::Error) -> Self {
        ErrorKind::InvalidInput.cause(f).into()
    }
}

/// Possible error kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Input is invalid.
    InvalidInput,

    /// RPC server is temporarily unavailable.
    Unavailable,

    /// Request timed out.
    Timeout,

    /// Other errors.
    Other,
}
impl TrackableErrorKind for ErrorKind {}
