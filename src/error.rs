use std;
use bytecodec;
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
impl From<bytecodec::Error> for Error {
    fn from(f: bytecodec::Error) -> Self {
        let kind = match *f.kind() {
            bytecodec::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            _ => ErrorKind::Other,
        };
        kind.takes_over(f).into()
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
