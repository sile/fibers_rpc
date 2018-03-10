use std;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt, Failure, TrackableError};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum ErrorKind {
    InvalidInput,
    Unavailable,
    Other,
}
impl TrackableErrorKind for ErrorKind {}
