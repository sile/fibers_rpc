use std::fmt;

use {ProcedureId, Result};
use codec::{Decode, Encode};

pub trait HandleCast<T: Cast>: Send + Sync + 'static {
    fn handle_cast(&self, notification: T::Notification) -> ::server_side_handlers::NoReply;
}

pub trait HandleCall<T: Call>: Send + Sync + 'static {
    fn handle_call(&self, request: T::Request) -> ::server_side_handlers::Reply<T::Response>;
}

pub trait Call: Send + Sync + 'static {
    const PROCEDURE: ProcedureId;
    type Request;
    type Response: Send + 'static;

    type RequestEncoder: Encode<Self::Request> + Send + 'static;
    type RequestDecoder: Decode<Self::Request> + Send + 'static;
    type ResponseEncoder: Encode<Self::Response> + Send + 'static;
    type ResponseDecoder: Decode<Self::Response> + Send + 'static;
}

pub trait Cast: Sync + Send + 'static {
    const PROCEDURE: ProcedureId;
    type Notification;

    type Encoder: Encode<Self::Notification> + Send + 'static;
    type Decoder: Decode<Self::Notification> + Send + 'static;
}

pub struct Encodable(Box<FnMut(&mut [u8]) -> Result<usize> + Send + 'static>);
impl Encodable {
    pub fn new<T, E>(mut encoder: E) -> Self
    where
        E: Encode<T> + Send + 'static,
    {
        Encodable(Box::new(move |buf| track!(encoder.encode(buf))))
    }
    pub fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        track!((self.0)(buf))
    }
}
impl fmt::Debug for Encodable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Encodable(_)")
    }
}
