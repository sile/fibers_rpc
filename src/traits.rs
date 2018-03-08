use std::fmt;
use std::io::{Cursor, Read};
use futures::{Future, Poll};

use {Error, ProcedureId, Result};

pub trait HandleCast<T: Cast>: Send + Sync + 'static {
    fn handle_cast(&self, notification: T::Notification) -> NoReply;
}

pub trait HandleCall<T: Call>: Send + Sync + 'static {
    fn handle_call(&self, request: T::Request) -> Reply<T::Response>;
}

#[derive(Debug)]
pub enum Response {
    NoReply(NoReply),
}

// TODO: add no future version
#[derive(Debug)]
pub struct NoReply;
impl Future for NoReply {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Reply<T>(T);
impl<T> Future for Reply<T> {
    type Item = T;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}

pub trait Call {
    const PROCEDURE: ProcedureId;
    type Request;
    type Response;

    type RequestEncoder: Encode<Self::Request> + Send + 'static;
    type RequestDecoder: Decode<Self::Request> + Send + 'static;
    type ResponseEncoder: Encode<Self::Response> + Send + 'static;
    type ResponseDecoder: Decode<Self::Response> + Send + 'static;
}

pub trait Cast {
    const PROCEDURE: ProcedureId;
    type Notification;

    type Encoder: Encode<Self::Notification> + Send + 'static;
    type Decoder: Decode<Self::Notification> + Send + 'static;
}

pub trait Decode<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<()>;
    fn finish(self) -> Result<T>;
}

pub trait Encode<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize>;

    fn into_encodable(mut self) -> Encodable
    where
        Self: Sized + Send + 'static,
    {
        Encodable(Box::new(move |buf| track!(self.encode(buf))))
    }
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

impl<T: AsRef<[u8]>> Encode<T> for Cursor<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        track!(self.read(buf).map_err(Error::from))
    }
}

impl Decode<Vec<u8>> for Vec<u8> {
    fn decode(&mut self, buf: &[u8]) -> Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
    fn finish(self) -> Result<Vec<u8>> {
        Ok(self)
    }
}
