use std::fmt;
use std::io::{Cursor, Read};
use std::marker::PhantomData;
use futures::{Async, Future, Poll};

use {Error, ProcedureId, Result};

pub trait HandleCast<T: Cast>: Send + Sync + 'static {
    fn handle_cast(&self, notification: T::Notification) -> ::server_side_handlers::NoReply;
}

pub trait HandleCall<T: Call>: Send + Sync + 'static {
    fn handle_call(&self, request: T::Request) -> ::server_side_handlers::Reply<T::Response>;
}

#[derive(Debug)]
pub enum Response {
    NoReply(NoReply),
    Reply(BoxReply),
}
impl Future for Response {
    type Item = Option<Encodable>;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            Response::NoReply(ref mut f) => f.poll().map(|item| item.map(|_| None)),
            Response::Reply(ref mut f) => f.poll().map(|item| item.map(Some)),
        }
    }
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

// TODO:
#[derive(Debug)]
pub struct BoxReply(Option<Encodable>);
impl Future for BoxReply {
    type Item = Encodable;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let data = self.0.take().expect("Cannot poll BoxReply twice");
        Ok(Async::Ready(data))
    }
}

// TODO: where T:Call
#[derive(Debug)]
pub struct Reply<T> {
    // TODO
    data: Encodable,
    _t: ::std::marker::PhantomData<T>,
}
impl<T> Reply<T> {
    // TODO: support future
    pub fn new(data: Encodable) -> Self {
        Reply {
            data,
            _t: ::std::marker::PhantomData,
        }
    }
    pub fn boxed(self) -> BoxReply {
        BoxReply(Some(self.data))
    }
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

pub trait DecoderFactory<D>: Send + Sync + 'static {
    fn create_decoder(&self) -> D;
}

#[derive(Debug)]
pub struct DefaultDecoderFactory<D>(PhantomData<D>);
impl<D> DefaultDecoderFactory<D> {
    pub fn new() -> Self {
        DefaultDecoderFactory(PhantomData)
    }
}
unsafe impl<D> Sync for DefaultDecoderFactory<D> {}
unsafe impl<D> Send for DefaultDecoderFactory<D> {}
impl<D: Default + 'static> DecoderFactory<D> for DefaultDecoderFactory<D> {
    fn create_decoder(&self) -> D {
        D::default()
    }
}

pub trait Decode<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<()>;
    fn finish(&mut self) -> Result<T>;

    fn boxed(self) -> BoxDecoder<T>
    where
        Self: Sized + Send + 'static,
    {
        BoxDecoder(Box::new(self))
    }
}

// TODO: rename
pub struct BoxDecoder<T>(Box<Decode<T> + Send + 'static>);
unsafe impl<T> Send for BoxDecoder<T> {}
impl<T> Decode<T> for BoxDecoder<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<()> {
        track!(self.0.decode(buf))
    }
    fn finish(&mut self) -> Result<T> {
        self.0.finish()
    }
}
impl<T> fmt::Debug for BoxDecoder<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoxDecoder(_)")
    }
}

pub trait EncoderFactory<T, E>: Send + Sync + 'static
where
    E: Encode<T>,
{
    fn create_encoder(&self, value: T) -> E;
}

#[derive(Debug)]
pub struct IntoEncoderFactory<T, E>(PhantomData<(T, E)>);
impl<T, E> IntoEncoderFactory<T, E> {
    pub fn new() -> Self {
        IntoEncoderFactory(PhantomData)
    }
}
unsafe impl<T, E> Sync for IntoEncoderFactory<T, E> {}
unsafe impl<T, E> Send for IntoEncoderFactory<T, E> {}
impl<T, E> EncoderFactory<T, E> for IntoEncoderFactory<T, E>
where
    E: Encode<T> + 'static,
    T: Into<E> + 'static,
{
    fn create_encoder(&self, value: T) -> E {
        value.into()
    }
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
    fn finish(&mut self) -> Result<Vec<u8>> {
        Ok(::std::mem::replace(self, Vec::new()))
    }
}
