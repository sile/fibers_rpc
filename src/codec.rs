//! RPC message decoder/encoder.
use std::cmp;
use std::io::{Cursor, Read};
use std::marker::PhantomData;
use std::mem;

use {Error, ErrorKind, Result};

/// The maximum size of encoding/decoding buffers.
pub const MAX_BUFFER_SIZE: usize = 0xFFFF;

/// This trait allows for incrementally decoding an RPC message from a sequence of bytes.
pub trait Decode<T> {
    /// Proceeds decoding by consuming the given fragment of a byte sequence.
    ///
    /// The size of `buf` is `MAX_BUFFER_SIZE` or less than it.
    /// The latter case indicates that the end of the byte sequence reached.
    fn decode(&mut self, buf: &[u8]) -> Result<()>;

    /// Finishes decoding and returns the resulting message.
    fn finish(&mut self) -> Result<T>;
}
impl Decode<Vec<u8>> for Vec<u8> {
    fn decode(&mut self, buf: &[u8]) -> Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
    fn finish(&mut self) -> Result<Vec<u8>> {
        Ok(mem::replace(self, Vec::new()))
    }
}

/// This trait represents decodable messages (i.e., can be decoded without the help of any special decoders).
pub trait DecodeFrom: Sized {
    /// Decodes a message from `buf`.
    fn decode_from(buf: &[u8]) -> Result<Self>;
}
impl DecodeFrom for Vec<u8> {
    fn decode_from(buf: &[u8]) -> Result<Self> {
        Ok(buf.to_owned())
    }
}

/// An implementation of `Decode` trait which decodes messages by using `T::DecodeFrom` function.
#[derive(Debug)]
pub struct SelfDecoder<T>(Option<T>);
impl<T: DecodeFrom> Decode<T> for SelfDecoder<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<()> {
        track_assert!(self.0.is_none(), ErrorKind::InvalidInput);
        self.0 = Some(track!(T::decode_from(buf))?);
        Ok(())
    }
    fn finish(&mut self) -> Result<T> {
        Ok(track_assert_some!(self.0.take(), ErrorKind::InvalidInput))
    }
}
impl<T> Default for SelfDecoder<T> {
    fn default() -> Self {
        SelfDecoder(None)
    }
}

/// This trait allows for incrementally encoding an RPC message to a sequence of bytes.
pub trait Encode<T> {
    /// Proceeds encoding by writing the part of the resulting byte sequence to the given buffer.
    ///
    /// It returns the size of the written bytes.
    /// If the size is less than `buf.len()`, it means the encoding process completed.
    ///
    /// The size of `buf` always is `MAX_BUFFER_SIZE`.
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize>;
}
impl<T: AsRef<[u8]>> Encode<T> for Cursor<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        track!(self.read(buf).map_err(Error::from))
    }
}

/// This trait represents encodable messages (i.e., can be encoded without the help of any special encoders).
pub trait EncodeTo {
    /// Encodes a message to `buf`.
    ///
    /// Note that the size of `buf` always is `MAX_BUFFER_SIZE`.
    /// If you would like to encode large messages, please use other means
    /// (e.g., Implements `Encode` or `ToBytes` traits).
    fn encode_to(&self, buf: &mut [u8]) -> Result<usize>;
}
impl EncodeTo for Vec<u8> {
    fn encode_to(&self, buf: &mut [u8]) -> Result<usize> {
        track_assert!(self.len() <= buf.len(), ErrorKind::InvalidInput);
        (&mut buf[..self.len()]).copy_from_slice(self);
        Ok(self.len())
    }
}

/// An implementation of `Encode` trait which encodes messages by using `T::encode_to` method.
#[derive(Debug)]
pub struct SelfEncoder<T>(Option<T>);
impl<T: EncodeTo> Encode<T> for SelfEncoder<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(t) = self.0.take() {
            track!(t.encode_to(buf))
        } else {
            Ok(0)
        }
    }
}
impl<T> From<T> for SelfEncoder<T> {
    fn from(f: T) -> Self {
        SelfEncoder(Some(f))
    }
}

/// An implementation of `Encode` trait which simply copies the given byte sequence.
#[derive(Debug)]
pub struct BytesEncoder<T> {
    bytes: T,
    offset: usize,
}
impl<T: AsRef<[u8]>> BytesEncoder<T> {
    /// Makes a new `BytesEncoder` instance.
    pub fn new(bytes: T) -> Self {
        Self::from(bytes)
    }
}
impl<T: AsRef<[u8]>> From<T> for BytesEncoder<T> {
    fn from(f: T) -> Self {
        BytesEncoder {
            bytes: f,
            offset: 0,
        }
    }
}
impl<T: AsRef<[u8]>> Encode<T> for BytesEncoder<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        let size = cmp::min(buf.len(), self.bytes.as_ref().len() - self.offset);
        (&mut buf[..size]).copy_from_slice(&self.bytes.as_ref()[self.offset..self.offset + size]);
        self.offset += size;
        Ok(size)
    }
}

/// This trait allows for making decoder instances.
pub trait MakeDecoder<D>: Send + Sync + 'static {
    /// Makes a decoder instance.
    fn make_decoder(&self) -> D;
}

/// An implementation of `MakeDecoder` trait which makes decoders by using `D::default()`.
#[derive(Debug)]
pub struct DefaultDecoderMaker<D>(PhantomData<D>);
impl<D> DefaultDecoderMaker<D> {
    /// Makes a new `DefaultDecoderMaker` instance.
    pub fn new() -> Self {
        DefaultDecoderMaker(PhantomData)
    }
}
unsafe impl<D> Sync for DefaultDecoderMaker<D> {}
unsafe impl<D> Send for DefaultDecoderMaker<D> {}
impl<D: Default + 'static> MakeDecoder<D> for DefaultDecoderMaker<D> {
    fn make_decoder(&self) -> D {
        D::default()
    }
}
impl<D> Default for DefaultDecoderMaker<D> {
    fn default() -> Self {
        Self::new()
    }
}

/// This trait allows for making encoder instances.
pub trait MakeEncoder<T, E>: Send + Sync + 'static
where
    E: Encode<T>,
{
    /// Makes a encoder instance for encoding the given RPC message.
    fn make_encoder(&self, message: T) -> E;
}

/// An implementation of `MakeEncoder` trait which makes `E` encoders from `T` messages by using `T::into()`.
#[derive(Debug)]
pub struct IntoEncoderMaker<T, E>(PhantomData<(T, E)>);
impl<T, E> IntoEncoderMaker<T, E> {
    /// Makes a new `IntoEncoderMaker` instance.
    pub fn new() -> Self {
        IntoEncoderMaker(PhantomData)
    }
}
unsafe impl<T, E> Sync for IntoEncoderMaker<T, E> {}
unsafe impl<T, E> Send for IntoEncoderMaker<T, E> {}
impl<T, E> MakeEncoder<T, E> for IntoEncoderMaker<T, E>
where
    E: Encode<T> + 'static,
    T: Into<E> + 'static,
{
    fn make_encoder(&self, message: T) -> E {
        message.into()
    }
}
impl<T, E> Default for IntoEncoderMaker<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

/// This trait allows for converting a byte sequence to an instance of the implementation type.
pub trait FromBytes: Sized {
    /// Converts `bytes` to `Self`.
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// This trait allows for converting an instance of the implementation to a byte sequence.
pub trait ToBytes {
    /// Converts `Self` to `Vec<u8>`.
    fn to_bytes(&self) -> Result<Vec<u8>>;
}

/// `BatchDecoder` reads all bytes at first, then converts the bytes to a `T` message.
#[derive(Debug)]
pub struct BatchDecoder<T> {
    bytes: Vec<u8>,
    _message: PhantomData<T>,
}
impl<T> BatchDecoder<T> {
    /// Makes a new `BatchDecoder` instance.
    pub fn new() -> Self {
        BatchDecoder {
            bytes: Vec::new(),
            _message: PhantomData,
        }
    }
}
impl<T: FromBytes> Decode<T> for BatchDecoder<T> {
    fn decode(&mut self, buf: &[u8]) -> Result<()> {
        self.bytes.extend_from_slice(buf);
        Ok(())
    }
    fn finish(&mut self) -> Result<T> {
        track!(T::from_bytes(&self.bytes))
    }
}
impl<T> Default for BatchDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// `BatchEncoder` converts a whole `T` message to bytes at first,
/// then writing the bytes to encoding buffers incrementally.
#[derive(Debug)]
pub struct BatchEncoder<T> {
    bytes: Cursor<Vec<u8>>,
    message: Option<T>,
}
impl<T> BatchEncoder<T> {
    /// Makes a new `BatchEncoder` instance.
    pub fn new(message: T) -> Self {
        BatchEncoder {
            bytes: Cursor::new(Vec::new()),
            message: Some(message),
        }
    }
}
impl<T: ToBytes> Encode<T> for BatchEncoder<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(message) = self.message.take() {
            self.bytes = Cursor::new(track!(message.to_bytes())?);
        }
        track!(self.bytes.encode(buf))
    }
}
impl<T> From<T> for BatchEncoder<T> {
    fn from(f: T) -> Self {
        Self::new(f)
    }
}
