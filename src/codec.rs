//! RPC message decoder/encoder.
use std::cmp;
use std::io::{self, Cursor, Read, Write};
use std::marker::PhantomData;
use std::mem;

use {Error, Result};

/// This trait allows for incrementally decoding an RPC message from a sequence of bytes.
pub trait Decode<T> {
    /// Proceeds decoding by consuming the given fragment of a byte sequence.
    fn decode(&mut self, buf: &[u8], eos: bool) -> Result<()>;

    /// Finishes decoding and returns the resulting message.
    fn finish(&mut self) -> Result<T>;
}
impl Decode<Vec<u8>> for Vec<u8> {
    fn decode(&mut self, buf: &[u8], _eos: bool) -> Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
    fn finish(&mut self) -> Result<Vec<u8>> {
        Ok(mem::replace(self, Vec::new()))
    }
}

/// This trait represents decodable messages (i.e., can be decoded without the help of any special decoders).
pub trait DecodeFrom: Sized {
    /// Decodes a message from `reader`.
    fn decode_from<R: Read>(reader: R) -> Result<Self>;
}

/// An implementation of `Decode` trait which decodes messages by using `T::DecodeFrom` function.
#[derive(Debug)]
pub struct SelfDecoder<T> {
    buf: Vec<u8>,
    message: Option<T>,
}
impl<T: DecodeFrom> Decode<T> for SelfDecoder<T> {
    fn decode(&mut self, buf: &[u8], eos: bool) -> Result<()> {
        if self.buf.is_empty() && eos {
            self.message = Some(track!(T::decode_from(buf))?);
        } else {
            self.buf.extend_from_slice(buf);
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<T> {
        if let Some(message) = self.message.take() {
            Ok(message)
        } else {
            track!(T::decode_from(&self.buf[..]))
        }
    }
}
impl<T> Default for SelfDecoder<T> {
    fn default() -> Self {
        SelfDecoder {
            buf: Vec::new(),
            message: None,
        }
    }
}

/// This trait allows for incrementally encoding an RPC message to a sequence of bytes.
pub trait Encode<T> {
    /// Proceeds encoding by writing the part of the resulting byte sequence to the given buffer.
    ///
    /// It returns the size of the written bytes.
    /// If the size is less than `buf.len()`, it means the encoding process completed.
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize>;
}
impl<T: AsRef<[u8]>> Encode<T> for Cursor<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        track!(self.read(buf).map_err(Error::from))
    }
}

/// This trait represents encodable messages (i.e., can be encoded without the help of any special encoders).
pub trait EncodeTo {
    /// Encodes a message to `writer`.
    fn encode_to<W: Write>(&self, writer: W) -> Result<()>;
}

/// An implementation of `Encode` trait which encodes messages by using `T::encode_to` method.
#[derive(Debug)]
pub struct SelfEncoder<T> {
    buf: BytesEncoder<Vec<u8>>,
    message: Option<T>,
}
impl<T: EncodeTo> Encode<T> for SelfEncoder<T> {
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(t) = self.message.take() {
            let mut temp = TempBuf::new(buf);
            track!(t.encode_to(&mut temp))?;
            let (size, extra_buf) = temp.finish();
            if !extra_buf.is_empty() {
                self.buf = BytesEncoder::new(extra_buf);
            }
            Ok(size)
        } else {
            track!(self.buf.encode(buf))
        }
    }
}
impl<T> From<T> for SelfEncoder<T> {
    fn from(f: T) -> Self {
        SelfEncoder {
            buf: BytesEncoder::new(Vec::new()),
            message: Some(f),
        }
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

#[derive(Debug)]
struct TempBuf<'a> {
    buf: Cursor<&'a mut [u8]>,
    extra_buf: Vec<u8>,
}
impl<'a> TempBuf<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        TempBuf {
            buf: Cursor::new(buf),
            extra_buf: Vec::new(),
        }
    }

    fn finish(self) -> (usize, Vec<u8>) {
        (self.buf.position() as usize, self.extra_buf)
    }
}
impl<'a> Write for TempBuf<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.buf.write(buf)?;
        if size < buf.len() {
            self.extra_buf.extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
