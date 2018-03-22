//! RPC message decoder/encoder.
use std::cmp;
use std::io::{self, Cursor, Read, Write};
use std::marker::PhantomData;

#[cfg(feature = "bincode_codec")]
pub use codec_bincode::{BincodeDecoder, BincodeEncoder};

#[cfg(feature = "json_codec")]
pub use codec_json::{JsonDecoder, JsonEncoder};

#[cfg(feature = "msgpack_codec")]
pub use codec_msgpack::{MsgPackDecoder, MsgPackEncoder};

use {Error, ErrorKind, Result};

/// This trait allows for incrementally decoding an RPC message from a sequence of bytes.
pub trait Decode {
    /// Message to be decoded.
    type Message;

    /// Proceeds decoding by consuming the given fragment of a byte sequence.
    fn decode(&mut self, buf: &[u8], eos: bool) -> Result<()>;

    /// Finishes decoding and returns the resulting message.
    fn finish(self) -> Result<Self::Message>;
}
impl Decode for Vec<u8> {
    type Message = Vec<u8>;
    fn decode(&mut self, buf: &[u8], _eos: bool) -> Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
    fn finish(self) -> Result<Vec<u8>> {
        Ok(self)
    }
}

/// An implementation of `Decode` traits which fills the given buffer `T`.
#[derive(Debug)]
pub struct BytesDecoder<T> {
    bytes: T,
    offset: usize,
}
impl<T> BytesDecoder<T> {
    /// Makes new `BytesDecoder` instance.
    pub fn new(bytes: T) -> Self {
        BytesDecoder { bytes, offset: 0 }
    }

    /// Returns a reference to the underlying bytes in this decoder.
    pub fn get_ref(&self) -> &T {
        &self.bytes
    }

    /// Returns a mutable reference to the underlying bytes in this decoder.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.bytes
    }
}
impl<T: AsMut<[u8]>> Decode for BytesDecoder<T> {
    type Message = T;
    fn decode(&mut self, buf: &[u8], _eos: bool) -> Result<()> {
        track_assert!(
            self.offset + buf.len() <= self.bytes.as_mut().len(),
            ErrorKind::InvalidInput,
            "Too much input bytes: required={}, capacity={}",
            self.offset + buf.len(),
            self.bytes.as_mut().len(),
        );
        (&mut self.bytes.as_mut()[self.offset..][..buf.len()]).copy_from_slice(buf);
        self.offset += buf.len();
        Ok(())
    }
    fn finish(mut self) -> Result<T> {
        track_assert_eq!(
            self.offset,
            self.bytes.as_mut().len(),
            ErrorKind::InvalidInput,
            "Too few input bytes"
        );
        Ok(self.bytes)
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
impl<T: DecodeFrom> Decode for SelfDecoder<T> {
    type Message = T;

    fn decode(&mut self, buf: &[u8], eos: bool) -> Result<()> {
        if self.buf.is_empty() && eos {
            self.message = Some(track!(T::decode_from(buf))?);
        } else {
            self.buf.extend_from_slice(buf);
        }
        Ok(())
    }
    fn finish(self) -> Result<T> {
        if let Some(message) = self.message {
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
pub trait Encode {
    /// Message to be encoded.
    type Message;

    /// Proceeds encoding by writing the part of the resulting byte sequence to the given buffer.
    ///
    /// It returns the size of the written bytes.
    /// If the size is less than `buf.len()`, it means the encoding process completed.
    fn encode(&mut self, buf: &mut [u8]) -> Result<usize>;
}
impl<T: AsRef<[u8]>> Encode for Cursor<T> {
    type Message = T;

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
impl<T: EncodeTo> Encode for SelfEncoder<T> {
    type Message = T;

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

    /// Returns a reference to the underlying bytes in this decoder.
    pub fn get_ref(&self) -> &T {
        &self.bytes
    }

    /// Returns a mutable reference to the underlying bytes in this decoder.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.bytes
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
impl<T: AsRef<[u8]>> Encode for BytesEncoder<T> {
    type Message = T;

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
pub trait MakeEncoder<E>: Send + Sync + 'static
where
    E: Encode,
{
    /// Makes a encoder instance for encoding the given RPC message.
    fn make_encoder(&self, message: E::Message) -> E;
}

/// An implementation of `MakeEncoder` trait which makes `E` encoders by using `E::Message::into()`.
#[derive(Debug)]
pub struct IntoEncoderMaker<E>(PhantomData<E>);
impl<E> IntoEncoderMaker<E> {
    /// Makes a new `IntoEncoderMaker` instance.
    pub fn new() -> Self {
        IntoEncoderMaker(PhantomData)
    }
}
unsafe impl<E> Sync for IntoEncoderMaker<E> {}
unsafe impl<E> Send for IntoEncoderMaker<E> {}
impl<E> MakeEncoder<E> for IntoEncoderMaker<E>
where
    E: Encode + 'static,
    E::Message: Into<E> + 'static,
{
    fn make_encoder(&self, message: E::Message) -> E {
        message.into()
    }
}
impl<E> Default for IntoEncoderMaker<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct TempBuf<'a> {
    buf: Cursor<&'a mut [u8]>,
    extra_buf: Vec<u8>,
}
impl<'a> TempBuf<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        TempBuf {
            buf: Cursor::new(buf),
            extra_buf: Vec::new(),
        }
    }

    pub(crate) fn finish(self) -> (usize, Vec<u8>) {
        (self.buf.position() as usize, self.extra_buf)
    }
}
impl<'a> Write for TempBuf<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.buf.write(buf)?;
        if size < buf.len() {
            self.extra_buf.extend_from_slice(&buf[size..]);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use super::*;

    #[test]
    fn temp_buf_works() {
        let mut inner = [0; 1];
        let mut temp = TempBuf::new(&mut inner);

        temp.write_all(b"abc").unwrap();
        let (size, extra) = temp.finish();
        assert_eq!(size, 1);
        assert_eq!(extra, b"bc");
    }
}
