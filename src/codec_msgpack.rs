use rmp_serde;
use serde::{Deserialize, Serialize};

use {Error, Result};
use codec::{BytesEncoder, Decode, Encode, TempBuf};

/// An implementation of `Decode` trait which decodes messages
/// by using `rmp_serde::decode::from_read(..)` function.
#[derive(Debug)]
pub struct MsgPackDecoder<T> {
    buf: Vec<u8>,
    message: Option<T>,
}
impl<T> Decode for MsgPackDecoder<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Message = T;

    fn decode(&mut self, buf: &[u8], eos: bool) -> Result<()> {
        if self.buf.is_empty() && eos {
            self.message = Some(track!(
                rmp_serde::decode::from_read(buf).map_err(Error::from)
            )?);
        } else {
            self.buf.extend_from_slice(buf);
        }
        Ok(())
    }
    fn finish(self) -> Result<T> {
        if let Some(message) = self.message {
            Ok(message)
        } else {
            track!(rmp_serde::decode::from_read(&self.buf[..]).map_err(Error::from))
        }
    }
}
impl<T> Default for MsgPackDecoder<T> {
    fn default() -> Self {
        MsgPackDecoder {
            buf: Vec::new(),
            message: None,
        }
    }
}

/// An implementation of `Encode` trait which encodes messages
/// by using `rmp_serde::encode::write(.., &T)` function.
#[derive(Debug)]
pub struct MsgPackEncoder<T> {
    buf: BytesEncoder<Vec<u8>>,
    message: Option<T>,
}
impl<T: Serialize> Encode for MsgPackEncoder<T> {
    type Message = T;

    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(t) = self.message.take() {
            let mut temp = TempBuf::new(buf);
            track!(rmp_serde::encode::write(&mut temp, &t).map_err(Error::from))?;
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
impl<T> From<T> for MsgPackEncoder<T> {
    fn from(f: T) -> Self {
        MsgPackEncoder {
            buf: BytesEncoder::new(Vec::new()),
            message: Some(f),
        }
    }
}
