use bincode;
use serde::{Deserialize, Serialize};

use {Error, Result};
use codec::{BytesEncoder, Decode, Encode, TempBuf};

/// An implementation of `Decode` trait which decodes messages
/// by using `bincode::deserialize(..)` function.
#[derive(Debug)]
pub struct BincodeDecoder<T> {
    buf: Vec<u8>,
    message: Option<T>,
}
impl<T> Decode for BincodeDecoder<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Message = T;

    fn decode(&mut self, buf: &[u8], eos: bool) -> Result<()> {
        if self.buf.is_empty() && eos {
            self.message = Some(track!(bincode::deserialize(buf).map_err(Error::from))?);
        } else {
            self.buf.extend_from_slice(buf);
        }
        Ok(())
    }
    fn finish(self) -> Result<T> {
        if let Some(message) = self.message {
            Ok(message)
        } else {
            track!(bincode::deserialize(&self.buf[..]).map_err(Error::from))
        }
    }
}
impl<T> Default for BincodeDecoder<T> {
    fn default() -> Self {
        BincodeDecoder {
            buf: Vec::new(),
            message: None,
        }
    }
}

/// An implementation of `Encode` trait which encodes messages
/// by using `bincode::serialize_into(.., &T)` function.
#[derive(Debug)]
pub struct BincodeEncoder<T> {
    buf: BytesEncoder<Vec<u8>>,
    message: Option<T>,
}
impl<T: Serialize> Encode for BincodeEncoder<T> {
    type Message = T;

    fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(t) = self.message.take() {
            let mut temp = TempBuf::new(buf);
            track!(bincode::serialize_into(&mut temp, &t).map_err(Error::from))?;
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
impl<T> From<T> for BincodeEncoder<T> {
    fn from(f: T) -> Self {
        BincodeEncoder {
            buf: BytesEncoder::new(Vec::new()),
            message: Some(f),
        }
    }
}
