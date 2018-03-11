use std::fmt;

use Result;
use codec::Encode;

/// Message sequence number.
///
/// # MEMO
///
/// - Request and response messages has the same number
/// - The most significant bit of `MessageSeqNo` indicates the origin of the RPC
///    - `0` means it is started by clients
///    - `1` means it is started by servers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageSeqNo(u64);
impl MessageSeqNo {
    pub fn new_client_side_seqno() -> Self {
        MessageSeqNo(0)
    }

    pub fn next(&mut self) -> Self {
        let n = self.0;
        self.0 |= (self.0 + 1) & ((1 << 63) - 1);
        MessageSeqNo(n)
    }

    pub fn from_u64(n: u64) -> Self {
        MessageSeqNo(n)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
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
