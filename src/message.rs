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
        let msb = n >> 63;
        self.0 = (msb << 63) | (n.wrapping_add(1) & ((1 << 63) - 1));
        MessageSeqNo(n)
    }

    pub fn from_u64(n: u64) -> Self {
        MessageSeqNo(n)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

pub struct OutgoingMessage(Box<FnMut(&mut [u8]) -> Result<usize> + Send + 'static>);
impl OutgoingMessage {
    pub fn new<T, E>(mut encoder: E) -> Self
    where
        E: Encode<T> + Send + 'static,
    {
        OutgoingMessage(Box::new(move |buf| track!(encoder.encode(buf))))
    }

    pub fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        track!((self.0)(buf))
    }
}
impl fmt::Debug for OutgoingMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutgoingMessage(_)")
    }
}

#[cfg(test)]
mod test {
    use std::u64;

    use super::*;

    #[test]
    fn message_seqno() {
        let mut seqno = MessageSeqNo::new_client_side_seqno();
        assert_eq!(seqno.as_u64(), 0);

        let prev = seqno.next();
        assert_eq!(prev.as_u64(), 0);
        assert_eq!(seqno.as_u64(), 1);
    }

    #[test]
    fn seqno_overflow() {
        // for client
        let mut seqno = MessageSeqNo::from_u64((1 << 63) - 1);
        assert_eq!(seqno.as_u64(), (1 << 63) - 1);

        seqno.next();
        assert_eq!(seqno.as_u64(), 0);

        // for server
        let mut seqno = MessageSeqNo::from_u64(u64::MAX);
        assert_eq!(seqno.as_u64(), u64::MAX);

        seqno.next();
        assert_eq!(seqno.as_u64(), 1 << 63);
    }
}
