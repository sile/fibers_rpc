use std::fmt;
use bytecodec::{Encode, Eos};
use byteorder::{BigEndian, ByteOrder};

use {ErrorKind, ProcedureId, Result};

/// Message identifier.
///
/// This value is unique within a channel.
///
/// # MEMO
///
/// - Request and response messages has the same identifier
/// - The most significant bit of `MessageId` indicates the origin of the RPC
///    - `0` means it is started by clients
///    - `1` means it is started by servers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(u64);
impl MessageId {
    pub fn new_client_side_id() -> Self {
        MessageId(0)
    }

    pub fn next(&mut self) -> Self {
        let n = self.0;
        let msb = n >> 63;
        self.0 = (msb << 63) | (n.wrapping_add(1) & ((1 << 63) - 1));
        MessageId(n)
    }

    pub fn from_u64(n: u64) -> Self {
        MessageId(n)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

pub struct OutgoingMessage {
    id: Option<ProcedureId>,
    priority: u8,
    encode: Box<FnMut(&mut [u8]) -> Result<usize> + Send + 'static>,
}
impl OutgoingMessage {
    pub fn new<E>(id: Option<ProcedureId>, priority: u8, mut encoder: E) -> Self
    where
        E: Encode + Send + 'static,
    {
        OutgoingMessage {
            id,
            priority,
            encode: Box::new(move |buf| {
                let message = track!(encoder.encode(buf, Eos::new(false)))?;
                Ok(message)
            }),
        }
    }

    pub fn error() -> Self {
        OutgoingMessage {
            id: None,
            priority: 128,
            encode: Box::new(|_| track_panic!(ErrorKind::Other)),
        }
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn encode(&mut self, buf: &mut [u8]) -> Result<usize> {
        let offset = if let Some(id) = self.id.take() {
            BigEndian::write_u32(buf, id.0);
            4
        } else {
            0
        };
        let size = track!((self.encode)(&mut buf[offset..]))?;
        Ok(offset + size)
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
    fn message_message_id() {
        let mut message_id = MessageId::new_client_side_id();
        assert_eq!(message_id.as_u64(), 0);

        let prev = message_id.next();
        assert_eq!(prev.as_u64(), 0);
        assert_eq!(message_id.as_u64(), 1);
    }

    #[test]
    fn message_id_overflow() {
        // for client
        let mut message_id = MessageId::from_u64((1 << 63) - 1);
        assert_eq!(message_id.as_u64(), (1 << 63) - 1);

        message_id.next();
        assert_eq!(message_id.as_u64(), 0);

        // for server
        let mut message_id = MessageId::from_u64(u64::MAX);
        assert_eq!(message_id.as_u64(), u64::MAX);

        message_id.next();
        assert_eq!(message_id.as_u64(), 1 << 63);
    }
}
