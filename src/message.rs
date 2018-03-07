use std::fmt;

use ProcedureId;
use traits::IncrementalSerialize;

#[derive(Debug)]
pub enum Message<T> {
    Notification { procedure: ProcedureId, data: T },
}

pub struct OutgoingMessage(pub Message<Box<IncrementalSerialize + Send>>);
impl fmt::Debug for OutgoingMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutgoingMessage(_)")
    }
}
