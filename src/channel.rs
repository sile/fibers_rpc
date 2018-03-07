use std::net::SocketAddr;
use slog::Logger;
use futures::{Future, Poll};

use Error;
use message::OutgoingMessage;

#[derive(Debug)]
pub struct RpcChannel {
    logger: Logger,
}
impl RpcChannel {
    pub fn new(logger: Logger, peer: SocketAddr) -> (Self, RpcChannelHandle) {
        (RpcChannel { logger }, RpcChannelHandle {})
    }
}
impl Future for RpcChannel {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct RpcChannelHandle;
impl RpcChannelHandle {
    pub fn send_message(&self, message: OutgoingMessage) {
        unimplemented!()
    }
}
