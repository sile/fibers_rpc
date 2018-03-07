use std::net::SocketAddr;

pub use self::service::{RpcClientService, RpcClientServiceBuilder};

use message::{Message, OutgoingMessage};
use traits::{Cast, IncrementalSerialize};
use self::service::RpcClientServiceHandle;

mod service;

#[derive(Debug, Clone)]
pub struct RpcClient {
    service: RpcClientServiceHandle,
}
impl RpcClient {
    pub fn cast<T: Cast>(&self, server: SocketAddr, notification: T::Notification) {
        let data: Box<IncrementalSerialize + Send> = Box::new(notification);
        let message = OutgoingMessage(Message::Notification {
            procedure: T::PROCEDURE,
            data,
        });
        self.service.send_message(server, message);
    }
}
