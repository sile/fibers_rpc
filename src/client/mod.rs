use std::net::SocketAddr;

pub use self::service::{RpcClientService, RpcClientServiceBuilder};

use message::{Message, OutgoingMessage};
use traits::Cast;
use self::service::RpcClientServiceHandle;

mod service;

#[derive(Debug, Clone)]
pub struct RpcClient {
    service: RpcClientServiceHandle,
}
impl RpcClient {
    pub fn cast<T: Cast>(&self, server: SocketAddr, notification: T::Notification) {
        let message = OutgoingMessage::new(Message::Notification {
            procedure: T::PROCEDURE,
            data: notification,
        });
        self.service.send_message(server, message);
    }
}
