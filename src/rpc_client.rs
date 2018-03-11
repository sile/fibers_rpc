use std::net::SocketAddr;

use {Call, Cast};
use client_service::{Message, RpcClientServiceHandle};
use client_side_handlers::{Response, ResponseHandler};
use message::OutgoingMessage;

#[derive(Debug, Clone)]
pub struct RpcClient {
    // TODO:
    pub(crate) service: RpcClientServiceHandle,
}
impl RpcClient {
    pub fn cast<T: Cast>(&self, server: SocketAddr, notification: T::Notification)
    where
        T::Encoder: From<T::Notification>,
    {
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), T::Encoder::from(notification)),
            response_handler: None,
            force_wakeup: false,
        };
        self.service.send_message(server, message);
    }

    pub fn call<T: Call>(&self, server: SocketAddr, request: T::Request) -> Response<T::Response>
    where
        T::RequestEncoder: From<T::Request>,
        T::ResponseDecoder: Default,
    {
        let (handler, response) = ResponseHandler::new(T::ResponseDecoder::default());
        let message = Message {
            message: OutgoingMessage::new(Some(T::ID), T::RequestEncoder::from(request)),
            response_handler: Some(Box::new(handler)),
            force_wakeup: false,
        };
        self.service.send_message(server, message);
        response
    }
}
