use std::net::SocketAddr;

use traits::Cast;

#[derive(Debug, Clone)]
pub struct RpcClient {}
impl RpcClient {
    pub fn cast<T: Cast>(&mut self, server: SocketAddr, notification: T::Notification) {}
}
