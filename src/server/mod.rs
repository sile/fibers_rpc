use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use slog::{Discard, Logger};
use byteorder::{BigEndian, ByteOrder};
use fibers::{BoxSpawn, Spawn};
use fibers::net::{TcpListener, TcpStream};
use fibers::net::futures::{Connected, TcpListenerBind};
use fibers::net::streams::Incoming;
use futures::{Async, Future, Poll, Stream};

use {Error, ErrorKind, ProcedureId, Result};
use channel::{RpcChannel, RpcChannelHandle};
use frame::{Frame, HandleFrame};
use traits::{Cast, Decode, HandleCast};

// TODO: rename (message handler?)
pub trait Deserializer {
    type Target;
    fn deserialize(&mut self, buf: &[u8]) -> Result<()>;
    fn finish(&mut self) -> Result<Self::Target>;
}

struct CastHandler(
    Box<for<'a> FnMut(&'a [u8], bool) -> Result<Option<::traits::Response>> + Send + 'static>,
);
impl Deserializer for CastHandler {
    type Target = ::traits::Response;
    fn deserialize(&mut self, buf: &[u8]) -> Result<()> {
        track!((self.0)(buf, false)).map(|_| ())
    }
    fn finish(&mut self) -> Result<Self::Target> {
        track!((self.0)(&[][..], true)).map(|r| r.unwrap())
    }
}

struct CastHandlerFactory(
    Box<
        Fn() -> Box<Deserializer<Target = ::traits::Response> + Send + 'static>
            + Send
            + Sync
            + 'static,
    >,
);
impl CastHandlerFactory {
    fn new<T: Cast, H: HandleCast<T>>(handler: H) -> Self
    where
        T::Decoder: Default,
    {
        let handler = Arc::new(handler);
        let f = move || {
            let mut decoder = Some(<T::Decoder as Default>::default()); //Some(handler.create_decoder());
            let handler = handler.clone();
            let handle = move |buf: &[u8], eof| {
                track!(decoder.as_mut().unwrap().decode(buf))?;
                if eof {
                    let data = track!(decoder.take().unwrap().finish())?;
                    let future = handler.handle_cast(data);
                    Ok(Some(::traits::Response::NoReply(future)))
                } else {
                    Ok(None)
                }
            };
            let cast_handler = CastHandler(Box::new(handle));
            let cast_handler: Box<
                Deserializer<Target = ::traits::Response> + Send + 'static,
            > = Box::new(cast_handler);
            cast_handler
        };
        CastHandlerFactory(Box::new(f))
    }
}
impl MsgHandleFactory for CastHandlerFactory {
    fn create(&self) -> Box<Deserializer<Target = ::traits::Response> + Send + 'static> {
        (self.0)()
    }
}

pub struct RpcServerBuilder {
    bind_addr: SocketAddr,
    logger: Logger,
    handlers: HashMap<ProcedureId, Box<MsgHandleFactory + Send + Sync + 'static>>,
}
impl RpcServerBuilder {
    pub fn new(bind_addr: SocketAddr) -> Self {
        RpcServerBuilder {
            bind_addr,
            logger: Logger::root(Discard, o!()),
            handlers: HashMap::new(),
        }
    }
    pub fn logger(mut self, logger: Logger) -> Self {
        self.logger = logger;
        self
    }

    pub fn register_cast_handler<T: Cast, H: HandleCast<T>>(mut self, handler: H) -> Self
    where
        T::Decoder: Default,
    {
        // TODO: check duplication
        self.handlers
            .insert(T::PROCEDURE, Box::new(CastHandlerFactory::new(handler)));
        self
    }

    pub fn finish<S>(self, spawner: S) -> RpcServer
    where
        S: Spawn + Send + 'static,
    {
        let logger = self.logger.new(o!("server" => self.bind_addr.to_string()));
        info!(logger, "Starts RPC server");
        RpcServer {
            listener: Listener::Binding(TcpListener::bind(self.bind_addr)),
            logger,
            spawner: spawner.boxed(),
            frame_handler: FrameHandler::new(self.handlers),
        }
    }
}

pub struct RpcServer {
    listener: Listener,
    logger: Logger,
    spawner: BoxSpawn,
    frame_handler: FrameHandler,
}
impl Future for RpcServer {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Async::Ready(item) = track!(self.listener.poll())? {
            if let Some((client, addr)) = item {
                let logger = self.logger.new(o!("client" => addr.to_string()));
                info!(logger, "New TCP client");

                let exit_logger = logger.clone();
                let frame_handler = self.frame_handler.clone();
                self.spawner.spawn(
                    client
                        .map_err(|e| track!(Error::from(e)))
                        .and_then(move |stream| {
                            TcpStreamHandler::new(logger, stream, frame_handler)
                        })
                        .then(move |result| {
                            if let Err(e) = result {
                                error!(exit_logger, "TCP connection aborted: {}", e);
                            } else {
                                info!(exit_logger, "TCP connection was closed");
                            }
                            Ok(())
                        }),
                );
            } else {
                info!(self.logger, "RPC server stopped");
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}

pub trait MsgHandleFactory {
    fn create(&self) -> Box<Deserializer<Target = ::traits::Response> + Send + 'static>;
}

struct FrameHandler {
    handlers: Arc<HashMap<ProcedureId, Box<MsgHandleFactory + Send + Sync + 'static>>>,
    active_handlers: HashMap<u32, Box<Deserializer<Target = ::traits::Response> + Send + 'static>>,
}
impl FrameHandler {
    fn new(handlers: HashMap<ProcedureId, Box<MsgHandleFactory + Send + Sync + 'static>>) -> Self {
        FrameHandler {
            handlers: Arc::new(handlers),
            active_handlers: HashMap::new(),
        }
    }
}
impl Clone for FrameHandler {
    fn clone(&self) -> Self {
        FrameHandler {
            handlers: self.handlers.clone(),
            active_handlers: HashMap::new(),
        }
    }
}
impl HandleFrame for FrameHandler {
    type Future = ::traits::Response;
    fn handle_frame(&mut self, frame: &Frame) -> Result<Option<Self::Future>> {
        track_assert!(!frame.is_error(), ErrorKind::Other, "TODO");

        let mut offset = 0;
        if !self.active_handlers.contains_key(&frame.seqno) {
            track_assert!(frame.data.len() >= 4, ErrorKind::InvalidInput);
            let procedure = BigEndian::read_u32(&frame.data);
            offset = 4;

            let factory =
                track_assert_some!(self.handlers.get(&procedure), ErrorKind::InvalidInput);
            let handler = factory.create();
            self.active_handlers.insert(frame.seqno, handler);
        }

        let mut handler = self.active_handlers
            .remove(&frame.seqno)
            .expect("Never fails");
        track!(handler.deserialize(&frame.data[offset..]))?; // TODO: handle error
        if frame.is_eof() {
            let future = track!(handler.finish())?;
            Ok(Some(future))
        } else {
            Ok(None)
        }
    }
}

struct TcpStreamHandler {
    channel: RpcChannel<FrameHandler>,
    channel_handle: RpcChannelHandle,
}
impl TcpStreamHandler {
    fn new(logger: Logger, stream: TcpStream, frame_handler: FrameHandler) -> Self {
        let (channel, channel_handle) = RpcChannel::with_stream(logger, stream, frame_handler);
        TcpStreamHandler {
            channel,
            channel_handle,
        }
    }
}
impl Future for TcpStreamHandler {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(_todo) = track!(self.channel.poll())? {
            // TODO: spawn future
        }
        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
enum Listener {
    Binding(TcpListenerBind),
    Listening(Incoming),
}
impl Stream for Listener {
    type Item = (Connected, SocketAddr);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let next = match *self {
                Listener::Binding(ref mut f) => {
                    if let Async::Ready(listener) = track!(f.poll().map_err(Error::from))? {
                        Listener::Listening(listener.incoming())
                    } else {
                        break;
                    }
                }
                Listener::Listening(ref mut s) => return track!(s.poll().map_err(Error::from)),
            };
            *self = next;
        }
        Ok(Async::NotReady)
    }
}
