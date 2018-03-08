extern crate clap;
extern crate fibers;
extern crate fibers_rpc;
extern crate futures;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use std::net::ToSocketAddrs;
use std::io::{self, Cursor, Read, Write};
use clap::{App, Arg, SubCommand};
use fibers::{Executor, InPlaceExecutor, Spawn};
use fibers_rpc::ProcedureId;
use fibers_rpc::client::RpcClientServiceBuilder;
use fibers_rpc::server::RpcServerBuilder;
use fibers_rpc::traits::{Call, Cast, HandleCall, HandleCast};
use futures::Future;
use sloggers::Build;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::types::Severity;
use trackable::error::{ErrorKindExt, Failed, Failure};

struct EchoRpc;
impl Cast for EchoRpc {
    const PROCEDURE: ProcedureId = 0;
    type Notification = Vec<u8>;
    type Encoder = Cursor<Vec<u8>>;
    type Decoder = Vec<u8>;
}
impl Call for EchoRpc {
    const PROCEDURE: ProcedureId = 1;
    type Request = Vec<u8>;
    type RequestEncoder = Cursor<Vec<u8>>;
    type RequestDecoder = Vec<u8>;

    type Response = Vec<u8>;
    type ResponseEncoder = Cursor<Vec<u8>>;
    type ResponseDecoder = Vec<u8>;
}

#[derive(Clone)]
struct EchoHandler;
impl HandleCast<EchoRpc> for EchoHandler {
    fn handle_cast(
        &self,
        notification: <EchoRpc as Cast>::Notification,
    ) -> fibers_rpc::traits::NoReply {
        println!("# RECV: {:?}", notification);
        fibers_rpc::traits::NoReply
    }
}
impl HandleCall<EchoRpc> for EchoHandler {
    fn handle_call(
        &self,
        request: <EchoRpc as Call>::Request,
    ) -> fibers_rpc::traits::Reply<<EchoRpc as Call>::Response> {
        println!("# Request: {:?}", request);
        unimplemented!()
    }
}

fn main() {
    let matches = App::new("echo")
        .arg(
            Arg::with_name("ADDRESS")
                .long("addr")
                .takes_value(true)
                .default_value("127.0.0.1:4567"),
        )
        .arg(
            Arg::with_name("LOG_LEVEL")
                .long("log-level")
                .takes_value(true)
                .default_value("debug")
                .possible_values(&["debug", "info", "warning", "error"]),
        )
        .subcommand(SubCommand::with_name("server"))
        .subcommand(SubCommand::with_name("client").arg(Arg::with_name("CAST").long("cast")))
        .get_matches();

    let addr = track_try_unwrap!(
        matches
            .value_of("ADDRESS")
            .unwrap()
            .to_socket_addrs()
            .map_err(Failure::from_error)
            .and_then(|mut addrs| addrs
                .next()
                .ok_or_else(|| Failed.cause("No available address").into()))
    );

    let log_level: Severity = track_try_unwrap!(matches.value_of("LOG_LEVEL").unwrap().parse());
    let logger = track_try_unwrap!(TerminalLoggerBuilder::new().level(log_level).build());

    let mut executor = track_try_unwrap!(InPlaceExecutor::new().map_err(Failure::from_error));

    if let Some(_matches) = matches.subcommand_matches("server") {
        let server = RpcServerBuilder::new(addr)
            .logger(logger)
            .register_cast_handler(EchoHandler)
            .call_handler(EchoHandler)
            .finish(executor.handle());
        let fiber = executor.spawn_monitor(server);
        let _ = track_try_unwrap!(executor.run_fiber(fiber).map_err(Failure::from_error))
            .map_err(|e| panic!("{}", e));
    } else if let Some(_matches) = matches.subcommand_matches("client") {
        let service = RpcClientServiceBuilder::new()
            .logger(logger)
            .finish(executor.handle());
        let client = service.client();
        executor.spawn(service.map_err(|e| panic!("{}", e)));

        let mut buf = Vec::new();
        track_try_unwrap!(
            io::stdin()
                .read_to_end(&mut buf)
                .map_err(Failure::from_error)
        );
        if matches.is_present("CAST") {
            client
                .cast_options::<EchoRpc>()
                .with_encoder(Cursor::new)
                .cast(addr, buf);
            track_try_unwrap!(executor.run().map_err(Failure::from_error));
        } else {
            let future = client
                .call_options::<EchoRpc>()
                .with_encoder(Cursor::new)
                .call(addr, buf);
            let result =
                track_try_unwrap!(executor.run_future(future).map_err(Failure::from_error));
            let response = track_try_unwrap!(result);
            let _ = std::io::stdout().write(&response);
        }
    } else {
        println!("{}", matches.usage());
        std::process::exit(1);
    }
}
