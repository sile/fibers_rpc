extern crate clap;
extern crate fibers;
extern crate fibers_rpc;
extern crate futures;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use std::io::{self, Read, Write};
use std::net::ToSocketAddrs;
use std::time::Duration;
use clap::{App, Arg, SubCommand};
use fibers::{Executor, InPlaceExecutor, Spawn};
use fibers_rpc::{Call, ProcedureId};
use fibers_rpc::client::RpcClientServiceBuilder;
use fibers_rpc::codec::BytesEncoder;
use fibers_rpc::server::{HandleCall, Reply, RpcServerBuilder};
use futures::Future;
use sloggers::Build;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::types::Severity;
use trackable::error::{ErrorKindExt, Failed, Failure};

struct EchoRpc;
impl Call for EchoRpc {
    const ID: ProcedureId = ProcedureId(0);
    const NAME: &'static str = "echo";

    type Req = Vec<u8>;
    type ReqEncoder = BytesEncoder<Vec<u8>>;
    type ReqDecoder = Vec<u8>;

    type Res = Vec<u8>;
    type ResEncoder = BytesEncoder<Vec<u8>>;
    type ResDecoder = Vec<u8>;
}

#[derive(Clone)]
struct EchoHandler;
impl HandleCall<EchoRpc> for EchoHandler {
    fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<<EchoRpc as Call>::Res> {
        Reply::done(request)
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
        .subcommand(
            SubCommand::with_name("client").arg(
                Arg::with_name("TIMEOUT")
                    .long("timeout")
                    .takes_value(true)
                    .default_value("5000"),
            ),
        )
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
            .call_handler(EchoHandler)
            .finish(executor.handle());
        let fiber = executor.spawn_monitor(server);
        let _ = track_try_unwrap!(executor.run_fiber(fiber).map_err(Failure::from_error))
            .map_err(|e| panic!("{}", e));
    } else if let Some(matches) = matches.subcommand_matches("client") {
        let timeout = Duration::from_millis(track_try_unwrap!(track_any_err!(
            matches.value_of("TIMEOUT").unwrap().parse()
        )));

        let service = RpcClientServiceBuilder::new()
            .logger(logger)
            .finish(executor.handle());
        let client_service = service.handle();
        executor.spawn(service.map_err(|e| panic!("{}", e)));

        let mut buf = Vec::new();
        track_try_unwrap!(
            io::stdin()
                .read_to_end(&mut buf)
                .map_err(Failure::from_error)
        );

        let future = EchoRpc::client(&client_service)
            .timeout(Some(timeout))
            .call(addr, buf);
        let result = track_try_unwrap!(executor.run_future(future).map_err(Failure::from_error));
        let response = track_try_unwrap!(result);
        let _ = std::io::stdout().write(&response);
    } else {
        println!("{}", matches.usage());
        std::process::exit(1);
    }
}
