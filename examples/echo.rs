extern crate clap;
extern crate fibers;
extern crate fibers_rpc;
extern crate futures;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use std::net::ToSocketAddrs;
use std::io::{self, Cursor, Read};
use clap::{App, Arg, SubCommand};
use fibers::{Executor, InPlaceExecutor, Spawn};
use fibers_rpc::ProcedureId;
use fibers_rpc::client::RpcClientServiceBuilder;
use fibers_rpc::traits::Cast;
use futures::Future;
use sloggers::Build;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::types::Severity;
use trackable::error::{ErrorKindExt, Failed, Failure};

struct EchoRpc;
impl Cast for EchoRpc {
    const PROCEDURE: ProcedureId = 0;
    type Notification = Cursor<Vec<u8>>;
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
        .subcommand(SubCommand::with_name("client"))
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

    let executor = track_try_unwrap!(InPlaceExecutor::new().map_err(Failure::from_error));

    if let Some(_matches) = matches.subcommand_matches("server") {
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
        client.cast::<EchoRpc>(addr, Cursor::new(buf));

        track_try_unwrap!(executor.run().map_err(Failure::from_error));
    } else {
        println!("{}", matches.usage());
        std::process::exit(1);
    }
}
