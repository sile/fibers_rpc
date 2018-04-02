extern crate bytecodec;
extern crate clap;
extern crate fibers;
extern crate fibers_rpc;
extern crate futures;
extern crate prometrics;
extern crate sloggers;
#[macro_use]
extern crate trackable;

use std::io::{self, Read, Write};
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use bytecodec::bytes::{BytesEncoder, RemainingBytesDecoder};
use clap::{App, Arg, SubCommand};
use fibers::{Executor, Spawn, ThreadPoolExecutor};
use fibers_rpc::{Call, ProcedureId};
use fibers_rpc::client::{ClientServiceBuilder, ClientServiceHandle, Options as RpcOptions};
use fibers_rpc::server::{HandleCall, Reply, ServerBuilder};
use futures::{Async, Future, Poll};
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
    type ReqDecoder = RemainingBytesDecoder;

    type Res = Vec<u8>;
    type ResEncoder = BytesEncoder<Vec<u8>>;
    type ResDecoder = RemainingBytesDecoder;
}

struct EchoHandler;
impl HandleCall<EchoRpc> for EchoHandler {
    fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<EchoRpc> {
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
            SubCommand::with_name("client")
                .arg(
                    Arg::with_name("TIMEOUT")
                        .long("timeout")
                        .takes_value(true)
                        .default_value("5000"),
                )
                .arg(
                    Arg::with_name("REPEAT")
                        .long("repeat")
                        .takes_value(true)
                        .default_value("1"),
                ),
        )
        .subcommand(
            SubCommand::with_name("bench")
                .arg(
                    Arg::with_name("CONCURRENCY")
                        .long("concurrency")
                        .short("c")
                        .takes_value(true)
                        .default_value("256"),
                )
                .arg(
                    Arg::with_name("REQUESTS")
                        .long("requests")
                        .short("n")
                        .takes_value(true)
                        .default_value("1000"),
                )
                .arg(
                    Arg::with_name("MAX_QUEUE_LEN")
                        .long("max-queue-len")
                        .takes_value(true)
                        .default_value("10000"),
                )
                .arg(
                    Arg::with_name("PRIORITY")
                        .long("priority")
                        .takes_value(true)
                        .default_value("128"),
                )
                .arg(Arg::with_name("SHOW_METRICS").long("show-metrics")),
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

    let mut executor = track_try_unwrap!(ThreadPoolExecutor::new().map_err(Failure::from_error));

    if let Some(_matches) = matches.subcommand_matches("server") {
        let server = ServerBuilder::new(addr)
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
        let repeat: usize =
            track_try_unwrap!(track_any_err!(matches.value_of("REPEAT").unwrap().parse()));

        let service = ClientServiceBuilder::new()
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

        for _ in 0..repeat {
            let mut client = EchoRpc::client(&client_service);
            client.options_mut().timeout = Some(timeout);
            let future = client.call(addr, buf.clone());
            let result =
                track_try_unwrap!(executor.run_future(future).map_err(Failure::from_error));
            let response = track_try_unwrap!(result);
            let _ = std::io::stdout().write(&response);
        }
    } else if let Some(matches) = matches.subcommand_matches("bench") {
        let concurrency: usize = track_try_unwrap!(track_any_err!(
            matches.value_of("CONCURRENCY").unwrap().parse()
        ));
        let requests: usize = track_try_unwrap!(track_any_err!(
            matches.value_of("REQUESTS").unwrap().parse()
        ));
        let max_queue_len: u64 = track_try_unwrap!(track_any_err!(
            matches.value_of("MAX_QUEUE_LEN").unwrap().parse()
        ));
        let priority: u8 = track_try_unwrap!(track_any_err!(
            matches.value_of("PRIORITY").unwrap().parse()
        ));

        let service = ClientServiceBuilder::new()
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
        let options = RpcOptions {
            max_queue_len: Some(max_queue_len),
            priority,
            ..RpcOptions::default()
        };

        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let start_time = Instant::now();
        for i in 0..concurrency {
            let finish_tx = finish_tx.clone();
            let future = Bench {
                i,
                step: concurrency,
                n: requests,
                input: buf.clone(),
                options: options.clone(),
                service: client_service.clone(),
                server: addr,
                future: None,
            };
            executor.spawn(future.then(move |result| {
                let _ = finish_tx.send(());
                if let Err(e) = result {
                    println!("{}", e);
                }
                Ok(())
            }));
        }
        std::thread::spawn(move || {
            track_try_unwrap!(track_any_err!(executor.run()));
        });
        for _ in 0..concurrency {
            track_try_unwrap!(track_any_err!(finish_rx.recv()));
        }
        let elapsed = start_time.elapsed();
        let seconds =
            elapsed.as_secs() as f64 + (f64::from(elapsed.subsec_nanos()) / 1_000_000_000.0);
        println!("# ELAPSED: {}", seconds);
        println!("# RPS: {}", requests as f64 / seconds);
        if matches.is_present("SHOW_METRICS") {
            let metrics = prometrics::default_gatherer().lock().unwrap().gather();
            println!("------");
            println!("{}", metrics.to_text());
        }
    } else {
        println!("{}", matches.usage());
        std::process::exit(1);
    }
}

struct Bench {
    i: usize,
    step: usize,
    n: usize,
    service: ClientServiceHandle,
    server: std::net::SocketAddr,
    options: RpcOptions,
    input: Vec<u8>,
    future: Option<fibers_rpc::client::Response<Vec<u8>>>,
}
impl Future for Bench {
    type Item = ();
    type Error = fibers_rpc::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if self.future.is_none() {
                if self.i >= self.n {
                    return Ok(Async::Ready(()));
                }

                let mut client = EchoRpc::client(&self.service);
                *client.options_mut() = self.options.clone();
                let future = client.call(self.server, self.input.clone());
                self.future = Some(future);
                self.i += self.step;
            }
            if let Async::Ready(Some(_)) = track!(self.future.poll())? {
                self.future = None;
                continue;
            }
            break;
        }
        Ok(Async::NotReady)
    }
}
