fibers_rpc
==========

[![fibers_rpc](http://meritbadge.herokuapp.com/fibers_rpc)](https://crates.io/crates/fibers_rpc)
[![Documentation](https://docs.rs/fibers_rpc/badge.svg)](https://docs.rs/fibers_rpc)
[![Build Status](https://travis-ci.org/sile/fibers_rpc.svg?branch=master)](https://travis-ci.org/sile/fibers_rpc)
[![Code Coverage](https://codecov.io/gh/sile/fibers_rpc/branch/master/graph/badge.svg)](https://codecov.io/gh/sile/fibers_rpc/branch/master)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Rust RPC library built on top of [fibers] crate.

[Documentation](https://docs.rs/fibers_rpc)

[fibers]: https://github.com/dwango/fibers-rs

Examples
--------

Simple echo RPC server:
```rust
use fibers::{Executor, InPlaceExecutor, Spawn};
use fibers_rpc::{Call, ProcedureId};
use fibers_rpc::client::RpcClientServiceBuilder;
use fibers_rpc::codec::BytesEncoder;
use fibers_rpc::server::{HandleCall, Reply, RpcServerBuilder};
use futures::Future;

// RPC definition
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

// Executor
let mut executor = InPlaceExecutor::new().unwrap();

// RPC server
struct EchoHandler;
impl HandleCall<EchoRpc> for EchoHandler {
    fn handle_call(&self, request: <EchoRpc as Call>::Req) -> Reply<EchoRpc> {
        Reply::done(request)
    }
}
let server_addr = "127.0.0.1:1919".parse().unwrap();
let server = RpcServerBuilder::new(server_addr)
    .call_handler(EchoHandler)
    .finish(executor.handle());
executor.spawn(server.map_err(|e| panic!("{}", e)));

// RPC client
let service = RpcClientServiceBuilder::new().finish(executor.handle());

let request = Vec::from(&b"hello"[..]);
let response = EchoRpc::client(&service.handle()).call(server_addr, request.clone());

executor.spawn(service.map_err(|e| panic!("{}", e)));
let result = executor.run_future(response).unwrap();
assert_eq!(result.ok(), Some(request));
```
