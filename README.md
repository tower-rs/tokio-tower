[![Crates.io](https://img.shields.io/crates/v/tokio-tower.svg)](https://crates.io/crates/tokio-tower)
[![Documentation](https://docs.rs/tokio-tower/badge.svg)](https://docs.rs/tokio-tower/)
[![Build Status](https://dev.azure.com/tower-rs/tokio-tower/_apis/build/status/tokio-tower?branchName=master)](https://dev.azure.com/tower-rs/tokio-tower/_build/latest?definitionId=4&branchName=master)
[![Dependency status](https://deps.rs/repo/github/tower-rs/tokio-tower/status.svg)](https://deps.rs/repo/github/tower-rs/tokio-tower)


This crate provides convenient wrappers to make
[Tokio](https://tokio.rs) and [Tower](https://github.com/tower-rs/tower)
work together. In particular, it provides:

 - server bindings wrappers that combine a `tower::Service` with a
   transport that implements `Sink<SinkItem = Request>` and `Stream<Item
   = Response>`.
 - client wrappers that implement `tower::Service` for transports that
   implement `Sink<SinkItem = Request>` and `Stream<Item = Response>`.

Take a look at the [crate documentation](https://docs.rs/tokio-tower)
for details.
