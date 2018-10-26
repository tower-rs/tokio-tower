//! This crate provides utilities for using protocols that follow certain common patterns on
//! top of [Tokio](https://tokio.rs) and [Tower](https://github.com/tower-rs/tower).
//!
//! # Protocols
//!
//! At a high level, a protocol is a mechanism that lets you take a bunch of requests and turn them
//! into responses. Tower provides the [`Service`](https://docs.rs/tower-service/) trait, which is
//! an interface for mapping requests into responses, but it does not deal with how those requests
//! are sent between clients and servers. Tokio, on the other hand, provides asynchronous
//! communication primitives, but it does not deal with high-level abstractions like services. This
//! crate attempts to bridge that gap.
//!
//! There are many types of protocols in the wild, but they generally come in two forms:
//! *pipelining* and *multiplexing*. A pipelining protocol sends requests and responses in-order
//! between the consumer and provider of a service, and processes requests one at a time. A
//! multiplexing protocol on the other hand constructs requests in such a way that they can be
//! handled and responded to in *any* order while still allowing the client to know which response
//! is for which request. Pipelining and multiplexing both have their advantages and disadvantages;
//! see the module-level documentation for [`pipeline`] and [`multiplex`] for details. There is
//! also good deal of discussion in [this StackOverflow
//! answer](https://softwareengineering.stackexchange.com/a/325888/79642).
//!
//! # Transports
//!
//! A key part of any protocol is its transport, which is the way that it transmits requests and
//! responses. In general, `tokio-tower` leaves the on-the-wire implementations of protocols to
//! other crates (like [`tokio-codec`](https://docs.rs/tokio-codec/) or
//! [`async-bincode`](https://docs.rs/async-bincode)) and instead operates at the level of
//! [`Sink`](https://docs.rs/futures/0.1/futures/sink/trait.Sink.html)s and
//! [`Stream`](https://docs.rs/futures/0.15/futures/stream/trait.Stream.html)s. In particular, it
//! assumes that there exists a `Sink + Stream` transport where it can send `Request`s and receive
//! `Response`s, or vice-versa for the server side.
//!
//! # Servers and clients
//!
//! This crate provides utilities that make writing both clients and servers easier. You'll find
//! the client helper as `Client` in the protocol module you're working with (e.g.,
//! [`pipeline::Client`]), and the server helper as `Server` in the same place.
#![deny(missing_docs)]

#[macro_use]
extern crate futures;
extern crate tower_service;

use futures::{Async, Future};

/// https://github.com/tower-rs/tower/issues/110#issuecomment-433193941
pub trait DirectService<Request> {
    /// Responses given by the service.
    type Response;

    /// Errors produced by the service.
    type Error;

    /// The future response value.
    type Future: Future<Item = Self::Response, Error = Self::Error>;

    /// Called to make room for another request.
    ///
    /// Should call `poll_outstanding` as necessary to finish in-flight requests.
    fn poll_ready(&mut self) -> Result<Async<()>, Self::Error>;

    /// Called to make progress on in-flight requests.
    ///
    /// Should return `Ready` when all outstanding requests have been serviced.
    fn poll_outstanding(&mut self) -> Result<Async<()>, Self::Error>;

    /// Called after there will be no more calls to `call`.
    ///
    /// `poll_close` should ensure that all in-progress requests resolve, as well as perform and
    /// finish any required service cleanup.
    fn poll_close(&mut self) -> Result<Async<()>, Self::Error>;

    /// Like today's Service::call, but with the caveat that poll_outstanding must
    /// continue to be called for the returned futures to resolve.
    fn call(&mut self, req: Request) -> Self::Future;
}

pub mod buffer;
pub mod multiplex;
pub mod pipeline;
