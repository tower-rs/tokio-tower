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

macro_rules! event {
    ($span:expr, $($rest:tt)*) => {
        #[cfg(feature = "tokio-trace")]
        {
            if let Some(ref span) = $span {
                span.in_scope(|| tokio_trace::event!($($rest)*))
            }
        }
    };
}

mod error;
mod mediator;
pub(crate) mod wrappers;
pub use error::Error;
pub use wrappers::{ClientResponseFut, Request};

use futures::{Future, Poll, Sink, Stream};
use tower_service::Service;

/// Creates new `Transport` (i.e., `Sink + Stream`) instances.
///
/// Acts as a transport factory. This is useful for cases where new `Sink + Stream`
/// values must be produced.
///
/// This is essentially a trait alias for a `Service` of `Sink + Stream`s.
pub trait MakeTransport<Target, Request>: self::sealed::Sealed<Target, Request> {
    /// Items produced by the transport
    type Item;

    /// Errors produced when receiving from the transport
    type Error;

    /// Errors produced when sending to the transport
    type SinkError;

    /// The `Sink + Stream` implementation created by this factory
    type Transport: Stream<Item = Self::Item, Error = Self::Error>
        + Sink<SinkItem = Request, SinkError = Self::SinkError>;

    /// Errors produced while building a transport.
    type MakeError;

    /// The future of the `Service` instance.
    type Future: Future<Item = Self::Transport, Error = Self::MakeError>;

    /// Returns `Ready` when the factory is able to create more transports.
    ///
    /// If the service is at capacity, then `NotReady` is returned and the task
    /// is notified when the service becomes ready again. This function is
    /// expected to be called while on a task.
    ///
    /// This is a **best effort** implementation. False positives are permitted.
    /// It is permitted for the service to return `Ready` from a `poll_ready`
    /// call and the next invocation of `make_transport` results in an error.
    fn poll_ready(&mut self) -> Poll<(), Self::MakeError>;

    /// Create and return a new transport asynchronously.
    fn make_transport(&mut self, target: Target) -> Self::Future;
}

impl<M, T, Target, Request> self::sealed::Sealed<Target, Request> for M
where
    M: Service<Target, Response = T>,
    T: Stream + Sink<SinkItem = Request>,
{
}

impl<M, T, Target, Request> MakeTransport<Target, Request> for M
where
    M: Service<Target, Response = T>,
    T: Stream + Sink<SinkItem = Request>,
{
    type Item = T::Item;
    type Error = T::Error;
    type SinkError = T::SinkError;
    type Transport = T;
    type MakeError = M::Error;
    type Future = M::Future;

    fn poll_ready(&mut self) -> Poll<(), Self::MakeError> {
        Service::poll_ready(self)
    }

    fn make_transport(&mut self, target: Target) -> Self::Future {
        Service::call(self, target)
    }
}

mod sealed {
    pub trait Sealed<A, B> {}
}

pub mod multiplex;
pub mod pipeline;
