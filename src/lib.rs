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
//! [`Stream`](https://docs.rs/futures/0.15/futures/stream/trait.Stream.html)s.
//!
//! At its core, `tokio-tower` wraps a type that is `Sink + Stream`. On the client side, the Sink
//! is used to send requests, and the Stream is used to receive responses (from the server) to
//! those requests. On the server side, the Stream is used to receive requests, and the Sink is
//! used to send the responses.
//!
//! # Servers and clients
//!
//! This crate provides utilities that make writing both clients and servers easier. You'll find
//! the client helper as `Client` in the protocol module you're working with (e.g.,
//! [`pipeline::Client`]), and the server helper as `Server` in the same place.
//!
//! # Example
//! ```rust
//! # use std::pin::Pin;
//! # use std::boxed::Box;
//! # use tokio::sync::mpsc;
//! # use tokio::io::{AsyncWrite, AsyncRead};
//! # use futures_core::task::{Context, Poll};
//! # use futures_util::{never::Never, future::{poll_fn, ready, Ready}};
//! # use tokio_tower::pipeline;
//! # use core::fmt::Debug;
//! type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
//!
//! /// A transport implemented using a pair of `mpsc` channels.
//! ///
//! /// `mpsc::Sender` and `mpsc::Receiver` are both unidirectional. So, if we want to use `mpsc`
//! /// to send requests and responses between a client and server, we need *two* channels, one
//! /// that lets requests flow from the client to the server, and one that lets responses flow the
//! /// other way.
//! ///
//! /// In this echo server example, requests and responses are both of type `T`, but for "real"
//! /// services, the two types are usually different.
//! struct ChannelTransport<T> {
//!     rcv: mpsc::Receiver<T>,
//!     snd: mpsc::Sender<T>,
//! }
//!
//! impl<T: Debug> futures_sink::Sink<T> for ChannelTransport<T> {
//!     type Error = StdError;
//!
//!     fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//!         self.snd.poll_ready(cx).map_err(|e| e.into())
//!     }
//!
//!     fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
//!         // unwrap ok because of poll_ready()
//!         self.snd.try_send(item).unwrap();
//!         Ok(())
//!     }
//!
//!     fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//!         Poll::Ready(Ok(())) // no-op because all sends succeed immediately
//!     }
//!
//!     fn poll_close( self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//!         Poll::Ready(Ok(())) // no-op because channel is closed on drop and flush is no-op
//!     }
//! }
//!
//! impl<T> futures_util::stream::Stream for ChannelTransport<T> {
//!     type Item = Result<T, StdError>;
//!
//!     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
//!         self.rcv.poll_recv(cx).map(|s| s.map(Ok))
//!     }
//! }
//!
//! /// A service that tokio-tower should serve over the transport.
//! /// This one just echoes whatever it gets.
//! struct Echo;
//!
//! impl<T> tower_service::Service<T> for Echo {
//!     type Response = T;
//!     type Error = Never;
//!     type Future = Ready<Result<Self::Response, Self::Error>>;
//!
//!     fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
//!         Poll::Ready(Ok(()))
//!     }
//!
//!     fn call(&mut self, req: T) -> Self::Future {
//!         ready(Ok(req))
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let (s1, r1) = mpsc::channel(2);
//!     let (s2, r2) = mpsc::channel(2);
//!     let pair1 = ChannelTransport{snd: s1, rcv: r2};
//!     let pair2 = ChannelTransport{snd: s2, rcv: r1};
//!
//!     tokio::spawn(pipeline::Server::new(pair1, Echo));
//!     let mut client = pipeline::Client::<_, tokio_tower::Error<_, _>, _>::new(pair2);
//!
//!     use tower_service::Service;
//!     poll_fn(|cx| client.poll_ready(cx)).await;
//!
//!     let msg = "Hello, tokio-tower";
//!     let resp = client.call(String::from(msg)).await.expect("client call");
//!     assert_eq!(resp, msg);
//! }
//!
//! ```
#![deny(missing_docs)]

const YIELD_EVERY: usize = 24;

mod error;
mod mediator;
pub(crate) mod wrappers;
pub use error::Error;

use futures_core::{
    future::Future,
    stream::TryStream,
    task::{Context, Poll},
};
use futures_sink::Sink;
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
    type Transport: TryStream<Ok = Self::Item, Error = Self::Error>
        + Sink<Request, Error = Self::SinkError>;

    /// Errors produced while building a transport.
    type MakeError;

    /// The future of the `Service` instance.
    type Future: Future<Output = Result<Self::Transport, Self::MakeError>>;

    /// Returns `Ready` when the factory is able to create more transports.
    ///
    /// If the service is at capacity, then `NotReady` is returned and the task
    /// is notified when the service becomes ready again. This function is
    /// expected to be called while on a task.
    ///
    /// This is a **best effort** implementation. False positives are permitted.
    /// It is permitted for the service to return `Ready` from a `poll_ready`
    /// call and the next invocation of `make_transport` results in an error.
    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::MakeError>>;

    /// Create and return a new transport asynchronously.
    fn make_transport(&mut self, target: Target) -> Self::Future;
}

impl<M, T, Target, Request> self::sealed::Sealed<Target, Request> for M
where
    M: Service<Target, Response = T>,
    T: TryStream + Sink<Request>,
{
}

impl<M, T, Target, Request> MakeTransport<Target, Request> for M
where
    M: Service<Target, Response = T>,
    T: TryStream + Sink<Request>,
{
    type Item = <T as TryStream>::Ok;
    type Error = <T as TryStream>::Error;
    type SinkError = <T as Sink<Request>>::Error;
    type Transport = T;
    type MakeError = M::Error;
    type Future = M::Future;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::MakeError>> {
        Service::poll_ready(self, cx)
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
