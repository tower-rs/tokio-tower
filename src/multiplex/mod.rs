//! In a multiplexed protocol, the server responds to client requests in the order they complete.
//! Request IDs ([`TagStore::Tag`]) are used to match up responses with the request that triggered
//! them. This allows the server to process requests out-of-order, and eliminates the
//! application-level head-of-line blocking that pipelined protocols suffer from. Example
//! multiplexed protocols include SSH, HTTP/2, and AMQP. [This
//! page](https://250bpm.com/blog:18/) has some further details about how multiplexing protocols
//! operate.
//!
//! Note: multiplexing with the max number of in-flight requests set to 1 implies that for each
//! request, the response must be received before sending another request on the same connection.

use futures_core::stream::{Stream, TryStream};
use futures_sink::Sink;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Client bindings for a multiplexed protocol.
pub mod client;
pub use self::client::{Client, TagStore};

/// Server bindings for a multiplexed protocol.
pub mod server;
pub use self::server::Server;

mod pending;
pub use pending::Pending;

/// A convenience wrapper that lets you take separate transport and tag store types and use them as
/// a single [`client::Transport`].
#[pin_project]
#[derive(Debug)]
pub struct MultiplexTransport<T, S> {
    #[pin]
    transport: T,
    #[pin]
    tagger: S,
}

impl<T, S> MultiplexTransport<T, S> {
    /// Fuse together the given `transport` and `tagger` into a single `Transport`.
    pub fn new(transport: T, tagger: S) -> Self {
        MultiplexTransport { transport, tagger }
    }
}

impl<T, S, Request> Sink<Request> for MultiplexTransport<T, S>
where
    T: Sink<Request>,
{
    type Error = <T as Sink<Request>>::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().transport.poll_ready(cx)
    }
    fn start_send(self: Pin<&mut Self>, item: Request) -> Result<(), Self::Error> {
        self.project().transport.start_send(item)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().transport.poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().transport.poll_close(cx)
    }
}

impl<T, S> Stream for MultiplexTransport<T, S>
where
    T: TryStream,
{
    type Item = Result<<T as TryStream>::Ok, <T as TryStream>::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().transport.try_poll_next(cx)
    }
}

impl<T, S, Request> TagStore<Request, <T as TryStream>::Ok> for MultiplexTransport<T, S>
where
    T: Sink<Request> + TryStream,
    S: TagStore<Request, <T as TryStream>::Ok>,
{
    type Tag = <S as TagStore<Request, <T as TryStream>::Ok>>::Tag;
    fn assign_tag(self: Pin<&mut Self>, req: &mut Request) -> Self::Tag {
        self.project().tagger.assign_tag(req)
    }
    fn finish_tag(self: Pin<&mut Self>, rsp: &<T as TryStream>::Ok) -> Self::Tag {
        self.project().tagger.finish_tag(rsp)
    }
}
