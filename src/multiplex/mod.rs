//! In a multiplexed protocol, the server responds to client requests in the order they complete.
//! Request IDs ([`TagStore::Tag`]) are used to match up responses with the request that triggered
//! them. This allows the server to process requests out-of-order, and eliminates the
//! application-level head-of-line blocking that pipelined protocols suffer from. Example
//! multiplexed protocols include SSH, HTTP/2, and AMQP. [This
//! page](http://250bpm.com/multiplexing) has some further details about how multiplexing protocols
//! operate.
//!
//! Note: multiplexing with the max number of in-flight requests set to 1 implies that for each
//! request, the response must be received before sending another request on the same connection.

use futures::{Async, AsyncSink, Sink, Stream};

mod client;
pub use self::client::Error as ClientError;
pub use self::client::{Client, TagStore, Transport};
mod server;
pub use self::server::Error as ServerError;
pub use self::server::Server;

/// A convenience wrapper that lets you take separate transport and tag store types and use them as
/// a single [`multiplex::Transport`].
pub struct MultiplexTransport<T, S> {
    transport: T,
    tagger: S,
}

impl<T, S> MultiplexTransport<T, S> {
    /// Fuse together the given `transport` and `tagger` into a single `Transport`.
    pub fn new(transport: T, tagger: S) -> Self {
        MultiplexTransport { transport, tagger }
    }
}

impl<T, S> Sink for MultiplexTransport<T, S>
where
    T: Sink,
{
    type SinkItem = <T as Sink>::SinkItem;
    type SinkError = <T as Sink>::SinkError;
    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        self.transport.start_send(item)
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.transport.poll_complete()
    }
}

impl<T, S> Stream for MultiplexTransport<T, S>
where
    T: Stream,
{
    type Item = <T as Stream>::Item;
    type Error = <T as Stream>::Error;
    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.transport.poll()
    }
}

impl<T, S> TagStore<<T as Sink>::SinkItem, <T as Stream>::Item> for MultiplexTransport<T, S>
where
    T: Sink + Stream,
    S: TagStore<<T as Sink>::SinkItem, <T as Stream>::Item>,
{
    type Tag = <S as TagStore<<T as Sink>::SinkItem, <T as Stream>::Item>>::Tag;
    fn assign_tag(&mut self, req: &mut <T as Sink>::SinkItem) -> Self::Tag {
        self.tagger.assign_tag(req)
    }
    fn finish_tag(&mut self, rsp: &<T as Stream>::Item) -> Self::Tag {
        self.tagger.finish_tag(rsp)
    }
}

impl<T, S> Transport for MultiplexTransport<T, S>
where
    T: Sink + Stream,
    S: TagStore<<T as Sink>::SinkItem, <T as Stream>::Item>,
{}
