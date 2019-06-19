use crate::Error;
use futures::{Async, Future, Poll, Sink, Stream};

/// A request to be submitted to a tokio-tower client.
///
/// The request primarily consists of the inner request type, but may also be annotated with
/// additional metadata if support for them has been enabled. For the time being, the only
/// supported meta-data is a `tokio-trace::Span`, which enables tracing the request's path through
/// the client. To enable this tracing, add this to your dependency on `tokio-tower`:
///
/// ```toml
/// features = ["tokio-trace"]
/// ```
#[derive(Clone, Debug)]
pub struct Request<T> {
    pub(crate) req: T,
    #[cfg(feature = "tokio-trace")]
    pub(crate) span: Option<tokio_trace::Span>,
}

impl<T> Request<T> {
    /// Create a new tokio-trace request.
    pub fn new(t: T) -> Self {
        t.into()
    }
}

impl<T> Request<T> {
    /// Set the span that should be used to trace this request's path through `tokio-tower`.
    #[cfg(feature = "tokio-trace")]
    pub fn with_span(mut self, span: tokio_trace::Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl<T: PartialEq> PartialEq for Request<T> {
    fn eq(&self, other: &Request<T>) -> bool {
        self.req.eq(&other.req)
    }
}
impl<T: Eq> Eq for Request<T> {}

impl<T: PartialOrd> PartialOrd for Request<T> {
    fn partial_cmp(&self, other: &Request<T>) -> Option<std::cmp::Ordering> {
        self.req.partial_cmp(&other.req)
    }
}
impl<T: Ord> Ord for Request<T> {
    fn cmp(&self, other: &Request<T>) -> std::cmp::Ordering {
        self.req.cmp(&other.req)
    }
}

impl<T: std::hash::Hash> std::hash::Hash for Request<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.req.hash(state)
    }
}

impl<T> From<T> for Request<T> {
    fn from(t: T) -> Self {
        Request {
            req: t,
            #[cfg(feature = "tokio-trace")]
            span: None,
        }
    }
}

pub(crate) struct ClientRequest<T>
where
    T: Sink + Stream,
{
    pub(crate) req: Request<T::SinkItem>,
    pub(crate) res: tokio_sync::oneshot::Sender<ClientResponse<T::Item>>,
}

pub(crate) struct ClientResponse<T> {
    pub(crate) response: T,
    #[cfg(feature = "tokio-trace")]
    pub(crate) span: Option<tokio_trace::Span>,
}

pub(crate) enum ClientResponseFutInner<T, E>
where
    T: Stream,
{
    Failed(Option<E>),
    Pending(tokio_sync::oneshot::Receiver<ClientResponse<T::Item>>),
}

impl<T, E> Future for ClientResponseFutInner<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
{
    type Item = ClientResponse<T::Item>;
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            ClientResponseFutInner::Failed(ref mut e) => Err(e
                .take()
                .expect("ClientResponseFut::poll called after Err returned")),
            ClientResponseFutInner::Pending(ref mut os) => match os.poll() {
                Ok(Async::Ready(r)) => {
                    event!(r.span, tokio_trace::Level::TRACE, "response returned");
                    Ok(Async::Ready(r))
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(_) => return Err(E::from(Error::ClientDropped)),
            },
        }
    }
}

impl<T, E> Into<ClientResponseFut<T, E>> for ClientResponseFutInner<T, E>
where
    T: Stream,
{
    fn into(self) -> ClientResponseFut<T, E> {
        ClientResponseFut { fut: self }
    }
}

/// A future that resolves a previously submitted [`Request`].
pub struct ClientResponseFut<T, E>
where
    T: Stream,
{
    fut: ClientResponseFutInner<T, E>,
}

impl<T, E> ClientResponseFut<T, E>
where
    T: Stream,
{
    /// Make the future also resolve with its associated `tokio-trace::Span` (if any).
    #[cfg(feature = "tokio-trace")]
    pub fn with_span(self) -> SpannedClientResponseFut<T, E> {
        SpannedClientResponseFut { fut: self.fut }
    }
}

impl<T, E> Future for ClientResponseFut<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
{
    type Item = T::Item;
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(try_ready!(self.fut.poll()).response))
    }
}

/// A future that resolves a previously submitted [`Request`] that includes a `tokio-trace::Span`.
#[cfg(feature = "tokio-trace")]
pub struct SpannedClientResponseFut<T, E>
where
    T: Stream,
{
    fut: ClientResponseFutInner<T, E>,
}

#[cfg(feature = "tokio-trace")]
impl<T, E> Future for SpannedClientResponseFut<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
{
    type Item = (T::Item, Option<tokio_trace::Span>);
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ClientResponse { response, span, .. } = try_ready!(self.fut.poll());
        Ok(Async::Ready((response, span)))
    }
}
