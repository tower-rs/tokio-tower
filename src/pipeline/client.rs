use futures::future;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::{error, fmt};
use tower_service;

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Request>` and `Stream<Item = Response>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Client<T, E>
where
    T: Sink + Stream,
{
    requests: VecDeque<<T as Sink>::SinkItem>,
    responses: VecDeque<oneshot::Sender<<T as Stream>::Item>>,
    transport: T,

    max_in_flight: Option<usize>,
    in_flight: usize,

    #[allow(unused)]
    error: PhantomData<E>,
}

/// An error that occurred while servicing a request.
pub enum Error<T>
where
    T: Sink + Stream,
{
    /// The underlying transport failed to send a request.
    BrokenTransportSend(<T as Sink>::SinkError),

    /// The underlying transport failed while attempting to receive a response.
    ///
    /// If `None`, the transport closed without error while there were pending requests.
    BrokenTransportRecv(Option<<T as Stream>::Error>),

    /// Attempted to issue a `call` when no more requests can be in flight.
    ///
    /// See [`tower_service::Service::poll_ready`] and [`Client::with_limit`].
    TransportFull,

    /// Attempted to issue a `call`, but the underlying transport has been closed.
    ClientDropped,
}

impl<T> fmt::Display for Error<T>
where
    T: Sink + Stream,
    <T as Sink>::SinkError: fmt::Display,
    <T as Stream>::Error: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::BrokenTransportSend(ref se) => fmt::Display::fmt(se, f),
            Error::BrokenTransportRecv(Some(ref se)) => fmt::Display::fmt(se, f),
            Error::BrokenTransportRecv(None) => f.pad("transport closed with in-flight requests"),
            Error::TransportFull => f.pad("no more in-flight requests allowed"),
            Error::ClientDropped => f.pad("Client was dropped"),
        }
    }
}

impl<T> fmt::Debug for Error<T>
where
    T: Sink + Stream,
    <T as Sink>::SinkError: fmt::Debug,
    <T as Stream>::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::BrokenTransportSend(ref se) => write!(f, "BrokenTransportSend({:?})", se),
            Error::BrokenTransportRecv(Some(ref se)) => write!(f, "BrokenTransportRecv({:?})", se),
            Error::BrokenTransportRecv(None) => f.pad("BrokenTransportRecv"),
            Error::TransportFull => f.pad("TransportFull"),
            Error::ClientDropped => f.pad("ClientDropped"),
        }
    }
}

impl<T> error::Error for Error<T>
where
    T: Sink + Stream,
    <T as Sink>::SinkError: error::Error,
    <T as Stream>::Error: error::Error,
{
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::BrokenTransportSend(ref se) => Some(se),
            Error::BrokenTransportRecv(Some(ref se)) => Some(se),
            _ => None,
        }
    }

    fn description(&self) -> &str {
        match *self {
            Error::BrokenTransportSend(ref se) => se.description(),
            Error::BrokenTransportRecv(Some(ref se)) => se.description(),
            Error::BrokenTransportRecv(None) => "transport closed with in-flight requests",
            Error::TransportFull => "no more in-flight requests allowed",
            Error::ClientDropped => "Client was dropped",
        }
    }
}

impl<T> Error<T>
where
    T: Sink + Stream,
{
    fn from_sink_error(e: <T as Sink>::SinkError) -> Self {
        Error::BrokenTransportSend(e)
    }

    fn from_stream_error(e: <T as Stream>::Error) -> Self {
        Error::BrokenTransportRecv(Some(e))
    }
}

impl<T, E> Client<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
{
    /// Construct a new [`Client`] over the given `transport` with no limit on the number of
    /// in-flight requests.
    pub fn new(transport: T) -> Self {
        Client {
            requests: VecDeque::default(),
            responses: VecDeque::default(),
            transport,
            max_in_flight: None,
            in_flight: 0,
            error: PhantomData::<E>,
        }
    }

    /// Construct a new [`Client`] over the given `transport` with a maxmimum limit on the number
    /// of in-flight requests.
    ///
    /// Note that setting the limit to 1 implies that for each `Request`, the `Response` must be
    /// received before another request is sent on the same transport.
    pub fn with_limit(transport: T, max_in_flight: usize) -> Self {
        Client {
            requests: VecDeque::with_capacity(max_in_flight),
            responses: VecDeque::with_capacity(max_in_flight),
            transport,
            max_in_flight: Some(max_in_flight),
            in_flight: 0,
            error: PhantomData::<E>,
        }
    }
}

impl<T, E> tower_service::Service for Client<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
    E: Send + 'static,
    <T as Sink>::SinkItem: Send + 'static,
    <T as Stream>::Item: Send + 'static,
{
    type Request = <T as Sink>::SinkItem;
    type Response = <T as Stream>::Item;
    type Error = E;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Result<Async<()>, Self::Error> {
        loop {
            // send more requests if we have them
            while let Some(req) = self.requests.pop_front() {
                if let AsyncSink::NotReady(req) = self
                    .transport
                    .start_send(req)
                    .map_err(Error::from_sink_error)?
                {
                    self.requests.push_front(req);
                    break;
                } else {
                    self.in_flight += 1;
                }
            }

            // flush out any stuff we've sent in the past
            // if it returns not_ready, we still want to see if we've got some responses
            self.transport
                .poll_complete()
                .map_err(Error::from_sink_error)?;

            // and start looking for replies.
            //
            // note that we *could* have this just be a loop, but we don't want to poll the stream
            // if we know there's nothing for it to produce.
            while self.in_flight != 0 {
                match self.transport.poll().map_err(Error::from_stream_error)? {
                    Async::Ready(Some(r)) => {
                        // ignore send failures
                        // the client may just no longer care about the response
                        let sender = self
                            .responses
                            .pop_front()
                            .expect("got a request with no sender?");
                        let _ = sender.send(r);
                        self.in_flight -= 1;
                    }
                    Async::Ready(None) => {
                        // the transport terminated while we were waiting for a response!
                        // TODO: it'd be nice if we could return the transport here..
                        return Err(E::from(Error::BrokenTransportRecv(None)));
                    }
                    Async::NotReady => {
                        if let Some(mif) = self.max_in_flight {
                            if self.in_flight + self.requests.len() < mif {
                                return Ok(Async::Ready(()));
                            }
                        }

                        return Ok(Async::NotReady);
                    }
                }
            }

            if self.requests.is_empty() {
                return Ok(Async::Ready(()));
            }
        }
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        if let Some(mif) = self.max_in_flight {
            if self.in_flight + self.requests.len() < mif {
                return Box::new(future::err(E::from(Error::TransportFull)));
            }
        }

        let (tx, rx) = oneshot::channel();
        self.requests.push_back(req);
        self.responses.push_back(tx);
        Box::new(rx.map_err(|_| E::from(Error::ClientDropped)))
    }
}

#[cfg(test)]
mod tests {
    // TODO: O:)
}
