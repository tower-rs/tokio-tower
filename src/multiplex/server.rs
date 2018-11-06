use futures::stream::futures_unordered::FuturesUnordered;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::{error, fmt};
//use tower_service::{NewService, Service};
use tower_direct_service::DirectService;

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// multiplexed protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Response>` and `Stream<Item = Request>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
///
/// Note that the `Server` does *not* tag requests or responses automatically; it is the
/// responsibility of the `Service` to include the necessary request identifier with the response
/// so the client can later match them up.
pub struct Server<T, S>
where
    T: Sink + Stream,
    S: DirectService<T::Item>,
{
    responses: VecDeque<S::Response>,
    pending: FuturesUnordered<S::Future>,
    transport: T,
    service: S,

    max_in_flight: Option<usize>,
    in_flight: usize,
    finish: bool,
}

/// An error that occurred while servicing a request.
pub enum Error<T, S>
where
    T: Sink + Stream,
    S: DirectService<T::Item>,
{
    /// The underlying transport failed to produce a request.
    BrokenTransportRecv(T::Error),

    /// The underlying transport failed while attempting to send a response.
    BrokenTransportSend(T::SinkError),

    /// The underlying service failed to process a request.
    Service(S::Error),
}

impl<T, S> fmt::Display for Error<T, S>
where
    T: Sink + Stream,
    T::SinkError: fmt::Display,
    T::Error: fmt::Display,
    S: DirectService<T::Item>,
    S::Error: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::BrokenTransportRecv(ref se) => fmt::Display::fmt(se, f),
            Error::BrokenTransportSend(ref se) => fmt::Display::fmt(se, f),
            Error::Service(ref se) => fmt::Display::fmt(se, f),
        }
    }
}

impl<T, S> fmt::Debug for Error<T, S>
where
    T: Sink + Stream,
    T::SinkError: fmt::Debug,
    T::Error: fmt::Debug,
    S: DirectService<T::Item>,
    S::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::BrokenTransportRecv(ref se) => write!(f, "BrokenTransportRecv({:?})", se),
            Error::BrokenTransportSend(ref se) => write!(f, "BrokenTransportSend({:?})", se),
            Error::Service(ref se) => write!(f, "Service({:?})", se),
        }
    }
}

impl<T, S> error::Error for Error<T, S>
where
    T: Sink + Stream,
    T::SinkError: error::Error,
    T::Error: error::Error,
    S: DirectService<T::Item>,
    S::Error: error::Error,
{
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::BrokenTransportSend(ref se) => Some(se),
            Error::BrokenTransportRecv(ref se) => Some(se),
            Error::Service(ref se) => Some(se),
        }
    }

    fn description(&self) -> &str {
        match *self {
            Error::BrokenTransportSend(ref se) => se.description(),
            Error::BrokenTransportRecv(ref se) => se.description(),
            Error::Service(ref se) => se.description(),
        }
    }
}

impl<T, S> Error<T, S>
where
    T: Sink + Stream,
    S: DirectService<T::Item>,
{
    fn from_sink_error(e: T::SinkError) -> Self {
        Error::BrokenTransportSend(e)
    }

    fn from_stream_error(e: T::Error) -> Self {
        Error::BrokenTransportRecv(e)
    }

    fn from_service_error(e: S::Error) -> Self {
        Error::Service(e)
    }
}

impl<T, S> Server<T, S>
where
    T: Sink + Stream,
    S: DirectService<T::Item>,
{
    /// Construct a new [`Server`] over the given `transport` that services requests using the
    /// given `service`.
    ///
    /// Requests are passed to [`Service::call`] as they arrive, and responses are written back to
    /// the underlying `transport` in the order that they complete. If a later request completes
    /// before an earlier request, its response is still sent immediately.
    ///
    /// If `limit` is `Some(n)`, at most `n` requests are allowed to be pending at any given point
    /// in time.
    pub fn multiplexed(transport: T, service: S, limit: Option<usize>) -> Self {
        let cap = limit.unwrap_or(16);
        Server {
            responses: VecDeque::with_capacity(cap),
            pending: FuturesUnordered::new(),
            transport,
            service,
            max_in_flight: limit,
            in_flight: 0,
            finish: false,
        }
    }

    /*
    /// Manage incoming new transport instances using the given service constructor.
    ///
    /// For each transport that `incoming` yields, a new instance of `service` is created to
    /// manage requests on that transport. This is roughly equivalent to:
    ///
    /// ```rust,ignore
    /// incoming.map(|t| Server::multiplexed(t, service.new_service(), limit))
    /// ```
    pub fn serve_on<TS, SS, E>(
        incoming: TS,
        service: SS,
        limit: Option<usize>,
    ) -> impl Stream<Item = Self, Error = E>
    where
        TS: Stream<Item = T>,
        SS: NewService<Request = S::Request, Response = S::Response, Error = S::Error, Service = S>,
        E: From<TS::Error>,
        E: From<SS::InitError>,
    {
        incoming.map_err(E::from).and_then(move |transport| {
            service
                .new_service()
                .map_err(E::from)
                .map(move |s| Server::multiplexed(transport, s, limit))
        })
    }
    */
}

impl<T, S> Future for Server<T, S>
where
    S: DirectService<<T as Stream>::Item>,
    T: Sink<SinkItem = S::Response>,
    T: Stream,
{
    type Item = ();
    type Error = Error<T, S>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            // first, if there are any pending requests, try to make service progress
            // TODO: only if any are ::Pending
            if !self.pending.is_empty() {
                if self.finish {
                    self.service
                        .poll_close()
                        .map_err(Error::from_service_error)?;
                } else {
                    self.service
                        .poll_service()
                        .map_err(Error::from_service_error)?;
                }
            }

            // first, poll pending futures to see if any have produced responses
            loop {
                match self.pending.poll().map_err(Error::from_service_error)? {
                    Async::Ready(Some(rsp)) => {
                        self.responses.push_back(rsp);
                    }
                    _ => break,
                }
            }

            // next, try to send any responses we have, but haven't sent yet
            while let Some(rsp) = self.responses.pop_front() {
                // try to send the request!
                if let AsyncSink::NotReady(rsp) = self
                    .transport
                    .start_send(rsp)
                    .map_err(Error::from_sink_error)?
                {
                    self.responses.push_front(rsp);
                    break;
                } else {
                    self.in_flight -= 1;
                }
            }

            // also try to make progress on sending
            if let Async::Ready(()) = self
                .transport
                .poll_complete()
                .map_err(Error::from_sink_error)?
            {
                if self.finish && self.pending.is_empty() && self.responses.is_empty() {
                    // there are no more requests
                    // and we've finished all the work!
                    return Ok(Async::Ready(()));
                }
            }

            if self.finish {
                // there's still work to be done, but there are no more requests
                // so no need to check the incoming transport
                return Ok(Async::NotReady);
            }

            // we can't send any more, so see if there are more requests for us
            if let Some(max) = self.max_in_flight {
                if self.in_flight >= max {
                    // we can't accept any more requests until we finish some responses
                    return Ok(Async::NotReady);
                }
            }

            // we are allowed to receive another request
            // is the service ready?
            try_ready!(self.service.poll_ready().map_err(Error::from_service_error));

            let rq = try_ready!(self.transport.poll().map_err(Error::from_stream_error));
            if let Some(rq) = rq {
                // the service is ready, and we have another request!
                // you know what that means:
                self.pending.push(self.service.call(rq));
                self.in_flight += 1;
            } else {
                // there are no more requests coming -- shut down
                assert!(!self.finish);
                self.finish = true;
            }
        }
    }
}
