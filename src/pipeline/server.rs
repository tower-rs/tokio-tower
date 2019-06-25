use futures::stream::FuturesOrdered;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::{error, fmt};
use tower_service::Service;

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Response>` and `Stream<Item = Request>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Server<T, S>
where
    T: Sink + Stream,
    S: Service<T::Item>,
{
    waiting: Option<S::Response>,
    pending: FuturesOrdered<S::Future>,
    transport: T,
    service: S,

    in_flight: usize,
    finish: bool,
}

/// An error that occurred while servicing a request.
pub enum Error<T, S>
where
    T: Sink + Stream,
    S: Service<T::Item>,
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
    S: Service<T::Item>,
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
    S: Service<T::Item>,
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
    S: Service<T::Item>,
    S::Error: error::Error,
{
    fn cause(&self) -> Option<&dyn error::Error> {
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
    S: Service<T::Item>,
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
    S: Service<T::Item>,
{
    /// Construct a new [`Server`] over the given `transport` that services requests using the
    /// given `service`.
    ///
    /// Requests are passed to `Service::call` as they arrive, and responses are written back to
    /// the underlying `transport` in the order that the requests arrive. If a later request
    /// completes before an earlier request, its result will be buffered until all preceeding
    /// requests have been sent.
    pub fn new(transport: T, service: S) -> Self {
        Server {
            waiting: None,
            pending: FuturesOrdered::new(),
            transport,
            service,
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
    /// incoming.map(|t| Server::pipelined(t, service.new_service(), limit))
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
                .map(move |s| Server::pipelined(transport, s, limit))
        })
    }
    */
}

impl<T, S> Future for Server<T, S>
where
    S: Service<<T as Stream>::Item>,
    T: Sink<SinkItem = S::Response>,
    T: Stream,
{
    type Item = ();
    type Error = Error<T, S>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            // first, if we have a response ready to send, try to send it
            if let Some(rsp) = self.waiting.take() {
                if let AsyncSink::NotReady(rsp) = self
                    .transport
                    .start_send(rsp)
                    .map_err(Error::from_sink_error)?
                {
                    self.waiting = Some(rsp);
                } else {
                    self.in_flight -= 1;
                }
            }

            // then, poll pending futures to see if any have produced responses
            while self.waiting.is_none() {
                match self.pending.poll().map_err(Error::from_service_error)? {
                    Async::Ready(Some(rsp)) => {
                        // try to send the response!
                        if let AsyncSink::NotReady(rsp) = self
                            .transport
                            .start_send(rsp)
                            .map_err(Error::from_sink_error)?
                        {
                            self.waiting = Some(rsp);
                        } else {
                            self.in_flight -= 1;
                        }
                    }
                    _ => break,
                }
            }

            // also try to make progress on sending
            if let Async::Ready(()) = self
                .transport
                .poll_complete()
                .map_err(Error::from_sink_error)?
            {
                if self.finish && self.waiting.is_none() && self.pending.is_empty() {
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
