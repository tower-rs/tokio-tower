use futures_core::{
    future::Future,
    ready,
    stream::TryStream,
    task::{Context, Poll},
};
use futures_sink::Sink;
use futures_util::stream::FuturesOrdered;
use pin_project::pin_project;
use std::pin::Pin;
use std::{error, fmt};
use tower_service::Service;

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Response>` and `Stream<Item = Request>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
#[pin_project]
pub struct Server<T, S>
where
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
{
    #[pin]
    pending: FuturesOrdered<S::Future>,
    #[pin]
    transport: T,
    service: S,

    in_flight: usize,
    finish: bool,
}

/// An error that occurred while servicing a request.
pub enum Error<T, S>
where
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
{
    /// The underlying transport failed to produce a request.
    BrokenTransportRecv(<T as TryStream>::Error),

    /// The underlying transport failed while attempting to send a response.
    BrokenTransportSend(<T as Sink<S::Response>>::Error),

    /// The underlying service failed to process a request.
    Service(S::Error),
}

impl<T, S> fmt::Display for Error<T, S>
where
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
    <T as Sink<S::Response>>::Error: fmt::Display,
    <T as TryStream>::Error: fmt::Display,
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
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
    <T as Sink<S::Response>>::Error: fmt::Debug,
    <T as TryStream>::Error: fmt::Debug,
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
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
    <T as Sink<S::Response>>::Error: error::Error,
    <T as TryStream>::Error: error::Error,
    S::Error: error::Error,
{
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::BrokenTransportSend(ref se) => Some(se),
            Error::BrokenTransportRecv(ref se) => Some(se),
            Error::Service(ref se) => Some(se),
        }
    }

    #[allow(deprecated)]
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
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
{
    fn from_sink_error(e: <T as Sink<S::Response>>::Error) -> Self {
        Error::BrokenTransportSend(e)
    }

    fn from_stream_error(e: <T as TryStream>::Error) -> Self {
        Error::BrokenTransportRecv(e)
    }

    fn from_service_error(e: S::Error) -> Self {
        Error::Service(e)
    }
}

impl<T, S> Server<T, S>
where
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
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
    T: Sink<S::Response> + TryStream,
    S: Service<<T as TryStream>::Ok>,
{
    type Output = Result<(), Error<T, S>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // go through the deref so we can do partial borrows
        let this = self.project();

        // we never move transport or pending, nor do we ever hand out &mut to it
        let mut transport: Pin<_> = this.transport;
        let mut pending: Pin<_> = this.pending;

        loop {
            // first, poll pending futures to see if any have produced responses
            // note that we only poll for completed service futures if we can send the response
            while let Poll::Ready(r) = transport.as_mut().poll_ready(cx) {
                if let Err(e) = r {
                    return Poll::Ready(Err(Error::from_sink_error(e)));
                }

                match pending.as_mut().try_poll_next(cx) {
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Err(Error::from_service_error(e)));
                    }
                    Poll::Ready(Some(Ok(rsp))) => {
                        // try to send the response!
                        transport
                            .as_mut()
                            .start_send(rsp)
                            .map_err(Error::from_sink_error)?;
                        *this.in_flight -= 1;
                    }
                    _ => {
                        // XXX: should we "release" the poll_ready we got from the Sink?
                        break;
                    }
                }
            }

            // also try to make progress on sending
            if let Poll::Ready(()) = transport
                .as_mut()
                .poll_flush(cx)
                .map_err(Error::from_sink_error)?
            {
                if *this.finish && pending.as_mut().is_empty() {
                    // there are no more requests
                    // and we've finished all the work!
                    return Poll::Ready(Ok(()));
                }
            }

            if *this.finish {
                // there's still work to be done, but there are no more requests
                // so no need to check the incoming transport
                return Poll::Pending;
            }

            // is the service ready?
            ready!(this.service.poll_ready(cx)).map_err(Error::from_service_error)?;

            let rq = ready!(transport.as_mut().try_poll_next(cx))
                .transpose()
                .map_err(Error::from_stream_error)?;
            if let Some(rq) = rq {
                // the service is ready, and we have another request!
                // you know what that means:
                pending.push(this.service.call(rq));
                *this.in_flight += 1;
            } else {
                // there are no more requests coming -- shut down
                assert!(!*this.finish);
                *this.finish = true;
            }
        }
    }
}
