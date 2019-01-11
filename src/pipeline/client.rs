use crate::MakeTransport;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::{error, fmt};
use tokio_executor::DefaultExecutor;
use tower_buffer::{Buffer, DirectServiceRef};
use tower_direct_service::DirectService;
use tower_service::Service;

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Request>` and `Stream<Item = Response>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Client<T, E>
where
    T: Sink + Stream,
{
    requests: VecDeque<T::SinkItem>,
    responses: VecDeque<oneshot::Sender<T::Item>>,
    transport: T,

    in_flight: usize,
    finish: bool,

    #[allow(unused)]
    error: PhantomData<E>,
}

/// A factory that makes new [`Client`] instances by creating new transports and wrapping them in
/// fresh `Client`s.
pub struct Maker<NT, Request> {
    t_maker: NT,
    _req: PhantomData<Request>,
    in_flight: Option<usize>,
}

impl<NT, Request> Maker<NT, Request> {
    /// Make a new `Client` factory that uses the given transport factory.
    pub fn new(t: NT) -> Self {
        Maker {
            t_maker: t,
            _req: PhantomData,
            in_flight: None,
        }
    }

    /// Limit each new `Client` instance to `in_flight` pending requests.
    pub fn with_limit(mut self, in_flight: usize) -> Self {
        self.in_flight = Some(in_flight);
        self
    }
}

/// A `Future` that will resolve into a `Buffer<Client<T::Transport>>`.
pub struct NewSpawnedClientFuture<NT, Target, Request>
where
    NT: MakeTransport<Target, Request>,
{
    maker: Option<NT::Future>,
}

/// A failure to spawn a new `Client`.
#[derive(Debug)]
pub enum SpawnError<E> {
    /// The executor failed to spawn the `tower_buffer::Worker`.
    SpawnFailed,

    /// A new `Transport` could not be produced.
    Inner(E),
}

impl<NT, Target, Request> Future for NewSpawnedClientFuture<NT, Target, Request>
where
    NT: MakeTransport<Target, Request>,
    NT::Transport: 'static + Send,
    <NT::Transport as Sink>::SinkItem: 'static + Send,
    <NT::Transport as Stream>::Item: 'static + Send,
    <NT::Transport as Sink>::SinkError: 'static + Send + Sync,
    <NT::Transport as Stream>::Error: 'static + Send + Sync,
{
    type Item = Buffer<DirectServiceRef<Client<NT::Transport, Error<NT::Transport>>>, Request>;
    type Error = SpawnError<NT::MakeError>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.maker.take() {
            None => unreachable!("poll called after future resolved"),
            Some(mut fut) => match fut.poll().map_err(SpawnError::Inner)? {
                Async::Ready(t) => {
                    let c = Client::new(t);

                    Ok(Async::Ready(
                        Buffer::new_direct(c, 0, &DefaultExecutor::current())
                            .map_err(|_| SpawnError::SpawnFailed)?,
                    ))
                }
                Async::NotReady => {
                    self.maker = Some(fut);
                    Ok(Async::NotReady)
                }
            },
        }
    }
}

impl<NT, Target, Request> Service<Target> for Maker<NT, Request>
where
    NT: MakeTransport<Target, Request>,
    NT::Transport: 'static + Send,
    Request: 'static + Send,
    <NT::Transport as Stream>::Item: 'static + Send,
    <NT::Transport as Sink>::SinkError: 'static + Send + Sync,
    <NT::Transport as Stream>::Error: 'static + Send + Sync,
{
    type Error = SpawnError<NT::MakeError>;
    type Response = Buffer<DirectServiceRef<Client<NT::Transport, Error<NT::Transport>>>, Request>;
    type Future = NewSpawnedClientFuture<NT, Target, Request>;

    fn call(&mut self, target: Target) -> Self::Future {
        NewSpawnedClientFuture {
            maker: Some(self.t_maker.make_transport(target)),
        }
    }

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.t_maker.poll_ready().map_err(SpawnError::Inner)
    }
}

/// An error that occurred while servicing a request.
pub enum Error<T>
where
    T: Sink + Stream,
{
    /// The underlying transport failed to send a request.
    BrokenTransportSend(T::SinkError),

    /// The underlying transport failed while attempting to receive a response.
    ///
    /// If `None`, the transport closed without error while there were pending requests.
    BrokenTransportRecv(Option<T::Error>),

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
    T::SinkError: fmt::Display,
    T::Error: fmt::Display,
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
    T::SinkError: fmt::Debug,
    T::Error: fmt::Debug,
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
    T::SinkError: error::Error,
    T::Error: error::Error,
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
    fn from_sink_error(e: T::SinkError) -> Self {
        Error::BrokenTransportSend(e)
    }

    fn from_stream_error(e: T::Error) -> Self {
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
            in_flight: 0,
            error: PhantomData::<E>,
            finish: false,
        }
    }
}

impl<T, E> DirectService<T::SinkItem> for Client<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
    E: 'static + Send,
    T::SinkItem: 'static + Send,
    T::Item: 'static + Send,
{
    type Response = T::Item;
    type Error = E;

    // TODO: get rid of Box + Send bound here by using existential types
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Result<Async<()>, Self::Error> {
        // NOTE: it'd be great if we could poll_ready the Sink, but alas..
        return Ok(Async::Ready(()));
    }

    fn poll_service(&mut self) -> Result<Async<()>, Self::Error> {
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

            if self.in_flight != 0 {
                // flush out any stuff we've sent in the past
                // don't return on NotReady since we have to check for responses too
                if self.finish && self.requests.is_empty() {
                    // we're closing up shop!
                    //
                    // note that the check for requests.is_empty() is necessary, because
                    // Sink::close() requires that we never call start_send ever again!
                    //
                    // close() implies poll_complete()
                    //
                    // FIXME: if close returns Ready, are we allowed to call close again?
                    self.transport.close().map_err(Error::from_sink_error)?;
                } else {
                    self.transport
                        .poll_complete()
                        .map_err(Error::from_sink_error)?;
                }
            }

            // and start looking for replies.
            //
            // note that we *could* have this just be a loop, but we don't want to poll the stream
            // if we know there's nothing for it to produce.
            while self.in_flight != 0 {
                match try_ready!(self.transport.poll().map_err(Error::from_stream_error)) {
                    Some(r) => {
                        // ignore send failures
                        // the client may just no longer care about the response
                        let sender = self
                            .responses
                            .pop_front()
                            .expect("got a request with no sender?");
                        let _ = sender.send(r);
                        self.in_flight -= 1;
                    }
                    None => {
                        // the transport terminated while we were waiting for a response!
                        // TODO: it'd be nice if we could return the transport here..
                        return Err(E::from(Error::BrokenTransportRecv(None)));
                    }
                }
            }

            if self.requests.is_empty() && self.in_flight == 0 {
                if self.finish {
                    // we're completely done once close() finishes!
                    try_ready!(self.transport.close().map_err(Error::from_sink_error));
                }
                return Ok(Async::Ready(()));
            }
        }
    }

    fn poll_close(&mut self) -> Result<Async<()>, Self::Error> {
        self.finish = true;
        self.poll_service()
    }

    fn call(&mut self, req: T::SinkItem) -> Self::Future {
        assert!(!self.finish, "invoked call() after poll_close()");

        let (tx, rx) = oneshot::channel();
        self.requests.push_back(req);
        self.responses.push_back(tx);
        Box::new(rx.map_err(|_| E::from(Error::ClientDropped)))
    }
}

// ===== impl SpawnError =====

impl<T> fmt::Display for SpawnError<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SpawnError::SpawnFailed => write!(f, "error spawning multiplex client"),
            SpawnError::Inner(ref te) => {
                write!(f, "error making new multiplex transport: {:?}", te)
            }
        }
    }
}

impl<T> error::Error for SpawnError<T>
where
    T: error::Error,
{
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            SpawnError::SpawnFailed => None,
            SpawnError::Inner(ref te) => Some(te),
        }
    }

    fn description(&self) -> &str {
        "error creating new multiplex client"
    }
}
