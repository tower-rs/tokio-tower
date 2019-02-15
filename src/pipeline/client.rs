use crate::mediator;
use crate::ClientRequest;
use crate::MakeTransport;
use futures::{future, Async, AsyncSink, Future, Poll, Sink, Stream};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::{error, fmt};
use tower_service::Service;

/// A factory that makes new [`Client`] instances by creating new transports and wrapping them in
/// fresh `Client`s.
pub struct Maker<NT, Request> {
    t_maker: NT,
    _req: PhantomData<Request>,
}

impl<NT, Request> Maker<NT, Request> {
    /// Make a new `Client` factory that uses the given `MakeTransport` factory.
    pub fn new(t: NT) -> Self {
        Maker {
            t_maker: t,
            _req: PhantomData,
        }
    }

    // NOTE: it'd be *great* if the user had a way to specify a service error handler for all
    // spawned services, but without https://github.com/rust-lang/rust/pull/49224 or
    // https://github.com/rust-lang/rust/issues/29625 that's pretty tricky (unless we're willing to
    // require Fn + Clone)
}

/// A `Future` that will resolve into a `Client<T::Transport>`.
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

    /// A new transport could not be produced.
    Inner(E),
}

impl<NT, Target, Request> Future for NewSpawnedClientFuture<NT, Target, Request>
where
    NT: MakeTransport<Target, Request>,
    NT::Transport: 'static + Send,
    Request: 'static + Send,
    NT::Item: 'static + Send,
    NT::SinkError: 'static + Send + Sync,
    NT::Error: 'static + Send + Sync,
{
    type Item = Client<NT::Transport, Error<NT::Transport>>;
    type Error = SpawnError<NT::MakeError>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.maker.take() {
            None => unreachable!("poll called after future resolved"),
            Some(mut fut) => match fut.poll().map_err(SpawnError::Inner)? {
                Async::Ready(t) => Ok(Async::Ready(Client::new(t))),
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
    NT::Item: 'static + Send,
    NT::SinkError: 'static + Send + Sync,
    NT::Error: 'static + Send + Sync,
{
    type Error = SpawnError<NT::MakeError>;
    type Response = Client<NT::Transport, Error<NT::Transport>>;
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

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Request>` and `Stream<Item = Response>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Client<T, E>
where
    T: Sink + Stream,
{
    mediator: mediator::Sender<ClientRequest<T>>,
    _error: PhantomData<E>,
}

struct ClientInner<T, E>
where
    T: Sink + Stream,
{
    mediator: mediator::Receiver<ClientRequest<T>>,
    responses: VecDeque<tokio_sync::oneshot::Sender<T::Item>>,
    waiting: Option<ClientRequest<T>>,
    transport: T,

    in_flight: usize,
    finish: bool,

    #[allow(unused)]
    error: PhantomData<E>,
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
    T: Sink + Stream + Send + 'static,
    E: From<Error<T>>,
    E: 'static + Send,
    T::SinkItem: 'static + Send,
    T::Item: 'static + Send,
{
    /// Construct a new [`Client`] over the given `transport`.
    ///
    /// If the Client errors, the error is dropped when `new` is used -- use `with_error_handler`
    /// to handle such an error explicitly.
    pub fn new(transport: T) -> Self where {
        Self::with_error_handler(transport, |_| {})
    }

    /// Construct a new [`Client`] over the given `transport`.
    ///
    /// If the `Client` errors, its error is passed to `on_service_error`.
    pub fn with_error_handler<F>(transport: T, on_service_error: F) -> Self
    where
        F: FnOnce(E) + Send + 'static,
    {
        let (tx, rx) = mediator::new();
        tokio_executor::spawn(
            ClientInner {
                mediator: rx,
                responses: Default::default(),
                waiting: None,
                transport,
                in_flight: 0,
                error: PhantomData::<E>,
                finish: false,
            }
            .map_err(move |e| on_service_error(e)),
        );
        Client {
            mediator: tx,
            _error: PhantomData,
        }
    }
}

impl<T, E> Future for ClientInner<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
    E: 'static + Send,
    T::SinkItem: 'static + Send,
    T::Item: 'static + Send,
{
    type Item = ();
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // if Stream had a poll_ready, we could call that here to
        // make sure there's room for at least one more request

        if let Some(ClientRequest { req, res }) = self.waiting.take() {
            if let AsyncSink::NotReady(req) = self
                .transport
                .start_send(req)
                .map_err(Error::from_sink_error)?
            {
                self.waiting = Some(ClientRequest { req, res });
            } else {
                self.responses.push_back(res);
                self.in_flight += 1;
            }
        }

        while self.waiting.is_none() {
            // send more requests if we have them
            match self.mediator.try_recv() {
                Async::Ready(Some(ClientRequest { req, res })) => {
                    if let AsyncSink::NotReady(req) = self
                        .transport
                        .start_send(req)
                        .map_err(Error::from_sink_error)?
                    {
                        assert!(self.waiting.is_none());
                        self.waiting = Some(ClientRequest { req, res });
                    } else {
                        self.responses.push_back(res);
                        self.in_flight += 1;
                    }
                }
                Async::Ready(None) => {
                    self.finish = true;
                    break;
                }
                Async::NotReady => {
                    break;
                }
            }
        }

        if self.in_flight != 0 {
            // flush out any stuff we've sent in the past
            // don't return on NotReady since we have to check for responses too
            if self.finish && self.waiting.is_none() {
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

        if self.finish && self.waiting.is_none() && self.in_flight == 0 {
            // we're completely done once close() finishes!
            try_ready!(self.transport.close().map_err(Error::from_sink_error));
            return Ok(Async::Ready(()));
        }

        // to get here, we must have no requests in flight and have gotten a NotReady from
        // self.mediator.try_recv or self.transport.start_send. we *could* also have messages
        // waiting to be sent (transport.poll_complete), but if that's the case it must also have
        // returned NotReady. so, at this point, we know that all of our subtasks are either done
        // or have returned NotReady, so the right thing for us to do is return NotReady too!
        Ok(Async::NotReady)
    }
}

impl<T, E> Service<T::SinkItem> for Client<T, E>
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
        self.mediator
            .poll_ready()
            .map_err(|_| E::from(Error::ClientDropped))
    }

    fn call(&mut self, req: T::SinkItem) -> Self::Future {
        let (tx, rx) = tokio_sync::oneshot::channel();
        let req = ClientRequest { req: req, res: tx };
        match self.mediator.try_send(req) {
            Ok(AsyncSink::Ready) => Box::new(rx.map_err(|_| E::from(Error::ClientDropped))),
            Ok(AsyncSink::NotReady(_)) | Err(_) => {
                Box::new(future::err(E::from(Error::TransportFull)))
            }
        }
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
