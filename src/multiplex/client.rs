use crate::mediator;
use crate::wrappers::*;
use crate::Error;
use crate::MakeTransport;
use futures_core::{
    future::Future,
    ready,
    stream::TryStream,
    task::{Context, Poll},
};
use futures_sink::Sink;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{atomic, Arc};
use std::{error, fmt};
use tower_service::Service;

#[cfg(feature = "tracing")]
use tracing::Level;

// NOTE: this implementation could be more opinionated about request IDs by using a slab, but
// instead, we allow the user to choose their own identifier format.

/// A transport capable of transporting tagged requests and responses must implement this
/// interface in order to be used with a [`Client`].
pub trait TagStore<Request, Response> {
    /// The type used for tags.
    type Tag: Eq;

    /// Assign a fresh tag to the given `Request`, and return that tag.
    fn assign_tag(self: Pin<&mut Self>, r: &mut Request) -> Self::Tag;

    /// Retire and return the tag contained in the given `Response`.
    fn finish_tag(self: Pin<&mut Self>, r: &Response) -> Self::Tag;
}

/// A factory that makes new [`Client`] instances by creating new transports and wrapping them in
/// fresh `Client`s.
pub struct Maker<NT, Request> {
    t_maker: NT,
    _req: PhantomData<fn(Request)>,
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

/// A failure to spawn a new `Client`.
#[derive(Debug)]
pub enum SpawnError<E> {
    /// The executor failed to spawn the `tower_buffer::Worker`.
    SpawnFailed,

    /// A new transport could not be produced.
    Inner(E),
}

impl<NT, Target, Request> Service<Target> for Maker<NT, Request>
where
    NT: MakeTransport<Target, Request>,
    NT::Transport: 'static + Send + TagStore<Request, NT::Item>,
    <NT::Transport as TagStore<Request, NT::Item>>::Tag: 'static + Send,
    Request: 'static + Send,
    NT::Item: 'static + Send,
    NT::SinkError: 'static + Send + Sync,
    NT::Error: 'static + Send + Sync,
    NT::Future: 'static,
{
    type Error = SpawnError<NT::MakeError>;
    type Response = Client<NT::Transport, Error<NT::Transport, Request>, Request>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn call(&mut self, target: Target) -> Self::Future {
        let maker = self.t_maker.make_transport(target);
        Box::pin(async move { Ok(Client::new(maker.await.map_err(SpawnError::Inner)?)) })
    }

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.t_maker.poll_ready(cx).map_err(SpawnError::Inner)
    }
}

impl<NT, Request> tower_load::Load for Maker<NT, Request> {
    type Metric = u8;

    fn load(&self) -> Self::Metric {
        0
    }
}

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Request>` and `Stream<Item = Response>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Client<T, E, Request>
where
    T: Sink<Request> + TryStream,
{
    mediator: mediator::Sender<ClientRequest<T, Request>>,
    in_flight: Arc<atomic::AtomicUsize>,
    _error: PhantomData<fn(E)>,
}

struct Pending<Tag, Item> {
    tag: Tag,
    tx: tokio_sync::oneshot::Sender<ClientResponse<Item>>,
    #[cfg(feature = "tracing")]
    span: tracing::Span,
}

struct ClientInner<T, E, Request>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok>,
{
    mediator: mediator::Receiver<ClientRequest<T, Request>>,
    responses: VecDeque<Pending<T::Tag, T::Ok>>,
    transport: T,

    in_flight: Arc<atomic::AtomicUsize>,
    finish: bool,

    #[allow(unused)]
    error: PhantomData<fn(E)>,
}

impl<T, E, Request> Unpin for ClientInner<T, E, Request> where T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok> {}

impl<T, E, Request> Client<T, E, Request>
where
    T: Sink<Request>
        + TryStream
        + TagStore<Request, <T as TryStream>::Ok>
        + Send
        + 'static,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
    T::Tag: Send,
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
        let in_flight = Arc::new(atomic::AtomicUsize::new(0));
        tokio_executor::spawn({
            let c = ClientInner {
                mediator: rx,
                responses: Default::default(),
                transport,
                in_flight: in_flight.clone(),
                error: PhantomData::<fn(E)>,
                finish: false,
            };
            async move {
                if let Err(e) = c.await {
                    on_service_error(e);
                }
            }
        });
        Client {
            mediator: tx,
            in_flight,
            _error: PhantomData,
        }
    }
}

impl<T, E, Request> Future for ClientInner<T, E, Request>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok>,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
{
    type Output = Result<(), E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // go through the deref so we can do partial borrows
        let this = &mut *self;

        // we never move transport, nor do we ever hand out &mut to it
        let mut transport = unsafe { Pin::new_unchecked(&mut this.transport) };

        while let Poll::Ready(r) = transport.as_mut().poll_ready(cx) {
            if let Err(e) = r {
                return Poll::Ready(Err(E::from(Error::from_sink_error(e))));
            }

            // send more requests if we have them
            match this.mediator.try_recv(cx) {
                Poll::Ready(Some(ClientRequest { mut req, span, res })) => {
                    let id = transport.as_mut().assign_tag(&mut req);

                    #[cfg(feature = "tracing")]
                    let guard = span.enter();
                    #[cfg(feature = "tracing")]
                    tracing::event!(Level::TRACE, "request received by worker; sending to Sink");

                    transport
                        .as_mut()
                        .start_send(req)
                        .map_err(Error::from_sink_error)?;
                    tracing::event!(Level::TRACE, "request sent");
                    drop(guard);

                    this.responses.push_back(Pending {
                        tag: id,
                        tx: res,
                        #[cfg(feature = "tracing")]
                        span,
                    });
                    this.in_flight.fetch_add(1, atomic::Ordering::AcqRel);
                }
                Poll::Ready(None) => {
                    // XXX: should we "give up" the Sink::poll_ready here?
                    this.finish = true;
                    break;
                }
                Poll::Pending => {
                    // XXX: should we "give up" the Sink::poll_ready here?
                    break;
                }
            }
        }

        if this.in_flight.load(atomic::Ordering::Acquire) != 0 {
            // flush out any stuff we've sent in the past
            // don't return on NotReady since we have to check for responses too
            if this.finish {
                // we're closing up shop!
                //
                // poll_close() implies poll_flush()
                //
                // FIXME: if close returns Ready, are we allowed to call close again?
                let _ = transport
                    .as_mut()
                    .poll_close(cx)
                    .map_err(Error::from_sink_error)?;
            } else {
                let _ = transport
                    .as_mut()
                    .poll_flush(cx)
                    .map_err(Error::from_sink_error)?;
            }
        }

        // and start looking for replies.
        //
        // note that we *could* have this just be a loop, but we don't want to poll the stream
        // if we know there's nothing for it to produce.
        while this.in_flight.load(atomic::Ordering::Acquire) != 0 {
            match ready!(transport.as_mut().try_poll_next(cx))
                .transpose()
                .map_err(Error::from_stream_error)?
            {
                Some(r) => {
                    // find the appropriate response channel.
                    // note that we do a _linear_ scan of the identifiers. this saves us from
                    // keeping a HashMap around, and is _usually_ fast as long as the requests
                    // that have been pending the longest are most likely to complete next.
                    let id = transport.as_mut().finish_tag(&r);
                    let pending = this
                        .responses
                        .iter()
                        .position(|&Pending { ref tag, .. }| tag == &id)
                        .expect("got a request with no sender?");

                    // this request just finished, which means it's _probably_ near the front
                    // (i.e., was issued a while ago). so, for the swap needed for efficient
                    // remove, we want to swap with something else that is close to the front.
                    let pending = this.responses.swap_remove_front(pending).unwrap();
                    event!(pending.span, Level::TRACE, "response arrived; forwarding");

                    // ignore send failures
                    // the client may just no longer care about the response
                    let sender = pending.tx;
                    let _ = sender.send(ClientResponse {
                        response: r,
                        #[cfg(feature = "tracing")]
                        span: pending.span,
                    });
                    this.in_flight.fetch_sub(1, atomic::Ordering::AcqRel);
                }
                None => {
                    // the transport terminated while we were waiting for a response!
                    // TODO: it'd be nice if we could return the transport here..
                    return Poll::Ready(Err(E::from(Error::BrokenTransportRecv(None))));
                }
            }
        }

        if this.finish && this.in_flight.load(atomic::Ordering::Acquire) == 0 {
            // we're completely done once close() finishes!
            ready!(transport.poll_close(cx)).map_err(Error::from_sink_error)?;
            return Poll::Ready(Ok(()));
        }

        // to get here, we must have no requests in flight and have gotten a NotReady from
        // self.mediator.try_recv or self.transport.start_send. we *could* also have messages
        // waiting to be sent (transport.poll_complete), but if that's the case it must also have
        // returned NotReady. so, at this point, we know that all of our subtasks are either done
        // or have returned NotReady, so the right thing for us to do is return NotReady too!
        Poll::Pending
    }
}

impl<T, E, Request> Service<Request> for Client<T, E, Request>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok>,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T: 'static,
    T::Ok: 'static + Send,
{
    type Response = T::Ok;
    type Error = E;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), E>> {
        Poll::Ready(ready!(self.mediator.poll_ready(cx)).map_err(|_| E::from(Error::ClientDropped)))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (tx, rx) = tokio_sync::oneshot::channel();
        #[cfg(feature = "tracing")]
        let span = tracing::Span::current();
        #[cfg(not(feature = "tracing"))]
        let span = ();
        event!(span, Level::TRACE, "issuing request");
        let req = ClientRequest { req, span, res: tx };
        let r = self.mediator.try_send(req);
        Box::pin(async move {
            match r {
                Ok(()) => match rx.await {
                    Ok(r) => {
                        // TODO: provide a variant that lets you get at the span too
                        event!(r.span, tracing::Level::TRACE, "response returned");
                        Ok(r.response)
                    }
                    Err(_) => Err(E::from(Error::ClientDropped)),
                },
                Err(_) => Err(E::from(Error::TransportFull)),
            }
        })
    }
}

impl<T, E, Request> tower_load::Load for Client<T, E, Request>
where
    T: Sink<Request> + TryStream,
{
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        self.in_flight.load(atomic::Ordering::Acquire)
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
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            SpawnError::SpawnFailed => None,
            SpawnError::Inner(ref te) => Some(te),
        }
    }

    fn description(&self) -> &str {
        "error creating new multiplex client"
    }
}
