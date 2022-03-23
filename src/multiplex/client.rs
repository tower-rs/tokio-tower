use crate::mediator;
use crate::mediator::TrySendError;
use crate::wrappers::*;
use crate::Error;
use crate::MakeTransport;
use futures_core::{ready, stream::TryStream};
use futures_sink::Sink;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{atomic, Arc};
use std::task::{Context, Poll};
use std::{error, fmt};
use tower_service::Service;

// NOTE: this implementation could be more opinionated about request IDs by using a slab, but
// instead, we allow the user to choose their own identifier format.

/// A transport capable of transporting tagged requests and responses must implement this
/// interface in order to be used with a [`Client`].
///
/// Note that we require self to be pinned here as `assign_tag` and `finish_tag` are called on the
/// transport, which is already pinned so that we can use it as a `Stream + Sink`. It wouldn't be
/// safe to then give out `&mut` to the transport without `Pin`, as that might move the transport.
pub trait TagStore<Request, Response> {
    /// The type used for tags.
    type Tag: Eq;

    /// Assign a fresh tag to the given `Request`, and return that tag.
    fn assign_tag(self: Pin<&mut Self>, r: &mut Request) -> Self::Tag;

    /// Retire and return the tag contained in the given `Response`.
    fn finish_tag(self: Pin<&mut Self>, r: &Response) -> Self::Tag;
}

/// A store used to track pending requests.
///
/// Each request that is sent will get its pending type representation
/// passed into `sent` and when the client has received the completed
/// request it will provide that tag for the pending store to look up.
/// This allows the ability to customize the data structure used to track
/// in-flight requests.
pub trait PendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
{
    /// Store the provided pending request.
    fn sent(self: Pin<&mut Self>, pending: Pending<T::Tag, T::Ok>);

    /// Retrive the pending request associated with this tag.
    ///
    /// Returning `Ok(None)` will indicate that the pending request could not
    /// be found in the pending store and for the client future to continue
    /// processing other responses. If you would like to terminate the client
    /// on a pending tag that doesn't exist the `Error::Desynchronized` error
    /// can be returned.
    fn completed(
        self: Pin<&mut Self>,
        res: T::Tag,
    ) -> Result<Option<Pending<T::Tag, T::Ok>>, Error<T, Request>>;
}

/// A [`PendingStore`] implementation that uses a [`VecDeque`]
/// to store pending requests.
#[pin_project]
pub struct VecDequePendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
{
    pending: VecDeque<Pending<T::Tag, T::Ok>>,
    _pd: PhantomData<fn((T, Request))>,
}

impl<T, Request> Default for VecDequePendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
{
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            _pd: PhantomData,
        }
    }
}

impl<T, Request> fmt::Debug for VecDequePendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
    T::Tag: fmt::Debug,
    T::Ok: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecDequePendingStore")
            .field("pending", &self.pending)
            .finish()
    }
}

impl<T, Request> PendingStore<T, Request> for VecDequePendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
{
    fn sent(self: Pin<&mut Self>, pending: Pending<T::Tag, T::Ok>) {
        let this = self.project();
        this.pending.push_back(pending);
    }

    fn completed(
        self: Pin<&mut Self>,
        tag: T::Tag,
    ) -> Result<Option<Pending<T::Tag, T::Ok>>, Error<T, Request>> {
        let id = tag;
        let this = self.project();

        let pending = this
            .pending
            .iter()
            .position(|&Pending { ref tag, .. }| tag == &id)
            .ok_or(Error::Desynchronized)?;

        // this request just finished, which means it's _probably_ near the front
        // (i.e., was issued a while ago). so, for the swap needed for efficient
        // remove, we want to swap with something else that is close to the front.
        let response = this.pending.swap_remove_front(pending).unwrap();

        Ok(response.into())
    }
}

/// A factory that makes new [`Client`] instances by creating new transports and wrapping them in
/// fresh `Client`s.
pub struct Maker<NT, P, Request> {
    t_maker: NT,
    _req: PhantomData<fn(P, Request)>,
}

impl<NT, P, Request> fmt::Debug for Maker<NT, P, Request>
where
    NT: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Maker")
            .field("t_maker", &self.t_maker)
            .finish()
    }
}

impl<NT, P, Request> Maker<NT, P, Request> {
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

impl<NT, P, Target, Request> Service<Target> for Maker<NT, P, Request>
where
    NT: MakeTransport<Target, Request>,
    NT::Transport: 'static + Send + TagStore<Request, NT::Item>,
    <NT::Transport as TagStore<Request, NT::Item>>::Tag: 'static + Send,
    P: PendingStore<NT::Transport, Request> + Default + Send + 'static,
    Request: 'static + Send,
    NT::Item: 'static + Send,
    NT::SinkError: 'static + Send + Sync,
    NT::Error: 'static + Send + Sync,
    NT::Future: 'static + Send,
{
    type Error = SpawnError<NT::MakeError>;
    type Response = Client<NT::Transport, Error<NT::Transport, Request>, Request, P>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, target: Target) -> Self::Future {
        let maker = self.t_maker.make_transport(target);
        Box::pin(async move {
            let transport = maker.await.map_err(SpawnError::Inner)?;
            Ok(Client::new(transport, P::default(), |_| {}))
        })
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.t_maker.poll_ready(cx).map_err(SpawnError::Inner)
    }
}

impl<NT, P, Request> tower::load::Load for Maker<NT, P, Request> {
    type Metric = u8;

    fn load(&self) -> Self::Metric {
        0
    }
}

// ===== Client =====

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Request>` and `Stream<Item = Response>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Client<T, E, Request, P = VecDequePendingStore<T, Request>>
where
    T: Sink<Request> + TryStream,
{
    mediator: mediator::Sender<ClientRequest<T, Request>>,
    in_flight: Arc<atomic::AtomicUsize>,
    _error: PhantomData<fn(P, E)>,
}

impl<T, E, Request, P> fmt::Debug for Client<T, E, Request, P>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok>,
    P: PendingStore<T, Request>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("mediator", &self.mediator)
            .field("in_flight", &self.in_flight)
            .finish()
    }
}

// ===== Pending =====

/// A pending response used to track in-flight requests.
///
/// Each pending response contains an associated `Tag` that is provided
/// from the `TagStore` used to uniquely identify a request/response pair.
pub struct Pending<Tag, Response> {
    tag: Tag,
    tx: tokio::sync::oneshot::Sender<ClientResponse<Response>>,
    span: tracing::Span,
}

impl<Tag, Response> Pending<Tag, Response> {
    /// Get the tag associated with this pending request.
    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    /// Get the span associated with this pending request.
    pub fn span(&self) -> &tracing::Span {
        &self.span
    }
}

impl<Tag, Response> fmt::Debug for Pending<Tag, Response>
where
    Tag: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pending")
            .field("tag", &self.tag)
            .field("span", &self.span)
            .finish()
    }
}

// ===== Builder =====

/// Builder for [`Client`] this is used to configure the transport, pending store
/// and service_handler.
///
/// # Defaults
///
/// By default this builder only requires a transport and sets a default pending store and
/// error handler. The default pending store is just a [`VecDeque`] and the default
/// error handler is just an empty closure.
pub struct Builder<T, E, Request, P = VecDequePendingStore<T, Request>> {
    transport: T,
    on_service_error: Box<dyn FnOnce(E) + Send>,
    pending_store: P,
    _pd: PhantomData<fn(Request, E)>,
}

impl<T, E, Request, P> Builder<T, E, Request, P>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok> + Send + 'static,
    P: PendingStore<T, Request> + Send + 'static,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
    T::Tag: Send,
{
    fn new(transport: T) -> Builder<T, E, Request, VecDequePendingStore<T, Request>> {
        Builder {
            transport,
            on_service_error: Box::new(|_| {}),
            pending_store: VecDequePendingStore::default(),
            _pd: PhantomData,
        }
    }

    /// Set the provided pending store.
    pub fn pending_store<P2>(self, pending_store: P2) -> Builder<T, E, Request, P2> {
        Builder {
            pending_store,
            on_service_error: self.on_service_error,
            transport: self.transport,
            _pd: PhantomData,
        }
    }

    /// Set the provided service error handler.
    pub fn on_service_error<F>(self, on_service_error: F) -> Builder<T, E, Request, P>
    where
        F: FnOnce(E) + Send + 'static,
    {
        Builder {
            on_service_error: Box::new(on_service_error),
            pending_store: self.pending_store,
            transport: self.transport,
            _pd: PhantomData,
        }
    }

    /// Build a client based on the configured items on the builder.
    pub fn build(self) -> Client<T, E, Request, P> {
        Client::new(self.transport, self.pending_store, self.on_service_error)
    }
}

impl<T, E, Request, P> fmt::Debug for Builder<T, E, Request, P>
where
    T: fmt::Debug,
    P: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("transport", &self.transport)
            .field("pending_store", &self.pending_store)
            .finish()
    }
}

// ===== ClientInner =====

#[pin_project]
struct ClientInner<T, P, E, Request>
where
    T: Sink<Request> + TryStream + TagStore<Request, T::Ok>,
    P: PendingStore<T, Request>,
{
    mediator: mediator::Receiver<ClientRequest<T, Request>>,
    #[pin]
    pending: P,
    #[pin]
    transport: T,

    in_flight: Arc<atomic::AtomicUsize>,
    finish: bool,
    rx_only: bool,

    #[allow(unused)]
    error: PhantomData<fn(E)>,
}

impl<T, E, Request> Client<T, E, Request>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok> + Send + 'static,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
    T::Tag: Send,
{
    /// Create a new builder with the provided transport.
    pub fn builder(transport: T) -> Builder<T, E, Request> {
        Builder::<_, _, _, VecDequePendingStore<T, Request>>::new(transport)
    }
}

impl<T, E, Request, P> Client<T, E, Request, P>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok> + Send + 'static,
    P: PendingStore<T, Request> + Send + 'static,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
    T::Tag: Send,
{
    fn new<F>(transport: T, pending: P, on_service_error: F) -> Self
    where
        F: FnOnce(E) + Send + 'static,
    {
        let (tx, rx) = mediator::new();
        let in_flight = Arc::new(atomic::AtomicUsize::new(0));
        tokio::spawn({
            let c = ClientInner {
                mediator: rx,
                transport,
                pending,
                in_flight: in_flight.clone(),
                error: PhantomData::<fn(E)>,
                finish: false,
                rx_only: false,
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

impl<T, P, E, Request> Future for ClientInner<T, P, E, Request>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok>,
    P: PendingStore<T, Request>,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
{
    type Output = Result<(), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // go through the deref so we can do partial borrows
        let this = self.project();

        // we never move transport, nor do we ever hand out &mut to it
        let mut transport: Pin<_> = this.transport;
        let mut pending: Pin<_> = this.pending;

        // track how many times we have iterated
        let mut i = 0;

        if !*this.finish {
            while let Poll::Ready(r) = transport.as_mut().poll_ready(cx) {
                if let Err(e) = r {
                    return Poll::Ready(Err(E::from(Error::from_sink_error(e))));
                }

                // send more requests if we have them
                match this.mediator.try_recv(cx) {
                    Poll::Ready(Some(ClientRequest {
                        mut req,
                        span: _span,
                        res,
                    })) => {
                        let id = transport.as_mut().assign_tag(&mut req);

                        let guard = _span.enter();
                        tracing::trace!("request received by worker; sending to Sink");

                        transport
                            .as_mut()
                            .start_send(req)
                            .map_err(Error::from_sink_error)?;
                        tracing::trace!("request sent");
                        drop(guard);

                        pending.as_mut().sent(Pending {
                            tag: id,
                            tx: res,
                            span: _span,
                        });
                        this.in_flight.fetch_add(1, atomic::Ordering::AcqRel);

                        // if we have run for a while without yielding, yield so we can make progress
                        i += 1;
                        if i == crate::YIELD_EVERY {
                            // we're forcing a yield, so need to ensure we get woken up again
                            cx.waker().wake_by_ref();
                            // we still want to execute the code below the loop
                            break;
                        }
                    }
                    Poll::Ready(None) => {
                        // XXX: should we "give up" the Sink::poll_ready here?
                        *this.finish = true;
                        break;
                    }
                    Poll::Pending => {
                        // XXX: should we "give up" the Sink::poll_ready here?
                        break;
                    }
                }
            }
        }

        if this.in_flight.load(atomic::Ordering::Acquire) != 0 && !*this.rx_only {
            // flush out any stuff we've sent in the past
            // don't return on NotReady since we have to check for responses too
            if *this.finish {
                // we're closing up shop!
                //
                // poll_close() implies poll_flush()
                let r = transport
                    .as_mut()
                    .poll_close(cx)
                    .map_err(Error::from_sink_error)?;

                if r.is_ready() {
                    // now that close has completed, we should never send anything again
                    // we only need to receive to make the in-flight requests complete
                    *this.rx_only = true;
                }
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
                    let pending = match pending.as_mut().completed(id)? {
                        Some(pending) => pending,
                        None => {
                            tracing::trace!(
                                "response arrived but no associated pending tag; ignoring response"
                            );
                            continue;
                        }
                    };

                    tracing::trace!(parent: &pending.span, "response arrived; forwarding");

                    // ignore send failures
                    // the client may just no longer care about the response
                    let sender = pending.tx;
                    let _ = sender.send(ClientResponse {
                        response: r,
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

        if *this.finish && this.in_flight.load(atomic::Ordering::Acquire) == 0 {
            if *this.rx_only {
                // we have already closed the send side.
            } else {
                // we're completely done once close() finishes!
                ready!(transport.poll_close(cx)).map_err(Error::from_sink_error)?;
            }
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

impl<T, E, Request, P> Service<Request> for Client<T, E, Request, P>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok>,
    P: PendingStore<T, Request>,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T: 'static,
    T::Ok: 'static + Send,
{
    type Response = T::Ok;
    type Error = E;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        Poll::Ready(ready!(self.mediator.poll_ready(cx)).map_err(|_| E::from(Error::ClientDropped)))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let span = tracing::Span::current();
        tracing::trace!("issuing request");
        let req = ClientRequest { req, span, res: tx };
        let r = self.mediator.try_send(req);
        Box::pin(async move {
            match r {
                Ok(()) => match rx.await {
                    Ok(r) => {
                        tracing::trace!(parent: &r.span, "response returned");
                        Ok(r.response)
                    }
                    Err(_) => Err(E::from(Error::ClientDropped)),
                },
                Err(TrySendError::Pending(_)) => Err(E::from(Error::TransportFull)),
                Err(TrySendError::Closed(_)) => Err(E::from(Error::ClientDropped)),
            }
        })
    }
}

impl<T, E, Request, P> tower::load::Load for Client<T, E, Request, P>
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SpawnError::SpawnFailed => f.pad("error spawning multiplex client"),
            SpawnError::Inner(_) => f.pad("error making new multiplex transport"),
        }
    }
}

impl<T> error::Error for SpawnError<T>
where
    T: error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            SpawnError::SpawnFailed => None,
            SpawnError::Inner(ref te) => Some(te),
        }
    }
}
