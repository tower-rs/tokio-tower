use crate::mediator;
use crate::mediator::TrySendError;
use crate::wrappers::*;
use crate::Error;
use futures_core::{ready, stream::TryStream};
use futures_sink::Sink;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
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
    type Tag;

    /// Assign a fresh tag to the given `Request`, and return that tag.
    fn assign_tag(self: Pin<&mut Self>, r: &mut Request) -> Self::Tag;

    /// Retire and return the tag contained in the given `Response`.
    fn finish_tag(self: Pin<&mut Self>, r: &Response) -> Self::Tag;
}

/// A store used to track pending requests.
///
/// Each request that is `sent` is passed to the local state used to track
/// each pending request, and is expected to be able to recall that state
/// through `completed` when a response later comes in with the same tag as
/// the original request.
pub trait PendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
{
    /// Store the provided tag and pending request.
    fn sent(self: Pin<&mut Self>, tag: T::Tag, pending: Pending<T::Ok>, transport: Pin<&mut T>);

    /// Retrieve the pending request associated with this tag.
    ///
    /// This method should return `Ok(Some(p))` where `p` is the [`Pending`]
    /// that was passed to `sent` with `tag`. Implementors can choose
    /// to ignore a given response, such as to support request cancellation,
    /// by returning `Ok(None)` for a tag and dropping the corresponding
    /// `Pending` type. Doing so will make the original request future resolve as `Err(Error::Cancelled)`.
    ///
    /// If `tag` is not recognized as belonging to an in-flight request, implementors
    /// should return `Err(Error::Desynchronized)`.
    fn completed(
        self: Pin<&mut Self>,
        tag: T::Tag,
        transport: Pin<&mut T>,
    ) -> Result<Option<Pending<T::Ok>>, Error<T, Request>>;

    /// Return the count of in-flight pending responses in the [`PendingStore`].
    fn in_flight(&self, transport: &T) -> usize;
}

/// A [`PendingStore`] implementation that uses a [`VecDeque`]
/// to store pending requests.
///
/// When the [`Client`] recives a response with a `Tag` that does not
/// exist in the internal [`PendingStore`] this implementation will return
/// an `Error::Desynchronized` error.
#[pin_project]
pub struct VecDequePendingStore<T, Request>
where
    T: TryStream + Sink<Request> + TagStore<Request, T::Ok>,
{
    pending: VecDeque<(T::Tag, Pending<T::Ok>)>,
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
    T::Tag: Eq,
{
    fn sent(self: Pin<&mut Self>, tag: T::Tag, pending: Pending<T::Ok>, _transport: Pin<&mut T>) {
        let this = self.project();
        this.pending.push_back((tag, pending));
    }

    fn completed(
        self: Pin<&mut Self>,
        tag: T::Tag,
        _transport: Pin<&mut T>,
    ) -> Result<Option<Pending<T::Ok>>, Error<T, Request>> {
        let this = self.project();

        let pending = this
            .pending
            .iter()
            .position(|(t, _)| t == &tag)
            .ok_or(Error::Desynchronized)?;

        // this request just finished, which means it's _probably_ near the front
        // (i.e., was issued a while ago). so, for the swap needed for efficient
        // remove, we want to swap with something else that is close to the front.
        let response = this.pending.swap_remove_front(pending).unwrap();

        Ok(Some(response.1))
    }

    fn in_flight(&self, _transport: &T) -> usize {
        self.pending.len()
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
    _error: PhantomData<fn(P, E)>,
}

impl<T, E, Request, P> fmt::Debug for Client<T, E, Request, P>
where
    T: Sink<Request> + TryStream,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("mediator", &self.mediator)
            .finish()
    }
}

// ===== Pending =====

/// A type used to track in-flight requests.
///
/// Each pending response has an associated `Tag` that is provided
/// by the [`TagStore`], which is used to uniquely identify a request/response pair.
pub struct Pending<Response> {
    tx: tokio::sync::oneshot::Sender<ClientResponse<Response>>,
    span: tracing::Span,
}

impl<Response> fmt::Debug for Pending<Response> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pending").field("span", &self.span).finish()
    }
}

// ===== Builder =====

/// The default service error handler.
pub type DefaultOnServiceError<E> = Box<dyn FnOnce(E) + Send>;

/// Builder for [`Client`] this is used to configure the transport, pending store
/// and service_handler.
///
/// # Defaults
///
/// By default this builder only requires a transport and sets a default [`PendingStore`]
/// and error handler. The default pending store is a [`VecDeque`] and the default
/// error handler is just a closure that silently drops all errors.
pub struct Builder<
    T,
    E,
    Request,
    F = DefaultOnServiceError<E>,
    P = VecDequePendingStore<T, Request>,
> {
    transport: T,
    on_service_error: F,
    pending_store: P,
    _pd: PhantomData<fn(Request, E)>,
}

impl<T, E, Request, F, P> Builder<T, E, Request, F, P>
where
    T: Sink<Request> + TryStream + TagStore<Request, <T as TryStream>::Ok> + Send + 'static,
    P: PendingStore<T, Request> + Send + 'static,
    E: From<Error<T, Request>>,
    E: 'static + Send,
    Request: 'static + Send,
    T::Ok: 'static + Send,
    T::Tag: Send,
    F: FnOnce(E) + Send + 'static,
{
    fn new(
        transport: T,
    ) -> Builder<T, E, Request, DefaultOnServiceError<E>, VecDequePendingStore<T, Request>> {
        Builder {
            transport,
            on_service_error: Box::new(|_| {}),
            pending_store: VecDequePendingStore::default(),
            _pd: PhantomData,
        }
    }

    /// Set the constructed client's [`PendingStore`].
    pub fn pending_store<P2>(self, pending_store: P2) -> Builder<T, E, Request, F, P2> {
        Builder {
            pending_store,
            on_service_error: self.on_service_error,
            transport: self.transport,
            _pd: PhantomData,
        }
    }

    /// Set the constructed client's service error handler.
    ///
    /// If the [`Client`] encounters an error, it passes that error to `on_service_error`
    /// before exiting.
    ///
    /// `on_service_error` will be run from within a `Drop` implementation when the transport task
    /// panics, so it will likely abort if it panics.
    pub fn on_service_error<F2>(self, on_service_error: F2) -> Builder<T, E, Request, F2, P>
    where
        F: FnOnce(E) + Send + 'static,
    {
        Builder {
            on_service_error,
            pending_store: self.pending_store,
            transport: self.transport,
            _pd: PhantomData,
        }
    }

    /// Build a client based on the configured items on the builder.
    pub fn build(self) -> Client<T, E, Request, P> {
        Client::new_internal(self.transport, self.pending_store, self.on_service_error)
    }
}

impl<T, E, Request, F, P> fmt::Debug for Builder<T, E, Request, F, P>
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
    T::Tag: Eq + Send,
{
    /// Construct a new [`Client`] over the given `transport`.
    ///
    /// If the Client errors, the error is dropped when `new` is used -- use `with_error_handler`
    /// to handle such an error explicitly.
    pub fn new(transport: T) -> Self {
        Self::builder(transport).build()
    }

    /// Create a new builder with the provided transport.
    pub fn builder(transport: T) -> Builder<T, E, Request> {
        Builder::<_, _, _, DefaultOnServiceError<E>, VecDequePendingStore<T, Request>>::new(
            transport,
        )
    }
}

/// Handles executing the service error handler in case awaiting the `ClientInner` Future panics.
struct ClientInnerCleanup<Request, T, E, F>
where
    T: Sink<Request> + TryStream,
    E: From<Error<T, Request>>,
    F: FnOnce(E),
{
    on_service_error: Option<F>,
    _phantom_data: PhantomData<(Request, T, E)>,
}

impl<Request, T, E, F> Drop for ClientInnerCleanup<Request, T, E, F>
where
    T: Sink<Request> + TryStream,
    E: From<Error<T, Request>>,
    F: FnOnce(E),
{
    fn drop(&mut self) {
        if let Some(handler) = self.on_service_error.take() {
            (handler)(E::from(Error::<T, Request>::TransportDropped))
        }
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
    fn new_internal<F>(transport: T, pending: P, on_service_error: F) -> Self
    where
        F: FnOnce(E) + Send + 'static,
    {
        let (tx, rx) = mediator::new();
        tokio::spawn({
            let c = ClientInner {
                mediator: rx,
                transport,
                pending,
                error: PhantomData::<fn(E)>,
                finish: false,
                rx_only: false,
            };
            async move {
                let mut cleanup = ClientInnerCleanup {
                    on_service_error: Some(on_service_error),
                    _phantom_data: PhantomData::default(),
                };

                let result = c.await;
                let error = cleanup.on_service_error.take().unwrap();
                if let Err(e) = result {
                    error(e);
                }
            }
        });
        Client {
            mediator: tx,
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

                        pending.as_mut().sent(
                            id,
                            Pending {
                                tx: res,
                                span: _span,
                            },
                            transport.as_mut(),
                        );

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

        if pending.as_ref().in_flight(&transport) != 0 && !*this.rx_only {
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
        while pending.as_ref().in_flight(&transport) != 0 {
            let poll_next = match transport.as_mut().try_poll_next(cx) {
                Poll::Pending => {
                    // try_poll_next could mutate the pending store and actually change the number
                    // of in_flight requests, so we check again if we have an inflight request or not
                    if pending.as_ref().in_flight(&transport) == 0 {
                        break;
                    }
                    return Poll::Pending;
                }
                Poll::Ready(x) => x,
            };

            match poll_next.transpose().map_err(Error::from_stream_error)? {
                Some(r) => {
                    let id = transport.as_mut().finish_tag(&r);

                    let pending = if let Some(pending) =
                        pending.as_mut().completed(id, transport.as_mut())?
                    {
                        pending
                    } else {
                        tracing::trace!(
                            "response arrived but no associated pending tag; ignoring response"
                        );
                        continue;
                    };

                    tracing::trace!(parent: &pending.span, "response arrived; forwarding");

                    // ignore send failures
                    // the client may just no longer care about the response
                    let sender = pending.tx;
                    let _ = sender.send(ClientResponse {
                        response: r,
                        span: pending.span,
                    });
                }
                None => {
                    // the transport terminated while we were waiting for a response!
                    // TODO: it'd be nice if we could return the transport here..
                    return Poll::Ready(Err(E::from(Error::BrokenTransportRecv(None))));
                }
            }
        }

        if *this.finish && pending.as_ref().in_flight(&transport) == 0 {
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
                    Err(_) => Err(E::from(Error::Cancelled)),
                },
                Err(TrySendError::Pending(_)) => Err(E::from(Error::TransportFull)),
                Err(TrySendError::Closed(_)) => Err(E::from(Error::ClientDropped)),
            }
        })
    }
}
