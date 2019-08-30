use futures::{Sink, TryStream};

pub(crate) struct ClientRequest<T, I>
where
    T: Sink<I> + TryStream,
{
    pub(crate) req: I,
    #[cfg(not(feature = "tracing"))]
    pub(crate) span: (),
    #[cfg(feature = "tracing")]
    pub(crate) span: tracing::Span,
    pub(crate) res: tokio_sync::oneshot::Sender<ClientResponse<T::Ok>>,
}

pub(crate) struct ClientResponse<T> {
    pub(crate) response: T,
    #[cfg(feature = "tracing")]
    pub(crate) span: tracing::Span,
}

/*
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
                    event!(r.span, tracing::Level::TRACE, "response returned");
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
    /// Make the future also resolve with its associated `tracing::Span` (if any).
    #[cfg(feature = "tracing")]
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

/// A future that resolves a previously submitted [`Request`] that includes a `tracing::Span`.
#[cfg(feature = "tracing")]
pub struct SpannedClientResponseFut<T, E>
where
    T: Stream,
{
    fut: ClientResponseFutInner<T, E>,
}

#[cfg(feature = "tracing")]
impl<T, E> Future for SpannedClientResponseFut<T, E>
where
    T: Sink + Stream,
    E: From<Error<T>>,
{
    type Item = (T::Item, tracing::Span);
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ClientResponse { response, span, .. } = try_ready!(self.fut.poll());
        Ok(Async::Ready((response, span)))
    }
}
*/
