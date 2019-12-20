use futures_core::stream::TryStream;
use futures_sink::Sink;

pub(crate) struct ClientRequest<T, I>
where
    T: Sink<I> + TryStream,
{
    pub(crate) req: I,
    #[cfg(not(feature = "tracing"))]
    pub(crate) span: (),
    #[cfg(feature = "tracing")]
    pub(crate) span: tracing::Span,
    pub(crate) res: tokio::sync::oneshot::Sender<ClientResponse<T::Ok>>,
}

pub(crate) struct ClientResponse<T> {
    pub(crate) response: T,
    #[cfg(feature = "tracing")]
    pub(crate) span: tracing::Span,
}
