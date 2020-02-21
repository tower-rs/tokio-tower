use futures_core::stream::TryStream;
use futures_sink::Sink;

pub(crate) struct ClientRequest<T, I>
where
    T: Sink<I> + TryStream,
{
    pub(crate) req: I,
    pub(crate) span: tracing::Span,
    pub(crate) res: tokio::sync::oneshot::Sender<ClientResponse<T::Ok>>,
}

pub(crate) struct ClientResponse<T> {
    pub(crate) response: T,
    pub(crate) span: tracing::Span,
}
