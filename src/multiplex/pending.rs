use std::{
    fmt,
    sync::{atomic, Arc},
};

use tokio::sync::oneshot;

use crate::wrappers::ClientResponse;

/// A type used to track in-flight requests.
///
/// Each pending response contains an associated `Tag` that is provided
/// by the [`TagStore`], which is used to uniquely identify a request/response pair.
///
/// # Drop
///
/// On `drop` this type will internally decrement the internal in-flight count and
/// will return `Error::Cancelled` to the caller.
pub struct Pending<Response> {
    in_flight: Arc<atomic::AtomicUsize>,
    inner: Option<PendingInner<Response>>,
}

struct PendingInner<Response> {
    tx: oneshot::Sender<ClientResponse<Response>>,
    span: tracing::Span,
}

impl<Response> fmt::Debug for Pending<Response> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let span = &self.inner.as_ref().unwrap().span;
        f.debug_struct("Pending").field("span", &span).finish()
    }
}

impl<Response> Pending<Response> {
    pub(crate) fn new(
        tx: oneshot::Sender<ClientResponse<Response>>,
        span: tracing::Span,
        in_flight: Arc<atomic::AtomicUsize>,
    ) -> Self {
        Self {
            in_flight,
            inner: Some(PendingInner { tx, span }),
        }
    }

    pub(crate) fn span(&self) -> &tracing::Span {
        let inner = self.inner.as_ref().unwrap();
        &inner.span
    }

    pub(crate) fn send(mut self, response: Response) {
        let PendingInner { tx, span } = self
            .inner
            .take()
            .expect("BUG: This value should only be taken in a private");

        // ignore send failures
        // the client may just no longer care about the response
        let _ = tx.send(ClientResponse { response, span });
    }
}

impl<Response> Drop for Pending<Response> {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, atomic::Ordering::AcqRel);
    }
}
