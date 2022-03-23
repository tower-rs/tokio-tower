use futures_core::stream::TryStream;
use futures_sink::Sink;
use std::{error, fmt};

/// An error that occurred while servicing a request.
#[non_exhaustive]
pub enum Error<T, I>
where
    T: Sink<I> + TryStream,
{
    /// The underlying transport failed to send a request.
    BrokenTransportSend(<T as Sink<I>>::Error),

    /// The underlying transport failed while attempting to receive a response.
    ///
    /// If `None`, the transport closed without error while there were pending requests.
    BrokenTransportRecv(Option<<T as TryStream>::Error>),

    /// The internal pending data store has
    Cancelled,

    /// Attempted to issue a `call` when no more requests can be in flight.
    ///
    /// See [`tower_service::Service::poll_ready`] and [`Client::with_limit`].
    TransportFull,

    /// Attempted to issue a `call`, but the underlying transport has been closed.
    ClientDropped,

    /// The server sent a response that the client was not expecting.
    Desynchronized,
}

impl<T, I> fmt::Display for Error<T, I>
where
    T: Sink<I> + TryStream,
    <T as Sink<I>>::Error: fmt::Display,
    <T as TryStream>::Error: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::BrokenTransportSend(_) => f.pad("underlying transport failed to send a request"),
            Error::BrokenTransportRecv(Some(_)) => {
                f.pad("underlying transport failed while attempting to receive a response")
            }
            Error::BrokenTransportRecv(None) => f.pad("transport closed with in-flight requests"),
            Error::Cancelled => f.pad("request was cancelled internally"),
            Error::TransportFull => f.pad("no more in-flight requests allowed"),
            Error::ClientDropped => f.pad("Client was dropped"),
            Error::Desynchronized => f.pad("server sent a response the client did not expect"),
        }
    }
}

impl<T, I> fmt::Debug for Error<T, I>
where
    T: Sink<I> + TryStream,
    <T as Sink<I>>::Error: fmt::Debug,
    <T as TryStream>::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::BrokenTransportSend(ref se) => write!(f, "BrokenTransportSend({:?})", se),
            Error::BrokenTransportRecv(Some(ref se)) => write!(f, "BrokenTransportRecv({:?})", se),
            Error::BrokenTransportRecv(None) => f.pad("BrokenTransportRecv"),
            Error::Cancelled => f.pad("Cancelled"),
            Error::TransportFull => f.pad("TransportFull"),
            Error::ClientDropped => f.pad("ClientDropped"),
            Error::Desynchronized => f.pad("Desynchronized"),
        }
    }
}

impl<T, I> error::Error for Error<T, I>
where
    T: Sink<I> + TryStream,
    <T as Sink<I>>::Error: error::Error + 'static,
    <T as TryStream>::Error: error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::BrokenTransportSend(ref se) => Some(se),
            Error::BrokenTransportRecv(Some(ref se)) => Some(se),
            _ => None,
        }
    }
}

impl<T, I> Error<T, I>
where
    T: Sink<I> + TryStream,
{
    pub(crate) fn from_sink_error(e: <T as Sink<I>>::Error) -> Self {
        Error::BrokenTransportSend(e)
    }

    pub(crate) fn from_stream_error(e: <T as TryStream>::Error) -> Self {
        Error::BrokenTransportRecv(Some(e))
    }
}
