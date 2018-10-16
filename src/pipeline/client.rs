use futures::future;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::marker::PhantomData;
use tower_service;

/// This type provides an implementation of a Tower
/// [`Service`](https://docs.rs/tokio-service/0.1/tokio_service/trait.Service.html) on top of a
/// request-at-a-time protocol transport. In particular, it wraps a transport that implements
/// `Sink<SinkItem = Request>` and `Stream<Item = Response>` with the necessary bookkeeping to
/// adhere to Tower's convenient `fn(Request) -> Future<Response>` API.
pub struct Client<Request, Response, T, E> {
    requests: VecDeque<Request>,
    responses: VecDeque<oneshot::Sender<Response>>,
    transport: T,

    max_in_flight: Option<usize>,
    in_flight: usize,

    #[allow(unused)]
    error: PhantomData<E>,
}

pub enum Error {
    BrokenTransport,
    TransportFull,
    ClientDropped,
}

impl<Request, Response, T, E> Client<Request, Response, T, E>
where
    T: Sink<SinkItem = Request>,
    T: Stream<Item = Response>,
    E: From<<T as Sink>::SinkError>,
    E: From<<T as Stream>::Error>,
    E: From<Error>,
{
    /// Construct a new [`Client`] over the given `transport` with no limit on the number of
    /// in-flight requests.
    pub fn new(transport: T) -> Self {
        Client {
            requests: VecDeque::default(),
            responses: VecDeque::default(),
            transport,
            max_in_flight: None,
            in_flight: 0,
            error: PhantomData::<E>,
        }
    }

    /// Construct a new [`Client`] over the given `transport` with a maxmimum limit on the number
    /// of in-flight requests.
    ///
    /// Note that setting the limit to 1 implies that for each `Request`, the `Response` must be
    /// received before another request is sent on the same transport.
    pub fn with_limit(transport: T, max_in_flight: usize) -> Self {
        Client {
            requests: VecDeque::with_capacity(max_in_flight),
            responses: VecDeque::with_capacity(max_in_flight),
            transport,
            max_in_flight: Some(max_in_flight),
            in_flight: 0,
            error: PhantomData::<E>,
        }
    }
}

impl<Request, Response, T, E> tower_service::Service for Client<Request, Response, T, E>
where
    T: Sink<SinkItem = Request>,
    T: Stream<Item = Response>,
    E: From<<T as Sink>::SinkError>,
    E: From<<T as Stream>::Error>,
    E: From<Error>,
    E: 'static,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Error = E;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Result<Async<()>, Self::Error> {
        loop {
            // send more requests if we have them
            while let Some(req) = self.requests.pop_front() {
                if let AsyncSink::NotReady(req) = self.transport.start_send(req)? {
                    self.requests.push_front(req);
                    break;
                } else {
                    self.in_flight += 1;
                }
            }

            // flush out any stuff we've sent in the past
            // if it returns not_ready, we still want to see if we've got some responses
            self.transport.poll_complete()?;

            // and start looking for replies.
            //
            // note that we *could* have this just be a loop, but we don't want to poll the stream
            // if we know there's nothing for it to produce.
            while self.in_flight != 0 {
                match self.transport.poll()? {
                    Async::Ready(Some(r)) => {
                        // ignore send failures
                        // the client may just no longer care about the response
                        let sender = self
                            .responses
                            .pop_front()
                            .expect("got a request with no sender?");
                        let _ = sender.send(r);
                        self.in_flight -= 1;
                    }
                    Async::Ready(None) => {
                        // the transport terminated while we were waiting for a response!
                        // TODO: it'd be nice if we could return the transport here..
                        return Err(E::from(Error::BrokenTransport));
                    }
                    Async::NotReady => {
                        if let Some(mif) = self.max_in_flight {
                            if self.in_flight + self.requests.len() < mif {
                                return Ok(Async::Ready(()));
                            }
                        }

                        return Ok(Async::NotReady);
                    }
                }
            }

            if self.requests.is_empty() {
                return Ok(Async::Ready(()));
            }
        }
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        if let Some(mif) = self.max_in_flight {
            if self.in_flight + self.requests.len() < mif {
                return Box::new(future::err(E::from(Error::TransportFull)));
            }
        }

        let (tx, rx) = oneshot::channel();
        self.requests.push_back(req);
        self.responses.push_back(tx);
        Box::new(rx.map_err(|_| E::from(Error::ClientDropped)))
    }
}

#[cfg(test)]
mod tests {
    // TODO: O:)
}
