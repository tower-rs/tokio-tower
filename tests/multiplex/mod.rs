use async_bincode::*;
use slab::Slab;
use tokio;
use tokio::prelude::*;
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};
use tower_service::Service;
use {PanicError, Request, Response};

pub(crate) struct SlabStore(Slab<()>);

impl TagStore<Request, Response> for SlabStore {
    type Tag = usize;
    fn assign_tag(&mut self, request: &mut Request) -> usize {
        let tag = self.0.insert(());
        request.set_tag(tag);
        tag
    }
    fn finish_tag(&mut self, response: &Response) -> usize {
        let tag = response.get_tag();
        self.0.remove(tag);
        tag
    }
}

#[test]
fn integration() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let rx = tokio::net::tcp::TcpListener::bind(&addr).unwrap();
    let addr = rx.local_addr().unwrap();
    let tx = tokio::net::tcp::TcpStream::connect(&addr)
        .map(AsyncBincodeStream::from)
        .map(AsyncBincodeStream::for_async)
        .map_err(PanicError::from)
        .map(|s| {
            let s = MultiplexTransport::new(s, SlabStore(Slab::new()));

            // need to limit to one-in-flight for poll_ready to be sufficient to drive Service
            Client::with_limit(s, 1)
        });

    struct EchoService;
    impl Service for EchoService {
        type Request = Request;
        type Response = Response;
        type Error = ();
        type Future = future::FutureResult<Self::Response, Self::Error>;

        fn poll_ready(&mut self) -> Result<Async<()>, Self::Error> {
            Ok(Async::Ready(()))
        }

        fn call(&mut self, r: Self::Request) -> Self::Future {
            future::ok(Response::from(r))
        }
    }

    let rx = rx
        .incoming()
        .into_future()
        .map_err(PanicError::from)
        .map(|(stream, _)| stream.unwrap())
        .map(AsyncBincodeStream::from)
        .map(AsyncBincodeStream::for_async)
        .map_err(PanicError::from)
        .map(|stream| Server::pipelined(stream, EchoService, None));

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(
        rx.and_then(|srv| srv.map_err(PanicError::from))
            .map_err(|_| ()),
    );

    let fut = tx.map_err(PanicError::from).and_then(
        move |mut tx: Client<MultiplexTransport<AsyncBincodeStream<_, Response, _, _>, _>, _>| {
            let fut1 = tx.call(Request::new(1));

            // continue to drive the service
            tokio::spawn(
                future::poll_fn(move || tx.poll_ready())
                    .map_err(PanicError::from)
                    .map_err(|_| ()),
            );

            fut1.inspect(|r| r.check(1))
        },
    );
    assert!(rt.block_on(fut).is_ok());
}
