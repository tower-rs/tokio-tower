use async_bincode::*;
use tokio;
use tokio::prelude::*;
use tokio_tower::pipeline::{Client, Server};
use tower_service::Service;
use {PanicError, Request, Response};

mod client;

#[test]
fn integration() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let rx = tokio::net::tcp::TcpListener::bind(&addr).unwrap();
    let addr = rx.local_addr().unwrap();
    let tx = tokio::net::tcp::TcpStream::connect(&addr)
        .map(AsyncBincodeStream::from)
        .map_err(PanicError::from)
        .map(Client::new);

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
        .map_err(PanicError::from)
        .map(|stream| Server::new(stream, EchoService));

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(
        rx.and_then(|srv| srv.map_err(PanicError::from))
            .map_err(|_| ()),
    );

    // TODO: drive tx...
    let fut = tx.map_err(PanicError::from).and_then(
        move |mut tx: Client<AsyncBincodeStream<_, Response, _, _>, _>| {
            tx.call(Request).map(move |r| (tx, r))
        },
    );
    assert!(rt.block_on(fut).is_ok());
}
