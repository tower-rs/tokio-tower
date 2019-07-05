use crate::{EchoService, PanicError, Request, Response};
use async_bincode::*;
use tokio;
use tokio::prelude::*;
use tokio_tower::pipeline::{Client, Server};
use tower::util::ServiceExt;
use tower::Service;

mod client;

#[test]
fn integration() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let rx = tokio::net::tcp::TcpListener::bind(&addr).unwrap();
    let addr = rx.local_addr().unwrap();
    let tx = tokio::net::tcp::TcpStream::connect(&addr)
        .map(AsyncBincodeStream::from)
        .map(AsyncBincodeStream::for_async)
        .map_err(PanicError::from)
        .map(Client::new);

    let rx = rx
        .incoming()
        .into_future()
        .map_err(PanicError::from)
        .map(|(stream, _)| stream.unwrap())
        .map(AsyncBincodeStream::from)
        .map(AsyncBincodeStream::for_async)
        .map_err(PanicError::from)
        .map(|stream| Server::new(stream, EchoService));

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(
        rx.and_then(|srv| srv.map_err(PanicError::from))
            .map_err(|_| ()),
    );

    let fut = tx.map_err(PanicError::from).and_then(
        move |tx: Client<AsyncBincodeStream<_, Response, _, _>, _>| {
            tx.ready()
                .and_then(|mut tx| {
                    let fut1 = tx.call(Request::new(1).into());
                    tx.ready().map(move |tx| (tx, fut1))
                })
                .and_then(|(mut tx, fut1)| {
                    let fut2 = tx.call(Request::new(2).into());
                    tx.ready().map(move |tx| (tx, fut1, fut2))
                })
                .and_then(|(mut tx, fut1, fut2)| {
                    let fut3 = tx.call(Request::new(3).into());
                    fut1.inspect(|r| r.check(1))
                        .and_then(move |_| fut2)
                        .inspect(|r| r.check(2))
                        .and_then(move |_| fut3)
                        .inspect(|r| r.check(3))
                        .map(move |_| tx)
                })
        },
    );
    assert!(rt.block_on(fut).is_ok());
}
