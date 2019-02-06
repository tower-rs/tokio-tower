use async_bincode::*;
use crate::{EchoService, PanicError, Request, Response};
use tokio;
use tokio::prelude::*;
use tokio_tower::pipeline::{Client, Server};
use tower_service::Service;
use tower_util::ServiceExt;

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
            // Send a sequence of incrementing requests
            let futs = stream::unfold((1, tx), |(i, tx)| {
                Some(tx.ready().and_then(move |mut tx| {
                    Ok((tx.call(i), (i+1, tx)))
                }))
            });

            futs
                .take(3)
                .enumerate()
                .for_each(|(i, fut)| {
                    fut
                        .inspect(move |r| r.check(i as u32))
                        .map(|_| ())
                })
        },
    );
    assert!(rt.block_on(fut).is_ok());
}
