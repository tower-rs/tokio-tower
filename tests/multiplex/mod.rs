use crate::{EchoService, PanicError, Request, Response};
use async_bincode::*;
use slab::Slab;
use tokio;
use tokio::prelude::*;
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};
use tower::util::ServiceExt;
use tower::Service;

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
        .map(|s| Client::new(MultiplexTransport::new(s, SlabStore(Slab::new()))));

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
        move |tx: Client<MultiplexTransport<AsyncBincodeStream<_, Response, _, _>, _>, _>| {
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
