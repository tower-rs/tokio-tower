use crate::{EchoService, PanicError, Request, Response};
use async_bincode::*;
use slab::Slab;
use tokio;
use tokio::prelude::*;
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};
use tower_service::Service;
use tower_util::ServiceExt;

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
            // inject cross-traffic
            for i in 10..20 {
                tokio::spawn(
                    future::loop_fn(tx.clone(), move |tx| {
                        tx.ready().and_then(move |mut tx| {
                            tx.call(Request::new(i)).map(move |r| {
                                r.check(i);
                                future::Loop::Continue(tx)
                            })
                        })
                    })
                    .map_err(|_| unreachable!()),
                );
            }

            tx.ready()
                .and_then(|mut tx| {
                    let fut1 = tx.call(Request::new(1));
                    tx.ready().map(move |tx| (tx, fut1))
                })
                .and_then(|(mut tx, fut1)| {
                    let fut2 = tx.call(Request::new(2));
                    tx.ready().map(move |tx| (tx, fut1, fut2))
                })
                .and_then(|(mut tx, fut1, fut2)| {
                    let fut3 = tx.call(Request::new(3));
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
