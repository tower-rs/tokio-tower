use crate::{ready, PanicError, EchoService, Request, Response, unwrap};
use async_bincode::*;
use slab::Slab;
use std::pin::Pin;
use tokio;
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};
use tower_service::Service;

pub(crate) struct SlabStore(Slab<()>);

impl TagStore<Request, Response> for SlabStore {
    type Tag = usize;
    fn assign_tag(mut self: Pin<&mut Self>, request: &mut Request) -> usize {
        let tag = self.0.insert(());
        request.set_tag(tag);
        tag
    }
    fn finish_tag(mut self: Pin<&mut Self>, response: &Response) -> usize {
        let tag = response.get_tag();
        self.0.remove(tag);
        tag
    }
}

#[tokio::test]
async fn integration() {
    let mut rx = tokio::net::tcp::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = rx.local_addr().unwrap();

    // connect
    let tx = tokio::net::tcp::TcpStream::connect(&addr).await.unwrap();
    let tx = AsyncBincodeStream::from(tx).for_async();
    let mut tx: Client<_, PanicError, _> = Client::new(MultiplexTransport::new(tx, SlabStore(Slab::new())));

    // accept
    let (rx, _) = rx.accept().await.unwrap();
    let rx = AsyncBincodeStream::from(rx).for_async();
    let server = Server::new(rx, EchoService);

    tokio::spawn(async move {
        server.await.unwrap()
    });

    unwrap(ready(&mut tx).await);
    let fut1 = tx.call(Request::new(1));
    unwrap(ready(&mut tx).await);
    let fut2 = tx.call(Request::new(2));
    unwrap(ready(&mut tx).await);
    let fut3 = tx.call(Request::new(3));
    unwrap(fut1.await).check(1);
    unwrap(fut2.await).check(2);
    unwrap(fut3.await).check(3);
}
