use crate::{ready, unwrap, EchoService, PanicError, Request, Response};
use async_bincode::AsyncBincodeStream;
use futures_util::pin_mut;
use slab::Slab;
use std::pin::Pin;
use tokio::net::{TcpListener, TcpStream};
use tokio_tower::multiplex::{
    client::VecDequePendingStore, Client, MultiplexTransport, Server, TagStore,
};
use tower_service::Service;
use tower_test::mock;

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
    let rx = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = rx.local_addr().unwrap();

    // connect
    let tx = TcpStream::connect(&addr).await.unwrap();
    let tx = AsyncBincodeStream::from(tx).for_async();
    let mut tx: Client<_, PanicError, _> =
        Client::builder(MultiplexTransport::new(tx, SlabStore(Slab::new()))).build();

    // accept
    let (rx, _) = rx.accept().await.unwrap();
    let rx = AsyncBincodeStream::from(rx).for_async();
    let server = Server::new(rx, EchoService);

    tokio::spawn(async move { server.await.unwrap() });

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

#[tokio::test]
async fn racing_close() {
    let rx = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = rx.local_addr().unwrap();

    // connect
    let tx = TcpStream::connect(&addr).await.unwrap();
    let tx = AsyncBincodeStream::from(tx).for_async();
    let mut tx: Client<_, PanicError, _> =
        Client::builder(MultiplexTransport::new(tx, SlabStore(Slab::new())))
            .pending_store(VecDequePendingStore::default())
            .on_service_error(|_| {})
            .build();

    let (service, handle) = mock::pair::<Request, Response>();
    pin_mut!(handle);

    // accept
    let (rx, _) = rx.accept().await.unwrap();
    let rx = AsyncBincodeStream::from(rx).for_async();
    let server = Server::new(rx, service);

    tokio::spawn(async move { server.await.unwrap() });

    // we now want to set up a situation where a request has been sent to the server, and then the
    // client goes away while the request is still outstanding. in this case, the connection to the
    // server will be shut down in the write direction, but not in the read direction.

    // send a couple of request
    unwrap(ready(&mut tx).await);
    let fut1 = tx.call(Request::new(1));
    unwrap(ready(&mut tx).await);
    let fut2 = tx.call(Request::new(2));
    unwrap(ready(&mut tx).await);
    // drop client to indicate no more requests
    drop(tx);
    // respond to both requests one after the other
    // the response to the first should trigger the state machine to handle
    // a read after it has poll_closed on the transport.
    let (req1, rsp1) = handle.as_mut().next_request().await.unwrap();
    req1.check(1);
    rsp1.send_response(Response::from(req1));
    unwrap(fut1.await).check(1);
    let (req2, rsp2) = handle.as_mut().next_request().await.unwrap();
    req2.check(2);
    rsp2.send_response(Response::from(req2));
    unwrap(fut2.await).check(2);
}
