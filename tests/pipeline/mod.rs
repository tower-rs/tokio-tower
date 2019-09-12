use crate::{ready, unwrap, EchoService, PanicError, Request, Response};
use async_bincode::*;
use tokio;
use tokio_tower::pipeline::{Client, Server};
use tower_service::Service;

mod client;

#[tokio::test]
async fn integration() {
    let mut rx = tokio::net::tcp::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = rx.local_addr().unwrap();

    // connect
    let tx = tokio::net::tcp::TcpStream::connect(&addr).await.unwrap();
    let tx: AsyncBincodeStream<_, Response, _, _> = AsyncBincodeStream::from(tx).for_async();
    let mut tx: Client<_, PanicError, _> = Client::new(tx);

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
