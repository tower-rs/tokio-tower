use crate::{PanicError, Request, Response};
use async_bincode::*;
use tokio;
use tokio::prelude::*;
use tokio_tower::pipeline::Client;
use tower_service::Service;
use tower_util::ServiceExt;

#[test]
fn it_works() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let rx = tokio::net::tcp::TcpListener::bind(&addr).unwrap();
    let addr = rx.local_addr().unwrap();
    let tx = tokio::net::tcp::TcpStream::connect(&addr)
        .map(AsyncBincodeStream::from)
        .map(AsyncBincodeStream::for_async)
        .map_err(PanicError::from)
        .map(Client::new);

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(
        rx.incoming()
            .map_err(PanicError::from)
            .for_each(move |stream| {
                let (r, w) = stream.split();
                let r = AsyncBincodeReader::from(r);
                let w = AsyncBincodeWriter::from(w).for_async();
                tokio::spawn(
                    r.map(|req: Request| Response::from(req))
                        .map_err(PanicError::from)
                        .forward(w.sink_map_err(PanicError::from))
                        .then(|_| {
                            // we're probably just shutting down
                            Ok(())
                        }),
                );

                Ok(())
            })
            .map_err(|_| ()),
    );

    let fut = tx.map_err(PanicError::from).and_then(
        move |tx: Client<AsyncBincodeStream<_, Response, _, _>, _>| {
            tx.ready().and_then(|mut tx| {
                tx.call(Request::new(1))
                    .inspect(|r| r.check(1))
                    .map(move |_| tx)
            })
        },
    );
    assert!(rt.block_on(fut).is_ok());
}
