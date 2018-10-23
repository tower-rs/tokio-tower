use async_bincode::*;
use tokio;
use tokio::prelude::*;
use tokio_tower::pipeline::Client;
use tower_service::Service;
use {Request, Response};

#[test]
fn it_works() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let rx = tokio::net::tcp::TcpListener::bind(&addr).unwrap();
    let addr = rx.local_addr().unwrap();
    let tx = tokio::net::tcp::TcpStream::connect(&addr)
        .map(AsyncBincodeStream::from)
        .map_err(PanicError::from)
        .map(Client::new);

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(
        rx.incoming()
            .map_err(|e| panic!("{:?}", e))
            .for_each(move |stream| {
                let (r, w) = stream.split();
                let w = AsyncBincodeWriter::from(w);
                let r = AsyncBincodeReader::from(r);
                tokio::spawn(
                    r.map(|req: Request| Response::from(req))
                        .map_err(|e| panic!("{:?}", e))
                        .forward(w.sink_map_err(|e| panic!("{:?}", e)))
                        .then(|_| {
                            // we're probably just shutting down
                            Ok(())
                        }),
                );

                Ok(())
            }),
    );

    // TODO: drive tx...
    let fut = tx.map_err(PanicError::from).and_then(
        move |mut tx: Client<AsyncBincodeStream<_, Response, _, _>, _>| {
            tx.call(Request).map(move |r| (tx, r))
        },
    );
    assert!(rt.block_on(fut).is_ok());
}

struct PanicError;
use std::fmt;
impl<E> From<E> for PanicError
where
    E: fmt::Debug,
{
    fn from(e: E) -> Self {
        panic!("{:?}", e)
    }
}
