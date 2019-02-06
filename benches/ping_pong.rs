#![feature(test)]

extern crate test;

use futures::{Async, AsyncSink, Future, StartSend, Poll, Sink, Stream};
use futures::future::{lazy, poll_fn};
use tokio::runtime::current_thread;
use tokio::sync::mpsc;
use tokio_tower::pipeline::{client, Client};
use tower_service::Service;

struct Transport {
    tx: mpsc::Sender<u64>,
    rx: mpsc::Receiver<u64>,
}

impl Sink for Transport {
    type SinkItem = u64;
    type SinkError = ();

    fn start_send(
        &mut self,
        item: Self::SinkItem
    ) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.tx.start_send(item)
            .map_err(|_| ())
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.tx.poll_complete()
            .map_err(|_| ())
    }
}

impl Stream for Transport {
    type Item = u64;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<u64>, Self::Error> {
        self.rx.poll()
            .map_err(|_| ())
    }
}

fn start_server() -> Transport {
    use std::thread;

    let (in_tx, in_rx) = mpsc::channel(10);
    let (out_tx, out_rx) = mpsc::channel(10);

    thread::spawn(move || {
        let out_rx = out_rx.wait();
        let mut in_tx = in_tx.wait();

        for i in out_rx {
            let i = i.unwrap();
            in_tx.send(i);
        }
    });

    Transport {
        tx: out_tx,
        rx: in_rx,
    }
}

#[bench]
fn ping_pong(b: &mut test::Bencher) {
    let mut rt = current_thread::Runtime::new().unwrap();

    let mut client = rt.block_on(lazy(|| {
        let transport = start_server();
        let client = Client::<_, client::Error<_>>::new(transport);
        Ok::<_, ()>(client)
    })).unwrap();

    let mut futs = vec![];

    b.iter(|| {
        futs.clear();

        for i in 0..15 {
            rt.block_on(poll_fn(|| client.poll_ready())).unwrap();
            futs.push(client.call(i));
        }

        for fut in futs.drain(..) {
            rt.block_on(fut).unwrap();
        }
    });
}
