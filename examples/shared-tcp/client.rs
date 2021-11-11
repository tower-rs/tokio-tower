use std::{pin::Pin, task::Poll, time::Duration};

use anyhow::Result;
use futures_core::Future;
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tower::Service;

use crate::message;

pub type ClientToShared = (message::Request, oneshot::Sender<message::Response>);

pub struct Client {
    sender: mpsc::UnboundedSender<ClientToShared>,
}

impl Client {
    pub fn new(sender: &mpsc::UnboundedSender<ClientToShared>) -> Self {
        Self {
            sender: sender.clone(),
        }
    }
}

// Clippy complains about complex types,
// so do a bit of indirection through aliases.
type ClientFuture =
    Pin<Box<dyn Future<Output = Result<ClientResponse, ClientError>> + 'static + Send>>;
type ClientError = tower::BoxError;
type ClientResponse = message::Response;

impl Service<message::Request> for Client {
    type Response = ClientResponse;
    type Error = ClientError;
    type Future = ClientFuture;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: message::Request) -> Self::Future {
        let (sender, receiver) = oneshot::channel();

        let result = self.sender.send((request, sender));

        // Let's make responses a bit less predictable
        let random_wait_millis = rand::thread_rng().gen_range(25..=250);

        Box::pin(async move {
            // Evaluate result here to avoid referencing `self` here.
            let _ = result?;

            tokio::time::sleep(Duration::from_millis(random_wait_millis)).await;

            Ok(receiver.await?)
        })
    }
}
