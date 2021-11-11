use anyhow::Result;
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use std::net::SocketAddr;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tower::Service;
use tracing::{error, info};

use crate::{client::ClientToShared, message};

pub struct SharedTcp {
    sender: mpsc::UnboundedSender<ClientToShared>,
}

type TowerClient = tokio_tower::pipeline::Client<
    AsyncBincodeStream<TcpStream, message::Response, message::Request, AsyncDestination>,
    tower::BoxError,
    message::Request,
>;

impl SharedTcp {
    pub async fn new(addr: SocketAddr) -> Result<Self> {
        let tx = TcpStream::connect(&addr).await?;
        let tx = AsyncBincodeStream::from(tx).for_async();

        let (sender, mut receiver) = mpsc::unbounded_channel();

        let mut tower_client: TowerClient = tokio_tower::pipeline::Client::new(tx);

        tokio::spawn(async move {
            loop {
                let (request, response_channel): (message::Request, oneshot::Sender<_>) =
                    receiver.recv().await.expect("All senders will not drop");
                info!("Handling request: `{}`", request);

                futures::future::poll_fn(|cx| tower_client.poll_ready(cx))
                    .await
                    .expect("Service is always ready");

                let response = match tower_client.call(request).await {
                    Ok(response) => response,
                    Err(e) => {
                        error!(
                            "Could not use tower service: `{}`, panicking",
                            e.to_string()
                        );
                        panic!("Tower client call failure")
                    }
                };

                if let Err(e) = response_channel.send(response) {
                    error!("Could not send response `{}`, panicking", e.to_string());
                    panic!("Response send failure");
                }
            }
        });

        Ok(Self { sender })
    }

    pub fn new_client(&self) -> crate::client::Client {
        crate::client::Client::new(&self.sender)
    }
}
