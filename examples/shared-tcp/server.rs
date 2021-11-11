use std::net::SocketAddr;

use anyhow::Result;
use async_bincode::{AsyncBincodeReader, AsyncBincodeWriter};
use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::message;

pub struct Server;

impl Server {
    async fn connection_handler(mut stream: TcpStream) {
        let (r, w) = stream.split();

        let mut r: AsyncBincodeReader<_, message::Request> = AsyncBincodeReader::from(r);
        let mut w: AsyncBincodeWriter<_, message::Response, _> =
            AsyncBincodeWriter::from(w).for_async();

        info!("Handling connection");
        while let Some(Ok(request)) = r.next().await {
            if let Err(e) = w.send(message::Response::from(request)).await {
                error!("Problem sending response: {}", e.to_string())
            }
        }

        info!("No more requests");
    }

    pub async fn run() -> Result<SocketAddr> {
        let rx = TcpListener::bind("127.0.0.1:0").await?;
        let addr = rx.local_addr()?;
        info!("Server bound to {:?}", addr);

        tokio::spawn(async move {
            info!("Awaiting connections...");
            loop {
                let stream = match rx.accept().await {
                    Ok((stream, addr)) => {
                        info!("New connection at {:?}", addr);
                        stream
                    }
                    Err(e) => {
                        error!("Accept error, panicking: {}", e.to_string());
                        panic!("Could not accept");
                    }
                };

                tokio::spawn(Server::connection_handler(stream));
            }
        });

        Ok(addr)
    }
}
