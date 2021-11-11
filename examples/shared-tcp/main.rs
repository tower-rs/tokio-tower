use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use tower::{util::BoxService, Service, ServiceBuilder};
use tracing::{error, info};

mod client;
mod message;
mod server;
mod shared_tcp;

// This example uses the following architecture:
/*
                                                            ┌───────────────────────┐
                                                            │   Middlewares         │
                                                            │                       │
                                                            │                       │
                                            ┌───────────────►  ┌──────────────────┐ │
                                          ┌─┼───────────────┐  │                  │ │
                                          │ │               │  │  Client          │ │
                                          │ │               │  │                  │ │
                                          │ │               │  └──────────────────┘ │
                                          │ │               │                       │
                                          │ │               └───────────────────────┘
┌───────────────┐            ┌────────────▼─┴────┐
│               │            │                   │
│               │            │                   │          ┌───────────────────────┐
│    Server     └────────────►    SharedTcp      │          │   Middlewares         │
│               ◄────────────┐                   ◄──────────┘                       │
│               │            │                   ┌──────────►                       │
└───────────────┘            └──────────┬─▲──────┘          │  ┌──────────────────┐ │
                                        │ │                 │  │                  │ │
                                        │ │                 │  │  Client          │ │
                                        │ │                 │  │                  │ │
                                        │ │                 │  └──────────────────┘ │
                                        │ │                 │                       │
                                        │ │                 └───────────────────────┘
                                        │ │
                                        │ │
                                        │ │                 ┌───────────────────────┐
                                        │ │                 │   Middlewares         │
                                        │ │                 │                       │
                                        │ │                 │                       │
                                        └─┼─────────────────►  ┌──────────────────┐ │
                                          └─────────────────┐  │                  │ │
                                                            │  │  Client          │ │
                                                            │  │                  │ │
                                                            │  └──────────────────┘ │
                                                            │                       │
                                                            └───────────────────────┘
*/

// The point is that we have a server receiving requests and sending responses,
// but over a single TCP connection.
//
// The TCP connection is shared between clients.
// Clients can have any middlewares they like, independent of each other.
//
// Clients also operate independently of each other- i.e. they will typically
// reside in async tasks.
//
// Note that this architecture does not specify if we should use a _pipelined_ or a _multiplexed_ protocol
// in tokio-tower.
// A pipelined approach is used for simplicity in the example.

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("Running shared-tcp example");

    // Create a TCP listener available at the returned address.
    let server_addr = server::Server::run().await?;

    // Create a connection to it.
    let shared = shared_tcp::SharedTcp::new(server_addr).await?;

    // We can spawn independent clients which will be using the same TCP connection.
    // By using tower's [BoxService] we can treat them independently of the middlewares
    // (as long as we are OK with paying for dynamic dispatch).
    let clients = vec![
        BoxService::new(
            ServiceBuilder::new()
                .concurrency_limit(1)
                .service(shared.new_client()),
        ),
        BoxService::new(
            ServiceBuilder::new()
                .timeout(Duration::from_secs(2))
                .service(shared.new_client()),
        ),
        BoxService::new(
            ServiceBuilder::new()
                .concurrency_limit(1)
                .timeout(Duration::from_secs(2))
                .service(shared.new_client()),
        ),
    ];

    // Clients can now do their thing concurrently.
    futures::stream::iter(clients.into_iter().enumerate())
        .for_each_concurrent(None, |(index, mut client)| async move {
            futures::future::poll_fn(|cx| client.poll_ready(cx))
                .await
                .expect("Client should be ready");

            match client
                .call(message::Request::new(format!(
                    "Hi from client index {}",
                    index
                )))
                .await
            {
                Ok(response) => info!("Client#{} - response: `{}`", index, response),
                Err(e) => error!("Client#{} - problem: `{}`", index, e.to_string()),
            }
        })
        .await;

    Ok(())
}
