use anyhow::Result;
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures_util::stream::StreamExt;
use std::{net::SocketAddr, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tower::pipeline;
use tower::{
    balance::p2c::Balance, buffer::Buffer, discover::ServiceList, filter::Filter, steer::Steer,
    util::BoxService, Service, ServiceBuilder, ServiceExt,
};
use tracing::{error, info};

use crate::{
    hardware::unit::{HardwarePicker, HardwareUnit},
    message::HardwareRequest,
};

mod hardware;
mod message;

// Initializes and spins up a server.
// The server manages some hardware resources.
// Each incoming TCP connection will contest these same resources.
//
// The server uses a hardware manager.
// This manager checks which hardware unit the requester is interested in,
// and forwards this request to the correct hardware unit.
async fn run_server() -> Result<SocketAddr> {
    info!("Initializing server");

    let tcp = TcpListener::bind("127.0.0.1:0").await?;

    let local_addr = tcp.local_addr()?;

    // These are the hardware units the server offers for use.
    let hw_units = vec![
        HardwareUnit::new(0),
        HardwareUnit::new(1),
        HardwareUnit::new(2),
    ];

    // We use [Steer] in order to route the request to the correct hardware unit.
    let hw_manager = Steer::new(hw_units, HardwarePicker);

    // [Steer] is not fallible.
    // But users may request hardware the server does not have.
    // We can place a [Filter] in front of any service to make the request fallible.
    let hw_manager = Filter::new(hw_manager, |request: HardwareRequest| {
        if request.id < 3 {
            Ok(request)
        } else {
            Err(anyhow::anyhow!("No such hardware unit"))
        }
    });

    // The buffer allows cloning handles to the service.
    let hw_manager = Buffer::new(hw_manager, 1024);

    tokio::spawn(async move {
        let hw_manager_handle = hw_manager.clone();

        loop {
            let (stream, peer) = tcp.accept().await.expect("Could not accept on socket");
            let hw_manager_handle = hw_manager_handle.clone();
            tokio::spawn(async move {
                info!(?peer, "Peer accepted");

                let transport = AsyncBincodeStream::from(stream).for_async();
                let server = pipeline::server::Server::new(transport, hw_manager_handle);

                match server.await {
                    Ok(()) => info!("Server stopped"),
                    // TODO: If we encounter an issue (e.g. user asks for invalid hardware id)
                    // the server stops here.
                    //
                    // How do we propagate the issue back to the user, but keep the server running?
                    Err(e) => error!("Server stopped with an issue: {:?}", e),
                }
            });
        }
    });

    Ok(local_addr)
}

type TowerClient = pipeline::Client<
    AsyncBincodeStream<TcpStream, message::Response, message::HardwareRequest, AsyncDestination>,
    tower::BoxError,
    message::HardwareRequest,
>;

// Set up two TCP connections and put them behind a load balancer.
// Note that this might not be necessary, profiling should be done in order to prove the necessity.
// Here it serves as an example of things we can do with (tokio-)tower.
async fn make_server_connections(
    server: SocketAddr,
) -> Result<Balance<ServiceList<Vec<TowerClient>>, message::HardwareRequest>> {
    info!("Making connections to {:#?}", server);

    let tx_1 = TcpStream::connect(&server).await?;
    let tx_1 = AsyncBincodeStream::from(tx_1).for_async();

    let tx_2 = TcpStream::connect(&server).await?;
    let tx_2 = AsyncBincodeStream::from(tx_2).for_async();

    let connection_1: TowerClient = pipeline::Client::new(tx_1);
    let connection_2: TowerClient = pipeline::Client::new(tx_2);

    Ok(Balance::new(ServiceList::new(vec![
        connection_1,
        connection_2,
    ])))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Running hardware-resource example");

    let server_addr = run_server().await?;

    // Buffering the service allows us to clone- essentially providing handles which can be used independently.
    let hw_service = Buffer::new(make_server_connections(server_addr).await?, 1024);

    // We can add middlewares at our discretion to handles.
    //
    // Using [BoxService] allows us to put them into a vector and
    // operate on them as a single collection of services.
    let clients = vec![
        BoxService::new(
            ServiceBuilder::new()
                .concurrency_limit(10)
                .timeout(Duration::from_secs(10))
                .service(hw_service.clone()),
        ),
        BoxService::new(
            ServiceBuilder::new()
                .timeout(Duration::from_secs(10))
                .service(hw_service.clone()),
        ),
        BoxService::new(
            ServiceBuilder::new()
                .concurrency_limit(10)
                .timeout(Duration::from_secs(10))
                .service(hw_service.clone()),
        ),
        BoxService::new(ServiceBuilder::new().service(hw_service)),
    ];

    // Now simulate concurrent work for each client.
    futures::stream::iter(clients.into_iter().enumerate())
        .for_each_concurrent(None, |(index, mut client)| async move {
            // Run a few rounds of request -> response for each client.
            for round in 0..3 {
                client.ready().await.expect("Client should be ready");

                // We have 3 hardware resources.
                // Request using one of them- we have 4 clients so use modulo to not go out of range.
                let hw_id = index % 3;
                let request = message::HardwareRequest::new(
                    hw_id,
                    format!(
                        "Client #{} wants hardware resource #{} -- round #{}",
                        index, hw_id, round
                    ),
                );

                info!(%index, ?request, "Sending request");

                match client.call(request).await {
                    Ok(response) => info!(%index, ?response, "Response OK"),
                    Err(e) => error!(%index, ?e, "Response problem!"),
                }
            }
        })
        .await;

    Ok(())
}
