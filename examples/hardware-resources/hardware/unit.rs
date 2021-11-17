use std::{task::Poll, time::Duration};

use anyhow::Result;
use futures::channel::oneshot;
use rand::Rng;
use tokio::sync::mpsc;
use tower::{steer::Picker, Service};
use tracing::{error, info};

use crate::message;

use super::shared::{HardwareError, HardwareFuture, HardwareResponse, HardwareResult};

pub struct HardwarePicker;
impl Picker<HardwareUnit, message::HardwareRequest> for HardwarePicker {
    fn pick(&mut self, request: &message::HardwareRequest, services: &[HardwareUnit]) -> usize {
        services
            .iter()
            .position(|unit| unit.id == request.id)
            .expect("No such id")
    }
}

// Simulates some hardware unit.
pub struct HardwareUnit {
    requests: mpsc::UnboundedSender<WrappedRequest>,
    pub(crate) id: usize,
}

// Simulated work.
async fn use_hardware(request: message::Request) -> HardwareResult {
    // Let's make responses a bit less predictable
    let random_wait_millis = rand::thread_rng().gen_range(100..=250);
    tokio::time::sleep(Duration::from_millis(random_wait_millis)).await;

    Ok(message::Response::from(request))
}

impl HardwareUnit {
    pub fn new(id: usize) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            info!(id, "Awaiting requests");
            while let Some(WrappedRequest {
                request,
                response_channel,
            }) = rx.recv().await
            {
                let response = use_hardware(request).await;
                response_channel
                    .send(response)
                    .expect("Oneshot receiver will not drop");
            }
            error!("No more requests");
        });

        Self { requests: tx, id }
    }
}

#[derive(Debug)]
struct WrappedRequest {
    request: message::Request,
    response_channel: oneshot::Sender<HardwareResult>,
}

impl Service<message::HardwareRequest> for HardwareUnit {
    type Response = HardwareResponse;
    type Error = HardwareError;
    type Future = HardwareFuture;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: message::HardwareRequest) -> Self::Future {
        let (tx, rx) = oneshot::channel();

        let message::HardwareRequest { id, inner } = request;
        assert!(id == self.id);

        let res = self.requests.send(WrappedRequest {
            request: inner,
            response_channel: tx,
        });

        Box::pin(async move {
            let _ = res?;
            let response = rx.await.expect("Oneshot sender will not drop")?;
            Ok(response)
        })
    }
}
