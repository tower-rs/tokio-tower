use std::{collections::HashMap, task::Poll};

use crate::message;
use anyhow::Result;
use tower::{buffer::Buffer, Service, ServiceExt};

use super::{
    shared::{HardwareError, HardwareFuture, HardwareResponse},
    unit::HardwareUnit,
};

// Manages some exclusive hardware resources.
pub struct HardwareManager {
    // TODO:
    // Could we use tower::steer::Steer for something like this?
    // Since this is essentially just mapping the hardware ID
    // to one of the contained services.
    units: HashMap<usize, Buffer<HardwareUnit, message::Request>>,
}

impl HardwareManager {
    pub async fn new(num_units: usize) -> Result<Self> {
        let mut units = HashMap::with_capacity(num_units);

        for id in 0..num_units {
            units.insert(id, Buffer::new(HardwareUnit::new(id).await?, 32));
        }

        Ok(Self { units })
    }
}

impl Service<message::HardwareRequest> for HardwareManager {
    type Response = HardwareResponse;
    type Error = HardwareError;
    type Future = HardwareFuture;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: message::HardwareRequest) -> Self::Future {
        let message::HardwareRequest { id, inner } = request;

        // Since hardware units are buffered,
        // we can clone the handle and move it into the output
        // future.
        //
        // TODO: Is it be more clean to return a custom struct
        // implementing the Future trait?
        let unit = self
            .units
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("No such hardware unit"))
            .map(|hw| hw.clone());

        Box::pin(async move {
            let mut unit = unit?;
            unit.ready().await?.call(inner).await
        })
    }
}
