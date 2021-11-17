use std::pin::Pin;

use futures::Future;

use crate::message;

// Clippy complains about complex types,
// so do a bit of indirection through aliases.
pub type HardwareResult = Result<HardwareResponse, HardwareError>;
pub type HardwareFuture = Pin<Box<dyn Future<Output = HardwareResult> + 'static + Send>>;
pub type HardwareError = tower::BoxError;
pub type HardwareResponse = message::Response;
