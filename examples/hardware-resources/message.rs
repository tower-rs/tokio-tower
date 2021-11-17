use serde_derive::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize)]
pub struct HardwareRequest {
    pub(crate) id: usize,
    pub(crate) inner: Request,
}

impl HardwareRequest {
    pub fn new(id: usize, message: String) -> Self {
        Self {
            id,
            inner: Request::new(message),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    message: String,
}

impl Request {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    message: String,
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<Request> for Response {
    fn from(request: Request) -> Self {
        Self {
            // Simply respond by SCREAMING back.
            message: request.message.to_ascii_uppercase(),
        }
    }
}
