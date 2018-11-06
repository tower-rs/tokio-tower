#[macro_use]
extern crate serde_derive;

#[derive(Serialize, Deserialize)]
pub struct Request {
    tag: usize,
    value: u32,
}

impl Request {
    pub fn new(val: u32) -> Self {
        Request { tag: 0, value: val }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    tag: usize,
    value: u32,
}

impl From<Request> for Response {
    fn from(r: Request) -> Response {
        Response {
            tag: r.tag,
            value: r.value,
        }
    }
}

impl Response {
    pub fn check(&self, expected: u32) {
        assert_eq!(self.value, expected);
    }

    pub fn get_tag(&self) -> usize {
        self.tag
    }
}

impl Request {
    pub fn set_tag(&mut self, tag: usize) {
        self.tag = tag;
    }
}

struct PanicError;
use std::fmt;
impl<E> From<E> for PanicError
where
    E: fmt::Debug,
{
    fn from(e: E) -> Self {
        panic!("{:?}", e)
    }
}

use tokio::prelude::*;
//use tower_service::Service;
use tower_direct_service::DirectService;

struct EchoService;
impl DirectService<Request> for EchoService {
    type Response = Response;
    type Error = ();
    type Future = future::FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Result<Async<()>, Self::Error> {
        Ok(Async::Ready(()))
    }

    fn poll_service(&mut self) -> Result<Async<()>, Self::Error> {
        Ok(Async::Ready(()))
    }

    fn poll_close(&mut self) -> Result<Async<()>, Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, r: Request) -> Self::Future {
        future::ok(Response::from(r))
    }
}

mod multiplex;
mod pipeline;
