extern crate futures;
//extern crate bincode;
extern crate tower_service;

#[cfg(test)]
extern crate tokio;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate async_bincode;

extern crate tokio_tower;

#[derive(Serialize, Deserialize)]
pub struct Request(u32);

#[derive(Serialize, Deserialize)]
pub struct Response(u32);

impl From<Request> for Response {
    fn from(r: Request) -> Response {
        Response(r.0)
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

mod pipeline;
