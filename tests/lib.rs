extern crate futures;
extern crate serde;
extern crate slab;
extern crate tokio;
extern crate tower_service;
#[macro_use]
extern crate serde_derive;
extern crate async_bincode;
extern crate tokio_tower;

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

mod multiplex;
mod pipeline;
