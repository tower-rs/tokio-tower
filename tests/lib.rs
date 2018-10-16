#[macro_use]
extern crate futures;
extern crate bincode;
extern crate tower_service;

#[cfg(test)]
extern crate tokio;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate async_bincode;

#[derive(Serialize, Deserialize)]
pub struct Request;

#[derive(Serialize, Deserialize)]
pub struct Response;

mod pipeline;
