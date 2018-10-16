//! In a pipelined protocol, the server responds to client requests in the order they were sent.
//! Example pipelined protocols include HTTP/1.1 and Redis.
//!
//! Note: pipelining with the max number of in-flight requests set to 1 implies that for each
//! request, the response must be received before sending another request on the same connection.

mod client;
pub use self::client::Client;
