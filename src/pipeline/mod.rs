//! In a pipelined protocol, the server responds to client requests in the order they were sent.
//! Many requests can be in flight at the same time, but no request sees a response until all
//! previous requests have been satisfied. Pipelined protocols can experience head-of-line
//! blocking wherein a slow-to-process request prevents any subsequent request from being
//! processed, but are often to easier to implement on the server side, and provide clearer request
//! ordering semantics. Example pipelined protocols include HTTP/1.1, MySQL, and Redis.
//!
//! Note: pipelining with the max number of in-flight requests set to 1 implies that for each
//! request, the response must be received before sending another request on the same connection.

mod client;
pub use self::client::Client;
pub use self::client::Error as ClientError;
mod server;
pub use self::server::Error as ServerError;
pub use self::server::Server;
