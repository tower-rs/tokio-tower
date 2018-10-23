//! In a multiplexed protocol, the server responds to client requests in the order they complete.
//! Request IDs ([`IdentifierStore::Identifier`]) are used to match up responses with the request
//! that triggered them. This allows the server to process requests out-of-order, and eliminates
//! the application-level head-of-line blocking that pipelined protocols suffer from. Example
//! multiplexed protocols include SSH, HTTP/2, and AMQP. [This
//! page](http://250bpm.com/multiplexing) has some further details about how multiplexing protocols
//! operate.
//!
//! Note: multiplexing with the max number of in-flight requests set to 1 implies that for each
//! request, the response must be received before sending another request on the same connection.

mod client;
pub use self::client::Client;
pub use self::client::Error as ClientError;
