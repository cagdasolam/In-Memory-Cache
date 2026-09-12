pub mod aof;
pub mod cmd;
pub mod connection;
pub mod db;
pub mod frame;
pub mod pubsub;

pub use aof::Aof;
pub use cmd::Command;
pub use connection::Connection;
pub use db::Db;
pub use frame::Frame;
pub use pubsub::PubSub;

/// Convenient type alias for generic error handling across the crate.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Convenient type alias for Results returning crate::Error.
pub type Result<T> = std::result::Result<T, Error>;
