use std::any::Any;

use thiserror::Error;
use tokio::sync::{broadcast, mpsc};

use crate::stream::StreamError;

#[derive(Error, Debug)]
pub enum RedisError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArgs(&'static str),
    #[error("ERR syntax error")]
    Syntax,
    #[error("{0}")]
    Stream(#[from] StreamError),
    #[error("Internal server error")]
    Mpsc(mpsc::error::SendError<Box<dyn Any + Send>>),
    #[error("ERR Internal server error")]
    Broadcast(broadcast::error::SendError<Box<dyn Any + Send>>),
    #[error("Internal server error")]
    InternalError,
    #[error("ERR value is not an integer or out of range")]
    OutOfRange,
    #[error("ERR invalid expire time in '{0}' command")]
    InvalidExpiration(&'static str),
    #[error("ERR unknown subcommand '{0}'. Try CONFIG HELP.")]
    InvalidConfigCommand(String),
    #[error("ERR {0} not allowed")]
    CommandNotAllowed(&'static str),
    #[error("ERR timeout is not an integer or is out of range")]
    InvalidTimeout,
    #[error("ERR timeout is negative")]
    NegativeTimeout,
    #[error("ERR internal server error")]
    TypedInternalError(#[from] InternalError),
}

impl<T: Send + 'static> From<mpsc::error::SendError<T>> for RedisError {
    fn from(err: mpsc::error::SendError<T>) -> Self {
        let boxed_value = Box::new(err) as Box<dyn Any + Send>;
        RedisError::Mpsc(mpsc::error::SendError(boxed_value))
    }
}

impl<T: Send + 'static> From<broadcast::error::SendError<T>> for RedisError {
    fn from(err: broadcast::error::SendError<T>) -> Self {
        let boxed_value = Box::new(err) as Box<dyn Any + Send>;
        RedisError::Mpsc(mpsc::error::SendError(boxed_value))
    }
}

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("Tried to run a master function on a replica")]
    MasterCommandOnReplica,
    #[error("Tried to run a replica function on a master")]
    ReplicaCommandOnMaster,
    #[error("Failed to send subscription message")]
    SubscriptionSendFailure,
}
