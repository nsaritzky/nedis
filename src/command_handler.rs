use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    error::RedisError,
    state::{ConnectionState, ServerState},
};

#[async_trait]
pub trait CommandHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        connection_state: ConnectionState,
        message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError>;
}
