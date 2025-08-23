use async_trait::async_trait;
use bytes::Bytes;

use crate::{error::RedisError, state::{ConnectionState, ServerState}};

#[async_trait]
pub trait CommandHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        mut connection_state: ConnectionState,
        message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError>;
}
