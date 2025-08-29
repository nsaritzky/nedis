use crate::command_handler::CommandHandler;
use crate::error::RedisError;
use crate::response::Response;
use crate::state::{ConnectionState, ServerState};
use async_trait::async_trait;
use bytes::Bytes;

pub struct GeoAddHandler;
#[async_trait]
impl CommandHandler for GeoAddHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        _server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 5 {
            return Err(RedisError::WrongArgs("GEOADD"));
        }
        Ok(vec![Response::Int(1).to_bytes()])
    }
}
