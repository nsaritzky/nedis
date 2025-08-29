use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::interval;

use crate::{
    command_handler::CommandHandler,
    error::RedisError,
    response::Response,
    state::{ConnectionState, ConnectionType, ServerState},
    EMPTY_RDB_BYTES, GLOBAL_CONFIG,
};

pub struct PSyncHandler;
#[async_trait]
impl CommandHandler for PSyncHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if let Some(master_state) = server_state.clone().master_state() {
            let global_config = GLOBAL_CONFIG.get().unwrap();
            let master_config = global_config.get_master_config().unwrap();

            let new_replica_id = master_state.create_replica().await;
            connection_state.set_replica_id(new_replica_id)?;
            connection_state.set_connection_type(ConnectionType::MasterToReplica);
            Ok(vec![
                format!("+FULLRESYNC {} 0\r\n", master_config.replication_id).into(),
                EMPTY_RDB_BYTES.clone(),
            ])
        } else {
            Err(RedisError::CommandNotAllowed("PSYNC"))
        }
    }
}

pub struct ReplConfHandler;
#[async_trait]
impl CommandHandler for ReplConfHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        connection_state: ConnectionState,
        message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if let Some(s) = args.get(1) {
            match s.to_ascii_uppercase().as_str() {
                "GETACK" => {
                    let offset = server_state.load_offset() - message_len;
                    let resp = Response::List(vec![
                        Response::Str("REPLCONF".to_string()),
                        Response::Str("ACK".to_string()),
                        Response::Str(offset.to_string()),
                    ]);
                    Ok(vec![resp.to_bytes()])
                }
                "ACK" => {
                    let offset: usize = args
                        .get(2)
                        .and_then(|s| s.parse().ok())
                        .expect("ACK: Invalid offset");

                    if let Some(master_state) = server_state.master_state() {
                        if let Some(replica_id) = connection_state.get_replica_id() {
                            master_state.update_offset_tracker(replica_id, offset).await;
                            Ok(vec![])
                        } else {
                            return Err(RedisError::CommandNotAllowed("REPLCONF"));
                        }
                    } else {
                        return Err(RedisError::CommandNotAllowed("REPLCONF"));
                    }
                }
                _ => Ok(vec![Response::Ok.to_bytes()]),
            }
        } else {
            Ok(vec![Response::Ok.to_bytes()])
        }
    }
}

pub struct WaitHandler;
#[async_trait]
impl CommandHandler for WaitHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 3 {
            return Err(RedisError::WrongArgs("WAIT"));
        }
        if let Some(master_state) = server_state.clone().master_state() {
            let GETACK_MSG_LEN = 37;
            let n = args[1]
                .parse::<usize>()
                .map_err(|_| RedisError::InvalidTimeout)?;
            let timeout = args[2]
                .parse::<i64>()
                .map_err(|_| RedisError::InvalidTimeout)?;
            if timeout < 0 {
                return Err(RedisError::NegativeTimeout);
            }

            let expires = SystemTime::now()
                .checked_add(Duration::from_millis(timeout as u64))
                .expect("WAIT: Invalid expiration");

            let mut interval = interval(Duration::from_millis(10));

            master_state.broadcast_to_replicas(
                "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".into(),
            )?;

            // The master offset is not updated while WAIT blocks
            let master_offset = server_state.load_offset();

            loop {
                interval.tick().await;

                let offsets = &master_state.get_replica_tracker().await.offsets;

                let caught_up_replicas = offsets.iter().fold(0usize, |acc, (_, offset)| {
                    if *offset == master_offset - GETACK_MSG_LEN || *offset == master_offset {
                        acc + 1
                    } else {
                        acc
                    }
                });

                if expires < SystemTime::now() || n <= caught_up_replicas {
                    return Ok(vec![format!(":{caught_up_replicas}\r\n").into()]);
                }
            }
        }
        Err(RedisError::CommandNotAllowed("WAIT"))
    }
}
