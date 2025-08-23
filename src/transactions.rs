use std::collections::hash_map::Entry;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler, db_item::DbItem, db_value::DbValue, error::RedisError, redis_value::RedisValue, state::{ConnectionState, ServerState}
};

pub struct IncrHandler;
#[async_trait]
impl CommandHandler for IncrHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 2 {
            return Err(RedisError::WrongArgs("INCR"));
        }
        let key = &args[1];

        let updated_int = server_state
            .db
            .with_entry(key.clone(), |entry| match entry {
                Entry::Occupied(mut occ) => {
                    let item = occ.get();
                    if let DbValue::String(old_val) = item.value() {
                        if let Ok(n) = old_val.parse::<isize>() {
                            occ.insert(DbItem::new(DbValue::String((n + 1).to_string()), item.expires_at()));
                            Some(n + 1)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Entry::Vacant(vac) => {
                    vac.insert(DbItem::new(DbValue::String("1".to_string()), None));
                    Some(1isize)
                }
            })
            .await;

        if let Some(n) = updated_int {
            let resp_val: RedisValue = n.into();
            Ok(vec![resp_val.to_bytes()])
        } else {
            Err(RedisError::OutOfRange)
        }
    }
}

pub struct MultiHandler;
#[async_trait]
impl CommandHandler for MultiHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        _server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        connection_state.set_transaction_active(true);
        Ok(vec!["+OK\r\n".into()])
    }
}

pub struct WatchHandler;
#[async_trait]
impl CommandHandler for WatchHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        _server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize
    ) -> Result<Vec<Bytes>, RedisError> {
        connection_state.watch_keys(args.drain(1..).collect()).await;
        Ok(vec!["+OK\r\n".into()])
    }
}

pub struct UnwatchHandler;
#[async_trait]
impl CommandHandler for UnwatchHandler {
    async fn execute(
        &self,
        _args: Vec<String>,
        _server_state: ServerState,
        mut connection_state: ConnectionState,
        _message_len: usize
    ) -> Result<Vec<Bytes>, RedisError> {
        connection_state.drain_watched_keys().await;
        Ok(vec!["$-1\r\n".into()])
    }
}
