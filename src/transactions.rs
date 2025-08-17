use std::collections::hash_map::Entry;

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    db_value::DbValue,
    redis_value::RedisValue,
    state::{ConnectionState, ServerState},
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
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() != 2 {
            bail!("INCR: Wrong number of args")
        }
        let key = &args[1];

        let updated_int = server_state
            .db
            .with_entry(key.clone(), |entry| match entry {
                Entry::Occupied(mut occ) => {
                    if let (DbValue::String(old_val), expired) = occ.get() {
                        if let Ok(n) = old_val.parse::<isize>() {
                            occ.insert((DbValue::String((n + 1).to_string()), *expired));
                            Some(n + 1)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Entry::Vacant(vac) => {
                    vac.insert((DbValue::String("1".to_string()), None));
                    Some(1isize)
                }
            })
            .await;

        if let Some(n) = updated_int {
            let resp_val: RedisValue = n.into();
            Ok(vec![resp_val.to_bytes()])
        } else {
            Ok(vec![
                "-ERR value is not an integer or out of range\r\n".into()
            ])
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
    ) -> anyhow::Result<Vec<Bytes>> {
        connection_state.set_transaction_active(true);
        Ok(vec!["+OK\r\n".into()])
    }
}
