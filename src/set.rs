use std::collections::HashSet;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    db_value::DbValue,
    response::RedisResponse,
    shard_map::ShardMapEntry,
    state::{ConnectionState, ServerState},
};

pub struct SADDHandler;
#[async_trait]
impl CommandHandler for SADDHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() <= 3 {
            bail!("SADD: Not enough arguments")
        }
        let mut drain = args.drain(1..);
        let key = drain.next().unwrap();
        let members: HashSet<_> = drain.collect();

        let size = match server_state.db.entry(key).await {
            ShardMapEntry::Occupied(mut occ) => {
                if let (DbValue::Set(ref mut set), _) = occ.get_mut() {
                    for member in members {
                        set.insert(member);
                    }
                    set.len()
                } else {
                    bail!("SADD: Value at key is not a set")
                }
            }
            ShardMapEntry::Vacant(vac) => {
                let size = members.len();
                vac.insert((DbValue::Set(members.into_iter().collect()), None));
                size
            }
        };

        let resp = RedisResponse::Int(size as isize);
        Ok(vec![resp.to_bytes()])
    }
}

pub struct SREMHandler;
#[async_trait]
impl CommandHandler for SREMHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() <= 3 {
            bail!("SREM: Not enough arguments")
        }
        let mut drain = args.drain(1..);
        let key = drain.next().unwrap();
        let members: HashSet<_> = drain.collect();

        let result = match server_state.db.entry(key).await {
            ShardMapEntry::Occupied(mut occ) => {
                if let (DbValue::Set(ref mut set), _) = occ.get_mut() {
                    set.extract_if(|item| members.contains(item)).count()
                } else {
                    bail!("SREM: Value at key is not a set")
                }
            }
            ShardMapEntry::Vacant(_) => 0,
        };

        let resp = RedisResponse::Int(result as isize);
        Ok(vec![resp.to_bytes()])
    }
}

pub struct SISMEMBERHandler;
#[async_trait]
impl CommandHandler for SISMEMBERHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        let [_, key, entry] = <[String; 3]>::try_from(args)
            .map_err(|_| anyhow!("SISMEMBER: Wrong number of args"))?;

        let value = server_state.db.get(&key).await;
        let result = match value.as_deref() {
            Some((DbValue::Set(set), _)) => set.contains(&entry),
            Some(_) => bail!("SISMEMBER: Value at key is not a set"),
            None => false,
        };
        if result {
            Ok(vec![":1\r\n".into()])
        } else {
            Ok(vec![":0\r\n".into()])
        }
    }
}

pub struct SINTERHandler;
#[async_trait]
impl CommandHandler for SINTERHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() != 3 {
            bail!("SINTER: Wrong number of args")
        }
        let [_, key1, key2] =
            <[String; 3]>::try_from(args).map_err(|_| anyhow!("SINTER: Wrong number of args"))?;

        let value1 = server_state.db.get(&key1).await;
        let value2 = server_state.db.get(&key2).await;

        let result = match (value1.as_deref(), value2.as_deref()) {
            (Some((DbValue::Set(set1), _)), Some((DbValue::Set(set2), _))) => {
                set1.intersection(set2).collect()
            }
            (Some((DbValue::Set(_), _)), None)
            | (None, Some((DbValue::Set(_), _)))
            | (None, None) => {
                vec![]
            }
            (Some(_), Some(_)) => bail!("SINTER: Values at keys are not sets"),
            (Some(_), None) => bail!("SINTER: Value at key {key1} is not a set"),
            (None, Some(_)) => bail!("SINTER: Value at key {key2} is not a set"),
        };

        let resp: RedisResponse = result.into_iter().collect();

        Ok(vec![resp.to_bytes()])
    }
}

pub struct SCARDHandler;
#[async_trait]
impl CommandHandler for SCARDHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if let Some(key) = args.get(1) {
            let value = server_state.db.get(key).await;

            let result = match value.as_deref() {
                Some((DbValue::Set(set), _)) => set.len(),
                Some(_) => bail!("SCARD: Value at key is not a set"),
                None => 0,
            };

            Ok(vec![RedisResponse::Int(result as isize).to_bytes()])
        } else {
            bail!("SCARD: No key provided")
        }
    }
}
