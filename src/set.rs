use std::collections::HashSet;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler,
    db_item::DbItem,
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

        let size;
        match server_state.db.entry(key).await {
            ShardMapEntry::Occupied(mut occ) => {
                let item = occ.get_mut();
                let mut update_flag = false;
                if let DbValue::Set(ref mut set) = item.value_mut() {
                    for member in members {
                        if set.insert(member) {
                            update_flag = true;
                        };
                    }
                    size = set.len()
                } else {
                    bail!("SADD: Value at key is not a set")
                }
                if update_flag {
                    item.update_timestamp();
                }
            }
            ShardMapEntry::Vacant(vac) => {
                size = members.len();
                vac.insert(DbItem::new(
                    DbValue::Set(members.into_iter().collect()),
                    None,
                ));
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

        let result;
        match server_state.db.entry(key).await {
            ShardMapEntry::Occupied(mut occ) => {
                let item = occ.get_mut();
                if let DbValue::Set(ref mut set) = item.value_mut() {
                    result = set.extract_if(|item| members.contains(item)).count()
                } else {
                    bail!("SREM: Value at key is not a set")
                }
                if result > 0 {
                    item.update_timestamp();
                }
            }
            ShardMapEntry::Vacant(_) => result = 0,
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
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        let [_, key, entry] = <[String; 3]>::try_from(args)
            .map_err(|_| anyhow!("SISMEMBER: Wrong number of args"))?;

        let value = server_state.db.get(&key).await;
        let result = match value.as_deref() {
            Some(DbValue::Set(set)) => set.contains(&entry),
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

        let result = server_state.db.with_values(&[key1.clone(), key2.clone()], |values| {
            let item1 = values[0];
            let item2 = values[1];
            let value1 = item1.map(|it| it.value());
            let value2 = item2.map(|it| it.value());

            match (value1, value2) {
                (Some(DbValue::Set(set1)), Some(DbValue::Set(set2))) => {
                    Ok(set1.intersection(set2).cloned().collect())
                }
                (Some(DbValue::Set(_)), None) | (None, Some(DbValue::Set(_))) | (None, None) => {
                    Ok(vec![])
                }
                (Some(_), Some(_)) => bail!("SINTER: Values at keys are not sets"),
                (Some(_), None) => bail!("SINTER: Value at key {key1} is not a set"),
                (None, Some(_)) => bail!("SINTER: Value at key {key2} is not a set"),
            }
        }).await?;

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
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if let Some(key) = args.get(1) {
            let value = server_state.db.get(key).await;

            let result = match value.as_deref() {
                Some(DbValue::Set(set)) => set.len(),
                Some(_) => bail!("SCARD: Value at key is not a set"),
                None => 0,
            };

            Ok(vec![RedisResponse::Int(result as isize).to_bytes()])
        } else {
            bail!("SCARD: No key provided")
        }
    }
}
