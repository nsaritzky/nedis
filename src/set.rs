use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    command_handler::CommandHandler, db_item::DbItem, db_value::DbValue, error::RedisError, response::Response, shard_map::ShardMapEntry, state::{ConnectionState, ServerState}
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
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() <= 3 {
            return Err(RedisError::WrongArgs("SADD"));
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
                    return Err(RedisError::WrongType);
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

        let resp = Response::Int(size as isize);
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
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() <= 3 {
            return Err(RedisError::WrongArgs("SREM"));
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
                    return Err(RedisError::WrongType);
                }
                if result > 0 {
                    item.update_timestamp();
                }
            }
            ShardMapEntry::Vacant(_) => result = 0,
        };

        let resp = Response::Int(result as isize);
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
    ) -> Result<Vec<Bytes>, RedisError> {
        let [_, key, entry] = <[String; 3]>::try_from(args)
            .map_err(|_| RedisError::WrongArgs("SISMEMBER"))?;

        let value = server_state.db.get(&key).await;
        let result = match value.as_deref() {
            Some(DbValue::Set(set)) => set.contains(&entry),
            Some(_) => return Err(RedisError::WrongType),
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
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 3 {
            return Err(RedisError::WrongArgs("SINTER"))
        }
        let [_, key1, key2] =
            <[String; 3]>::try_from(args).unwrap();

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
                (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => return Err(RedisError::WrongType),
            }
        }).await?;

        let resp: Response = result.into_iter().collect();

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
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 2 {
            return Err(RedisError::WrongArgs("SCARD"));
        }
        let value = server_state.db.get(&args[1]).await;

        let result = match value.as_deref() {
            Some(DbValue::Set(set)) => set.len(),
            Some(_) => return Err(RedisError::WrongType),
            None => 0,
        };

        Ok(vec![Response::Int(result as isize).to_bytes()])
    }
}
