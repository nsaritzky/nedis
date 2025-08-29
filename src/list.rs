use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::{
    sync::oneshot,
    time::Instant,
};

use crate::{
    blocking::BlockMsg,
    command_handler::CommandHandler,
    db_item::DbItem,
    db_value::DbValue,
    error::RedisError,
    response::Response,
    shard_map::ShardMapEntry,
    state::{ConnectionState, ServerState},
    utils::{bulk_string, sleep_until_if},
};

pub struct RPushHandler;
#[async_trait]
impl CommandHandler for RPushHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 3 {
            return Err(RedisError::WrongArgs("RPUSH"));
        }
        let mut iter = args.into_iter();
        let _ = iter.next();
        let key = iter.next().unwrap();
        let values: Vec<_> = iter.collect();

        let (mut item, size) = match server_state.db.entry(key.clone()).await {
            ShardMapEntry::Occupied(mut occ) => {
                let value = occ.get_mut().value_mut();
                if let DbValue::List(list) = value {
                    list.append(&mut values.into());
                    let size = list.len();
                    (occ.guard(), size)
                } else {
                    return Err(RedisError::WrongType);
                }
            }
            ShardMapEntry::Vacant(vac) => {
                let size = values.len();
                (
                    vac.insert(DbItem::new(DbValue::List(values.into()), None)),
                    size,
                )
            }
        };

        item.update_timestamp();

        let msg = BlockMsg::BlPopUnblock(key.clone(), item);
        server_state.block_tx.send(msg).await?;

        Ok(vec![Response::Int(size as isize).to_bytes()])
    }
}

pub struct LRangeHandler;
#[async_trait]
impl CommandHandler for LRangeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArgs("LRANGE"));
        }
        let key = &args[1];

        let result = server_state
            .db
            .with_value(key, |item| {
                if let DbValue::List(array) = item.value() {
                    if let (Some(a), Some(b)) = (args.get(2), args.get(3)) {
                        let a: isize = a.parse()?;
                        let a: usize = if a >= 0 {
                            a as usize
                        } else {
                            array.len().checked_add_signed(a).unwrap_or(0)
                        };
                        let b: isize = b.parse()?;
                        let b: usize = if b >= 0 {
                            b as usize
                        } else {
                            array.len().checked_add_signed(b).unwrap_or(0)
                        };

                        if a >= array.len() {
                            Ok(vec![Response::Empty.to_bytes()])
                        } else if b > array.len() - 1 {
                            let resp = Response::List(
                                array.range(a..).map(|s| Response::Str(s.clone())).collect(),
                            );
                            Ok(vec![resp.to_bytes()])
                        } else {
                            let resp = Response::List(
                                array
                                    .range(a..=b)
                                    .map(|s| Response::Str(s.clone()))
                                    .collect(),
                            );
                            Ok(vec![resp.to_bytes()])
                        }
                    } else {
                        bail!("Failed to get array bounds");
                    }
                } else {
                    Ok(vec![Response::Empty.to_bytes()])
                }
            })
            .await;

        if let Some(Ok(resp)) = result {
            Ok(resp)
        } else {
            Ok(vec![Response::Empty.to_bytes()])
        }
    }
}

pub struct LPushHandler;
#[async_trait]
impl CommandHandler for LPushHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 3 {
            return Err(RedisError::WrongArgs("LPUSH"));
        }
        let (key, values) = {
            let mut drain = args.drain(1..);
            (drain.next().unwrap(), drain.collect::<Vec<_>>())
        };

        let (mut item, size) = match server_state.db.entry(key.clone()).await {
            ShardMapEntry::Occupied(mut occ) => {
                let value = occ.get_mut().value_mut();
                if let DbValue::List(list) = value {
                    for v in values {
                        list.push_front(v);
                    }
                    let size = list.len();
                    (occ.guard(), size)
                } else {
                    return Err(RedisError::WrongType);
                }
            }
            ShardMapEntry::Vacant(vac) => {
                let size = values.len();
                (
                    vac.insert(DbItem::new(DbValue::List(values.into()), None)),
                    size,
                )
            }
        };

        item.update_timestamp();

        let msg = BlockMsg::BlPopUnblock(key.clone(), item);
        server_state.block_tx.send(msg).await?;

        Ok(vec![Response::Int(size as isize).to_bytes()])
    }
}

pub struct LLenHandler;
#[async_trait]
impl CommandHandler for LLenHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 2 {
            return Err(RedisError::WrongArgs("LLEN"));
        }
        let key = &args[1];
        let value = server_state.db.get(key).await;
        if let Some(DbValue::List(array)) = value.as_deref() {
            let size = array.len() as isize;
            let resp = Response::Int(size);

            Ok(vec![resp.to_bytes()])
        } else {
            let zero_resp = Response::Int(0);

            Ok(vec![zero_resp.to_bytes()])
        }
    }
}

pub struct LPopHandler;
#[async_trait]
impl CommandHandler for LPopHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 2 {
            return Err(RedisError::WrongArgs("LPOP"));
        }
        let key = &args[1];
        let n: Option<usize> = args.get(2).and_then(|s| s.parse().ok());

        let result;
        if let Some(mut item) = server_state.db.get_mut(key).await {
            let value = item.value_mut();
            if let DbValue::List(ref mut array) = value {
                if array.len() == 0 {
                    return Ok(vec![Response::NilStr.to_bytes()]);
                } else if let Some(n) = n {
                    let resp_vec: Response = if n >= array.len() {
                        array.drain(..).collect()
                    } else {
                        array.drain(..n).collect()
                    };
                    result = Ok(vec![resp_vec.to_bytes()])
                } else {
                    let result_value = array.pop_front().unwrap();
                    result = Ok(vec![bulk_string(&result_value)]);
                }
            } else {
                return Err(RedisError::WrongType);
            }
            item.update_timestamp();
        } else {
            return Ok(vec![Response::NilStr.to_bytes()]);
        }

        return result;
    }
}

pub struct BLPopHandler;
#[async_trait]
impl CommandHandler for BLPopHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        let key = &args[1];

        if let Some(mut item) = server_state.db.get_mut(key).await {
            if let DbValue::List(list) = item.value_mut() {
                if let Some(result) = list.pop_front() {
                    item.update_timestamp();
                    let resp: Response = vec![key.clone(), result].into_iter().collect();
                    return Ok(vec![resp.to_bytes()]);
                }
            } else {
                return Err(RedisError::WrongType);
            }
        }

        let expires = if let Some(t) = args.get(2) {
            if t == "0" {
                None
            } else {
                let current_time = Instant::now();
                current_time.checked_add(Duration::from_secs_f64(t.parse().unwrap()))
            }
        } else {
            None
        };

        let (return_tx, return_rx) = oneshot::channel();
        let msg = BlockMsg::BlPop(key.clone(), expires, return_tx);

        server_state.block_tx.send(msg).await?;

        tokio::select! {
            result = return_rx => result.and_then(|value| {
                    let resp: Response = vec![key.clone(), value].into_iter().collect();
                    Ok(vec![resp.to_bytes()])
                }).map_err(|_| RedisError::InternalError),
            _ = sleep_until_if(expires) => Ok(vec![Response::NilArr.to_bytes()])
        }
    }
}
