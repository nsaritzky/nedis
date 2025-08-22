use std::time::Duration;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future;
use tokio::{
    sync::oneshot,
    time::{sleep_until, Instant},
};

use crate::{
    blocking::BlockMsg,
    command_handler::CommandHandler,
    db_item::DbItem,
    db_value::DbValue,
    response::RedisResponse,
    shard_map::ShardMapEntry,
    state::{ConnectionState, ServerState},
    utils::bulk_string,
};

pub struct RPushHandler;
#[async_trait]
impl CommandHandler for RPushHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        let (key, values) = {
            let mut drain = args.drain(1..);
            (drain.next(), drain.collect::<Vec<_>>())
        };

        if let Some(key) = key {
            let (mut item, size) = match server_state.db.entry(key.clone()).await {
                ShardMapEntry::Occupied(mut occ) => {
                    let value = occ.get_mut().value_mut();
                    if let DbValue::List(list) = value {
                        list.append(&mut values.into());
                        let size = list.len();
                        (occ.guard(), size)
                    } else {
                        bail!("RPUSH: Value at key is not a list")
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

            Ok(vec![RedisResponse::Int(size as isize).to_bytes()])
        } else {
            bail!("Bad key: {key:?}");
        }
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
    ) -> anyhow::Result<Vec<Bytes>> {
        let empty_array_response = RedisResponse::List(vec![]).to_bytes();
        let key = &args[1];

        let db = server_state.db;

        let result = db
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
                            Ok(vec![empty_array_response])
                        } else if b > array.len() - 1 {
                            let resp = RedisResponse::List(
                                array
                                    .range(a..)
                                    .map(|s| RedisResponse::Str(s.clone()))
                                    .collect(),
                            );
                            Ok(vec![resp.to_bytes()])
                        } else {
                            let resp = RedisResponse::List(
                                array
                                    .range(a..=b)
                                    .map(|s| RedisResponse::Str(s.clone()))
                                    .collect(),
                            );
                            Ok(vec![resp.to_bytes()])
                        }
                    } else {
                        bail!("Failed to get array bounds");
                    }
                } else {
                    Ok(vec![empty_array_response])
                }
            })
            .await;

        if let Some(Ok(resp)) = result {
            Ok(resp)
        } else {
            Ok(vec!["*0\r\n".into()])
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
    ) -> anyhow::Result<Vec<Bytes>> {
        let (key, values) = {
            let mut drain = args.drain(1..);
            (drain.next(), drain.collect::<Vec<_>>())
        };

        if let Some(key) = key {
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
                        bail!("LPUSH: Value at key is not a list")
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

            Ok(vec![RedisResponse::Int(size as isize).to_bytes()])
        } else {
            bail!("Bad key: {key:?}");
        }
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
    ) -> anyhow::Result<Vec<Bytes>> {
        let key = &args[1];
        let value = server_state.db.get(key).await;
        if let Some(DbValue::List(array)) = value.as_deref() {
            let size = array.len() as isize;
            let resp = RedisResponse::Int(size);

            Ok(vec![resp.to_bytes()])
        } else {
            let zero_resp = RedisResponse::Int(0);

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
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() < 2 {
            bail!("LPOP: Wrong number of arguments")
        }
        let empty_response = "$-1\r\n";
        let key = &args[1];
        let n: Option<usize> = args.get(2).and_then(|s| s.parse().ok());

        let result;
        if let Some(mut item) = server_state.db.get_mut(key).await {
            let value = item.value_mut();
            if let DbValue::List(ref mut array) = value {
                if array.len() == 0 {
                    return Ok(vec![empty_response.into()]);
                } else if let Some(n) = n {
                    let resp_vec: RedisResponse = if n >= array.len() {
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
                bail!("BPOP: Value at key is not a list")
            }
            item.update_timestamp();
        } else {
            return Ok(vec![empty_response.into()]);
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
    ) -> anyhow::Result<Vec<Bytes>> {
        let key = &args[1];

        if let Some(mut item) = server_state.db.get_mut(key).await {
            if let DbValue::List(list) = item.value_mut() {
                if let Some(result) = list.pop_front() {
                    item.update_timestamp();
                    let resp: RedisResponse = vec![key.clone(), result].into_iter().collect();
                    return Ok(vec![resp.to_bytes()]);
                }
            } else {
                bail!("BLPOP: Value at key is not a list")
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
                    let resp: RedisResponse = vec![key.clone(), value].into_iter().collect();
                    Ok(vec![resp.to_bytes()])
                }).map_err(|_| anyhow!("BLPOP: Failed to receive return value from blocking handler")),
            _ = sleep_until_if(expires) => Ok(vec!["$-1\r\n".into()])
        }
    }
}

async fn sleep_until_if(until: Option<Instant>) {
    if let Some(instant) = until {
        sleep_until(instant).await
    } else {
        future::pending::<()>().await
    }
}
