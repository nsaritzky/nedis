use std::{
    collections::{hash_map::Entry, BTreeSet, VecDeque},
    time::{Duration, SystemTime},
};

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::{task, time::interval};

use crate::{
    command_handler::CommandHandler,
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
            let size_res = server_state.db
                .with_entry(key, |entry| match entry {
                    Entry::Occupied(mut occ) => {
                        if let (DbValue::List(ref mut array), _) = occ.get_mut() {
                            array.append(&mut values.into());
                            Ok(array.len())
                        } else {
                            bail!("RPUSH: Value at key is not a list")
                        }
                    }
                    Entry::Vacant(vac) => {
                        let size = values.len();
                        vac.insert((DbValue::List(values.into()), None));
                        Ok(size)
                    }
                })
                .await;

            size_res.map(|n| vec![RedisResponse::Int(n as isize).to_bytes()])
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
            .with_value(key, |(val, _)| {
                if let DbValue::List(array) = val {
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
            (drain.next(), drain.collect::<VecDeque<_>>())
        };

        if let Some(key) = key {
            let entry = server_state.db.entry(key).await;

            let size = match entry {
                ShardMapEntry::Occupied(mut occ) => {
                    if let (DbValue::List(ref mut array), _) = occ.get_mut() {
                        for s in values {
                            array.push_front(s.into())
                        }
                        array.len()
                    } else {
                        bail!("LPUSH: Value is not a list")
                    }
                }
                ShardMapEntry::Vacant(vac) => {
                    let size = values.len();
                    vac.insert((
                        DbValue::List(values.into_iter().rev().map(|s| s.into()).collect()),
                        None,
                    ));
                    size
                }
            };

            let resp = RedisResponse::Int(size as isize);
            Ok(vec![resp.to_bytes()])
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
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        let key = &args[1];
        let value = server_state.db.get(key).await;
        if let Some((DbValue::List(array), _)) = value.as_deref() {
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
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        let empty_response = "$-1\r\n";
        let key = &args[1];
        let mut db = server_state.db;

        let mut value = db.get_mut(key).await;

        if let Some((DbValue::List(ref mut array), _)) = value.as_deref_mut() {
            if let Some(n) = args.get(2) {
                let n: usize = n.parse()?;
                if n >= array.len() {
                    let resp_vec: Vec<_> = array.drain(..).collect();
                    Ok(vec![RedisResponse::from_str_vec(&resp_vec).to_bytes()])
                } else {
                    let resp_vec: Vec<_> = array.drain(..n).collect();

                    Ok(vec![RedisResponse::from_str_vec(&resp_vec).to_bytes()])
                }
            } else {
                let resp = array.pop_front();
                if let Some(resp) = resp {
                    Ok(vec![bulk_string(&resp)])
                } else {
                    Ok(vec![empty_response.into()])
                }
            }
        } else {
            Ok(vec![empty_response.into()])
        }
    }
}

pub struct BLPopHandler;
#[async_trait]
impl CommandHandler for BLPopHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        let mut interval = interval(Duration::from_millis(10));
        let key = &args[1];
        {
            let mut blocks = server_state.blocks.lock().await;

            if let Some(blocks_set) = blocks.get_mut(key) {
                blocks_set.insert((SystemTime::now(), task::id().to_string()));
            } else {
                let mut blocks_set: BTreeSet<(SystemTime, String)> = BTreeSet::new();
                blocks_set.insert((SystemTime::now(), task::id().to_string()));
                blocks.insert(key.clone(), blocks_set);
            }
        }

        let expires = if let Some(t) = args.get(2) {
            if t == "0" {
                None
            } else {
                let current_time = SystemTime::now();
                current_time.checked_add(Duration::from_secs_f64(t.parse().unwrap()))
            }
        } else {
            None
        };

        loop {
            interval.tick().await;

            if let Some(expires) = expires {
                if expires < SystemTime::now() {
                    let mut blocks = server_state.blocks.lock().await;
                    let blocks_set = blocks.get_mut(key).expect("Blocks set should exist.");
                    blocks_set.retain(|(_, id)| *id != task::id().to_string());
                    if blocks_set.is_empty() {
                        blocks.remove(key);
                    }
                    return Ok(vec!["$-1\r\n".into()]);
                }
            }

            let mut db = server_state.clone().db;
            let mut value = db.get_mut(key).await;
            if let Some((DbValue::List(ref mut array), _)) = value.as_deref_mut() {
                if !array.is_empty() {
                    let mut blocks = server_state.blocks.lock().await;
                    if let Some(blocks_set) = blocks.get_mut(key) {
                        if let Some((_, first_id)) = blocks_set.first() {
                            if *first_id == task::id().to_string() {
                                let value = array.pop_front().unwrap();
                                blocks_set.pop_first();

                                if blocks_set.is_empty() {
                                    blocks.remove(key);
                                }

                                let resp_vec = vec![
                                    RedisResponse::Str(key.clone()),
                                    RedisResponse::Str(value),
                                ];

                                let resp = RedisResponse::List(resp_vec).to_bytes();

                                return Ok(vec![resp]);
                            }
                        }
                    }
                }
            }
        }
    }
}
