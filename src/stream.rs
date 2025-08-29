use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use indexmap::IndexMap;
use itertools::Itertools;
use thiserror::Error;
use tokio::{sync::oneshot, time::Instant};

use crate::{
    blocking::BlockMsg,
    command_handler::CommandHandler,
    db_item::DbItem,
    db_value::{DbValue, StreamElement, StreamId},
    error::RedisError,
    response::Response,
    state::{ConnectionState, ServerState},
    utils::sleep_until_if,
};

pub struct XADDHandler;
#[async_trait]
impl CommandHandler for XADDHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 5 {
            return Err(RedisError::WrongArgs("XADD"));
        }
        let key = args.remove(1);
        let mut item = server_state
            .db
            .get_mut_or_insert(key.clone(), DbItem::new(DbValue::Stream(vec![]), None))
            .await;
        if let DbValue::Stream(ref mut stream_vec) = item.value_mut() {
            let id = generate_and_validate_stream_id(&stream_vec[..], &args[1])?;

            let mut map = IndexMap::new();

            for (stream_key, value) in args.drain(2..).tuples() {
                map.insert(stream_key, value);
            }

            let new_stream_element = StreamElement::new(id, map);
            stream_vec.push(new_stream_element);

            let msg = BlockMsg::XReadUnblock(key, item);
            server_state.block_tx.send(msg).await?;

            Ok(vec![Response::Str(id.to_string()).to_bytes()])
        } else {
            Err(RedisError::WrongType)
        }
    }
}

pub struct XRangeHandler;
#[async_trait]
impl CommandHandler for XRangeHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArgs("XRANGE"));
        }
        let [_command, key, start, end] = args.try_into().unwrap();
        let value = server_state.db.get(&key).await;
        match value.as_deref() {
            Some(DbValue::Stream(stream_vec)) => {
                let start_id = {
                    if start == "-" {
                        StreamId::new(0, 0)
                    } else {
                        let (start_timestamp, start_sequence) = if start.contains("-") {
                            start.split_once("-").unwrap()
                        } else {
                            (start.as_str(), "0")
                        };
                        StreamId::new(
                            start_timestamp.parse().unwrap(),
                            start_sequence.parse().unwrap(),
                        )
                    }
                };
                let end_id: StreamId = {
                    if end == "+" {
                        StreamId::new(u128::MAX, usize::MAX)
                    } else {
                        let (end_timestamp, end_sequence) = if end.contains("-") {
                            end.split_once("-").unwrap()
                        } else {
                            (end.as_str(), "inf")
                        };
                        StreamId::new(
                            end_timestamp.parse().unwrap(),
                            if end_sequence == "inf" {
                                usize::MAX
                            } else {
                                end_sequence.parse().unwrap()
                            },
                        )
                    }
                };

                let mut result_vec = Vec::new();

                for value in stream_vec {
                    if value.id >= start_id {
                        if value.id > end_id {
                            break;
                        }
                        result_vec.push(value);
                    }
                }
                Ok(vec![Response::from(result_vec).to_bytes()])
            }
            Some(_) => return Err(RedisError::WrongType),
            None => Ok(vec![Response::Empty.to_bytes()]),
        }
    }
}

pub struct XReadHandler;
#[async_trait]
impl CommandHandler for XReadHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        mut server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> Result<Vec<Bytes>, RedisError> {
        if args.len() < 4 {
            return Err(RedisError::WrongArgs("XREAD"));
        }
        if let Some(arg) = args.get(1) {
            if arg.to_ascii_lowercase() == "block" {
                let timeout: i64 = args[2].parse().map_err(|_| StreamError::InvalidTimeout)?;
                if timeout < 0 {
                    return Err(StreamError::NegativeTimeout.into());
                }
                let timeout = timeout as u64;

                let expires = if timeout > 0 {
                    Some(
                        Instant::now()
                            .checked_add(Duration::from_millis(timeout))
                            .unwrap(),
                    )
                } else {
                    None
                };

                if args[3].to_ascii_uppercase() != "STREAMS" {
                    return Err(RedisError::Syntax);
                }

                let key = &args[4];
                let start = &args[5];

                // Extract the timestamp, sequence number, and list offset to look at.
                // If there are already results in scope, return immediately rather than block
                let (start_id, offset) = {
                    let value = server_state.db.get(key).await;
                    let stream_vec = value.as_deref().and_then(|val| val.to_stream_vec());
                    let start_id = parse_id_with_dollar_sign(&start, stream_vec)?;
                    let mut offset = 0;
                    if let Some(stream_vec) = stream_vec {
                        while offset < stream_vec.len() && stream_vec[offset].id <= start_id {
                            offset += 1;
                        }
                        println!("{offset}, {stream_vec:?}");
                        if offset < stream_vec.len() {
                            let result_vec = gather_stream_read_results(
                                stream_vec[offset..].iter().collect(),
                                start_id,
                                key.clone(),
                            )?;
                            let resp = Response::List(vec![result_vec]);
                            return Ok(vec![resp.to_bytes()]);
                        }
                    }
                    (start_id, offset)
                };

                let (returner_tx, returner_rx) = oneshot::channel();
                let msg = BlockMsg::XRead {
                    key: key.to_string(),
                    expires,
                    id: start_id,
                    offset,
                    returner: returner_tx,
                };
                server_state.block_tx.send(msg).await?;

                tokio::select! {
                    results = returner_rx => {
                        if let Ok(results) = results {
                            let resp_value = Response::List(vec![Response::from_stream_owned(key.to_string(), results)]);
                            return Ok(vec![resp_value.to_bytes()]);
                        }
                    }
                    _ = sleep_until_if(expires) => return Ok(vec![Response::NilArr.to_bytes()]),
                }
            }
        }

        if args
            .get(1)
            .is_none_or(|s| s.to_ascii_uppercase() != "STREAMS")
        {
            return Err(RedisError::Syntax);
        }

        if args.len() % 2 != 0 {
            return Err(RedisError::WrongArgs("XREAD"));
        }

        let (keys, starts) = args[2..].split_at((args.len() - 2) / 2);

        let futures_vec: Vec<_> = keys
            .into_iter()
            .zip(starts)
            .map(|(k, start)| async {
                let start_id = parse_id(start)?;
                gather_stream_read_results(
                    server_state
                        .clone()
                        .db
                        .get(k)
                        .await
                        .as_deref()
                        .unwrap_or(&DbValue::Stream(vec![]))
                        .to_stream_vec()
                        .ok_or(RedisError::WrongType)?
                        .iter()
                        .collect(),
                    start_id,
                    k.clone(),
                )
            })
            .collect();

        let result_vec: Result<Vec<Response>, _> =
            join_all(futures_vec).await.into_iter().collect();
        Ok(vec![Response::List(result_vec?).to_bytes()])
    }
}

fn generate_and_validate_stream_id(
    stream_vec: &[StreamElement],
    id: &str,
) -> Result<StreamId, StreamError> {
    if let Some(last_element) = stream_vec.last() {
        let (last_timestamp, last_sequence) = last_element.id.into();

        if id == "*" {
            let current_time = SystemTime::now();
            let since_epoch = current_time.duration_since(UNIX_EPOCH).unwrap();
            let timestamp = since_epoch.as_millis();

            let sequence = if timestamp == last_timestamp {
                last_sequence + 1
            } else {
                0
            };

            Ok(StreamId::new(timestamp, sequence))
        } else {
            if id == "0-0" {
                return Err(StreamError::IdPositive);
            }

            let (timestamp, sequence) = id.split_once("-").ok_or(StreamError::InvalidId)?;

            let timestamp: u128 = timestamp.parse().map_err(|_| StreamError::InvalidId)?;

            let sequence = if sequence == "*" {
                if timestamp < last_timestamp {
                    return Err(StreamError::IdMonotonic);
                } else if timestamp == last_timestamp {
                    last_sequence + 1
                } else {
                    0
                }
            } else {
                if timestamp < last_timestamp {
                    return Err(StreamError::IdMonotonic);
                }
                let sequence = sequence.parse().map_err(|_| StreamError::InvalidId)?;
                if timestamp == last_timestamp && sequence <= last_sequence {
                    return Err(StreamError::IdMonotonic);
                }
                sequence
            };

            Ok(StreamId::new(timestamp, sequence))
        }
    } else {
        if id == "*" {
            let current_time = SystemTime::now();
            let since_epoch = current_time.duration_since(UNIX_EPOCH).unwrap();
            let timestamp = since_epoch.as_millis();

            Ok(StreamId::new(timestamp, 0))
        } else {
            let (timestamp, sequence) = id.split_once("-").ok_or(StreamError::InvalidId)?;
            let timestamp: u128 = timestamp.parse().map_err(|_| StreamError::InvalidId)?;
            let sequence: usize = if sequence == "*" {
                if timestamp == 0 {
                    1
                } else {
                    0
                }
            } else {
                sequence.parse().map_err(|_| StreamError::InvalidId)?
            };

            Ok(StreamId::new(timestamp, sequence))
        }
    }
}

fn parse_id_with_dollar_sign(
    input: &str,
    stream_vec: Option<&Vec<StreamElement>>,
) -> Result<StreamId, StreamError> {
    if input == "$" {
        Ok(stream_vec
            .and_then(|stream_vec| stream_vec.last())
            .map(|entry| entry.id)
            .unwrap_or(StreamId::new(0, 0)))
    } else {
        let (timestamp, sequence) = input.split_once("-").ok_or(StreamError::InvalidId)?;
        Ok(StreamId::new(
            timestamp.parse().map_err(|_| StreamError::InvalidId)?,
            sequence.parse().map_err(|_| StreamError::InvalidId)?,
        ))
    }
}

fn gather_stream_read_results<'a>(
    stream_vec: Vec<&'a StreamElement>,
    start_id: StreamId,
    key: String,
) -> Result<Response, RedisError> {
    let mut results_vec = Vec::new();

    for element in stream_vec {
        if element.id >= start_id {
            results_vec.push(element);
        }
    }
    Ok(Response::from_stream(key, &results_vec[..]))
}

fn parse_id(input: &str) -> Result<StreamId, StreamError> {
    let (start_timestamp, start_sequence) = input.split_once("-").ok_or(StreamError::InvalidId)?;
    let start_timestamp = start_timestamp
        .parse()
        .map_err(|_| StreamError::InvalidId)?;
    let start_sequence = start_sequence.parse().map_err(|_| StreamError::InvalidId)?;
    Ok(StreamId::new(start_timestamp, start_sequence))
}

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    IdPositive,
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    IdMonotonic,
    #[error("ERR Invalid stream ID specified as stream command argument")]
    InvalidId,
    #[error("ERR timeout is not an integer or out of range")]
    InvalidTimeout,
    #[error("ERR timeout is negative")]
    NegativeTimeout,
}
