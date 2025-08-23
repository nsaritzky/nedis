use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use indexmap::IndexMap;
use itertools::Itertools;
use thiserror::Error;
use tokio::time::interval;

use crate::{
    command_handler::CommandHandler,
    db_item::DbItem,
    db_value::{DbValue, StreamElement},
    error::RedisError,
    response::Response,
    state::{ConnectionState, ServerState},
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
        let key = &args[1];
        server_state
            .db
            .with_entry(key.clone(), |entry| {
                let item = entry.or_insert(DbItem::new(DbValue::Stream(vec![]), None));

                let result;
                if let DbValue::Stream(ref mut stream_vec) = item.value_mut() {
                    let id = generate_and_validate_stream_id(&stream_vec[..], &args[2])?;

                    let mut map = IndexMap::new();

                    for (stream_key, value) in args.drain(3..).tuples() {
                        map.insert(stream_key, value);
                    }

                    let new_stream_element = StreamElement::new(id.clone(), map);
                    stream_vec.push(new_stream_element);

                    result = Ok(vec![Response::Str(id).to_bytes()]);
                } else {
                    result = Err(RedisError::WrongType);
                }
                item.update_timestamp();
                result
            })
            .await
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
                let (start_timestamp, start_sequence): (u128, usize) = {
                    if start == "-" {
                        (0, 0)
                    } else {
                        let (start_timestamp, start_sequence) = if start.contains("-") {
                            start.split_once("-").unwrap()
                        } else {
                            (start.as_str(), "0")
                        };
                        (
                            start_timestamp.parse().unwrap(),
                            start_sequence.parse().unwrap(),
                        )
                    }
                };
                let (end_timestamp, end_sequence): (u128, usize) = {
                    if end == "+" {
                        (u128::MAX, usize::MAX)
                    } else {
                        let (end_timestamp, end_sequence) = if end.contains("-") {
                            end.split_once("-").unwrap()
                        } else {
                            (end.as_str(), "inf")
                        };
                        (
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
                    let (timestamp, sequence) = value.id.split_once("-").unwrap();
                    let (timestamp, sequence): (u128, usize) =
                        (timestamp.parse().unwrap(), sequence.parse().unwrap());

                    if timestamp > start_timestamp
                        || (timestamp == start_timestamp && sequence >= start_sequence)
                    {
                        if timestamp > end_timestamp
                            || (timestamp == end_timestamp && sequence > end_sequence)
                        {
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
                let mut interval = interval(Duration::from_millis(10));

                let timeout: i64 = args[2].parse().map_err(|_| StreamError::InvalidTimeout)?;
                if timeout < 0 {
                    return Err(StreamError::NegativeTimeout.into());
                }
                let timeout = timeout as u64;

                let expires = if timeout > 0 {
                    Some(
                        SystemTime::now()
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

                let (start_timestamp, start_sequence) = {
                    let value = server_state.db.get(key).await;
                    let stream_vec = value.as_deref().and_then(|val| val.to_stream_vec());
                    parse_id_with_dollar_sign(&start, stream_vec)?
                };

                loop {
                    interval.tick().await;

                    if expires.is_some_and(|exp| exp < SystemTime::now()) {
                        return Ok(vec!["$-1\r\n".into()]);
                    }

                    let value = server_state.db.get(key).await;
                    if let Some(DbValue::Stream(stream_vec)) = value.as_deref() {
                        if let Some(last) = stream_vec.last() {
                            let (timestamp, sequence) = parse_id(&last.id)?;
                            if timestamp > start_timestamp
                                || (timestamp == start_timestamp && sequence > start_sequence)
                            {
                                let mut j = 0usize;

                                let (mut timestamp, mut sequence) = parse_id(&stream_vec[0].id)?;
                                while timestamp < start_timestamp
                                    || (timestamp == start_timestamp && sequence <= start_sequence)
                                {
                                    j += 1;

                                    (timestamp, sequence) = parse_id(&stream_vec[j].id)?;
                                }

                                let result_vec = gather_stream_read_results(
                                    stream_vec[j..].iter().collect(),
                                    start_timestamp,
                                    start_sequence,
                                    key.clone(),
                                )?;

                                let resp_value = Response::List(vec![result_vec].into());

                                return Ok(vec![resp_value.to_bytes()]);
                            }
                        }
                    }
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
                let (start_timestamp, start_sequence) = parse_id(start)?;
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
                    start_timestamp,
                    start_sequence,
                    k.clone(),
                )
            })
            .collect();

        let result_vec: Result<Vec<Response>, _> = join_all(futures_vec)
            .await
            .into_iter()
            .collect();
        Ok(vec![Response::List(result_vec?).to_bytes()])
    }
}

fn generate_and_validate_stream_id(
    stream_vec: &[StreamElement],
    id: &str,
) -> Result<String, StreamError> {
    if let Some(last_element) = stream_vec.last() {
        let (last_timestamp, last_sequence) = last_element.id.split_once("-").unwrap();
        let last_timestamp: u128 = last_timestamp.parse().unwrap();
        let last_sequence: usize = last_sequence.parse().unwrap();

        if id == "*" {
            let current_time = SystemTime::now();
            let since_epoch = current_time.duration_since(UNIX_EPOCH).unwrap();
            let timestamp = since_epoch.as_millis();

            let sequence = if timestamp == last_timestamp {
                last_sequence + 1
            } else {
                0
            };

            Ok(format!("{timestamp}-{sequence}"))
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

            Ok(format!("{timestamp}-{sequence}"))
        }
    } else {
        if id == "*" {
            let current_time = SystemTime::now();
            let since_epoch = current_time.duration_since(UNIX_EPOCH).unwrap();
            let timestamp = since_epoch.as_millis();

            Ok(format!("{timestamp}-0"))
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

            Ok(format!("{timestamp}-{sequence}"))
        }
    }
}

fn parse_id_with_dollar_sign(
    input: &str,
    stream_vec: Option<&Vec<StreamElement>>,
) -> Result<(u128, usize), StreamError> {
    let (start_timestamp, start_sequence) = if input == "$" {
        stream_vec
            .and_then(|stream_vec| stream_vec.last())
            .map(|entry| entry.id.split_once("-").unwrap())
            .unwrap_or(("0", "0"))
    } else {
        input
            .split_once("-")
            .expect(&format!("Start id is invalid: {input}"))
    };
    let start_timestamp = start_timestamp
        .parse()
        .map_err(|_| StreamError::InvalidId)?;
    let start_sequence = start_sequence.parse().map_err(|_| StreamError::InvalidId)?;
    Ok((start_timestamp, start_sequence))
}

fn gather_stream_read_results<'a>(
    stream_vec: Vec<&'a StreamElement>,
    start_timestamp: u128,
    start_sequence: usize,
    key: String,
) -> Result<Response, RedisError> {
    let mut results_vec = Vec::new();

    for element in stream_vec {
        let (timestamp, sequence) = element.id.split_once("-").ok_or(StreamError::InvalidId)?;
        let timestamp: u128 = timestamp.parse().map_err(|_| StreamError::InvalidId)?;
        let sequence: usize = sequence.parse().map_err(|_| StreamError::InvalidId)?;

        if timestamp > start_timestamp
            || (timestamp == start_timestamp && sequence > start_sequence)
        {
            results_vec.push(element);
        }
    }

    Ok(Response::from_stream(key, results_vec))
}

fn parse_id(input: &str) -> Result<(u128, usize), StreamError> {
    let (start_timestamp, start_sequence) = input
        .split_once("-")
        .expect(&format!("Start id is invalid: {input}"));
    let start_timestamp = start_timestamp.parse().map_err(|_| StreamError::InvalidId)?;
    let start_sequence = start_sequence.parse().map_err(|_| StreamError::InvalidId)?;
    Ok((start_timestamp, start_sequence))
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
