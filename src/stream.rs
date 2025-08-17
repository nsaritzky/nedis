use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use indexmap::IndexMap;
use itertools::Itertools;
use thiserror::Error;
use tokio::time::interval;

use crate::{
    command_handler::CommandHandler,
    db_value::{DbValue, StreamElement},
    response::RedisResponse,
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
    ) -> anyhow::Result<Vec<Bytes>> {
        if let Some(key) = args.get(1) {
            server_state.db.with_entry(key.clone(), |entry| {

            let (value, _) = entry.or_insert((DbValue::Stream(vec![]), None));
            let stream_vec = value.to_stream_vec_mut().unwrap();

        if let Some(id) = args.get(2) {
            let id = match generate_and_validate_stream_id(&stream_vec[..], id) {
                Ok(id) => id,
                Err(StreamIdValidationError::Zero) => {
                    return Ok(vec![
                        "-ERR The ID specified in XADD must be greater than 0-0\r\n".into(),
                    ])
                }
                Err(StreamIdValidationError::DecreasingTimestamp)
                | Err(StreamIdValidationError::NonincreasingSequence) => {
                    return Ok(vec!["-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".into()]);
                }
                Err(e) => bail!(e),
            };

            let mut map = IndexMap::new();

            for (stream_key, value) in args.drain(3..).tuples() {
                map.insert(stream_key, value);
            }

            let result = StreamElement::new(id.clone(), map);
            stream_vec.push(result);

            Ok(vec![RedisResponse::Str(id).to_bytes()])
        } else {
            bail!("Could not get id");
        }
        }).await
        } else {
            bail!("Could not extract key");
        }
    }
}

pub struct XRangeHandler;
#[async_trait]
impl CommandHandler for XRangeHandler {
    async fn execute(
        &self,
        mut args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if args.len() != 4 {
            bail!("XRANGE: Wrong number of arguments")
        }
        if let Some(key) = args.get(1) {
            let mut db = server_state.db;

            let mut value = db.get_mut(key).await;
            if let Some((DbValue::Stream(stream_vec), _)) = value.as_deref_mut() {
                let mut drain = args.drain(2..4);
                let start = drain.next().unwrap();
                let end = drain.next().unwrap();

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
                Ok(vec![RedisResponse::from(result_vec).to_bytes()])
            } else {
                Ok(vec![RedisResponse::List(vec![]).to_bytes()])
            }
        } else {
            bail!("XADD: No key argument")
        }
    }
}

pub struct XReadHandler;
#[async_trait]
impl CommandHandler for XReadHandler {
    async fn execute(
        &self,
        args: Vec<String>,
        server_state: ServerState,
        _connection_state: ConnectionState,
        _message_len: usize,
    ) -> anyhow::Result<Vec<Bytes>> {
        if let Some(arg) = args.get(1) {
            if arg.to_ascii_lowercase() == "block" {
                let mut interval = interval(Duration::from_millis(10));

                let timeout: u64 = args[2]
                    .parse()
                    .expect("Timeout arg should be an unsigned int");

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
                    bail!("XREAD: STREAMS arg required")
                }

                let key = &args[4];
                let start = &args[5];

                let (start_timestamp, start_sequence) = {
                    let db = server_state.clone().db;
                    let value = db.get(key).await;
                    let stream_vec = value.as_deref().and_then(|(val, _)| val.to_stream_vec());
                    parse_id_with_dollar_sign(&start, stream_vec)
                };

                loop {
                    interval.tick().await;

                    if expires.is_some_and(|exp| exp < SystemTime::now()) {
                        return Ok(vec!["$-1\r\n".into()]);
                    }

                    let value = server_state.db.get(key).await;
                    if let Some((DbValue::Stream(stream_vec), _)) = value.as_deref() {
                        if let Some(last) = stream_vec.last() {
                            let (timestamp, sequence) = parse_id(&last.id);
                            if timestamp > start_timestamp
                                || (timestamp == start_timestamp && sequence > start_sequence)
                            {
                                let mut j = 0usize;

                                let (mut timestamp, mut sequence) = parse_id(&stream_vec[0].id);
                                while timestamp < start_timestamp
                                    || (timestamp == start_timestamp && sequence <= start_sequence)
                                {
                                    j += 1;

                                    (timestamp, sequence) = parse_id(&stream_vec[j].id);
                                }

                                let result_vec = gather_stream_read_results(
                                    stream_vec[j..].iter().collect(),
                                    start_timestamp,
                                    start_sequence,
                                    key.clone(),
                                );

                                let resp_value = RedisResponse::List(vec![result_vec].into());

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
            bail!("XREAD: STREAMS arg required")
        }

        if args.len() % 2 != 0 {
            bail!("XREAD: An even number of arguments after STREAMS arg is required")
        }

        let (keys, starts) = args[2..].split_at((args.len() - 2) / 2);

        let futures_vec: Vec<_> = keys
            .into_iter()
            .zip(starts)
            .map(|(k, start)| async {
                let (start_timestamp, start_sequence) = parse_id(start);
                gather_stream_read_results(
                    server_state
                        .db
                        .get(k)
                        .await
                        .as_deref()
                        .unwrap_or(&(DbValue::Stream(vec![]), None))
                        .0
                        .to_stream_vec()
                        .expect("XREAD: a given key was not to a stream")
                        .iter()
                        .collect(),
                    start_timestamp,
                    start_sequence,
                    k.clone(),
                )
            })
            .collect();

        let result_vec = join_all(futures_vec).await;
        Ok(vec![RedisResponse::List(result_vec).to_bytes()])
    }
}

fn generate_and_validate_stream_id(
    stream_vec: &[StreamElement],
    id: &str,
) -> Result<String, StreamIdValidationError> {
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
                return Err(StreamIdValidationError::Zero);
            }

            let (timestamp, sequence) = id
                .split_once("-")
                .ok_or(StreamIdValidationError::InvalidId(id.to_string()))?;

            let timestamp: u128 = timestamp
                .parse()
                .map_err(|_| StreamIdValidationError::InvalidTimestamp(timestamp.to_string()))?;

            let sequence = if sequence == "*" {
                if timestamp < last_timestamp {
                    return Err(StreamIdValidationError::DecreasingTimestamp);
                } else if timestamp == last_timestamp {
                    last_sequence + 1
                } else {
                    0
                }
            } else {
                if timestamp < last_timestamp {
                    return Err(StreamIdValidationError::DecreasingTimestamp);
                }
                let sequence = sequence
                    .parse()
                    .map_err(|_| StreamIdValidationError::InvalidSequence(sequence.to_string()))?;
                if timestamp == last_timestamp && sequence <= last_sequence {
                    return Err(StreamIdValidationError::NonincreasingSequence);
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
            let (timestamp, sequence) = id
                .split_once("-")
                .ok_or(StreamIdValidationError::InvalidId(id.to_string()))?;
            let timestamp: u128 = timestamp
                .parse()
                .map_err(|_| StreamIdValidationError::InvalidTimestamp(timestamp.to_string()))?;
            let sequence: usize = if sequence == "*" {
                if timestamp == 0 {
                    1
                } else {
                    0
                }
            } else {
                sequence
                    .parse()
                    .map_err(|_| StreamIdValidationError::InvalidSequence(sequence.to_string()))?
            };

            Ok(format!("{timestamp}-{sequence}"))
        }
    }
}

fn parse_id_with_dollar_sign(
    input: &str,
    stream_vec: Option<&Vec<StreamElement>>,
) -> (u128, usize) {
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
    (
        start_timestamp.parse().unwrap(),
        start_sequence.parse().unwrap(),
    )
}

fn gather_stream_read_results<'a>(
    stream_vec: Vec<&'a StreamElement>,
    start_timestamp: u128,
    start_sequence: usize,
    key: String,
) -> RedisResponse {
    let mut results_vec = Vec::new();

    for element in stream_vec {
        let (timestamp, sequence): (u128, usize) = {
            let (timestamp, sequence) = element.id.split_once("-").unwrap();
            (timestamp.parse().unwrap(), sequence.parse().unwrap())
        };

        if timestamp > start_timestamp
            || (timestamp == start_timestamp && sequence > start_sequence)
        {
            results_vec.push(element);
        }
    }

    RedisResponse::from_stream(key, results_vec)
}

fn parse_id(input: &str) -> (u128, usize) {
    let (start_timestamp, start_sequence) = input
        .split_once("-")
        .expect(&format!("Start id is invalid: {input}"));
    (
        start_timestamp.parse().unwrap(),
        start_sequence.parse().unwrap(),
    )
}

#[derive(Error, Debug)]
enum StreamIdValidationError {
    #[error("Invalid stream id: {0}")]
    InvalidId(String),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("Invalid sequence: {0}")]
    InvalidSequence(String),
    #[error("Decreasing timestamp")]
    DecreasingTimestamp,
    #[error("Nonincreasing sequence")]
    NonincreasingSequence,
    #[error("Zero id: 0-0")]
    Zero,
}
