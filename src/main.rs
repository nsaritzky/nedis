#![allow(unused_imports)]
mod parser;
mod redis_value;
mod args;

use std::{
    collections::{btree_map::Keys, hash_map::Entry, BTreeSet, HashMap, VecDeque}, fmt::{self, Display, Formatter}, sync::{Arc, OnceLock}, time::{Duration, SystemTime, UNIX_EPOCH}
};

use crate::parser::*;
use crate::args::Args;
use anyhow::{anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use itertools::Itertools;
use num::ToPrimitive;
use redis_value::{PrimitiveRedisValue, RedisValue, StreamElement};
use thiserror::Error;
use tokio::task;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::interval,
};
use winnow::stream;

type Db = Arc<Mutex<HashMap<PrimitiveRedisValue, (RedisValue, Option<SystemTime>)>>>;
type Blocks = Arc<Mutex<HashMap<PrimitiveRedisValue, BTreeSet<(SystemTime, String)>>>>;
type Streams = Arc<Mutex<HashMap<PrimitiveRedisValue, Vec<StreamElement>>>>;

#[derive(PartialEq, Eq, Clone, Copy)]
enum InstanceType {
    Master,
    Slave
}

impl Display for InstanceType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let s = match self {
            InstanceType::Master => "master",
            InstanceType::Slave => "slave",
        };
        write!(f, "{s}")
    }
}

static INSTANCE_TYPE: OnceLock<InstanceType> = OnceLock::new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

INSTANCE_TYPE.set(if args.replicaof.is_some() {
        InstanceType::Slave
    } else {
        InstanceType::Master
    }).map_err(|_| anyhow!("Failed to set instance type"))?;

    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let blocks: Blocks = Arc::new(Mutex::new(HashMap::new()));
    let streams: Streams = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let db = Arc::clone(&db);
        let blocks = Arc::clone(&blocks);
        let streams = Arc::clone(&streams);

        tokio::spawn(async move {
            process(socket, db, blocks, streams)
                .await
                .expect("Process should be successful");
        });
    }
}

async fn process(
    mut socket: TcpStream,
    db: Db,
    blocks: Blocks,
    streams: Streams,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut transaction_active = false;
    let mut transaction_queue = VecDeque::<VecDeque<RedisValue>>::new();

    loop {
        match socket.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let results =
                    parse_value(&mut &buf[..n]).map_err(|e| anyhow!("Failed to parse: {e}"))?;
                if let RedisValue::Arr(mut v) = results {
                    if transaction_active {
                        match v[0].to_str() {
                            Some(s) if s.to_ascii_uppercase() == "EXEC" => {
                                let resp = handle_exec(
                                    &mut transaction_queue,
                                    &db,
                                    &blocks,
                                    &streams,
                                    &mut transaction_active,
                                )
                                .await?;
                                send_response(&mut socket, &resp).await?;
                            }
                            Some(s) if s.to_ascii_uppercase() == "DISCARD" => {
                                let resp = handle_discard(&mut transaction_queue, &mut transaction_active);
                                send_response(&mut socket, &resp).await?;
                            }
                            _ => {
                                let resp = queue_command(&mut transaction_queue, v);
                                send_response(&mut socket, &resp).await?;
                            }
                        }
                    } else {
                        let resp = execute_command(
                            &mut v,
                            &db,
                            &blocks,
                            &streams,
                            &mut transaction_active,
                        )
                        .await?;
                        send_response(&mut socket, &resp).await?;
                    }
                }

                buf.clear();
            }
            Err(_) => break,
        }
    }

    Ok(())
}

async fn execute_command(
    v: &mut VecDeque<RedisValue>,
    db: &Db,
    blocks: &Blocks,
    streams: &Streams,
    transaction_active: &mut bool,
) -> anyhow::Result<Bytes> {
    let mut i = 0;
    if let RedisValue::Primitive(PrimitiveRedisValue::Str(s)) = &v[i] {
        match s.to_ascii_uppercase().as_str() {
            "PING" => Ok("+PONG\r\n".into()),
            "ECHO" => {
                i += 1;
                Ok(v[i].to_bytes())
            }
            "SET" => handle_set(&db, v, &mut i).await,
            "GET" => handle_get(&db, v, &mut i).await,
            "RPUSH" => handle_rpush(&db, v, &mut i).await,
            "LRANGE" => handle_lrange(&db, v, &mut i).await,
            "LPUSH" => handle_lpush(&db, v, &mut i).await,
            "LLEN" => handle_llen(&db, v, &mut i).await,
            "LPOP" => handle_lpop(&db, v, &mut i).await,
            "BLPOP" => handle_blpop(&db, &blocks, v, &mut i).await,
            "TYPE" => handle_type(&db, &streams, v, &mut i).await,
            "XADD" => handle_xadd(&streams, v, &mut i).await,
            "XRANGE" => handle_xrange(&streams, v, &mut i).await,
            "XREAD" => handle_xread(&streams, v, &mut i).await,
            "INCR" => handle_incr(&db, v, &mut i).await,
            "MULTI" => handle_multi(transaction_active).await,
            "INFO" => handle_info(v),
            "EXEC" => Ok("-ERR EXEC without MULTI\r\n".into()),
            "DISCARD" => Ok("-ERR DISCARD without MULTI\r\n".into()),
            _ => {
                bail!("Invalid command")
            }
        }
    } else {
        bail!("Invalid arg");
    }
}

async fn handle_set(db: &Db, v: &mut VecDeque<RedisValue>, i: &mut usize) -> anyhow::Result<Bytes> {
    let (key, value) = {
        let mut iter = v.drain(*i + 1..*i + 3);
        (iter.next(), iter.next())
    };

    if let Some(RedisValue::Primitive(k)) = key {
        if let Some(value) = value {
            let mut db = db.lock().await;

            *i += 1;
            match v.get(*i) {
                Some(RedisValue::Primitive(PrimitiveRedisValue::Str(s)))
                    if s.to_ascii_uppercase() == "PX" =>
                {
                    *i += 1;

                    if let RedisValue::Primitive(PrimitiveRedisValue::Str(t)) = &v[*i] {
                        let expires_in: u64 = t.parse()?;

                        let current_time = SystemTime::now();
                        let expires = current_time
                            .checked_add(Duration::from_millis(expires_in))
                            .ok_or(anyhow!("Invalid expires durartion"))?;

                        db.insert(k, (value, Some(expires)));
                    }
                }
                _ => {
                    db.insert(k, (value, None));
                }
            }
            Ok("+OK\r\n".into())
        } else {
            bail!("SET: no value found")
        }
    } else {
        bail!("SET: bad key")
    }
}

async fn handle_get(db: &Db, v: &mut VecDeque<RedisValue>, i: &mut usize) -> anyhow::Result<Bytes> {
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        let mut db = db.lock().await;

        if let Some((value, expiry)) = db.get(key) {
            if let Some(expiry) = expiry {
                if *expiry < SystemTime::now() {
                    db.remove(key);
                    Ok("$-1\r\n".into())
                } else {
                    Ok(value.to_bytes())
                }
            } else {
                Ok(value.to_bytes())
            }
        } else {
            Ok("$-1\r\n".into())
        }
    } else {
        bail!(
            "Invalid key type: {}",
            String::from_utf8_lossy(&v[*i].to_bytes())
        );
    }
}

async fn handle_rpush(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    let (key, mut values) = {
        let mut drain = v.drain(*i + 1..);
        (drain.next(), drain.collect::<VecDeque<_>>())
    };

    if let Some(RedisValue::Primitive(key)) = key {
        let mut db = db.lock().await;

        if let Some((RedisValue::Arr(ref mut array), _)) = db.get_mut(&key) {
            array.append(&mut values);

            let resp =
                RedisValue::Primitive(PrimitiveRedisValue::Int(array.len().to_isize().unwrap()));
            Ok(resp.to_bytes())
        } else {
            let size = values.len().to_isize().unwrap();

            db.insert(key, (RedisValue::Arr(values), None));

            let resp = RedisValue::Primitive(PrimitiveRedisValue::Int(size));
            Ok(resp.to_bytes())
        }
    } else {
        bail!("Bad key: {key:?}");
    }
}

async fn handle_lrange(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;
    let empty_array_response = RedisValue::Arr(VecDeque::new()).to_bytes();

    if let RedisValue::Primitive(key) = &v[*i] {
        let db = db.lock().await;

        if let Some((RedisValue::Arr(array), _)) = db.get(key) {
            if let (
                Some(RedisValue::Primitive(PrimitiveRedisValue::Str(a))),
                Some(RedisValue::Primitive(PrimitiveRedisValue::Str(b))),
            ) = (v.get(*i + 1), v.get(*i + 2))
            {
                let a: isize = a.parse()?;
                let a: usize = if a >= 0 {
                    a.to_usize().unwrap()
                } else {
                    array.len().checked_add_signed(a).unwrap_or(0)
                };
                let b: isize = b.parse()?;
                let b: usize = if b >= 0 {
                    b.to_usize().unwrap()
                } else {
                    array.len().checked_add_signed(b).unwrap_or(0)
                };

                if a >= array.len() {
                    Ok(empty_array_response)
                } else if b > array.len() - 1 {
                    let resp = RedisValue::Arr(array.range(a..).cloned().collect());
                    Ok(resp.to_bytes())
                } else {
                    let resp = RedisValue::Arr(array.range(a..=b).cloned().collect());
                    Ok(resp.to_bytes())
                }
            } else {
                bail!("Failed to get array bounds");
            }
        } else {
            Ok(empty_array_response)
        }
    } else {
        bail!("Failed to get list key");
    }
}

async fn handle_lpush(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    let (key, mut values) = {
        let mut drain = v.drain(*i + 1..);
        (drain.next(), drain.rev().collect::<VecDeque<_>>())
    };

    if let Some(RedisValue::Primitive(key)) = key {
        let mut db = db.lock().await;

        if let Some((RedisValue::Arr(ref mut array), _)) = db.get_mut(&key) {
            values.append(array);
        }
        let size = values.len().to_isize().unwrap();
        db.insert(key, (RedisValue::Arr(values), None));

        let resp = RedisValue::Primitive(PrimitiveRedisValue::Int(size));
        Ok(resp.to_bytes())
    } else {
        bail!("Bad key: {key:?}");
    }
}

async fn handle_llen(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        let db = db.lock().await;

        if let Some((RedisValue::Arr(array), _)) = db.get(key) {
            let size = array.len().to_isize().unwrap();
            let resp = RedisValue::Primitive(PrimitiveRedisValue::Int(size));

            Ok(resp.to_bytes())
        } else {
            let zero_resp = RedisValue::Primitive(PrimitiveRedisValue::Int(0));

            Ok(zero_resp.to_bytes())
        }
    } else {
        bail!("Bad key: {:?}", v[*i]);
    }
}

async fn handle_lpop(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    let empty_response = "$-1\r\n";
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        let mut db = db.lock().await;

        if let Some((RedisValue::Arr(ref mut array), _)) = db.get_mut(key) {
            if let Some(RedisValue::Primitive(PrimitiveRedisValue::Str(n))) = v.get(*i + 1) {
                *i += 1;

                let n: usize = n.parse()?;
                if n >= array.len() {
                    let resp_vec: VecDeque<_> = array.drain(..).collect();

                    Ok(RedisValue::Arr(resp_vec).to_bytes())
                } else {
                    let resp_vec: VecDeque<_> = array.drain(..n).collect();

                    Ok(RedisValue::Arr(resp_vec).to_bytes())
                }
            } else {
                let resp = array.pop_front();
                if let Some(resp) = resp {
                    Ok(resp.to_bytes())
                } else {
                    Ok(empty_response.into())
                }
            }
        } else {
            Ok(empty_response.into())
        }
    } else {
        bail!("Bad key: {:?}", v[*i]);
    }
}

async fn handle_blpop(
    db: &Db,
    blocks: &Blocks,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    let mut interval = interval(Duration::from_millis(10));
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        {
            let mut blocks = blocks.lock().await;

            if let Some(blocks_set) = blocks.get_mut(&key) {
                blocks_set.insert((SystemTime::now(), task::id().to_string()));
            } else {
                let mut blocks_set: BTreeSet<(SystemTime, String)> = BTreeSet::new();
                blocks_set.insert((SystemTime::now(), task::id().to_string()));
                blocks.insert(key.clone(), blocks_set);
            }
        }

        let expires =
            if let Some(RedisValue::Primitive(PrimitiveRedisValue::Str(t))) = v.get(*i + 1) {
                *i += 1;
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
                    let mut blocks = blocks.lock().await;
                    let blocks_set = blocks.get_mut(&key).expect("Blocks set should exist.");
                    blocks_set.retain(|(_, id)| *id != task::id().to_string());
                    if blocks_set.is_empty() {
                        blocks.remove(&key);
                    }
                    return Ok("$-1\r\n".into());
                }
            }

            if let Ok(mut db) = db.try_lock() {
                if let Some((RedisValue::Arr(ref mut array), _)) = db.get_mut(&key) {
                    if let Some(value) = array.pop_front() {
                        let mut blocks = blocks.lock().await;
                        if let Some(blocks_set) = blocks.get_mut(&key) {
                            let (_, id) =
                                blocks_set.first().expect("Blocks set should be nonempty");

                            if *id == task::id().to_string() {
                                blocks_set.pop_first();

                                if blocks_set.is_empty() {
                                    blocks.remove(&key);
                                }

                                let resp_vec: VecDeque<_> =
                                    vec![RedisValue::Primitive(key.clone()), value].into();

                                let resp = RedisValue::Arr(resp_vec).to_bytes();

                                return Ok(resp);
                            }
                        }
                    }
                }
            }
        }
    } else {
        bail!("BLPOP: Invalid key")
    }
}

async fn handle_type(
    db: &Db,
    streams: &Streams,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;

    if let Some(RedisValue::Primitive(key)) = v.get(*i) {
        let db = db.lock().await;

        if db.get(key).is_some() {
            return Ok("+string\r\n".into());
        } else {
            let streams = streams.lock().await;
            if streams.get(key).is_some() {
                return Ok("+stream\r\n".into());
            }
        }
        Ok("+none\r\n".into())
    } else {
        bail!("Bad key");
    }
}

async fn handle_xadd(
    streams: &Streams,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;

    if let Some(RedisValue::Primitive(key)) = v.get(*i) {
        let mut streams = streams.lock().await;

        let stream_vec = if let Some(stream_vec) = streams.get_mut(key) {
            stream_vec
        } else {
            streams.insert(key.clone(), Vec::new());
            streams.get_mut(key).unwrap()
        };

        if let Some(RedisValue::Primitive(PrimitiveRedisValue::Str(id))) = v.get(*i + 1) {
            *i += 1;

            let id = match generate_and_validate_stream_id(stream_vec, id) {
                Ok(id) => id,
                Err(StreamIdValidationError::Zero) => {
                    return Ok("-ERR The ID specified in XADD must be greater than 0-0\r\n".into())
                }
                Err(StreamIdValidationError::DecreasingTimestamp)
                | Err(StreamIdValidationError::NonincreasingSequence) => {
                    return Ok("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".into());
                }
                Err(e) => bail!(e),
            };

            let mut args: Vec<_> = v.drain(*i + 1..).collect();

            let mut map = HashMap::new();

            for (stream_key, value) in args.drain(..).tuples() {
                if let RedisValue::Primitive(stream_key) = stream_key {
                    map.insert(stream_key, value);
                }
            }

            let result = StreamElement::new(id.clone(), map);
            stream_vec.push(result);

            Ok(RedisValue::Primitive(PrimitiveRedisValue::Str(id)).to_bytes())
        } else {
            bail!("Could not get id");
        }
    } else {
        bail!("Could not extract key");
    }
}

async fn handle_xrange(
    streams: &Streams,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;

    if let Some(RedisValue::Primitive(key)) = v.get(*i) {
        let streams = streams.lock().await;

        if let Some(stream_vec) = streams.get(key) {
            let mut drain = v.drain(*i + 1..*i + 3);
            let start = drain.next().unwrap();
            let end = drain.next().unwrap();

            let (start_timestamp, start_sequence): (u128, usize) = {
                let start = start.to_str().unwrap();
                if start == "-" {
                    (0, 0)
                } else {
                    let (start_timestamp, start_sequence) = if start.contains("-") {
                        start.split_once("-").unwrap()
                    } else {
                        (start, "0")
                    };
                    (
                        start_timestamp.parse().unwrap(),
                        start_sequence.parse().unwrap(),
                    )
                }
            };
            let (end_timestamp, end_sequence): (u128, usize) = {
                let end = end.to_str().unwrap();
                if end == "+" {
                    (u128::MAX, usize::MAX)
                } else {
                    let (end_timestamp, end_sequence) = if end.contains("-") {
                        end.split_once("-").unwrap()
                    } else {
                        (end, "inf")
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

            let mut result_vec: Vec<&StreamElement> = Vec::new();

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

            let result_vec: VecDeque<_> = result_vec
                .into_iter()
                .map(|element| {
                    RedisValue::Arr(
                        vec![
                            RedisValue::Primitive(PrimitiveRedisValue::Str(element.id.clone())),
                            RedisValue::Arr(
                                element
                                    .value
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        [RedisValue::Primitive(k.clone()), v.clone()]
                                    })
                                    .collect(),
                            ),
                        ]
                        .into(),
                    )
                })
                .collect();

            Ok(RedisValue::Arr(result_vec).to_bytes())
        } else {
            Ok(RedisValue::Arr(VecDeque::new()).to_bytes())
        }
    } else {
        bail!("Bad key");
    }
}

async fn handle_xread(
    streams: &Streams,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;

    if let Some(arg) = v.get(*i) {
        if arg
            .to_str()
            .is_some_and(|s| s.to_ascii_lowercase() == "block")
        {
            *i += 1;
            let mut interval = interval(Duration::from_millis(10));

            let timeout: u64 = v[*i]
                .to_str()
                .expect("Timeout arg should be a string")
                .parse()
                .expect("Timeout arg should be an unsigned int");

            *i += 1;

            let expires = if timeout > 0 {
                Some(
                    SystemTime::now()
                        .checked_add(Duration::from_millis(timeout))
                        .unwrap(),
                )
            } else {
                None
            };

            let key = v[*i + 1]
                .to_primitive()
                .expect("Key should be a primitive redis value");

            let start = v[*i + 2].to_str().expect("Start id should be a string");

            let (start_timestamp, start_sequence) = {
                let streams = streams.lock().await;
                parse_id_with_dollar_sign(start, streams.get(key).unwrap_or(&vec![]))
            };

            loop {
                interval.tick().await;

                if expires.is_some_and(|exp| exp < SystemTime::now()) {
                    return Ok("$-1\r\n".into());
                }

                if let Ok(streams) = streams.try_lock() {
                    if let Some(stream_vec) = streams.get(key) {
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

                                let resp_value =
                                    RedisValue::Arr(vec![RedisValue::Arr(result_vec)].into());

                                return Ok(resp_value.to_bytes());
                            }
                        }
                    }
                }
            }
        }
    }

    *i += 1;

    let streams = streams.lock().await;

    let mut keys = Vec::new();

    while v[*i]
        .to_primitive()
        .is_some_and(|key| streams.contains_key(key))
    {
        keys.push(v[*i].to_primitive().unwrap());
        *i += 1;
    }

    let starts: Vec<_> = v.range(*i..).map(|value| value.to_str().unwrap()).collect();

    if keys.len() != starts.len() {
        bail!(
            "Provided {} keys but {} start values",
            keys.len(),
            starts.len()
        );
    }

    let result_vec: VecDeque<_> = keys
        .into_iter()
        .zip(starts)
        .map(|(k, start)| {
            let (start_timestamp, start_sequence) = parse_id(start);
            RedisValue::Arr(gather_stream_read_results(
                streams.get(k).unwrap().iter().collect(),
                start_timestamp,
                start_sequence,
                k.clone(),
            ))
        })
        .collect();

    Ok(RedisValue::Arr(result_vec).to_bytes())
}

async fn handle_incr(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
) -> anyhow::Result<Bytes> {
    *i += 1;

    let key = v[*i].to_primitive().unwrap();

    let mut db = db.lock().await;

    let updated_int = match db.entry(key.clone()) {
        Entry::Occupied(mut occ) => {
            let (old_val, expired) = occ.get();
            if let Some(s) = old_val.to_str() {
                if let Ok(n) = s.parse::<isize>() {
                    occ.insert(((n + 1).to_string().into(), *expired));
                    Some(n + 1)
                } else {
                    None
                }
            } else {
                None
            }
        }
        Entry::Vacant(vac) => {
            vac.insert(("1".to_string().into(), None));
            Some(1isize)
        }
    };

    if let Some(n) = updated_int {
        let resp_val: RedisValue = n.into();
        Ok(resp_val.to_bytes())
    } else {
        Ok("-ERR value is not an integer or out of range\r\n".into())
    }
}

async fn handle_multi(transaction_active: &mut bool) -> anyhow::Result<Bytes> {
    *transaction_active = true;
    Ok("+OK\r\n".into())
}

async fn handle_exec(
    transaction_queue: &mut VecDeque<VecDeque<RedisValue>>,
    db: &Db,
    blocks: &Blocks,
    streams: &Streams,
    transaction_active: &mut bool,
) -> anyhow::Result<Bytes> {
    let mut buf = BytesMut::new();

    buf.put_slice(format!("*{}\r\n", transaction_queue.len()).as_bytes());

    for args in transaction_queue {
        let resp = execute_command(args, db, blocks, streams, transaction_active).await?;
        buf.put_slice(&resp);
    }

    *transaction_active = false;

    Ok(buf.freeze())
}

fn queue_command(
    transaction_queue: &mut VecDeque<VecDeque<RedisValue>>,
    args: VecDeque<RedisValue>,
) -> Bytes {
    transaction_queue.push_back(args);
    "+QUEUED\r\n".into()
}

fn handle_discard(
    transaction_queue: &mut VecDeque<VecDeque<RedisValue>>,
    transaction_active: &mut bool
) -> Bytes {
    transaction_queue.clear();
    *transaction_active = false;
    "+OK\r\n".into()
}

fn handle_info(v: &mut VecDeque<RedisValue>) -> anyhow::Result<Bytes> {
    match v[1].to_str().map(|s| s.to_ascii_lowercase()) {
        Some(s) if s == "replication" => {
            let instance_type = INSTANCE_TYPE.get().unwrap();
            let mut lines = vec![format!("role:{instance_type}")];
            if *instance_type == InstanceType::Master {
                lines.push(format!("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"));
                lines.push(format!("master_repl_offset:0"));
            }
            Ok(bulk_string(lines.join("\r\n").as_str()))
        },
        _ => bail!(format!("INFO: Invalid argument {:?}", v[1]))
    }
}

async fn send_response(socket: &mut TcpStream, resp: &[u8]) -> anyhow::Result<()> {
    socket.write_all(resp).await?;
    socket.flush().await.map_err(|e| anyhow!(e))
}

fn gather_stream_read_results(
    stream_vec: Vec<&StreamElement>,
    start_timestamp: u128,
    start_sequence: usize,
    key: PrimitiveRedisValue,
) -> VecDeque<RedisValue> {
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

    vec![
        RedisValue::Primitive(key.clone()),
        RedisValue::Arr(
            results_vec
                .into_iter()
                .map(|element| {
                    RedisValue::Arr(
                        vec![
                            element.id.clone().into(),
                            RedisValue::Arr(
                                element
                                    .value
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        [RedisValue::Primitive(k.clone()), v.clone()]
                                    })
                                    .collect(),
                            ),
                        ]
                        .into(),
                    )
                })
                .collect(),
        ),
    ]
    .into()
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
                let sequence = sequence
                    .parse()
                    .map_err(|_| StreamIdValidationError::InvalidSequence(sequence.to_string()))?;
                if timestamp < last_timestamp {
                    return Err(StreamIdValidationError::DecreasingTimestamp);
                }
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

fn bulk_string(s: &str) -> Bytes {
    format!("${}\r\n{s}\r\n", s.len()).into()
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

fn parse_id_with_dollar_sign(input: &str, stream_vec: &Vec<StreamElement>) -> (u128, usize) {
    let (start_timestamp, start_sequence) = if input == "$" {
        stream_vec
            .last()
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
