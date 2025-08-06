#![allow(unused_imports)]
mod parser;
mod redis_value;

use std::{
    collections::{btree_map::Keys, BTreeSet, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::parser::*;
use anyhow::{anyhow, bail};
use bytes::BytesMut;
use itertools::Itertools;
use num::ToPrimitive;
use redis_value::{PrimitiveRedisValue, RedisValue, StreamElement};
use tokio::task;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::interval,
};

type Db = Arc<Mutex<HashMap<PrimitiveRedisValue, (RedisValue, Option<SystemTime>)>>>;
type Blocks = Arc<Mutex<HashMap<PrimitiveRedisValue, BTreeSet<(SystemTime, String)>>>>;
type Streams = Arc<Mutex<HashMap<PrimitiveRedisValue, Vec<StreamElement>>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

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

    loop {
        match socket.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let results =
                    parse_value(&mut &buf[..n]).map_err(|e| anyhow!("Failed to parse: {e}"))?;
                if let RedisValue::Arr(mut v) = results {
                    let mut i = 0;
                    while i < v.len() {
                        if let RedisValue::Primitive(PrimitiveRedisValue::Str(s)) = &v[i] {
                            match s.to_ascii_uppercase().as_str() {
                                "PING" => send_response(&mut socket, b"+PONG\r\n").await?,
                                "ECHO" => {
                                    i += 1;
                                    if let RedisValue::Primitive(PrimitiveRedisValue::Str(resp)) =
                                        &v[i]
                                    {
                                        send_response(&mut socket, &bulk_string(resp)).await?
                                    }
                                }
                                "SET" => {
                                    handle_set(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "GET" => {
                                    handle_get(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "RPUSH" => {
                                    handle_rpush(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "LRANGE" => {
                                    handle_lrange(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "LPUSH" => {
                                    handle_lpush(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "LLEN" => {
                                    handle_llen(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "LPOP" => {
                                    handle_lpop(&db, &mut v, &mut i, &mut socket).await?;
                                }
                                "BLPOP" => {
                                    handle_blpop(&db, &blocks, &mut v, &mut i, &mut socket).await?;
                                }
                                "TYPE" => {
                                    handle_type(&db, &streams, &mut v, &mut i, &mut socket).await?;
                                }
                                "XADD" => {
                                    handle_xadd(&streams, &mut v, &mut i, &mut socket).await?;
                                }
                                _ => {}
                            }
                            i += 1;
                        }
                    }
                }

                buf.clear();
            }
            Err(_) => break,
        }
    }

    Ok(())
}

async fn handle_set(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
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
            send_response(socket, b"+OK\r\n").await?;
        }
    }
    Ok(())
}

async fn handle_get(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        let mut db = db.lock().await;

        if let Some((value, expiry)) = db.get(key) {
            if let Some(expiry) = expiry {
                if *expiry < SystemTime::now() {
                    db.remove(key);
                    send_response(socket, b"$-1\r\n").await
                } else {
                    send_response(socket, &value.to_bytes()).await
                }
            } else {
                send_response(socket, &value.to_bytes()).await
            }
        } else {
            send_response(socket, b"$-1\r\n").await
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
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
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
            send_response(socket, &resp.to_bytes()).await?;
        } else {
            let size = values.len().to_isize().unwrap();

            db.insert(key, (RedisValue::Arr(values), None));

            let resp = RedisValue::Primitive(PrimitiveRedisValue::Int(size));
            send_response(socket, &resp.to_bytes()).await?;
        }
        Ok(())
    } else {
        bail!("Bad key: {key:?}");
    }
}

async fn handle_lrange(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
    *i += 1;
    let empty_array_response = &RedisValue::Arr(VecDeque::new()).to_bytes();

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
                    send_response(socket, empty_array_response).await?;
                } else if b > array.len() - 1 {
                    let resp = RedisValue::Arr(array.range(a..).cloned().collect());
                    send_response(socket, &resp.to_bytes()).await?;
                } else {
                    let resp = RedisValue::Arr(array.range(a..=b).cloned().collect());
                    send_response(socket, &resp.to_bytes()).await?;
                }
                *i += 2;
                Ok(())
            } else {
                bail!("Failed to get array bounds");
            }
        } else {
            send_response(socket, empty_array_response).await?;
            Ok(())
        }
    } else {
        bail!("Failed to get list key");
    }
}

async fn handle_lpush(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
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
        send_response(socket, &resp.to_bytes()).await
    } else {
        bail!("Bad key: {key:?}");
    }
}

async fn handle_llen(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        let db = db.lock().await;

        if let Some((RedisValue::Arr(array), _)) = db.get(key) {
            let size = array.len().to_isize().unwrap();
            let resp = RedisValue::Primitive(PrimitiveRedisValue::Int(size));

            send_response(socket, &resp.to_bytes()).await
        } else {
            let zero_resp = RedisValue::Primitive(PrimitiveRedisValue::Int(0));

            send_response(socket, &zero_resp.to_bytes()).await
        }
    } else {
        bail!("Bad key: {:?}", v[*i]);
    }
}

async fn handle_lpop(
    db: &Db,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
    let empty_response = b"$-1\r\n";
    *i += 1;

    if let RedisValue::Primitive(key) = &v[*i] {
        let mut db = db.lock().await;

        if let Some((RedisValue::Arr(ref mut array), _)) = db.get_mut(key) {
            if let Some(RedisValue::Primitive(PrimitiveRedisValue::Str(n))) = v.get(*i + 1) {
                *i += 1;

                let n: usize = n.parse()?;
                if n >= array.len() {
                    let resp_vec: VecDeque<_> = array.drain(..).collect();

                    send_response(socket, &RedisValue::Arr(resp_vec).to_bytes()).await
                } else {
                    let resp_vec: VecDeque<_> = array.drain(..n).collect();

                    send_response(socket, &RedisValue::Arr(resp_vec).to_bytes()).await
                }
            } else {
                let resp = array.pop_front();
                if let Some(resp) = resp {
                    send_response(socket, &resp.to_bytes()).await
                } else {
                    send_response(socket, empty_response).await
                }
            }
        } else {
            send_response(socket, empty_response).await
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
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
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
                    return send_response(socket, b"$-1\r\n").await;
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

                                let resp = &RedisValue::Arr(resp_vec).to_bytes();

                                return send_response(socket, resp).await;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_type(
    db: &Db,
    streams: &Streams,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
    *i += 1;

    if let Some(RedisValue::Primitive(key)) = v.get(*i) {
        let db = db.lock().await;

        if db.get(key).is_some() {
            return send_response(socket, b"+string\r\n").await;
        } else {
            let streams = streams.lock().await;
            if streams.get(key).is_some() {
                return send_response(socket, b"+stream\r\n").await;
            }
        }
        send_response(socket, b"+none\r\n").await
    } else {
        bail!("Bad key");
    }
}

async fn handle_xadd(
    streams: &Streams,
    v: &mut VecDeque<RedisValue>,
    i: &mut usize,
    socket: &mut TcpStream,
) -> anyhow::Result<()> {
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

            let id = id.clone();

            let (timestamp, sequence) = id
                .split_once("-")
                .expect(&format!("Invalid stream element id: {id}"));

            let timestamp: u128 = timestamp.parse().expect("Invalid timestamp");
            let sequence: usize = sequence.parse().expect("Invalid sequence number");

            if timestamp == 0 && sequence == 0 {
                return send_response(
                    socket,
                    b"-ERR The ID specified in XADD must be greater than 0-0\r\n",
                )
                .await;
            }

            let last_element = stream_vec.last();
            if let Some(last_element) = last_element {
                let (last_timestamp, last_sequence) = last_element.id.split_once("-").unwrap();
                let last_timestamp: u128 = last_timestamp.parse().unwrap();
                let last_sequence: usize = last_sequence.parse().unwrap();

                if timestamp < last_timestamp || sequence <= last_sequence {
                    return send_response(socket, b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n").await;
                }
            }

            let mut args: Vec<_> = v.drain(*i + 2..).collect();

            let mut map = HashMap::new();

            for (stream_key, value) in args.drain(..).tuples() {
                if let RedisValue::Primitive(stream_key) = stream_key {
                    map.insert(stream_key, value);
                }
            }

            let result = StreamElement::new(id.clone(), map);
            stream_vec.push(result);

            let resp = &RedisValue::Primitive(PrimitiveRedisValue::Str(id)).to_bytes();
            send_response(socket, resp).await
        } else {
            bail!("Could not get id");
        }
    } else {
        bail!("Could not extract key");
    }
}

async fn send_response(socket: &mut TcpStream, resp: &[u8]) -> anyhow::Result<()> {
    socket.write_all(resp).await?;
    socket.flush().await.map_err(|e| anyhow!(e))
}

fn bulk_string<'a>(s: &'a str) -> Vec<u8> {
    format!("${}\r\n{s}\r\n", s.len()).as_bytes().to_owned()
}
