mod args;
mod db_value;
mod global_config;
mod hex;
mod parser;
mod rdb_parser;
mod redis_value;
mod replica_tracker;
mod response;
mod state;

use std::{
    collections::{hash_map::Entry, BTreeSet, VecDeque},
    future,
    sync::{LazyLock, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::args::Args;
use crate::hex::parse_hex_string;
use crate::parser::parse_multiple_resp_arrays_of_strings_with_len;
use anyhow::{anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use db_value::{DbValue, StreamElement};
use global_config::{GlobalConfig, MasterConfig, SlaveConfig};
use indexmap::IndexMap;
use itertools::Itertools;
use num::ToPrimitive;
use rdb_parser::parse_db;
use redis_value::{PrimitiveRedisValue, RedisValue};
use response::RedisResponse;
use state::{ConnectionState, ConnectionType, ServerState};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast::error::RecvError, mpsc},
    time::interval,
};
use tokio::{sync::broadcast, task};

const EMPTY_RDB_FILE: &str = include_str!("../resources/empty_rdb.hex");
const EMPTY_RDB_BYTES: LazyLock<Bytes> = LazyLock::new(|| {
    let mut res = BytesMut::new();
    let raw_bytes = &parse_hex_string(EMPTY_RDB_FILE).unwrap();
    res.extend_from_slice(format!("${}\r\n", raw_bytes.len()).as_bytes());
    res.extend_from_slice(raw_bytes);
    res.freeze()
});
const WRITE_COMMANDS: [&str; 7] = ["SET", "DEL", "RPUSH", "LPOP", "BLPOP", "XADD", "INCR"];
const SUBSCRIBED_ALLOWED_COMMANDS: [&str; 6] = [
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PING",
    "QUIT",
];

static GLOBAL_CONFIG: OnceLock<GlobalConfig> = OnceLock::new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();

    let is_master = args.replicaof.is_none();

    GLOBAL_CONFIG
        .set(if is_master {
            let master_config = MasterConfig::new();
            GlobalConfig::new_from_master_config(
                master_config,
                args.dir.clone(),
                args.dbfilename.clone(),
            )
        } else {
            let replicaof = args.replicaof.unwrap();
            let (addr, port) = replicaof.split_once(" ").expect("Invalid replicaof");
            let slave_config =
                SlaveConfig::new(addr.to_string(), port.parse().expect("Invalid master port"));
            GlobalConfig::new_from_slave_config(
                slave_config,
                args.dir.clone(),
                args.dbfilename.clone(),
            )
        })
        .map_err(|_| anyhow!("Tried to set global config, but it was already set"))?;

    let (replica_tx, _) = broadcast::channel::<Bytes>(32);

    let db = {
        if let (Some(dir), Some(dbfilename)) = (args.dir, args.dbfilename) {
            tokio::fs::read(format!("{dir}/{dbfilename}"))
                .await
                .ok()
                .and_then(|data| {
                    parse_db(&mut &data[..])
                        .map_err(|e| println!("parsing error: {e:?}"))
                        .ok()
                })
        } else {
            None
        }
    };

    if let Some(ref db) = db {
        println!("Preexisting db with length {}", db.len());
    } else {
        println!("No existing db");
    }

    println!("db: {db:?}");

    let server_state = ServerState::new(
        is_master,
        if is_master {
            Some(replica_tx.clone())
        } else {
            None
        },
        db,
    );
    let loop_server_state = server_state.clone();

    let server_handle = tokio::spawn(async move {
        loop {
            let (socket, addr) = listener.accept().await.unwrap();
            println!("Accepting connection from {addr}");

            let server_state = loop_server_state.clone();

            tokio::spawn(async move {
                process(socket, false, server_state)
                    .await
                    .expect("Process should be successful");
            });
        }
    });

    if is_master {
        let mut interval = interval(Duration::from_secs(1));

        tokio::spawn(async move {
            loop {
                interval.tick().await;

                if let Err(_) = send_getack(server_state.clone()) {
                    println!("Failed to send heartbeat request");
                    break;
                }
            }
        });
    } else {
        let global_config = GLOBAL_CONFIG.get().unwrap();
        let slave_config = global_config.get_slave_config().unwrap();

        let mut master_stream = TcpStream::connect(format!(
            "{}:{}",
            slave_config.master_address, slave_config.master_port
        ))
        .await?;

        send_handshake(&mut master_stream).await?;

        tokio::spawn(async move {
            process(master_stream, true, server_state)
                .await
                .expect("Master connection process should be successful");
        });
    }

    server_handle.await?;

    Ok(())
}

async fn process(
    stream: TcpStream,
    is_replication_conn: bool,
    mut server_state: ServerState,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);

    let mut replica_receiver = server_state
        .master_state()
        .map(|ms| ms.replica_tx.subscribe());

    let (mut stream_reader, mut stream_writer) = stream.into_split();
    let (stream_tx, mut stream_rx) = mpsc::channel::<Bytes>(100);

    let mut connection_state = ConnectionState::new(stream_tx.clone(), server_state.is_master());
    if is_replication_conn {
        connection_state.set_connection_type(ConnectionType::ReplicaToMaster);
    }

    let writer = tokio::spawn(async move {
        while let Some(msg) = stream_rx.recv().await {
            if let Err(e) = stream_writer.write_all(&msg).await {
                println!("Stream write error: {e}");
                break;
            }
            if let Err(e) = stream_writer.flush().await {
                println!("Stream flush error: {e}");
                break;
            }
        }
    });

    loop {
        tokio::select! {
            input_len = stream_reader.read_buf(&mut buf) => {
                match input_len {
                    Ok(0) => break,
                    Ok(n) => {
                        if n >= EMPTY_RDB_BYTES.len() &&
                            (buf[..EMPTY_RDB_BYTES.len()] == *EMPTY_RDB_BYTES) {
                                let _ = buf.split_to(EMPTY_RDB_BYTES.len());
                                println!("Received RDB file; resetting offset");
                                server_state.init_offset();
                            }
                        let results = parse_multiple_resp_arrays_of_strings_with_len(&mut &buf[..])
                            .map_err(|e| anyhow!(e))?;

                        println!("Received commands: {results:?}");

                        let server_state = server_state.clone();
                        let connection_state = connection_state.clone();

                        tokio::spawn(async move {
                            if let Err(e) = process_input(
                                results,
                                server_state,
                                connection_state,
                            ).await {
                                eprintln!("Error in processing task: {e}");
                            }
                        });

                        buf.clear();
                    }
                    Err(_) => break,
                }
            }

            msg = receive_if(&mut replica_receiver) => {
                if connection_state.get_connection_type() == ConnectionType::MasterToReplica {
                    match msg {
                        Ok(msg) => stream_tx.send(msg).await?,
                        Err(RecvError::Closed) => break,
                        Err(RecvError::Lagged(n)) => println!("Missed {n} messages"),
                    }
                }
            }
        }
    }

    let _ = writer.await;

    Ok(())
}

async fn process_input(
    results: Vec<(Vec<String>, usize)>,
    mut server_state: ServerState,
    connection_state: ConnectionState,
) -> anyhow::Result<()> {
    // let commands_nonempty = !results.is_empty();
    for (mut v, message_len) in results {
        if connection_state.get_transaction_active() {
            match v[0].to_ascii_uppercase().as_str() {
                "EXEC" => {
                    let responses =
                        handle_exec(server_state.clone(), connection_state.clone()).await?;
                    for resp in responses {
                        connection_state.stream_tx.send(resp).await?;
                    }
                }
                "DISCARD" => {
                    for resp in handle_discard(connection_state.clone()).await {
                        connection_state.stream_tx.send(resp).await?;
                    }
                }
                _ => {
                    for resp in queue_command(connection_state.clone(), message_len, v).await {
                        connection_state.stream_tx.send(resp).await?;
                    }
                }
            }
        } else {
            if let Some(master_state) = server_state.master_state() {
                if WRITE_COMMANDS.contains(&v[0].as_str()) {
                    master_state.add_to_offset(message_len);
                    master_state
                        .broadcast_to_replicas(RedisResponse::from_str_vec(&v).to_bytes())?;
                }
            } else if !v[0].to_ascii_uppercase().starts_with("FULLRESYNC") {
                server_state.add_to_replica_offset(message_len)?;
            }

            let responses = if connection_state.get_subscribe_mode()
                && !SUBSCRIBED_ALLOWED_COMMANDS.contains(&v[0].as_str())
            {
                vec![format!("-ERR Can't execute '{}'\r\n", v[0]).into()]
            } else {
                execute_command(
                    &mut v,
                    message_len,
                    server_state.clone(),
                    connection_state.clone(),
                )
                .await?
            };

            println!(
                "Processing command: {v:?}, Connection type: {:?}",
                connection_state.get_connection_type()
            );

            let should_send_response =
                if connection_state.get_connection_type() == ConnectionType::ReplicaToMaster {
                    match (v.get(0), v.get(1)) {
                        (Some(a), Some(b))
                            if a.to_ascii_uppercase() == "REPLCONF"
                                && b.to_ascii_uppercase() == "GETACK" =>
                        {
                            true
                        }
                        _ => false,
                    }
                } else {
                    true
                };

            println!("Should send response: {should_send_response}, command: {v:?}");

            if should_send_response {
                for resp in responses {
                    connection_state.stream_tx.send(resp).await?;
                }
            }
        }
    }

    // if connection_state.get_connection_type() == ConnectionType::ReplicaToMaster
    //     && commands_nonempty
    // {
    //     send_ack(server_state, connection_state).await?;
    // }

    Ok(())
}

async fn execute_command(
    v: &mut Vec<String>,
    message_len: usize,
    server_state: ServerState,
    connection_state: ConnectionState,
) -> anyhow::Result<Vec<Bytes>> {
    println!("Command: {}", v[0]);
    let mut i = 0;
    match v[0].to_ascii_uppercase().as_str() {
        "PING" => Ok(vec!["+PONG\r\n".into()]),
        "ECHO" => Ok(vec![bulk_string(&v[1])]),
        "SET" => handle_set(server_state, v).await,
        "GET" => handle_get(server_state, v).await,
        "RPUSH" => handle_rpush(server_state, v, &mut i).await,
        "LRANGE" => handle_lrange(server_state, v, &mut i).await,
        "LPUSH" => handle_lpush(server_state, v, &mut i).await,
        "LLEN" => handle_llen(server_state, v, &mut i).await,
        "LPOP" => handle_lpop(server_state, v, &mut i).await,
        "BLPOP" => handle_blpop(server_state, v, &mut i).await,
        "TYPE" => handle_type(server_state, v, &mut i).await,
        "XADD" => handle_xadd(server_state, v, &mut i).await,
        "XRANGE" => handle_xrange(server_state, v, &mut i).await,
        "XREAD" => handle_xread(server_state, v, &mut i).await,
        "INCR" => handle_incr(server_state, v, &mut i).await,
        "MULTI" => handle_multi(connection_state).await,
        "INFO" => handle_info(v),
        "EXEC" => Ok(vec!["-ERR EXEC without MULTI\r\n".into()]),
        "DISCARD" => Ok(vec!["-ERR DISCARD without MULTI\r\n".into()]),
        "REPLCONF" => handle_replconf(v, message_len, server_state, connection_state).await,
        "WAIT" => handle_wait(v, server_state).await,
        "PSYNC" => handle_psync(server_state, connection_state).await,
        "CONFIG" => handle_config(v),
        "KEYS" => handle_keys(server_state).await,
        "SUBSCRIBE" => handle_subscribe(connection_state, v).await,
        s if s.starts_with("FULLRESYNC") => Ok(vec![]),
        _ => {
            bail!("Invalid command")
        }
    }
}

async fn send_handshake(stream: &mut TcpStream) -> anyhow::Result<()> {
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    expect_response(stream, b"+PONG\r\n").await?;
    stream
        .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n")
        .await?;
    expect_response(stream, b"+OK\r\n").await?;
    stream
        .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await?;
    expect_response(stream, b"+OK\r\n").await?;
    stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await?;

    Ok(())
}

async fn expect_response(stream: &mut TcpStream, expected_response: &[u8]) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(expected_response.len());
    buf.resize(expected_response.len(), 0);

    stream.read_exact(&mut buf).await?;

    if &buf == expected_response {
        Ok(())
    } else {
        bail!(
            "Did not receive expected response {}. Got {buf:?}, instead.",
            String::from_utf8_lossy(expected_response)
        )
    }
}

async fn handle_set(server_state: ServerState, v: &mut Vec<String>) -> anyhow::Result<Vec<Bytes>> {
    let (key, value) = {
        let mut iter = v.drain(1..3);
        (iter.next(), iter.next())
    };

    if let (Some(key), Some(value)) = (key, value) {
        let mut db = server_state.db.write().await;

        match v.get(1) {
            Some(s) if s.to_ascii_uppercase() == "PX" => {
                let expires_in: u64 = v[2].parse()?;

                let current_time = SystemTime::now();
                let expires = current_time
                    .checked_add(Duration::from_millis(expires_in))
                    .ok_or(anyhow!("SET: Invalid expires durartion"))?;

                db.insert(key, (DbValue::String(value), Some(expires)));
            }
            _ => {
                db.insert(key, (DbValue::String(value), None));
            }
        }
        Ok(vec!["+OK\r\n".into()])
    } else {
        bail!("SET: bad key or value")
    }
}

async fn handle_get(server_state: ServerState, v: &mut Vec<String>) -> anyhow::Result<Vec<Bytes>> {
    let key = &v[1];

    let mut db = server_state.db.write().await;

    if let Some((value, expiry)) = db.get(key) {
        if let Some(expiry) = expiry {
            if *expiry < SystemTime::now() {
                db.remove(key);
                Ok(vec!["$-1\r\n".into()])
            } else {
                Ok(vec![RedisResponse::from(value).to_bytes()])
            }
        } else {
            Ok(vec![RedisResponse::from(value).to_bytes()])
        }
    } else {
        Ok(vec!["$-1\r\n".into()])
    }
}

async fn handle_rpush(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    let (key, values) = {
        let mut drain = v.drain(*i + 1..);
        (drain.next(), drain.collect::<Vec<_>>())
    };

    if let Some(key) = key {
        let mut db = server_state.db.write().await;

        if let Some((DbValue::List(ref mut array), _)) = db.get_mut(&key) {
            array.append(&mut values.into());

            let resp = RedisResponse::Int(array.len().to_isize().unwrap());
            Ok(vec![resp.to_bytes()])
        } else {
            let size = values.len().to_isize().unwrap();

            db.insert(key, (DbValue::List(values.into()), None));

            let resp = RedisResponse::Int(size);
            Ok(vec![resp.to_bytes()])
        }
    } else {
        bail!("Bad key: {key:?}");
    }
}

async fn handle_lrange(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;
    let empty_array_response = RedisResponse::List(vec![]).to_bytes();
    let key = &v[*i];

    let db = server_state.db.read().await;

    if let Some((DbValue::List(array), _)) = db.get(key) {
        if let (Some(a), Some(b)) = (v.get(*i + 1), v.get(*i + 2)) {
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
}

async fn handle_lpush(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    let (key, mut values) = {
        let mut drain = v.drain(*i + 1..);
        (drain.next(), drain.rev().collect::<VecDeque<_>>())
    };

    if let Some(key) = key {
        let mut db = server_state.db.write().await;

        if let Some((DbValue::List(ref mut array), _)) = db.get_mut(&key) {
            values.append(array);
        }
        let size = values.len().to_isize().unwrap();
        db.insert(key, (DbValue::List(values), None));

        let resp = RedisResponse::Int(size);
        Ok(vec![resp.to_bytes()])
    } else {
        bail!("Bad key: {key:?}");
    }
}

async fn handle_llen(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;

    let key = &v[*i];
    let db = server_state.db.read().await;

    if let Some((DbValue::List(array), _)) = db.get(key) {
        let size = array.len().to_isize().unwrap();
        let resp = RedisResponse::Int(size);

        Ok(vec![resp.to_bytes()])
    } else {
        let zero_resp = RedisResponse::Int(0);

        Ok(vec![zero_resp.to_bytes()])
    }
}

async fn handle_lpop(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    let empty_response = "$-1\r\n";
    *i += 1;

    let key = &v[*i];
    let mut db = server_state.db.write().await;

    if let Some((DbValue::List(ref mut array), _)) = db.get_mut(key) {
        if let Some(n) = v.get(*i + 1) {
            *i += 1;

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

async fn handle_blpop(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    let mut interval = interval(Duration::from_millis(10));
    *i += 1;

    let key = &v[*i];
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

    let expires = if let Some(t) = v.get(*i + 1) {
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
                let mut blocks = server_state.blocks.lock().await;
                let blocks_set = blocks.get_mut(key).expect("Blocks set should exist.");
                blocks_set.retain(|(_, id)| *id != task::id().to_string());
                if blocks_set.is_empty() {
                    blocks.remove(key);
                }
                return Ok(vec!["$-1\r\n".into()]);
            }
        }

        let mut db = server_state.db.write().await;
        if let Some((DbValue::List(ref mut array), _)) = db.get_mut(key) {
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

                            let resp_vec =
                                vec![RedisResponse::Str(key.clone()), RedisResponse::Str(value)];

                            let resp = RedisResponse::List(resp_vec).to_bytes();

                            return Ok(vec![resp]);
                        }
                    }
                }
            }
        }
    }
}

async fn handle_type(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;

    if let Some(key) = v.get(*i) {
        let db = server_state.db.read().await;

        match db.get(key) {
            Some((DbValue::String(_), _)) | Some((DbValue::List(_), _)) => {
                Ok(vec!["+string\r\n".into()])
            }
            Some((DbValue::Stream(_), _)) => Ok(vec!["+stream\r\n".into()]),
            None => Ok(vec!["+none\r\n".into()]),
        }
    } else {
        bail!("TYPE: No key");
    }
}

async fn handle_xadd(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;

    if let Some(key) = v.get(*i) {
        let mut db = server_state.db.write().await;

        let stream_vec = if let Some((DbValue::Stream(stream_vec), _)) = db.get_mut(key) {
            stream_vec
        } else {
            db.insert(key.clone(), (DbValue::Stream(Vec::new()), None));
            db.get_mut(key).unwrap().0.to_stream_vec_mut().unwrap()
        };

        if let Some(id) = v.get(*i + 1) {
            *i += 1;

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

            let mut args: Vec<_> = v.drain(*i + 1..).collect();

            let mut map = IndexMap::new();

            for (stream_key, value) in args.drain(..).tuples() {
                map.insert(stream_key, value);
            }

            let result = StreamElement::new(id.clone(), map);
            stream_vec.push(result);

            Ok(vec![
                RedisValue::Primitive(PrimitiveRedisValue::Str(id)).to_bytes()
            ])
        } else {
            bail!("Could not get id");
        }
    } else {
        bail!("Could not extract key");
    }
}

async fn handle_xrange(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;

    if let Some(key) = v.get(*i) {
        let db = server_state.db.read().await;

        if let Some((DbValue::Stream(stream_vec), _)) = db.get(key) {
            let mut drain = v.drain(*i + 1..*i + 3);
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

async fn handle_xread(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;

    if let Some(arg) = v.get(*i) {
        if arg.to_ascii_lowercase() == "block" {
            *i += 1;
            let mut interval = interval(Duration::from_millis(10));

            let timeout: u64 = v[*i]
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

            let key = &v[*i + 1];
            let start = &v[*i + 2];

            let (start_timestamp, start_sequence) = {
                let db = server_state.db.read().await;
                parse_id_with_dollar_sign(
                    &start,
                    db.get(key).and_then(|(val, _)| val.to_stream_vec()),
                )
            };

            loop {
                interval.tick().await;

                if expires.is_some_and(|exp| exp < SystemTime::now()) {
                    return Ok(vec!["$-1\r\n".into()]);
                }

                if let Ok(db) = server_state.db.try_read() {
                    if let Some((DbValue::Stream(stream_vec), _)) = db.get(key) {
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
    }

    *i += 1;

    let db = server_state.db.read().await;

    let mut keys = Vec::new();

    while db.contains_key(&v[*i]) {
        keys.push(&v[*i]);
        *i += 1;
    }

    let starts = &v[*i..];

    if keys.len() != starts.len() {
        bail!(
            "Provided {} keys but {} start values",
            keys.len(),
            starts.len()
        );
    }

    let result_vec: Vec<_> = keys
        .into_iter()
        .zip(starts)
        .map(|(k, start)| {
            let (start_timestamp, start_sequence) = parse_id(start);
            gather_stream_read_results(
                db.get(k)
                    .unwrap()
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

    Ok(vec![RedisResponse::List(result_vec).to_bytes()])
}

async fn handle_incr(
    server_state: ServerState,
    v: &mut Vec<String>,
    i: &mut usize,
) -> anyhow::Result<Vec<Bytes>> {
    *i += 1;

    let key = &v[*i];

    let mut db = server_state.db.write().await;

    let updated_int = match db.entry(key.clone()) {
        Entry::Occupied(mut occ) => {
            if let (DbValue::String(old_val), expired) = occ.get() {
                if let Ok(n) = old_val.parse::<isize>() {
                    occ.insert((DbValue::String((n + 1).to_string()), *expired));
                    Some(n + 1)
                } else {
                    None
                }
            } else {
                None
            }
        }
        Entry::Vacant(vac) => {
            vac.insert((DbValue::String("1".to_string()), None));
            Some(1isize)
        }
    };

    if let Some(n) = updated_int {
        let resp_val: RedisValue = n.into();
        Ok(vec![resp_val.to_bytes()])
    } else {
        Ok(vec![
            "-ERR value is not an integer or out of range\r\n".into()
        ])
    }
}

async fn handle_multi(mut connection_state: ConnectionState) -> anyhow::Result<Vec<Bytes>> {
    connection_state.set_transaction_active(true);
    Ok(vec!["+OK\r\n".into()])
}

async fn handle_exec(
    server_state: ServerState,
    mut connection_state: ConnectionState,
) -> anyhow::Result<Vec<Bytes>> {
    let mut buf = BytesMut::new();
    let cs = connection_state.clone();
    let mut transaction_queue = cs.transaction_queue.lock().await;

    buf.put_slice(format!("*{}\r\n", transaction_queue.len()).as_bytes());

    for (args, message_len) in transaction_queue.iter_mut() {
        let responses = execute_command(
            args,
            *message_len,
            server_state.clone(),
            connection_state.clone(),
        )
        .await?;
        for resp in responses {
            buf.put_slice(&resp);
        }
    }

    connection_state.set_transaction_active(false);

    Ok(vec![buf.freeze()])
}

async fn queue_command(
    mut connection_state: ConnectionState,
    message_len: usize,
    args: Vec<String>,
) -> Vec<Bytes> {
    connection_state.queue_command(message_len, args).await;
    vec!["+QUEUED\r\n".into()]
}

async fn handle_discard(mut connection_state: ConnectionState) -> Vec<Bytes> {
    connection_state.clear_transactions().await;
    connection_state.set_transaction_active(false);
    vec!["+OK\r\n".into()]
}

fn handle_info(v: &mut Vec<String>) -> anyhow::Result<Vec<Bytes>> {
    match v[1].to_ascii_lowercase().as_str() {
        "replication" => {
            let global_config = GLOBAL_CONFIG.get().unwrap();
            let mut lines = vec![format!("role:{}", global_config.role())];
            if let Some(master_config) = global_config.get_master_config() {
                lines.push(format!("master_replid:{}", master_config.replication_id));
                lines.push(format!("master_repl_offset:0"));
            }
            Ok(vec![bulk_string(lines.join("\r\n").as_str())])
        }
        _ => bail!(format!("INFO: Invalid argument {:?}", v[1])),
    }
}

async fn handle_psync(
    server_state: ServerState,
    mut connection_state: ConnectionState,
) -> anyhow::Result<Vec<Bytes>> {
    if let Some(master_state) = server_state.clone().master_state() {
        let global_config = GLOBAL_CONFIG.get().unwrap();
        let master_config = global_config.get_master_config().unwrap();

        let new_replica_id = master_state.create_replica().await;
        connection_state.set_replica_id(new_replica_id)?;
        connection_state.set_connection_type(ConnectionType::MasterToReplica);
        Ok(vec![
            format!("+FULLRESYNC {} 0\r\n", master_config.replication_id).into(),
            EMPTY_RDB_BYTES.clone(),
        ])
    } else {
        bail!("Tried to call PSYNC on a replica")
    }
}

async fn handle_replconf(
    v: &mut Vec<String>,
    message_len: usize,
    mut server_state: ServerState,
    connection_state: ConnectionState,
) -> anyhow::Result<Vec<Bytes>> {
    if let Some(s) = v.get(1) {
        match s.to_ascii_uppercase().as_str() {
            "GETACK" => {
                let offset = server_state.load_offset() - message_len;
                let resp = RedisResponse::List(vec![
                    RedisResponse::Str("REPLCONF".to_string()),
                    RedisResponse::Str("ACK".to_string()),
                    RedisResponse::Str(offset.to_string()),
                ]);
                Ok(vec![resp.to_bytes()])
            }
            "ACK" => {
                let offset: usize = v
                    .get(2)
                    .and_then(|s| s.parse().ok())
                    .expect("ACK: Invalid offset");

                if let Some(master_state) = server_state.master_state() {
                    if let Some(replica_id) = connection_state.get_replica_id() {
                        master_state.update_offset_tracker(replica_id, offset).await;
                        Ok(vec![])
                    } else {
                        bail!("ACK: ack was sent, but not from a replica")
                    }
                } else {
                    bail!("ACK: ack was sent, but not to a master")
                }
            }
            _ => Ok(vec!["+OK\r\n".into()]),
        }
    } else {
        Ok(vec!["+OK\r\n".into()])
    }
}

async fn handle_wait(v: &mut Vec<String>, server_state: ServerState) -> anyhow::Result<Vec<Bytes>> {
    if let Some(master_state) = server_state.clone().master_state() {
        let GETACK_MSG_LEN = 37;
        let n = v
            .get(1)
            .and_then(|s| s.parse::<usize>().ok())
            .expect("WAIT: Invalid replica count");
        let timeout = v
            .get(2)
            .and_then(|s| s.parse::<u64>().ok())
            .expect("WAIT: Invalid timeout");

        let expires = SystemTime::now()
            .checked_add(Duration::from_millis(timeout))
            .expect("WAIT: Invalid expiration");

        let mut interval = interval(Duration::from_millis(10));

        master_state
            .broadcast_to_replicas("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".into())?;

        // The master offset is not updated while WAIT blocks
        let master_offset = server_state.load_offset();

        loop {
            interval.tick().await;

            let offsets = &master_state.get_replica_tracker().await.offsets;

            let caught_up_replicas = offsets.iter().fold(0usize, |acc, (_, offset)| {
                if *offset == master_offset - GETACK_MSG_LEN || *offset == master_offset {
                    acc + 1
                } else {
                    acc
                }
            });

            if expires < SystemTime::now() || n <= caught_up_replicas {
                return Ok(vec![format!(":{caught_up_replicas}\r\n").into()]);
            }
        }
    }
    bail!("Called WAIT on a replica");
}

fn handle_config(v: &Vec<String>) -> anyhow::Result<Vec<Bytes>> {
    if v.len() <= 1 {
        bail!("CONFIG: No command given");
    }
    match v[1].to_ascii_uppercase().as_str() {
        "GET" => {
            let global_config = GLOBAL_CONFIG.get().unwrap();
            let key = v
                .get(2)
                .map(|s| s.to_ascii_lowercase())
                .ok_or(anyhow!("CONFIG: No key provided"))?;
            let value = match key.as_str() {
                "dir" => global_config.dir.clone(),
                "dbfilename" => global_config.dbfilename.clone(),
                _ => bail!("CONFIG GET: Invalid config key"),
            }
            .unwrap_or("".to_string());
            let resp = RedisResponse::from_str_vec(&vec![key, value]);
            Ok(vec![resp.to_bytes()])
        }
        cmd => bail!("CONFIG: invalid command: {cmd}"),
    }
}

async fn handle_keys(server_state: ServerState) -> anyhow::Result<Vec<Bytes>> {
    let mut db = server_state.db.write().await;
    let mut buf = BytesMut::new();
    let mut values_buf = BytesMut::new();
    let now = SystemTime::now();
    db.retain(|k, (_, expires)| {
        if expires.is_none_or(|exp| exp >= now) {
            values_buf.extend_from_slice(&RedisResponse::Str(k.clone()).to_bytes());
            true
        } else {
            false
        }
    });
    buf.extend_from_slice(format!("*{}\r\n", db.len()).as_bytes());
    buf.extend_from_slice(&values_buf);
    Ok(vec![buf.freeze()])
}

async fn handle_subscribe(
    mut connection_state: ConnectionState,
    v: &Vec<String>,
) -> anyhow::Result<Vec<Bytes>> {
    if let Some(key) = v.get(1) {
        connection_state.subscribe(key).await;
        let sub_count: isize = connection_state
            .subscription_count()
            .await
            .try_into()
            .unwrap();
        let resp: RedisResponse =
            vec!["subscribe".into(), key.to_string().into(), sub_count.into()]
                .into_iter()
                .collect();
        Ok(vec![resp.to_bytes()])
    } else {
        bail!("SUBSCRIBE: No key provided");
    }
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

fn send_getack(mut server_state: ServerState) -> anyhow::Result<()> {
    let resp: Bytes = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".into();
    server_state.add_to_master_offset(resp.len())?;
    server_state
        .master_state()
        .unwrap()
        .broadcast_to_replicas(resp)
}

async fn send_ack(
    server_state: ServerState,
    connection_state: ConnectionState,
) -> anyhow::Result<()> {
    let offset = server_state.load_offset();
    let resp_vec = vec![
        RedisValue::Primitive(PrimitiveRedisValue::Str("REPLCONF".to_string())),
        RedisValue::Primitive(PrimitiveRedisValue::Str("ACK".to_string())),
        RedisValue::Primitive(PrimitiveRedisValue::Str(offset.to_string())),
    ];
    connection_state
        .stream_tx
        .send(vec_of_values_to_resp(&resp_vec.into()))
        .await
        .map_err(|_| anyhow!("Failed to send ack"))
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

fn vec_of_values_to_resp(v: &VecDeque<RedisValue>) -> Bytes {
    let mut buf = BytesMut::new();

    buf.extend_from_slice(format!("*{}\r\n", v.len()).as_bytes());
    for val in v {
        buf.extend_from_slice(&val.to_bytes());
    }

    buf.freeze()
}

async fn receive_if<T>(receiver: &mut Option<broadcast::Receiver<T>>) -> Result<T, RecvError>
where
    T: Clone,
{
    if let Some(receiver) = receiver {
        receiver.recv().await
    } else {
        future::pending::<Result<T, RecvError>>().await
    }
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
