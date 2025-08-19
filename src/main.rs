mod args;
mod command_handler;
mod command_registry;
mod db;
mod db_item;
mod db_value;
mod global_config;
mod hash;
mod hex;
mod list;
mod parser;
mod pubsub;
mod rdb_parser;
mod redis_value;
mod replica_tracker;
mod replication;
mod response;
mod set;
mod shard_map;
mod simple_handlers;
mod skip_list;
mod sorted_set;
mod state;
mod stream;
mod transactions;
mod utils;

use std::{
    future,
    sync::{LazyLock, OnceLock},
    time::{Duration, SystemTime},
};

use crate::args::Args;
use crate::hex::parse_hex_string;
use crate::parser::parse_multiple_resp_arrays_of_strings_with_len;
use anyhow::{anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use command_registry::REGISTRY;
use global_config::{GlobalConfig, MasterConfig, SlaveConfig};
use itertools::Itertools;
use rdb_parser::parse_db;
use response::RedisResponse;
use state::{ConnectionState, ConnectionType, ServerState};
use tokio::sync::broadcast;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast::error::RecvError, mpsc},
    time::interval,
};

const EMPTY_RDB_FILE: &str = include_str!("../resources/empty_rdb.hex");
pub const EMPTY_RDB_BYTES: LazyLock<Bytes> = LazyLock::new(|| {
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
pub static GLOBAL_CONFIG: OnceLock<GlobalConfig> = OnceLock::new();

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

    let mut connection_state = ConnectionState::new(
        stream_tx.clone(),
        server_state.is_master(),
        server_state.get_connection_id(),
    );
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
                    Ok(_) => {
                        let results = parse_multiple_resp_arrays_of_strings_with_len(&mut &buf[..])
                            .map_err(|e| anyhow!("Parsing error: {}", e))?;

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
    for (args, message_len) in results {
        let should_send_response =
            if connection_state.get_connection_type() == ConnectionType::ReplicaToMaster {
                match (args.get(0), args.get(1)) {
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

        if connection_state.get_transaction_active() {
            match args[0].to_ascii_uppercase().as_str() {
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
                    for resp in queue_command(connection_state.clone(), message_len, args).await {
                        connection_state.stream_tx.send(resp).await?;
                    }
                }
            }
        } else {
            if let Some(master_state) = server_state.master_state() {
                if WRITE_COMMANDS.contains(&args[0].as_str()) {
                    master_state.add_to_offset(message_len);
                    master_state
                        .broadcast_to_replicas(RedisResponse::from_str_vec(&args).to_bytes())?;
                }
            } else if !args[0].to_ascii_uppercase().starts_with("FULLRESYNC") {
                server_state.add_to_replica_offset(message_len)?;
            }

            let responses = if connection_state.get_subscribe_mode()
                && !SUBSCRIBED_ALLOWED_COMMANDS.contains(&args[0].as_str())
            {
                vec![format!("-ERR Can't execute '{}'\r\n", args[0]).into()]
            } else {
                execute_command(
                    args,
                    message_len,
                    server_state.clone(),
                    connection_state.clone(),
                )
                .await?
            };

            if should_send_response {
                for resp in responses {
                    connection_state.stream_tx.send(resp).await?;
                }
            }
        }
    }

    Ok(())
}

pub async fn execute_command(
    v: Vec<String>,
    message_len: usize,
    server_state: ServerState,
    connection_state: ConnectionState,
) -> anyhow::Result<Vec<Bytes>> {
    let command = v[0].to_ascii_uppercase();
    let _ = server_state.transaction_lock.read().await;

    if let Some(handler) = REGISTRY.get(command.as_str()) {
        handler
            .execute(v, server_state, connection_state, message_len)
            .await
    } else if command.starts_with("FULLRESYNC") {
        println!("Got FULLRESYNC. Not responding.");
        Ok(vec![])
    } else {
        bail!("Invalid command: {command}")
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

async fn handle_exec(
    server_state: ServerState,
    mut connection_state: ConnectionState,
) -> anyhow::Result<Vec<Bytes>> {
    let mut buf = BytesMut::new();
    let cs = connection_state.clone();

    let transactions: Vec<_> = {
        let mut tq = cs.transaction_queue.lock().await;
        tq.drain(..).collect()
    };
    let _ = server_state.transaction_lock.write().await;

    // If any watched keys have been updated, abort the transaction
    let watched_keys = connection_state.drain_watched_keys().await;
    let keys = watched_keys.keys().map(|s| s.as_str()).collect_vec();
    if !server_state
        .db
        .with_key_values(&keys, |pairs| {
            pairs.into_iter().all(|(key, item)| {
                item.is_some_and(|it| {
                    it.expires_at().is_none_or(|t| t < SystemTime::now())
                        && it.updated_at() <= *watched_keys.get(*key).unwrap()
                })
            })
        })
        .await
    {
        return Ok(vec!["$-1\r\n".into()]);
    }

    buf.put_slice(format!("*{}\r\n", transactions.len()).as_bytes());

    for (args, message_len) in transactions {
        let responses = execute_command(
            args,
            message_len,
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

fn send_getack(mut server_state: ServerState) -> anyhow::Result<()> {
    let resp: Bytes = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".into();
    server_state.add_to_master_offset(resp.len())?;
    server_state
        .master_state()
        .unwrap()
        .broadcast_to_replicas(resp)
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
