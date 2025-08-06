#![allow(unused_imports)]
mod parser;
mod redis_value;

use std::{collections::HashMap, sync::Arc};

use crate::parser::*;
use anyhow::anyhow;
use bytes::BytesMut;
use redis_value::{PrimitiveRedisValue, RedisValue};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

type Db<'a> = Arc<Mutex<HashMap<PrimitiveRedisValue, RedisValue>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let db = Arc::clone(&db);

        tokio::spawn(async move {
            process(socket, db)
                .await
                .expect("Process should be successful");
        });
    }
}

async fn process(mut socket: TcpStream, db: Db<'_>) -> anyhow::Result<()> {
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
                                    let mut iter = v.drain(i + 1..i + 3);
                                    let key = iter.next();
                                    let value = iter.next();
                                    if let Some(RedisValue::Primitive(k)) = key {
                                        if let Some(value) = value {
                                            handle_set(k, value, &db).await?;
                                            send_response(&mut socket, b"+OK\r\n").await?
                                        }
                                    }
                                }
                                "GET" => {
                                    i += 1;
                                    if let RedisValue::Primitive(key) = &v[i] {
                                        let result = handle_get(key, &db).await;
                                        if let Ok(value) = result
                                        {
                                            send_response(&mut socket, &value.to_bytes()).await?
                                        } else if let Err(anyhow::Error { .. }) = result {
                                            send_response(&mut socket, b"$-1\r\n").await?
                                        }
                                    }
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

async fn handle_set<'a>(
    key: PrimitiveRedisValue,
    value: RedisValue,
    db: &Db<'a>,
) -> anyhow::Result<()> {
    let mut db = db.lock().await;
    db.insert(key, value);
    Ok(())
}

async fn handle_get<'a>(key: &PrimitiveRedisValue, db: &Db<'a>) -> anyhow::Result<RedisValue> {
    let db = db.lock().await;
    db.get(key).ok_or(anyhow!("Key error")).cloned()
}

async fn send_response(socket: &mut TcpStream, resp: &[u8]) -> anyhow::Result<()> {
    socket.write_all(resp).await?;
    socket.flush().await.map_err(|e| anyhow!(e))
}

fn bulk_string<'a>(s: &'a str) -> Vec<u8> {
    format!("${}\r\n{s}\r\n", s.len()).as_bytes().to_owned()
}
