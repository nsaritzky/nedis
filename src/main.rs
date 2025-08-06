#![allow(unused_imports)]
mod parser;
mod redis_value;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::parser::*;
use anyhow::{anyhow, bail};
use bytes::BytesMut;
use redis_value::{PrimitiveRedisValue, RedisValue};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

type Db = Arc<Mutex<HashMap<PrimitiveRedisValue, (RedisValue, Option<SystemTime>)>>>;

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

async fn process(mut socket: TcpStream, db: Db) -> anyhow::Result<()> {
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
    v: &mut Vec<RedisValue>,
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
                },
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
    v: &mut Vec<RedisValue>,
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

async fn send_response(socket: &mut TcpStream, resp: &[u8]) -> anyhow::Result<()> {
    socket.write_all(resp).await?;
    socket.flush().await.map_err(|e| anyhow!(e))
}

fn bulk_string<'a>(s: &'a str) -> Vec<u8> {
    format!("${}\r\n{s}\r\n", s.len()).as_bytes().to_owned()
}
