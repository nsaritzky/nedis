#![allow(unused_imports)]
mod parser;

use crate::parser::*;
use anyhow::anyhow;
use bytes::BytesMut;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            process(socket).await.expect("Process should be successful");
        });
    }
}

async fn process(mut socket: TcpStream) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        match socket.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let results =
                    parse_value(&mut &buf[..n]).map_err(|e| anyhow!("Failed to parse: {e}"))?;
                if let ParseResult::Arr(v) = results {
                    let mut i = 0;
                    while i < v.len() {
                        if let ParseResult::Primitive(PrimitiveParseResult::Str(s)) = &v[i] {
                            if s.to_ascii_uppercase() == "PING" {
                                socket.write_all(b"+PONG\r\n").await?;
                                socket.flush().await?;
                            } else if s.to_ascii_uppercase() == "ECHO" {
                                i += 1;
                                if let ParseResult::Primitive(PrimitiveParseResult::Str(resp)) =
                                    &v[i]
                                {
                                    socket.write_all(&bulk_string(resp)).await?;
                                    socket.flush().await?
                                }
                            }
                        }
                        i += 1;
                    }
                }

                buf.clear();
            }
            Err(_) => break,
        }
    }

    Ok(())
}

fn bulk_string<'a>(s: &'a str) -> Vec<u8> {
    format!("${}\r\n{s}\r\n", s.len()).as_bytes().to_owned()
}
