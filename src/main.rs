#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Read, Write};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = Vec::<u8>::new();
                stream.read_to_end(&mut buf).expect("Stream should read successfully");
                if let Ok(commands) = String::from_utf8(buf) {
                    for _ in commands.split_whitespace()
                    {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
