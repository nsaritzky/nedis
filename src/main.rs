#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = [0; 1024];

                loop {
                    match stream.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            let input = String::from_utf8_lossy(&buf[..n]);
                            for _ in input.rmatches("PING") {
                                stream
                                    .write_all(b"+PONG\r\n")
                                    .expect("Should be able to write PONG to stream");
                                stream.flush().unwrap();
                            }

                            buf = [0; 1024];
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
