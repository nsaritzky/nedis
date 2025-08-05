#![allow(unused_imports)]
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(mut socket: TcpStream) {
    let mut buf = [0; 1024];

    loop {
        match socket.read(&mut buf).await {
            Ok(0) => {
                break
            },
            Ok(n) => {
                let input = String::from_utf8_lossy(&buf[..n]);
                for _ in input.rmatches("PING") {
                    socket.write_all(b"+PONG\r\n").await.expect("Should be able to write PONG");
                    socket.flush().await.unwrap();
                }

                buf = [0; 1024];
            }
            Err(_) => break,
        }
    }
}
