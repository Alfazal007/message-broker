use std::collections::HashSet;
use std::error::Error;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Bind to address
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let current_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    println!("Listening on 127.0.0.1:8080");

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from {}", peer_addr);

        // Spawn a new task to handle the connection concurrently
        tokio::spawn(async move {
            let mut buf = [0u8; 1];

            match socket.read(&mut buf).await {
                Ok(0) => {
                    // Connection closed
                    println!("{} disconnected", peer_addr);
                    return;
                }
                Ok(_) => match buf[0] {
                    0 => {
                        println!("producer connected");
                        // TODO:: add to the producers list
                        let _ = socket.write_all("producer connected".as_bytes()).await;
                    }
                    1 => {
                        println!("consumer connected");
                        // TODO:: consume from a list
                        let _ = socket.write_all("consumer connected".as_bytes()).await;
                    }
                    _ => {
                        eprintln!("invalid data sent, first specify producer or consumer");
                        return;
                    }
                },
                Err(e) => {
                    eprintln!("failed to read from socket; err = {:?}", e);
                    return;
                }
            }
            println!("disconnecting");
        });
    }
}

