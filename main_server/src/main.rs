use std::collections::HashSet;
use std::error::Error;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::message::producer_message::ProducerMessage;

pub mod message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Bind to address
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    let current_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    println!("{:?}", current_topics);
    println!("Listening on 127.0.0.1:8000");

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from {}", peer_addr);

        // Spawn a new task to handle the connection concurrently
        tokio::spawn(async move {
            let mut buf = [0u8; 1];

            match socket.read(&mut buf).await {
                Ok(_) => match buf[0] {
                    0 => {
                        tokio::spawn(async move {
                            let _ = socket.write_all("producer connected".as_bytes()).await;
                            loop {
                                let mut producer_buffer = [0u8; 1024];
                                match socket.read(&mut producer_buffer).await {
                                    Err(e) => {
                                        eprintln!("failed to read from producer; err = {:?}", e);
                                        return;
                                    }
                                    Ok(0) => {
                                        println!("disconnecting");
                                        return;
                                    }
                                    Ok(n) => {
                                        let data = &producer_buffer[..n];
                                        println!("the valud of n is {:?}", n);
                                        let data = serde_json::from_slice::<ProducerMessage>(data);
                                        match data {
                                            Err(e) => {
                                                println!("wrong message {:?}", e);
                                                return;
                                            }
                                            Ok(data) => {
                                                println!("received message {:?}", data);
                                            }
                                        }
                                    }
                                }
                            }
                        });
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
        });
    }
}

