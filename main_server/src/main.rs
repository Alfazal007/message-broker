use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::message::producer_message::ProducerMessage;
use crate::producer_handler::handle_create_topic::handle_create_topic;

pub mod message;
pub mod producer_handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Bind to address
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    let current_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    println!("Listening on 127.0.0.1:8000");

    loop {
        let current_topics_handled = Arc::clone(&current_topics);
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
                            let mut reader = BufReader::new(socket);
                            loop {
                                let mut producer_buffer_vec = Vec::new();
                                match reader.read_until(b'\0', &mut producer_buffer_vec).await {
                                    Err(e) => {
                                        eprintln!("failed to read from producer; err = {:?}", e);
                                        return;
                                    }
                                    Ok(0) => {
                                        println!("disconnected");
                                        return;
                                    }
                                    Ok(n) => {
                                        let data = &producer_buffer_vec[..n - 1];
                                        let data = serde_json::from_slice::<ProducerMessage>(data);
                                        match data {
                                            Err(e) => {
                                                println!("wrong message {:?}", e);
                                                return;
                                            }
                                            Ok(data) => {
                                                match data.message {
                                                    message::producer_message::MessageTypes::CreateTopic(create_topic_message) =>{
                                                        println!("create topic message {:?}", create_topic_message);
                                                        let res = handle_create_topic(create_topic_message, Arc::clone(&current_topics_handled)).await;
                                                        if res.is_err() {
                                                            println!("{:?}", res.err());
                                                            return;
                                                        }
                                                    },
                                                    message::producer_message::MessageTypes::DeleteTopic(delete_topic_message) => {
                                                        println!("delete topic message {:?}", delete_topic_message);
                                                    },
                                                    message::producer_message::MessageTypes::SendMessage(send_message) => {
                                                        println!("send message {:?}", send_message);
                                                    }
                                                }
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
