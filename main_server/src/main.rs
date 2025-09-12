use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::state::consumer::consumer::Consumer;
use crate::state::message_from_client::init_struct::InitProducerConsumer;
use crate::state::message_to_client::success_message::Success;
use crate::state::producer::producer::Producer;
use crate::state::topic_state::topic_state::Topic;

pub mod state;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    let topics_data = Arc::new(RwLock::new(Topic::new()));
    println!("Server listening on 127.0.0.1:8000");

    loop {
        let (socket, addr) = listener.accept().await?;
        let (read_half, mut write_half) = socket.into_split();
        let mut reader = BufReader::new(read_half);
        println!("New connection from: {}", addr);
        let thread_topic = Arc::clone(&topics_data);

        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let n = match reader.read_until(b'\0', &mut buffer).await {
                Ok(0) => {
                    return;
                }
                Ok(n) => {
                    let data = &buffer[..n - 1];
                    match serde_json::from_slice::<InitProducerConsumer>(data) {
                        Err(e) => {
                            println!("Invalid data {:?}", e);
                            return;
                        }
                        Ok(init_struct) => match init_struct.message {
                            0 => {
                                println!("producer task in");
                                let success_message = Success::new();
                                success_message.send_message(&mut write_half).await;
                                let producer = Producer::new(Arc::clone(&thread_topic));
                                producer.handler(reader, write_half).await;
                                return;
                            }
                            1 => {
                                println!("consumer task in");
                                let success_message = Success::new();
                                success_message.send_message(&mut write_half).await;
                                let consumer = Consumer::new(Arc::clone(&thread_topic));
                                consumer.handler(reader, write_half).await;
                                return;
                            }
                            _ => {
                                println!("Invalid data");
                            }
                        },
                    };
                    n
                }
                Err(e) => {
                    eprintln!("Failed to read from socket; err = {:?}", e);
                    return;
                }
            };
            if let Err(e) = write_half.write_all(&buffer[..n]).await {
                eprintln!("Failed to write to socket; err = {:?}", e);
                return;
            }
        });
    }
}
