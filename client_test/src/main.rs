use std::error::Error;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::time::sleep;

use crate::message::producer_message::{CreateTopicMessage, MessageTypes, ProducerMessage};

pub mod message;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Connect to the server
    let stream = TcpStream::connect("127.0.0.1:8000").await?;
    println!("Connected to 127.0.0.1:8000");

    // Split into read/write halves using BufReader for line-based reading
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let handler = tokio::spawn(async move {
        let _ = producer_handler(write_half).await;
    });

    // Read one line (the echoed message)
    let bytes_read = reader.read_line(&mut line).await?;
    if bytes_read == 0 {
        println!("Connection closed by server before a response was received");
    } else {
        // Trim the trailing newline for nicer printing
        print!("Received: {}", line.trim_end());
    }
    let _ = tokio::join!(handler);
    Ok(())
}

async fn producer_handler(mut write_half: OwnedWriteHalf) -> Result<(), Box<dyn Error>> {
    println!("sendingf");
    // Send the message (include newline so the server echo can be read with lines())
    let msg: u8 = 0;
    write_half.write_all(&[msg]).await?;
    // Optionally flush (not strictly necessary for TCP here)
    write_half.flush().await?;
    let msg = ProducerMessage {
        message: MessageTypes::CreateTopic(CreateTopicMessage {
            topic_name: "orders".to_string(),
            partitions: 3,
        }),
    };
    let json_bytes = serde_json::to_vec(&msg)?;
    write_half.write_all(&json_bytes).await?;
    write_half.flush().await?;
    Ok(())
}
