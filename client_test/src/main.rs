use std::error::Error;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::time::sleep;

use crate::message::producer_message::{CreateTopicMessage, MessageTypes, ProducerMessage};
use crate::message_generate::generate_create_topic::create_topic_message;
use crate::message_generate::generate_delete_topic::delete_topic_message;
use crate::message_generate::generate_send_message::send_topic_message;

pub mod message;
pub mod message_generate;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Connect to the server
    let stream = TcpStream::connect("127.0.0.1:8000").await?;
    println!("Connected to 127.0.0.1:8000");

    // Split into read/write halves using BufReader for line-based reading
    let (read_half, write_half) = stream.into_split();
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
    let msg: u8 = 0;
    write_half.write_all(&[msg]).await?;
    write_half.flush().await?;
    let create_topic_message = create_topic_message("createdtopic".to_string(), 4)?;
    let delete_topic_message = delete_topic_message("createdtopic".to_string())?;
    let normal_send_message = send_topic_message("createdtopic".to_string())?;
    println!("sending craete topic message");
    write_half.write_all(&create_topic_message).await?;
    write_half.flush().await?;
    println!("sending delete topic message");
    write_half.write_all(&delete_topic_message).await?;
    write_half.flush().await?;
    println!("sending normal topic message");
    write_half.write_all(&normal_send_message).await?;
    write_half.flush().await?;
    Ok(())
}

