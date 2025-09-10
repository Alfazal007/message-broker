use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{
    message_from_client_to_server::init_struct::InitProducerConsumer,
    message_from_server_to_client::success_message::Success,
};

pub async fn produce_task() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:8000").await?;
    let (read_half, mut write_half) = stream.into_split();
    println!("producer connected to server at 127.0.0.1:8000");
    let init_message = InitProducerConsumer::new_producer_message();
    let _ = write_half.write_all(&init_message).await;
    let _ = write_half.flush().await;
    let mut reader = BufReader::new(read_half);
    let mut buffer = Vec::new();

    let n = reader.read_until(b'\0', &mut buffer).await.unwrap();
    serde_json::from_slice::<Success>(&buffer[..n - 1]).unwrap();
    // wait for success response
    Ok(())
}
