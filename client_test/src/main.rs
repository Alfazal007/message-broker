use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:8000").await?;
    println!("Connected to server at 127.0.0.1:8000");

    let msg = b"Hello from client!";
    stream.write_all(msg).await?;
    println!("Sent: {:?}", msg);

    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    println!("Received: {:?}", &buffer[..n]);

    Ok(())
}
