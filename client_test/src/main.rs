use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Connect to the server
    let stream = TcpStream::connect("127.0.0.1:8080").await?;
    println!("Connected to 127.0.0.1:8080");

    // Split into read/write halves using BufReader for line-based reading
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // Send the message (include newline so the server echo can be read with lines())
    let msg: u8 = 1;
    write_half.write_all(&[msg]).await?;
    // Optionally flush (not strictly necessary for TCP here)
    write_half.flush().await?;

    // Read one line (the echoed message)
    let bytes_read = reader.read_line(&mut line).await?;
    if bytes_read == 0 {
        println!("Connection closed by server before a response was received");
    } else {
        // Trim the trailing newline for nicer printing
        print!("Received: {}", line.trim_end());
    }
    Ok(())
}

