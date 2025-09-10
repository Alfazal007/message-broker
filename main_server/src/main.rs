use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    println!("Server listening on 127.0.0.1:8000");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("New connection from: {}", addr);

        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1024];
            loop {
                let n = match socket.read(&mut buffer).await {
                    Ok(0) => {
                        println!("Connection closed by {}", addr);
                        return;
                    }
                    Ok(n) => {
                        let data = &buffer[..n];
                        println!("data in {:?}", data.utf8_chunks());
                        n
                    }
                    Err(e) => {
                        eprintln!("Failed to read from socket; err = {:?}", e);
                        return;
                    }
                };
                if let Err(e) = socket.write_all(&buffer[..n]).await {
                    eprintln!("Failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}
