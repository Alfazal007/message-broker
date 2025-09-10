use serde::Serialize;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

#[derive(Serialize)]
pub struct Failure {}

impl Failure {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn send_message(&self, write_half: &mut OwnedWriteHalf) {
        let msg = Self {};
        let mut vec = serde_json::to_vec(&msg).unwrap();
        vec.push(b'\0');
        let _ = write_half.write_all(&vec).await;
        let _ = write_half.flush().await;
    }
}
