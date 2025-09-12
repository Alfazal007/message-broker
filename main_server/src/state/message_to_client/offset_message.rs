use serde::Serialize;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

#[derive(Serialize)]
pub struct OffsetMessage {
    pub message: Vec<u8>,
}

impl OffsetMessage {
    pub fn new(msg: Vec<u8>) -> Self {
        Self { message: msg }
    }

    pub async fn send_message(&self, write_half: &mut OwnedWriteHalf) {
        let msg = Self {
            message: self.message.clone(),
        };
        let mut vec = serde_json::to_vec(&msg).unwrap();
        vec.push(b'\0');
        let _ = write_half.write_all(&vec).await;
        let _ = write_half.flush().await;
    }
}
