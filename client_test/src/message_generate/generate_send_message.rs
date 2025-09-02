use std::error::Error;

use crate::message::producer_message::{MessageTypes, ProducerMessage, SendMessage};

pub fn send_topic_message(topic_name: String) -> Result<Vec<u8>, Box<dyn Error>> {
    let msg = ProducerMessage {
        message: MessageTypes::SendMessage(SendMessage {
            topic_name,
            message: "Hello world".as_bytes().to_vec(),
        }),
    };
    let mut json_bytes = serde_json::to_vec(&msg)?;
    json_bytes.push(b'\0');
    Ok(json_bytes)
}
