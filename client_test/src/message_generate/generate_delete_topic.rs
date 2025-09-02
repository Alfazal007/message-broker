use std::error::Error;

use crate::message::producer_message::{DeleteTopicMessage, MessageTypes, ProducerMessage};

pub fn delete_topic_message(topic_name: String) -> Result<Vec<u8>, Box<dyn Error>> {
    let msg = ProducerMessage {
        message: MessageTypes::DeleteTopic(DeleteTopicMessage { topic_name }),
    };
    let mut json_bytes = serde_json::to_vec(&msg)?;
    json_bytes.push(b'\0');
    Ok(json_bytes)
}
