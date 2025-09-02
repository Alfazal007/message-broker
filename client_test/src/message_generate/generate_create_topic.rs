use std::error::Error;

use crate::message::producer_message::{CreateTopicMessage, MessageTypes, ProducerMessage};

pub fn create_topic_message(
    topic_name: String,
    partitions: i32,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let msg = ProducerMessage {
        message: MessageTypes::CreateTopic(CreateTopicMessage {
            topic_name,
            partitions,
        }),
    };
    let mut json_bytes = serde_json::to_vec(&msg)?;
    json_bytes.push(b'\0');
    Ok(json_bytes)
}
