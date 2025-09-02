use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ProducerMessage {
    pub message: MessageTypes,
}

#[derive(Deserialize, Debug)]
pub enum MessageTypes {
    CreateTopic(CreateTopicMessage),
    DeleteTopic(DeleteTopicMessage),
    SendMessage(SendMessage),
}

#[derive(Deserialize, Debug)]
pub struct CreateTopicMessage {
    pub topic_name: String,
    pub partitions: i32,
}

#[derive(Deserialize, Debug)]
pub struct DeleteTopicMessage {
    pub topic_name: String,
}

#[derive(Deserialize, Debug)]
pub struct SendMessage {
    pub topic_name: String,
    pub message: Vec<u8>,
}
