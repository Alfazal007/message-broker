use serde::Serialize;

#[derive(Serialize)]
pub struct ProducerMessage {
    pub message: MessageTypes,
}

#[derive(Serialize)]
pub enum MessageTypes {
    CreateTopic(CreateTopicMessage),
    DeleteTopic(DeleteTopicMessage),
    SendMessage(SendMessage),
}

#[derive(Serialize)]
pub struct CreateTopicMessage {
    pub topic_name: String,
    pub partitions: i32,
}

#[derive(Serialize)]
pub struct DeleteTopicMessage {
    pub topic_name: String,
}

#[derive(Serialize)]
pub struct SendMessage {
    pub topic_name: String,
    pub message: Vec<u8>,
}
