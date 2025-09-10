use serde::Deserialize;

#[derive(Deserialize)]
pub struct ProducerMessage {
    pub message: Message,
}

#[derive(Deserialize)]
pub enum Message {
    CREATETOPIC(CreateTopic),
    DELETETOPIC(DeleteTopic),
    MESSAGETOPIC(MessageTopic),
}

#[derive(Deserialize)]
pub struct CreateTopic {
    pub topic_name: String,
    pub partitions: i32,
}

#[derive(Deserialize)]
pub struct DeleteTopic {
    pub topic_name: String,
}

#[derive(Deserialize)]
pub struct MessageTopic {
    pub key: Option<String>,
    pub topic_name: String,
    pub data: Vec<u8>,
}
