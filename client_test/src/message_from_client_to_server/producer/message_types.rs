use serde::Serialize;

#[derive(Serialize)]
pub struct ProducerMessage {
    pub message: Message,
}

#[derive(Serialize)]
pub enum Message {
    CREATETOPIC(CreateTopic),
    DELETETOPIC(DeleteTopic),
    MESSAGETOPIC(MessageTopic),
}

#[derive(Serialize)]
pub struct CreateTopic {
    pub topic_name: String,
    pub partitions: i32,
}

#[derive(Serialize)]
pub struct DeleteTopic {
    pub topic_name: String,
}

#[derive(Serialize)]
pub struct MessageTopic {
    pub topic_name: String,
    pub data: Vec<u8>,
}

impl ProducerMessage {
    pub fn new(type_of_msg: Message) -> Vec<u8> {
        let msg = ProducerMessage {
            message: type_of_msg,
        };
        let mut vec_data = serde_json::to_vec(&msg).unwrap();
        vec_data.push(b'\0');
        return vec_data;
    }
}
