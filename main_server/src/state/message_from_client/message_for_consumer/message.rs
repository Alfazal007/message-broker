use serde::Deserialize;

#[derive(Deserialize)]
pub struct ConsumerMessage {
    pub message: Message,
}

#[derive(Deserialize)]
pub enum Message {
    JOINCONSUMER(JoinConsumer),
    LEAVECONSUMER(LeaveConsumer),
    GETOFFSETMESSAGE(GetOffsetMessage),
    COMMITOFFSET(CommitOffset),
}

#[derive(Deserialize)]
pub struct JoinConsumer {
    pub topic_name: String,
}

#[derive(Deserialize)]
pub struct LeaveConsumer {
    pub topic_name: String,
}

#[derive(Deserialize)]
pub struct GetOffsetMessage {
    pub topic_name: String,
    pub partition: i32,
    pub offset: i32,
}

#[derive(Deserialize)]
pub struct CommitOffset {
    pub topic_name: String,
    pub partition: i32,
    pub offset: i32,
}
