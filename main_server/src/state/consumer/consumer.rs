use std::sync::Arc;

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::RwLock,
};

use crate::state::{
    helpers::helper::Helper,
    message_from_client::message_for_consumer::message::{
        ConsumerMessage, JoinConsumer, LeaveConsumer,
    },
    message_to_client::{failure_message::Failure, success_message::Success},
    topic_state::topic_state::Topic,
};

pub struct Consumer {
    pub topics_data: Arc<RwLock<Topic>>,
    pub id: String,
    pub helper: Helper,
}

impl Consumer {
    pub fn new(topics_data: Arc<RwLock<Topic>>) -> Self {
        let helper = Helper::new();
        Self {
            id: helper.generate_unique_id(),
            helper,
            topics_data,
        }
    }

    pub async fn handler(&self, mut reader: BufReader<OwnedReadHalf>, mut writer: OwnedWriteHalf) {
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            match reader.read_until(b'\0', &mut buffer).await {
                Ok(0) => {
                    {
                        let mut topic_guard = self.topics_data.write().await;
                        topic_guard.disconnect_user(&self.id);
                    }
                    return;
                }
                Ok(n) => {
                    let message = serde_json::from_slice::<ConsumerMessage>(&buffer[..n]);
                    match message {
                        Err(_) => {
                            Failure::new().send_message(&mut writer).await;
                            continue;
                        }
                        Ok(message) => match message.message {
                            crate::state::message_from_client::message_for_consumer::message::Message::JOINCONSUMER(join_consumer_message) => {
                                let JoinConsumer {topic_name} = join_consumer_message;
                                let res;
                                {
                                    let mut topic_guard = self.topics_data.write().await;
                                    res = topic_guard.add_consumer(&self.id, &topic_name);
                                }
                                if res == -1 {
                                    Failure::new().send_message(&mut writer).await;
                                } else {
                                    Success::new().send_message(&mut writer).await;
                                }
                            },
                            crate::state::message_from_client::message_for_consumer::message::Message::LEAVECONSUMER(leave_consumer) => {
                                let LeaveConsumer {topic_name} = leave_consumer;
                                {
                                    let mut topic_guard = self.topics_data.write().await;
                                    topic_guard.leave_consumer(&self.id, &topic_name);
                                }
                                Success::new().send_message(&mut writer).await;
                                return;
                            },
                            crate::state::message_from_client::message_for_consumer::message::Message::GETOFFSETMESSAGE(get_offset_message) =>{
                                // TODO:: complete this
                            },
                            crate::state::message_from_client::message_for_consumer::message::Message::GETNEXTMESSAGE(get_next_message) => {
                                // TODO:: complete this
                            },
                            crate::state::message_from_client::message_for_consumer::message::Message::COMMITOFFSET(commit_offset) => {
                                // TODO:: complete this
                            }
                        },
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read from socket; err = {:?}", e);
                    return;
                }
            };
        }
    }
}
