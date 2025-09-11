use std::sync::Arc;

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::RwLock,
};

use crate::state::{
    helpers::helper::Helper,
    message_from_client::message_for_producer::message::{
        CreateTopic, DeleteTopic, MessageTopic, ProducerMessage,
    },
    message_to_client::{failure_message::Failure, success_message::Success},
    topic_state::topic_state::Topic,
};

pub struct Producer {
    pub id: String,
    pub helper: Helper,
    pub topics_data: Arc<RwLock<Topic>>,
}

impl Producer {
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
                    println!("disconnect");
                    return;
                }
                Ok(n) => {
                    let message = serde_json::from_slice::<ProducerMessage>(&buffer[..n - 1]);
                    match message {
                        Err(_) => {
                            Failure::new().send_message(&mut writer).await;
                            continue;
                        }
                        Ok(msg) => match msg.message {
                            crate::state::message_from_client::message_for_producer::message::Message::CREATETOPIC(message) => {
                                let CreateTopic { topic_name, partitions } = message;
                                {
                                    let mut topics_guard = self.topics_data.write().await;
                                    topics_guard.add_topic(topic_name, partitions).await;
                                }
                                let success_message = Success::new();
                                success_message.send_message(&mut writer).await;
                            },
                            crate::state::message_from_client::message_for_producer::message::Message::DELETETOPIC(message) => {
                                let DeleteTopic { topic_name } = message;
                                {
                                    let mut topics_guard = self.topics_data.write().await;
                                    topics_guard.delete_topic(&topic_name).await;
                                }
                                let success_message = Success::new();
                                success_message.send_message(&mut writer).await;
                            },
                            crate::state::message_from_client::message_for_producer::message::Message::MESSAGETOPIC(message) => {
                                let MessageTopic { topic_name, data, key } = message;
                                {
                                    let mut topics_guard = self.topics_data.write().await;
                                    topics_guard.send_message(key, data, topic_name).await;
                                }
                                let success_message = Success::new();
                                success_message.send_message(&mut writer).await;
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
