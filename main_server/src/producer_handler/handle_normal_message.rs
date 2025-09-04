use crate::{CurrentTopics, PartitionToMessage, message::producer_message::SendMessage};
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};
use tokio::sync::Mutex;

pub async fn handle_normal_topic(
    message: SendMessage,
    topics: Arc<Mutex<HashMap<String, CurrentTopics>>>,
    messages: Arc<Mutex<HashMap<String, Vec<PartitionToMessage>>>>,
) -> Result<(), String> {
    let (topic_exists, partition_count, prev_inserted_partition) = 'topic_block: {
        let topic_exists = topics.lock().await;
        let topic_exists_in_map = topic_exists.contains_key(&message.topic_name);
        if !topic_exists_in_map {
            break 'topic_block (topic_exists_in_map, -1, -1);
        }
        let topic = topic_exists.get(&message.topic_name).unwrap();
        (
            topic_exists_in_map,
            topic.partitions_count,
            topic.prev_inserted_partition,
        )
    };
    if !topic_exists {
        return Err("Topic does not exist".to_string());
    }
    let partition_to_send_to;

    if message.key.is_some() {
        let mut hasher = DefaultHasher::new();
        message.key.as_ref().unwrap().hash(&mut hasher);
        let hash = hasher.finish();
        partition_to_send_to = (hash % partition_count as u64) as i32;
    } else {
        partition_to_send_to = (prev_inserted_partition + 1) % partition_count;
        let mut topics_guard = topics.lock().await;
        topics_guard
            .get_mut(&message.topic_name)
            .unwrap()
            .prev_inserted_partition = partition_to_send_to;
    }
    println!("Partition to send to {:?}", partition_to_send_to);
    let mut message_guard = messages.lock().await;
    message_guard
        .get_mut(&message.topic_name)
        .unwrap()
        .get_mut(partition_to_send_to as usize)
        .unwrap()
        .messages
        .push(message.message);

    let size = message_guard
        .get(&message.topic_name)
        .unwrap()
        .get(partition_to_send_to as usize)
        .unwrap()
        .messages
        .len();

    let list_of_values = message_guard
        .get(&message.topic_name)
        .unwrap()
        .get(partition_to_send_to as usize)
        .unwrap()
        .messages
        .clone();

    if size >= 10 {
        println!("Size is greater than 10 and size is {}", size);
        println!("{:?}", list_of_values);
    }
    Ok(())
}
