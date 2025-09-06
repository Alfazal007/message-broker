use crate::{
    CurrentTopics, PartitionToMessage,
    helpers::write_to_file::{append_vecs_to_file, create_file},
    message::producer_message::SendMessage,
};
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

    let cache_size = message_guard
        .get(&message.topic_name)
        .unwrap()
        .get(partition_to_send_to as usize)
        .unwrap()
        .messages
        .len() as i32;

    let list_of_values = message_guard
        .get(&message.topic_name)
        .unwrap()
        .get(partition_to_send_to as usize)
        .unwrap()
        .messages
        .clone();

    if cache_size != 10 {
        println!("cache size is {}", cache_size);
    }

    if cache_size == 10 {
        println!("Size is greater than 10 and size is {}", cache_size);

        let message_file_data = message_guard
            .get(&message.topic_name)
            .unwrap()
            .get(partition_to_send_to as usize)
            .unwrap();
        let messages_to_insert_here = 10 - message_file_data.lines_added_to_current_file as usize; // because
        // 10 is the sequence size in a file
        let values_to_put_in_current_file = &list_of_values[..messages_to_insert_here];
        let values_to_put_in_next_file = &list_of_values[messages_to_insert_here..];
        let res = append_vecs_to_file(
            message_file_data.current_file_number,
            values_to_put_in_current_file,
            &message.topic_name,
            partition_to_send_to,
        );
        if res.is_err() {
            println!("{:?}", res.err());
            return Err("Issue writing to the current file".to_string());
        }
        let res = create_file(
            message_file_data.current_file_number + 10,
            &message.topic_name,
            partition_to_send_to,
        );
        if res.is_err() {
            println!("{:?}", res.err());
            return Err("Issue creating the file".to_string());
        }
        if values_to_put_in_next_file.len() > 0 {
            let res = append_vecs_to_file(
                message_file_data.current_file_number + 10,
                values_to_put_in_next_file,
                &message.topic_name,
                partition_to_send_to,
            );
            if res.is_err() {
                println!("{:?}", res.err());
                return Err("Issue writing to the new file".to_string());
            }
        }
        if let Some(partitions) = message_guard.get_mut(&message.topic_name) {
            if let Some(partition) = partitions.get_mut(partition_to_send_to as usize) {
                partition.messages.clear();
                partition.current_file_number += 10;
                partition.lines_added_to_current_file = values_to_put_in_next_file.len() as i32;
            }
        }
    }
    Ok(())
}
