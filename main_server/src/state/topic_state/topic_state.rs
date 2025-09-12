use std::{
    collections::{HashMap, HashSet},
    env,
    hash::{DefaultHasher, Hash, Hasher},
};

use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};

use crate::state::message_state::store::MessageStore;

pub struct ConsumerState {
    pub consumer_id: String,
    pub assigned_partitions: Vec<i32>,
    pub last_accessed_partition_index: i32,
}

pub struct Topic {
    pub topics_set: HashSet<String>,
    pub topics_data: HashMap<String, Message>,
    pub messages_store: MessageStore,
    pub consumers: HashMap<String, Vec<ConsumerState>>,
}

pub struct Message {
    pub partition_count: i32,
    pub prev_written_partition: i32,
}

impl Topic {
    pub fn new() -> Self {
        Self {
            topics_set: HashSet::new(),
            topics_data: HashMap::new(),
            messages_store: MessageStore::new(),
            consumers: HashMap::new(),
        }
    }

    pub async fn add_topic(&mut self, topic_name: String, partitions: i32) {
        if self.topics_set.contains(&topic_name) {
            return;
        }
        let path = env::current_dir().unwrap().join("logs").join(&topic_name);
        fs::create_dir(&path).await.unwrap();
        for i in 0..partitions {
            let partition_path = path.join(format!("{}", i));
            fs::create_dir(&partition_path).await.unwrap();
        }
        self.topics_set.insert(topic_name.clone());
        self.topics_data.insert(
            topic_name.clone(),
            Message {
                partition_count: partitions,
                prev_written_partition: -1,
            },
        );
        self.messages_store
            .add_topic(topic_name.clone(), partitions);
        self.consumers.insert(topic_name, Vec::new());
    }

    pub async fn delete_topic(&mut self, topic_name: &str) {
        if !self.topics_set.contains(topic_name) {
            return;
        }
        let path = env::current_dir().unwrap().join("logs").join(&topic_name);
        fs::remove_dir_all(path).await.unwrap();
        self.topics_set.remove(topic_name);
        self.topics_data.remove(topic_name);
        self.messages_store.delete_topic(topic_name);
        self.consumers.remove(topic_name);
    }

    pub async fn send_message(&mut self, key: Option<String>, data: Vec<u8>, topic_name: String) {
        if !self.topics_set.contains(&topic_name) {
            return;
        }
        let required_topic = self.topics_data.get(&topic_name).unwrap();
        let index;
        if key.is_some() {
            index = self.string_to_index(&key.unwrap(), required_topic.partition_count) as i32;
        } else {
            index = (required_topic.prev_written_partition + 1) % required_topic.partition_count;
            self.topics_data
                .get_mut(&topic_name)
                .unwrap()
                .prev_written_partition = index;
        }
        let (cache_size, total_messages_count) =
            self.messages_store.write_to_cache(&topic_name, index, data);
        if cache_size == 10 {
            let file_number_to_write_to = (total_messages_count / 10) * 10;
            let vec_data = self.messages_store.clear_vec_and_return(&topic_name, index);
            let file_path = env::current_dir()
                .unwrap()
                .join("logs")
                .join(topic_name)
                .join(format!("{}", index))
                .join(format!("{}.log", file_number_to_write_to));
            let mut file = File::create(file_path).await.unwrap();
            for line in vec_data {
                let _ = file.write_all(&line).await.unwrap();
                let _ = file.write_all(b"\n").await.unwrap();
            }
        }
    }

    fn string_to_index(&self, s: &str, n: i32) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let hash = hasher.finish();
        hash % (n as u64)
    }

    pub fn add_consumer(&mut self, connection_id: &str, topic_name: &str) -> i32 {
        if !self.topics_set.contains(topic_name) {
            return -1;
        }
        let partition_count = self.topics_data.get(topic_name).unwrap().partition_count;
        // no consumers
        if self.consumers.get(topic_name).unwrap().is_empty() {
            let mut consumer_state_vec = Vec::new();
            for i in 0..partition_count {
                consumer_state_vec.push(i);
            }
            let consumer_state = ConsumerState {
                consumer_id: connection_id.to_string(),
                assigned_partitions: consumer_state_vec,
                last_accessed_partition_index: -1,
            };
            self.consumers
                .get_mut(topic_name)
                .unwrap()
                .push(consumer_state);
            return 0;
        }
        let consumers_count = self.consumers.get(topic_name).unwrap().len() as i32;
        if consumers_count >= partition_count {
            return -1;
        }
        let first_consumer_data = self
            .consumers
            .get_mut(topic_name)
            .unwrap()
            .get_mut(0)
            .unwrap()
            .assigned_partitions
            .pop()
            .unwrap();

        let partition_for_new_consumer = vec![first_consumer_data];
        let consumer_state = ConsumerState {
            consumer_id: connection_id.to_string(),
            assigned_partitions: partition_for_new_consumer,
            last_accessed_partition_index: -1,
        };
        self.consumers
            .get_mut(topic_name)
            .unwrap()
            .push(consumer_state);
        return 0;
    }

    pub fn leave_consumer(&mut self, connection_id: &str, topic_name: &str) {
        if !self.topics_set.contains(topic_name) {
            return;
        }
        let consumers_count = self.consumers.get(topic_name).unwrap().len() as i32;
        let first_vec = self.consumers.get(topic_name).unwrap().get(0).unwrap();
        if consumers_count <= 1 && first_vec.consumer_id == connection_id {
            self.consumers.get_mut(topic_name).unwrap().clear();
            return;
        }
        let mut index = 0;
        for vec_data in self.consumers.get(topic_name).unwrap() {
            if vec_data.consumer_id == connection_id {
                break;
            } else {
                index += 1;
            }
        }
        let mut removed_vec = self.consumers.get_mut(topic_name).unwrap().remove(index);
        self.consumers
            .get_mut(topic_name)
            .unwrap()
            .get_mut(0)
            .unwrap()
            .assigned_partitions
            .extend(removed_vec.assigned_partitions.drain(..));
    }

    pub fn disconnect_user(&mut self, connection_id: &str) {
        let mut topic_name = "".to_string();
        let mut index = 0;
        'outer: for (topic_name_of_consumer, consumer_vec) in self.consumers.iter() {
            let mut index_of_consumer = 0;
            for consumer in consumer_vec.iter() {
                if consumer.consumer_id == connection_id {
                    topic_name = topic_name_of_consumer.clone();
                    index = index_of_consumer;
                    break 'outer;
                } else {
                    index_of_consumer += 1;
                }
            }
        }
        if topic_name.is_empty() {
            return;
        }
        let consumers_count = self.consumers.get(&topic_name).unwrap().len() as i32;
        let first_vec = self.consumers.get(&topic_name).unwrap().get(0).unwrap();
        if consumers_count <= 1 && first_vec.consumer_id == connection_id {
            self.consumers.get_mut(&topic_name).unwrap().clear();
            return;
        }
        let mut removed_vec = self.consumers.get_mut(&topic_name).unwrap().remove(index);
        self.consumers
            .get_mut(&topic_name)
            .unwrap()
            .get_mut(0)
            .unwrap()
            .assigned_partitions
            .extend(removed_vec.assigned_partitions.drain(..));
    }
}
