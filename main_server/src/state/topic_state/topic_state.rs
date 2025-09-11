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

pub struct Topic {
    pub topics_set: HashSet<String>,
    pub topics_data: HashMap<String, Message>,
    pub messages_store: MessageStore,
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
        self.messages_store.add_topic(topic_name, partitions);
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
}
