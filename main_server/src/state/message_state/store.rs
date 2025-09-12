use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

#[derive(Debug)]
pub struct MessageAndTotalMessageCount {
    pub messages: Vec<Vec<u8>>,
    total_messages: i32,
}

pub struct MessageStore {
    pub store: HashMap<String, HashMap<i32, MessageAndTotalMessageCount>>,
}

impl MessageStore {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    pub fn add_topic(&mut self, topic_name: String, partitions: i32) {
        if self.store.contains_key(&topic_name) {
            return;
        }
        let partitions_count;
        if partitions < 1 {
            partitions_count = 1;
        } else {
            partitions_count = partitions;
        }
        let mut partitions_map = HashMap::new();
        for i in 0..partitions_count {
            let msgs_and_total_count = MessageAndTotalMessageCount {
                messages: Vec::new(),
                total_messages: 0,
            };
            partitions_map.insert(i, msgs_and_total_count);
        }
        self.store.insert(topic_name, partitions_map);
    }

    pub fn delete_topic(&mut self, topic_name: &str) {
        self.store.remove(topic_name);
    }

    pub fn write_to_cache(
        &mut self,
        topic_name: &str,
        partition: i32,
        message: Vec<u8>,
    ) -> (i32, i32) {
        self.store
            .get_mut(topic_name)
            .unwrap()
            .get_mut(&partition)
            .unwrap()
            .messages
            .push(message);

        self.store
            .get_mut(topic_name)
            .unwrap()
            .get_mut(&partition)
            .unwrap()
            .total_messages += 1;

        let cache_size = self
            .store
            .get(topic_name)
            .unwrap()
            .get(&partition)
            .unwrap()
            .messages
            .len() as i32;

        let total_messages = self
            .store
            .get(topic_name)
            .unwrap()
            .get(&partition)
            .unwrap()
            .total_messages;
        return (cache_size, total_messages);
    }

    pub fn clear_vec_and_return(&mut self, topic_name: &str, partition: i32) -> Vec<Vec<u8>> {
        let messages = &mut self
            .store
            .get_mut(topic_name)
            .unwrap()
            .get_mut(&partition)
            .unwrap()
            .messages;
        std::mem::take(messages)
    }

    pub async fn get_message_by_offset(
        &self,
        partition: &i32,
        topic: &str,
        offset: i32,
    ) -> Option<Vec<u8>> {
        let total_messages = self
            .store
            .get(topic)
            .unwrap()
            .get(partition)
            .unwrap()
            .total_messages;
        if total_messages <= offset {
            return None;
        }

        let cache_start_offset = (total_messages / 10) * 10;
        if offset >= cache_start_offset {
            let message = self
                .store
                .get(topic)
                .unwrap()
                .get(&partition)
                .unwrap()
                .messages
                .get((offset % 10) as usize);
            if message.is_none() {
                return None;
            } else {
                let message = message.unwrap().clone();
                return Some(message);
            }
        } else {
            let message = self.read_line(offset, topic, partition).await;
            return Some(message);
        }
    }

    async fn read_line(&self, offset: i32, topic: &str, partition: &i32) -> Vec<u8> {
        let file_with_data = (offset / 10) * 10;
        let file_path = env::current_dir()
            .unwrap()
            .join("logs")
            .join(topic)
            .join(format!("{}", partition))
            .join(format!("{}.log", file_with_data));
        let f = File::open(file_path).unwrap();
        let mut lines = BufReader::new(f).lines();
        if let Some(line) = lines.nth(offset as usize) {
            let s = line.unwrap();
            return s.into_bytes();
        }
        return Vec::new();
    }

    pub async fn commit_offset(&self, partition: &i32, topic: &str, offset: i32) -> Result<(), ()> {
        let total_messages = self
            .store
            .get(topic)
            .unwrap()
            .get(partition)
            .unwrap()
            .total_messages;
        if total_messages <= offset {
            return Err(());
        }
        let path = env::current_dir()
            .unwrap()
            .join("offsets")
            .join(&topic)
            .join(format!("{}", partition));
        let res = self.write_to_file(path, offset).await;
        if res.is_err() {
            return Err(());
        }
        return Ok(());
    }

    async fn write_to_file<P: AsRef<Path>>(&self, path: P, value: i32) -> std::io::Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await?;
        let bytes = value.to_le_bytes();
        file.write_all(&bytes).await?;
        Ok(())
    }
}
