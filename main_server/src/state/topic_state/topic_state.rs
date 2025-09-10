use std::{
    collections::{HashMap, HashSet},
    env,
};

use tokio::fs::{self, File};

pub struct Topic {
    pub topics_set: HashSet<String>,
    pub topics_data: HashMap<String, Message>,
}

pub struct Message {
    partition_count: i32,
}

impl Topic {
    pub fn new() -> Self {
        Self {
            topics_set: HashSet::new(),
            topics_data: HashMap::new(),
        }
    }

    pub async fn add_topic(&mut self, topic_name: String, partitions: i32) {
        if self.topics_set.contains(&topic_name) {
            return;
        }
        let path = env::current_dir().unwrap().join("logs").join(&topic_name);
        fs::create_dir(&path).await.unwrap();
        for i in 0..partitions {
            let partition_path = path.join(format!("{}.log", i));
            File::create(&partition_path).await.unwrap();
        }
        self.topics_set.insert(topic_name.clone());
        self.topics_data.insert(
            topic_name,
            Message {
                partition_count: partitions,
            },
        );
    }

    pub async fn delete_topic(&mut self, topic_name: &str) {
        if !self.topics_set.contains(topic_name) {
            return;
        }
        let path = env::current_dir().unwrap().join("logs").join(&topic_name);
        fs::remove_dir_all(path).await.unwrap();
        self.topics_set.remove(topic_name);
        self.topics_data.remove(topic_name);
    }
}
