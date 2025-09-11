use std::collections::HashMap;

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
}
