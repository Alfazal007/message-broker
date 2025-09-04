use crate::{CurrentTopics, PartitionToMessage, message::producer_message::CreateTopicMessage};
use std::{collections::HashMap, env, path::Path, sync::Arc};
use tokio::{
    fs::{self, File},
    sync::Mutex,
};

pub async fn handle_create_topic(
    message: CreateTopicMessage,
    topics: Arc<Mutex<HashMap<String, CurrentTopics>>>,
    messages: Arc<Mutex<HashMap<String, Vec<PartitionToMessage>>>>,
) -> Result<(), String> {
    let CreateTopicMessage {
        topic_name,
        mut partitions,
    } = message;

    if partitions <= 0 {
        partitions = 1;
    }

    let should_create = {
        let topic_exists = topics.lock().await;
        !topic_exists.contains_key(&topic_name)
    };

    if should_create {
        let logs_path = env::current_dir().unwrap().join("logs").join(&topic_name);
        let res = fs::create_dir(&logs_path).await;
        if res.is_err() {
            return Err(format!("Issue creating topic directory {:?}", res));
        }
        let path = Path::new(&logs_path);
        for i in 0..partitions {
            let inner_folder_path = path.join(format!("{}", i));
            _ = fs::create_dir(&inner_folder_path).await;
            let file_path = inner_folder_path.join("0000.log");
            let file_res = File::create(&file_path).await;
            if file_res.is_err() {
                return Err(format!("Issue creating topic file {:?}", file_res.err()));
            }
        }
        {
            topics.lock().await.insert(
                topic_name.clone(),
                CurrentTopics {
                    partitions_count: partitions,
                    prev_inserted_partition: 0,
                },
            );

            let mut partitions_to_message: Vec<PartitionToMessage> = Vec::new();
            for i in 0..partitions {
                partitions_to_message.push({
                    PartitionToMessage {
                        partition: i,
                        messages: Vec::new(),
                    }
                });
            }
            messages
                .lock()
                .await
                .insert(topic_name.clone(), partitions_to_message);
        }
    }
    Ok(())
}
