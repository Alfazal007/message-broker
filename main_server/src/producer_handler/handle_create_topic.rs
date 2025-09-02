use crate::message::producer_message::CreateTopicMessage;
use std::{collections::HashSet, env, path::Path, sync::Arc};
use tokio::{
    fs::{self, File},
    sync::Mutex,
};

pub async fn handle_create_topic(
    message: CreateTopicMessage,
    topics: Arc<Mutex<HashSet<String>>>,
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
        !topic_exists.contains(&topic_name)
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
        topics.lock().await.insert(topic_name);
    }
    Ok(())
}
