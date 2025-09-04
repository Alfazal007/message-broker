use crate::{CurrentTopics, PartitionToMessage, message::producer_message::DeleteTopicMessage};
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    fs::{self},
    sync::Mutex,
};

pub async fn handle_delete_topic(
    message: DeleteTopicMessage,
    topics: Arc<Mutex<HashMap<String, CurrentTopics>>>,
    messages: Arc<Mutex<HashMap<String, Vec<PartitionToMessage>>>>,
) -> Result<(), String> {
    let DeleteTopicMessage { topic_name } = message;
    let should_delete = {
        let topic_exists = topics.lock().await;
        topic_exists.contains_key(&topic_name)
    };

    if should_delete {
        let logs_path = env::current_dir().unwrap().join("logs").join(&topic_name);
        let res = fs::remove_dir_all(&logs_path).await;
        if res.is_err() {
            return Err(format!("Issue deleting topic directory {:?}", res));
        }
        topics.lock().await.remove(&topic_name);
        messages.lock().await.remove(&topic_name);
    }
    Ok(())
}
