use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{
    message_from_client_to_server::{
        init_struct::InitProducerConsumer,
        producer::message_types::{
            CreateTopic, DeleteTopic, Message, MessageTopic, ProducerMessage,
        },
    },
    message_from_server_to_client::success_message::Success,
};

pub async fn produce_task() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:8000").await?;
    let (read_half, mut write_half) = stream.into_split();
    println!("producer connected to server at 127.0.0.1:8000");
    let init_message = InitProducerConsumer::new_producer_message();
    let _ = write_half.write_all(&init_message).await;
    let _ = write_half.flush().await;
    let mut reader = BufReader::new(read_half);
    let mut buffer = Vec::new();

    let n = reader.read_until(b'\0', &mut buffer).await.unwrap();
    serde_json::from_slice::<Success>(&buffer[..n - 1]).unwrap();
    let create_topic_msg = ProducerMessage::new(Message::CREATETOPIC(CreateTopic {
        topic_name: "new_topic".to_string(),
        partitions: 4,
    }));
    let delete_topic_msg = ProducerMessage::new(Message::DELETETOPIC(DeleteTopic {
        topic_name: "new_topic".to_string(),
    }));

    let _ = write_half.write_all(&create_topic_msg).await;
    let _ = write_half.flush().await;
    let n = reader.read_until(b'\0', &mut buffer).await.unwrap();
    serde_json::from_slice::<Success>(&buffer[..n - 1]).unwrap();

    for i in 1..80 {
        let normal_message_without_key =
            ProducerMessage::new(Message::MESSAGETOPIC(MessageTopic {
                key: None,
                topic_name: "new_topic".to_string(),
                data: format!("Message without key = {:?}", i).into_bytes(),
            }));
        let _ = write_half.write_all(&normal_message_without_key).await;
        let _ = write_half.flush().await;

        let n = reader.read_until(b'\0', &mut buffer).await.unwrap();
        serde_json::from_slice::<Success>(&buffer[..n - 1]).unwrap();
    }

    for i in 1..23 {
        let normal_message_with_key = ProducerMessage::new(Message::MESSAGETOPIC(MessageTopic {
            key: Some("test".to_string()),
            topic_name: "new_topic".to_string(),
            data: format!("Message with key = {:?}", i).into_bytes(),
        }));
        let _ = write_half.write_all(&normal_message_with_key).await;
        let _ = write_half.flush().await;

        let n = reader.read_until(b'\0', &mut buffer).await.unwrap();
        serde_json::from_slice::<Success>(&buffer[..n - 1]).unwrap();
    }

    /*
        let _ = write_half.write_all(&delete_topic_msg).await;
        let _ = write_half.flush().await;
        let n = reader.read_until(b'\0', &mut buffer).await.unwrap();
        serde_json::from_slice::<Success>(&buffer[..n - 1]).unwrap();
    */
    Ok(())
}
