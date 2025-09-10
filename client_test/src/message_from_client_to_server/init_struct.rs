use serde::Serialize;

#[derive(Serialize)]
pub struct InitProducerConsumer {
    pub message: i32,
}

impl InitProducerConsumer {
    pub fn new_producer_message() -> Vec<u8> {
        let message = Self { message: 0 };
        let mut vec_data = serde_json::to_vec(&message).unwrap();
        vec_data.push(b'\0');
        return vec_data;
    }

    pub fn new_consumer_message() -> Vec<u8> {
        let message = Self { message: 1 };
        let mut vec_data = serde_json::to_vec(&message).unwrap();
        vec_data.push(b'\0');
        return vec_data;
    }
}
