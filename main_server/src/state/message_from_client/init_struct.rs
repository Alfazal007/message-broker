use serde::Deserialize;

#[derive(Deserialize)]
pub struct InitProducerConsumer {
    pub message: i32,
}
