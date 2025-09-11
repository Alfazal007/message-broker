use tokio::try_join;

use crate::producer_handler::producer_task::produce_task;

pub mod message_from_client_to_server;
pub mod message_from_server_to_client;
pub mod producer_handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer_handler_task = tokio::spawn(async move {
        let _ = produce_task().await;
    });

    try_join!(producer_handler_task)?;
    Ok(())
}
