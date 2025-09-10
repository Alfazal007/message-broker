use uuid::Uuid;

pub struct Helper {}

impl Helper {
    pub fn new() -> Self {
        Self {}
    }

    pub fn generate_unique_id(&self) -> String {
        let uuid = Uuid::new_v4();
        uuid.to_string()
    }
}
