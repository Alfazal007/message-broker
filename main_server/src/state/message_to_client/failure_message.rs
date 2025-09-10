use serde::Serialize;

#[derive(Serialize)]
pub struct Failure {}

impl Failure {
    pub fn new() -> Vec<u8> {
        let msg = Self {};
        let mut vec = serde_json::to_vec(&msg).unwrap();
        vec.push(b'\0');
        vec
    }
}
