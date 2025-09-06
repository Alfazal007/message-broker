use std::{
    env,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
};

pub fn create_file(file_number: i32, topic_name: &str, partition: i32) -> Result<(), String> {
    let file_name = format!("{}.log", file_number);
    let logs_path = env::current_dir().unwrap().join("logs").join(topic_name);
    let path = Path::new(&logs_path);
    let inner_folder_path = path.join(format!("{}", partition));
    let file_path = inner_folder_path.join(file_name);
    let file_create_result = File::create(file_path);
    if file_create_result.is_err() {
        return Err("Issue creating new file".to_string());
    }
    Ok(())
}

pub fn append_vecs_to_file(
    file_number: i32,
    data: &[Vec<u8>],
    topic_name: &str,
    partition: i32,
) -> io::Result<()> {
    let file_name = format!("{}.log", file_number);
    let logs_path = env::current_dir().unwrap().join("logs").join(topic_name);
    let path = Path::new(&logs_path);
    let inner_folder_path = path.join(format!("{}", partition));
    let file_path = inner_folder_path.join(file_name);
    let mut file = OpenOptions::new().append(true).open(file_path)?;
    for line in data {
        file.write_all(&line)?;
        file.write_all(b"\n")?;
    }
    Ok(())
}
