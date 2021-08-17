use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum TimelyConfig {
    Thread,
    Process(usize),
    ProcessBinary(usize),
    Cluster {
        threads: usize,
        process: usize,
        addresses: Vec<String>,
        report: bool,
    },
}


pub enum InputConfig {
    File(String),
    Kafka {
        host: String,
        port: String, 
        topic: String,
        password: String, 
        user: String,
    }
}