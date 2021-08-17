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
