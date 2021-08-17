use serde::{Deserialize, Serialize};
use timely::CommunicationConfig;


#[derive(Serialize, Deserialize, Debug)]
pub enum TimelyConfig {
    Thread,
    Process(usize),
    Cluster {
        threads: usize,
        process: usize,
        addresses: Vec<String>,
        report: bool,
    },
}


impl TimelyConfig {
    pub fn into_timely(self) -> CommunicationConfig {

        match self {
            TimelyConfig::Thread => CommunicationConfig::Thread,
            TimelyConfig::Process(process) => CommunicationConfig::Process(process),
            TimelyConfig::Cluster {threads, process, addresses, report} 
                => CommunicationConfig::Cluster{threads, process, addresses, report, log_fn: Box::new(|_| None) }
        }
    }
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