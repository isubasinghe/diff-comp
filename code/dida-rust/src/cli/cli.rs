use clap::{AppSettings, Clap};
use serde::{Deserialize, Serialize};

#[derive(Clap, Serialize, Deserialize, Debug)]
#[clap(
    version = "0.0.1",
    author = "Isitha Subasinghe <subasingheisitha@gmail.com>"
)]
#[clap(setting = AppSettings::ColoredHelp)]
pub struct Opts {
    #[clap(short, long)]
    pub data: String,
    #[clap(short, long, default_value = "communication.ron")]
    pub timely_config: String,
    #[clap(short, long, default_value = "0")]
    pub verbose: u8,
    #[clap(short, long, default_value = "5000")]
    pub run_time: u128,
}

pub fn parse_opts() -> Opts {
    Opts::parse()
}
