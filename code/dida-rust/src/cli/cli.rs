use clap::{AppSettings, Clap};
use serde::{Deserialize, Serialize};

#[derive(Clap, Serialize, Deserialize, Debug)]
#[clap(
    version = "0.0.1",
    author = "Isitha Subasinghe <subasingheisitha@gmail.com>"
)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(short, long, default_value = "peers.toml")]
    timely_config: String,
    #[clap(short, long)]
    verbose: i32,
}

pub fn parse_opts() {
    let opts = Opts::parse();
    println!("{:?}", opts)
}
