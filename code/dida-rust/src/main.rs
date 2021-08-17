use std::{cell::RefCell, env, fs::File, io::Read, sync::Mutex};

use shared::config::TimelyConfig;

mod cli;
mod utils;
mod shared;

fn read_timely_config(path: &str) -> timely::Config {
    let mut file = File::open(path).unwrap();
    let mut s = String::new();
    file.read_to_string(&mut s).unwrap();
    let config: TimelyConfig =  ron::from_str(&s).unwrap();

    println!("LOADED CONFIG: {:?}", config);
    
    let config = config.into_timely();

    let worker_config = timely::WorkerConfig::default();

    timely::Config {
        communication: config, 
        worker: worker_config,
    }

}

fn main() {

    let opts = cli::parse_opts();
    let config = read_timely_config(&opts.timely_config);

    

    timely::execute(config, |worker| {

    }).expect("timely failed to start");


}
