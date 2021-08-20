use std::{fs::File, hash, io::Read};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::Scope;
use timely::dataflow::operators::{ToStream, Map};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::consolidate::Consolidate;

use shared::config::TimelyConfig;
use utils::read_file;

mod cli;
mod utils;
mod shared;

fn read_timely_config(path: &str) -> (timely::Config, usize) {
    let mut file = File::open(path).unwrap();
    let mut s = String::new();
    file.read_to_string(&mut s).unwrap();
    let config: TimelyConfig =  ron::from_str(&s).unwrap();

    println!("LOADED CONFIG: {:?}", config);
    
    let num_peers = config.num_peers();

    let config = config.into_timely();

    let worker_config = timely::WorkerConfig::default();

    let config = timely::Config {
        communication: config, 
        worker: worker_config,
    };

    (config, num_peers)

}

fn main() {

    let opts = cli::parse_opts();
    let (config, num_peers) = read_timely_config(&opts.timely_config);
    
    let hashmap = read_file(&opts.data, num_peers);
    timely::execute(config,  move |worker| {
        
        let index = worker.index();
        let peers = worker.peers();
        
        let receiver = hashmap.get(&index).unwrap();

        // let mut probe = timely::dataflow::ProbeHandle::new();
        
        let sources = worker.dataflow(|scope| {
            
            let (handle, source) = scope.new_collection();
        });


    }).expect("timely failed to start");


}
