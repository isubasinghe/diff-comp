use std::sync::Arc;
use std::{fs::File, hash, io::Read};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::Scope;
use timely::dataflow::operators::{ToStream, Map};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::join::{Join};
use differential_dataflow::operators::arrange::*;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::consolidate::Consolidate;
use crossbeam::select;
use shared::{Community, Edge, ToEdge, FromEdge, Node};
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

        let timer = worker.timer();

        let (mut nodes,  mut edges) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection();
            let (edge_handle, edges) = scope.new_collection();

            let nodes = nodes.map(|node_id| (node_id, node_id));

            let communities = nodes
                .map(|(_, c)| c)
                .distinct();

            let paths = nodes.join(&edges);

            paths.inspect(|x| println!("{:?}", x));

            (node_handle, edge_handle)

        });

        let (nodes_receiver, edges_receiver) = hashmap.get(&index).unwrap();


        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();

        nodes.insert(Node{id: 0});
        
        edges.insert((0, (2, 1)));
   

    }).expect("timely failed to start");


}
