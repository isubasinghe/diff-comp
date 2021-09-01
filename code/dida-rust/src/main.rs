use std::sync::Arc;
use std::{fs::File, hash, io::Read};

use crossbeam::select;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::*;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::{AsCollection, Collection};
use shared::config::TimelyConfig;
use shared::{Community, Node, ToEdge, FromEdge, Unbind, Bind};
use timely::dataflow::operators::{Map, ToStream};
use timely::dataflow::Scope;
use utils::read_file;

mod cli;
mod shared;
mod utils;

fn read_timely_config(path: &str) -> (timely::Config, usize) {
    let mut file = File::open(path).unwrap();
    let mut s = String::new();
    file.read_to_string(&mut s).unwrap();
    let config: TimelyConfig = ron::from_str(&s).unwrap();

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
    timely::execute(config, move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        let receiver = hashmap.get(&index).unwrap();

        let timer = worker.timer();

        let (mut nodes, mut edges) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection();
            let (edge_handle, edges) = scope.new_collection();


            let communities = nodes
                                .map(|(_, c)| c).distinct();


            let paths = nodes.join_map(&edges, |_, _, to: &ToEdge<u32>| {
                ((), to.weight)
            });


            let m = paths.reduce(|_key, input, output| {
                let mut sum = 0;

                for (c, _) in input {
                    sum += **c;
                }
                output.push((sum, 1));
            });

            m.inspect(|(x, _, _)| println!("{:?}", x));

            (node_handle, edge_handle)
        });

        let (nodes_receiver, edges_receiver) = hashmap.get(&index).unwrap();

        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();

        let n1: Node<u32> = 20.bind();
        nodes.insert((n1, Community{id: 20, weights: 1}));
        nodes.insert((Node{id: 1}, Community{id: 1, weights: 1} ));
        edges.insert((Node{id: 1}, ToEdge{to: 2, weight: 3}));
        edges.insert((Node{id: 20}, ToEdge{to: 1, weight: 2}));
    })
    .expect("timely failed to start");
}
