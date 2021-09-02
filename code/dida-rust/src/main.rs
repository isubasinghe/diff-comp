use std::sync::Arc;
use std::{fs::File, hash, io::Read};

use crossbeam::select;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::*;
use differential_dataflow::operators::Iterate;
use differential_dataflow::{AsCollection, Collection};
use shared::config::TimelyConfig;
use shared::{Bind, Community, FromEdge, Node, ToEdge, Unbind};
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

        // let receiver = hashmap.get(&index).unwrap();

        let (mut nodes, mut edges) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection();
            let (edge_handle, edges) = scope.new_collection();

            let paths = nodes.join(&edges);
            paths.inspect(|(x, _, _)| println!("PATH1: {:?}", x));
            let paths = paths.map(
                |(node, (community, to_edge)): (Node<u32>, (Community, ToEdge<u32>))| {
                    (Node { id: to_edge.to }, (node, community))
                },
            );

            let paths = paths
                .join_map(&nodes, |key, a, b| {
                    let a = *a;
                    let community1 = a.1;
                    let community2 = *b;
                    let node1;
                    let node2;
                    if key < &a.0 {
                        node1 = *key;
                        node2 = a.0;
                    } else {
                        node1 = a.0;
                        node2 = *key;
                    }
                    ((node1, node2), (community1, community2))
                })
                .filter(|((_, _), (c1, c2))| c1.id == c2.id)
                .distinct();

            paths.inspect(|(x, _, _)| println!("PATHS: {:?}", x));

            let paths = nodes.join_map(&edges, |key, _, to: &ToEdge<u32>| (*key, to.weight));

            paths.inspect(|x| println!("{:?}", x));

            let k_i = paths.reduce(|_, input, output| {
                let mut sum = 0;

                for (c, _) in input {
                    sum += **c;
                }
                output.push((sum, 1));
            });

            k_i.inspect(|x| println!("REDUCE: {:?}", x));

            (node_handle, edge_handle)
        });

        let (nodes_receiver, edges_receiver) = hashmap.get(&index).unwrap();

        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();

        nodes.insert((Node { id: 20 }, Community { id: 1, weights: 1 }));
        nodes.insert((Node { id: 1 }, Community { id: 1, weights: 1 }));
        nodes.insert((Node { id: 3 }, Community { id: 1, weights: 1 }));

        edges.insert((Node { id: 1 }, ToEdge { to: 3, weight: 5 }));
        edges.insert((Node { id: 3 }, ToEdge { to: 1, weight: 5 }));

        edges.insert((Node { id: 3 }, ToEdge { to: 2, weight: 7 }));
        edges.insert((Node { id: 2 }, ToEdge { to: 3, weight: 7 }));

        edges.insert((Node { id: 1 }, ToEdge { to: 20, weight: 11 }));
        edges.insert(((Node { id: 20 }), ToEdge { to: 1, weight: 11 }));

        nodes.insert((Node { id: 5 }, Community { id: 5, weights: 1 }));
        nodes.insert((Node { id: 6 }, Community { id: 5, weights: 1 }));
        nodes.insert((Node { id: 7 }, Community { id: 5, weights: 1 }));
        nodes.insert((Node { id: 8 }, Community { id: 5, weights: 1 }));

        edges.insert((Node { id: 5 }, ToEdge { to: 6, weight: 13 }));
        edges.insert((Node { id: 6 }, ToEdge { to: 5, weight: 13 }));
    })
    .expect("timely failed to start");
}
