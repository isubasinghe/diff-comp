use std::{fs::File, io::Read};

use differential_dataflow::input::Input;
// use differential_dataflow::lattice::Lattice;
// use differential_dataflow::operators::arrange::*;
// use differential_dataflow::operators::consolidate::Consolidate;
// use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::Join;
// use differential_dataflow::operators::reduce::*;
// use differential_dataflow::operators::Iterate;
// use differential_dataflow::{AsCollection, Collection};
use difflouvain_utils::shared::config::TimelyConfig;
use difflouvain_utils::shared::{Community, Node, ToEdge};
use difflouvain_utils::cli;

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
    let (config, _num_peers) = read_timely_config(&opts.timely_config);

    timely::execute(config, move |worker| {

        // let receiver = hashmap.get(&index).unwrap();

        let (mut nodes, mut edges) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection();
            let (edge_handle, edges) = scope.new_collection();

            // Find every node in C
            // find all edges of node
            // (node, (community, edge))
            let paths = nodes.join(&edges);

            paths.inspect(|(x, _, _)| println!("EDGE {:?}", x));

            // (n1's edge_node,  (n1,n1's community, n1's edge))
            let paths_by_edge_node = paths.map(|(node, (community, edge)): (Node<u32>, (Community, ToEdge<u32>))| (Node{id: edge.to}, (node, community, edge) ) );
            // (n1's edge node: n2, ((n1, n1's community, n'1 edge), (n2's community, n2's edge to n1))
            let paths_by_edge_node_comm = paths_by_edge_node.join_map(&nodes, |key, a, b|{
                (*key, *a, *b)
            });
            paths_by_edge_node_comm.inspect(|(x, _, _)| println!("G: {:?}", x));

            (node_handle, edge_handle)
        });


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