use core::cmp::Ordering;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::*;
use differential_dataflow::operators::Iterate;
use differential_dataflow::{AsCollection, Collection};
use difflouvain_utils::cli;
use difflouvain_utils::shared::config::TimelyConfig;
use difflouvain_utils::shared::{CommEdge, Community, Node, ToEdge};
use num_traits::Float;

use std::sync::{Arc, Mutex};
use std::{fs::File, io::Read};

#[derive(Debug, Default, Clone, Copy)]
struct OrderedFloat<T>(pub T);

impl<T: Float> OrderedFloat<T> {
    #[inline]
    fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Float> AsMut<T> for OrderedFloat<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Float> PartialEq for OrderedFloat<T> {
    fn eq(&self, other: &Self) -> bool {
        false
    }
}

impl<T: Float> PartialOrd for OrderedFloat<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        None
    }
}

impl<T: Float> PartialEq<T> for OrderedFloat<T> {
    fn eq(&self, other: &T) -> bool {
        false
    }
}

impl<T: Float> PartialOrd<T> for OrderedFloat<T> {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        None
    }
}

impl<T: Float> Eq for OrderedFloat<T> {}

impl<T: Float> Ord for OrderedFloat<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        Ordering::Equal
    }
}

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
            // This is simply all the paths that exist
            // (node, (community, edge))
            let paths = nodes.join(&edges);

            let m_times_2 = paths
                .explode(|(_, (_, e)): (_, (_, ToEdge<u32>))| Some(((), e.weight as isize)))
                .count();

            let k_i = paths.reduce(
                |_key, input: &[(&(Community, ToEdge<u32>), isize)], output| {
                    let mut sum = 0;

                    for ((_c, e), _) in input {
                        sum += e.weight;
                    }
                    output.push((sum, 1));
                },
            );
            // Simple mapping to obtain the node from n1
            // (n1's edge_node,  (n1,n1's community, n1's edge))
            let paths_by_edge_node = paths.map(
                |(node, (community, edge)): (Node<u32>, (Community, ToEdge<u32>))| {
                    (Node { id: edge.to }, (node, community, edge))
                },
            );

            // Obtain the community of the edge
            // (n1's edge node: n2, ((n1, n1's community, n'1 edge), (n2's community, n2's edge to n1))
            let paths_by_edge_node_comm = paths_by_edge_node
                .join_map(&nodes, |n2, (n1, n1c, edge), n2c| {
                    (*n1c, (*n1, *n2, *edge, *n2c))
                });

            let sigma_k_in = paths_by_edge_node_comm
                .map(|(n1c, (n1, n2, edge, n2c))| ((n1, n2c), (n1c, n2, edge)))
                .filter(|((_n1, n2c), (n1c, _n2, _edge))| n1c != n2c)
                .reduce(|(n1, n2c), input, output| {
                    let mut sum: i64 = 0;
                    for ((n1c, n2, edge), _) in input {
                        sum += edge.weight as i64;
                    }
                    output.push((sum, 1));
                });

            // (n2,n1) and (n1,n2) present, this will double weights
            // so we sort and take the lowest essentially n1 < n2 does this
            // index by community as well
            let communities =
                paths_by_edge_node_comm.filter(|(n1c, (_n1, _n2, _edge, n2c))| n1c == n2c);

            let sigma_total = paths_by_edge_node_comm
                .filter(|(n1c, (_n1, _n2, _edge, n2c))| n1c != n2c)
                .reduce(|_key, input, output| {
                    let mut sum: i64 = 0;
                    for ((_n1, _n2, edge, _), _) in input {
                        sum += edge.weight as i64;
                    }
                    output.push((sum, 1));
                });

            // find all the weights for a given community
            let sigma_in = communities.reduce(|_key, input, output| {
                let mut sum: i64 = 0;

                for ((n1, n2, edge, _n2c), _) in input {
                    if n1 < n2 {
                        sum += edge.weight as i64;
                    }
                }
                output.push((sum, 1));
            });

            let aggreg = paths_by_edge_node_comm
                .join_map(&sigma_in, |n1c, (n1, n2, edge, n2c), sigma_in| {
                    (*n1c, (*n1, *n2, *edge, *n2c, *sigma_in))
                })
                .join_map(
                    &sigma_total,
                    |n1c, (n1, n2, edge, n2c, sigma_in), sigma_tot| {
                        ((*n2, *n1c), (*n1, *edge, *n2c, *sigma_in, *sigma_tot))
                    },
                )
                .join_map(
                    &sigma_k_in,
                    |(n2, n1c), (n1, edge, n2c, sigma_in, sigma_tot), sigma_k_in| {
                        (
                            *n2,
                            (*n1, *n1c, *edge, *n2c, *sigma_in, *sigma_tot, *sigma_k_in),
                        )
                    },
                )
                .join_map(
                    &k_i,
                    |n2, (n1, n1c, edge, n2c, sigma_in, sigma_tot, sigma_k_in), k_in| {
                        (
                            (),
                            (
                                *n1,
                                *n1c,
                                *edge,
                                *n2,
                                *n2c,
                                *sigma_in,
                                *sigma_tot,
                                *sigma_k_in,
                                *k_in,
                            ),
                        )
                    },
                )
                .join_map(
                    &m_times_2,
                    |_, (n1, n1c, edge, n2, n2c, sigma_in, sigma_tot, sigma_k_in, k_in), m| {},
                );

            aggreg.inspect(|(x, _, _)| println!("{:?}", x));

            (node_handle, edge_handle)
        });

        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();

        nodes.insert((Node { id: 20 }, Community { id: 1, weights: 1 }));
        nodes.insert((Node { id: 1 }, Community { id: 1, weights: 1 }));
        nodes.insert((Node { id: 3 }, Community { id: 1, weights: 1 }));

        nodes.insert((Node { id: 2 }, Community { id: 2, weights: 1 }));

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

        edges.insert((Node { id: 5 }, ToEdge { to: 1, weight: 1 }));
        edges.insert((Node { id: 1 }, ToEdge { to: 5, weight: 1 }));

        edges.insert((Node { id: 8 }, ToEdge { to: 20, weight: 23 }));
        edges.insert((Node { id: 20 }, ToEdge { to: 8, weight: 23 }));

        edges.insert((Node { id: 5 }, ToEdge { to: 7, weight: 17 }));
        edges.insert((Node { id: 7 }, ToEdge { to: 5, weight: 17 }));
    })
    .expect("timely failed to start");
}
