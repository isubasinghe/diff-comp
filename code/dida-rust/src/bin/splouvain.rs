use abomonation_derive::Abomonation;
use core::cmp::Ordering;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{Arrange, ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::{Join, JoinCore};
use differential_dataflow::operators::reduce::*;
use difflouvain_utils::cli;
use difflouvain_utils::shared::config::TimelyConfig;
use difflouvain_utils::shared::{Community, Node, ToEdge};
use difflouvain_utils::utils::ordered_float::OrderedFloat;
use difflouvain_utils::utils::read_file;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, BufRead};
use std::ops::AddAssign;
use std::ops::Mul;
use std::sync::{atomic::AtomicIsize, Arc};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs::File, io::Read};

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

lazy_static! {
    static ref END_TIME: u128 = {
        let opts = cli::parse_opts();
        opts.run_time
    };
}

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug, Abomonation)]
struct VertexState {
    community: u64,
    community_sigma_tot: u64,
    internal_weight: u64,
    node_weight: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug, Abomonation)]
struct VertNode {
    id: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug, Abomonation)]
struct CountHolder {
    count: u64,
    label: u64,
}

impl Mul for CountHolder {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        CountHolder {
            count: self.count,
            label: self.label,
        }
    }
}

impl<'a> AddAssign<&'a CountHolder> for CountHolder {
    fn add_assign(&mut self, other: &'a CountHolder) {
        self.count += other.count;
    }
}
impl Semigroup for CountHolder {
    #[inline(always)]
    fn is_zero(&self) -> bool {
        self.count == 0
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

fn q(
    curr_comm_id: u64,
    test_comm_id: u64,
    test_sigma_tot: u64,
    edge_weight_in_comm: u64,
    node_weight: u64,
    internal_weight: u64,
    total_edge_weight: u64,
) -> f64 {
    let is_current_comm = curr_comm_id == test_comm_id;
    let m = total_edge_weight;
    let k_i_in_l = match is_current_comm {
        true => edge_weight_in_comm + internal_weight,
        false => edge_weight_in_comm,
    };
    let k_i_in = k_i_in_l as f64;
    let k_i = node_weight + internal_weight;
    let sigma_total = match is_current_comm {
        true => test_sigma_tot - k_i,
        false => test_sigma_tot,
    };
    if !(is_current_comm && sigma_total == 0) {
        return k_i_in - (((k_i * sigma_total) as f64) / (m as f64));
    }
    0.0
}

fn main() {
    let opts = cli::parse_opts();
    let (config, num_peers) = read_timely_config(&opts.timely_config);

    let data_map = read_file(&opts.data, num_peers);

    timely::execute(config, move |worker| {
        let index = worker.index();
        let timer = worker.timer();
        let (node_reader, edge_reader) = data_map.get(&index).unwrap();
        let mut probe = timely::dataflow::ProbeHandle::new();
        let (mut nodes, mut edges) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection();
            let (edge_handle, edges) = scope.new_collection();

            let nodeweights = edges
                .explode(|(vid, (_ovid, cost)): (VertNode, (VertNode, u64))| {
                    Some((vid, cost as isize))
                })
                .count();

            let edges = edges
                .map(|(key, (other, cost))| ((key, other), cost))
                .explode(|(key, cost)| Some((key, cost as isize)))
                .count()
                .consolidate()
                .map(|((key, other), cost)| (key, (other, cost as u64)));

            let graph_weight = nodeweights
                .explode(|(_vid, cost)| Some(((), cost as isize)))
                .count();
            let graph_weight = graph_weight.arrange_by_key_named("gweight");

            let louvain = nodes.join_map(&nodeweights, |vid, _, cost| {
                let vstate = VertexState {
                    community: vid.id,
                    community_sigma_tot: *cost as u64,
                    internal_weight: 0,
                    node_weight: *cost as u64,
                };
                (*vid, vstate)
            });

            let ms = nodes
                .map(|(vid, _)| ((), vid))
                .join_core(&graph_weight, |_, vid, m| Some((*vid, *m)));
            let ms = ms.arrange_by_key_named("ms");

            let louvain = louvain.join_core(&ms, |vid, vstate, m| Some((*vid, (*vstate, *m))));

            let flouvain = louvain.map(|(vid, (vstate, m))| (vid, (vstate, m)));
            let flouvain = flouvain.arrange_by_key_named("flouvain");

            let possible_moves = edges
                .map(|(key, (other, cost))| (other, (key, cost)))
                .join_core(&flouvain, |_other, (key, edge_cost), (rhs, _)| {
                    Some(((*key, rhs.community, rhs.community_sigma_tot), *edge_cost))
                });

            let possible_moves = possible_moves
                .explode(|(k, edge_cost)| Some((k, edge_cost as isize)))
                .count();

            let eval = possible_moves.map(|((vid, comm, stot), cost)| (vid, (comm, stot, cost)));
            let eval = eval.join_core(&flouvain, |vid, (comm, stot, cost), (vstate, m)| {
                Some(((*vid, *vstate), (*comm, *stot, *cost, *m)))
            });

            let louvain = eval
                .reduce(|(vid, vstate), input, output| {
                    let time_diff = START_TIME.elapsed().as_millis();
                    let even = time_diff % 2 == 0;
                    let mut best_comm = vstate.community;
                    let starting_comm = best_comm;
                    let mut best_delta_q = 0.0;
                    let mut best_sigma_tot = 0;
                    let mut mym = 1;

                    for ((comm, stot, cost, m), _) in input {
                        mym = *m;
                        let delta_q = q(
                            starting_comm,
                            *comm,
                            *stot,
                            *cost as u64,
                            vstate.node_weight,
                            vstate.internal_weight,
                            mym as u64,
                        );
                        if delta_q > best_delta_q {
                            best_delta_q = delta_q;
                            best_comm = *comm;
                            best_sigma_tot = *stot;
                        }
                    }
                    let mut new_vert = *vstate;
                    if best_comm != vstate.community
                        && ((even && best_comm > vstate.community)
                            || (!even && best_comm < vstate.community))
                        && time_diff < *END_TIME
                    {
                        new_vert.community_sigma_tot = best_sigma_tot;
                        new_vert.community = best_comm;
                    }
                    output.push(((new_vert, mym), 1));
                })
                .map(|((vid, _), (new_vert, m))| (vid, (new_vert, m)))
                .distinct();

            louvain.probe_with(&mut probe);

            (node_handle, edge_handle)
        });

        let mut fin_node = false;
        let mut fin_edge = false;
        let mut node_count = 0;
        let mut edge_count = 0;
        loop {
            if fin_node && fin_edge {
                break;
            }
            if !fin_node {
                let node = match node_reader.recv() {
                    Ok(msg) => msg,
                    Err(_) => {
                        fin_node = true;
                        continue;
                    }
                };
                match node {
                    Some(node) => {
                        node_count += 1;
                        nodes.insert((VertNode { id: node.id as u64 }, ()));
                    }
                    None => {}
                };
            }

            if !fin_edge {
                let edge = match edge_reader.recv() {
                    Ok(msg) => msg,
                    Err(_) => {
                        fin_edge = true;
                        continue;
                    }
                };
                match edge {
                    Some(edge) => {
                        edge_count += 1;
                        let node1 = VertNode {
                            id: edge.0.id as u64,
                        };
                        let node2 = VertNode {
                            id: edge.1.to as u64,
                        };
                        let cost = edge.1.weight as u64;
                        edges.insert((node1, (node2, cost)));
                        edges.insert((node2, (node1, cost)));
                    }
                    None => {}
                }
            }
        }

        println!("LOADED {:?} NODES and {:?} EDGES", node_count, edge_count);
        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();

        while probe.less_than(edges.time()) {
            worker.step();
        }

        println!("Computation stable in {:?}", timer.elapsed());
        if index == 0 {
            let mut time = 2;
            // let parse_command = |s| {};
            let stdio = io::stdin();
            let mut iter = stdio.lock().lines();
            loop {
                println!("e: edit q: quit");
                let line = match iter.next() {
                    Some(res) => match res {
                        Ok(r) => r,
                        Err(_) => continue,
                    },
                    None => continue,
                };
                match line.as_str() {
                    "e" => {
                        let line = match iter.next() {
                            Some(res) => match res {
                                Ok(r) => r,
                                Err(_) => continue,
                            },
                            None => continue,
                        };
                        let edgesvec: Vec<_> = line
                            .split_whitespace()
                            .flat_map(|s| s.parse::<u32>())
                            .collect();
                        if edgesvec.len() != 3 {
                            println!("Only three edges can be accepted");
                            continue;
                        }

                        let node1 = VertNode {
                            id: edgesvec[0] as u64,
                        };
                        let node2 = VertNode {
                            id: edgesvec[1] as u64,
                        };

                        edges.insert((node1, (node2, edgesvec[2] as u64)));

                        nodes.advance_to(time);
                        nodes.flush();
                        edges.advance_to(time);
                        edges.flush();
                        let start = Instant::now();
                        while probe.less_than(edges.time()) {
                            worker.step();
                        }
                        println!("Computation stable in {:?}", start.elapsed());
                        time += 1;
                    }
                    "q" => break,
                    _ => continue,
                }
            }
        }
    })
    .expect("ASD");
}

/* let tomove = louvain.iterate(|transitive| {
    let edges = edges.enter(&transitive.scope());
    let louvain = transitive;
    let possible_moves = edges
        .map(|(key, (other, cost))| (other, (key, cost)))
        .join_map(&louvain, |_other, (key, edge_cost), (rhs, m)| {
            (
                (*key, rhs.community, rhs.community_sigma_tot, *m),
                *edge_cost,
            )
        })
        .reduce(|_, input, output| {
            let mut sum = 0;
            for (cost, count) in input {
                sum += *cost * (*count as u64);
            }
            output.push((sum, 1));
        })
        .map(|((key, comm, stot, m), cost)| (key, (comm, stot, cost, m)))
        .join_map(&louvain, |vid, (comm, stot, cost, m), (vstate, _)| {
            ((*vid, *vstate), (*comm, *stot, *cost, *m))
        });

    let new_louvain = possible_moves
        .reduce(|(vid, vstate), input, output| {
            let mut best_comm = vstate.community;
            let starting_comm = best_comm;
            let mut best_delta_q = 0.0;
            let mut best_sigma_tot = 0;
            let mut mym = 1;
            for ((comm, stot, cost, m), _) in input {
                mym = *m;
                let delta_q = q(
                    starting_comm,
                    *comm,
                    *stot,
                    *cost,
                    vstate.node_weight,
                    vstate.internal_weight,
                    *m,
                );
                if delta_q > best_delta_q {
                    best_delta_q = delta_q;
                    best_comm = *comm;
                    best_sigma_tot = *stot;
                }
            }

            let mut myvert = *vstate;
            if best_comm != myvert.community {
                myvert.changed = true;
                myvert.community_sigma_tot = best_sigma_tot;
                myvert.community = best_comm;
            }
            output.push(((myvert, mym), 1));
        })
        .map(|((vid, _vstate), (new_vstate, m))| (vid, (new_vstate, m)))
        .distinct();
    new_louvain
}); */

/* let possible_moves = edges
    .map(|(key, (other, cost))| (other, (key, cost)))
    .join_map(&louvain, |_other, (key, edge_cost), (rhs, m)| {
        (
            (*key, rhs.community, rhs.community_sigma_tot, *m),
            *edge_cost,
        )
    })
    .reduce(|_, input, output| {
        let mut sum = 0;
        for (cost, count) in input {
            sum += *cost * (*count as u64);
        }
        output.push((sum, 1));
    })
    .map(|((key, comm, stot, m), cost)| (key, (comm, stot, cost, m)))
    .join_map(&louvain, |vid, (comm, stot, cost, m), (vstate, _)| {
        ((*vid, *vstate), (*comm, *stot, *cost, *m))
    });

let louvain = possible_moves
    .reduce(|(_vid, vstate), input, output| {
        let mut best_comm = vstate.community;
        let starting_comm = best_comm;
        let mut best_delta_q = 0.0;
        let mut best_sigma_tot = 0;
        let mut mym = 1;
        for ((comm, stot, cost, m), _) in input {
            mym = *m;
            let delta_q = q(
                starting_comm,
                *comm,
                *stot,
                *cost,
                vstate.node_weight,
                vstate.internal_weight,
                *m,
            );
            if delta_q > best_delta_q {
                best_delta_q = delta_q;
                best_comm = *comm;
                best_sigma_tot = *stot;
            }
        }

        let mut myvert = *vstate;
        if best_comm != myvert.community {
            myvert.changed = true;
            myvert.community_sigma_tot = best_sigma_tot;
            myvert.community = best_comm;
        }

        output.push(((myvert, mym), 1));
    })
    .map(|((vid, _vstate), (new_vstate, m))| (vid, (new_vstate, m))); */

// tomove.probe_with(&mut probe);
//

/*.map(|((key, comm, stot, m), cost)| (key, (comm, stot, cost, m)))
.join_map(
    &louvain,
    |vid, (comm, stot, cost, m), (vstate, count, even, _)| {
        ((*vid, *vstate), (*comm, *stot, *cost, *count, *even, *m))
    },
); */

/* possible_moves
.reduce(|(_vid, vstate), input, output| {
    let mut best_comm = vstate.community;
    let starting_comm = best_comm;
    let mut best_delta_q = 0.0;
    let mut best_sigma_tot = 0;
    let mut mym = 1;
    let mut myeven = false;
    let mut mycount = 1;

    for ((comm, stot, cost, count, even, m), _) in input {
        mym = *m;
        myeven = *even;
        mycount = *count;
        let delta_q = q(
            starting_comm,
            *comm,
            *stot,
            *cost,
            vstate.node_weight,
            vstate.internal_weight,
            *m,
        );
        if delta_q > best_delta_q {
            best_delta_q = delta_q;
            best_comm = *comm;
            best_sigma_tot = *stot;
        }
    }

    let mut myvert = *vstate;
    if best_comm != myvert.community
        && ((myeven && vstate.community > best_comm)
            || (!myeven && vstate.community < best_comm))
    {
        myvert.changed = true;
        myvert.community_sigma_tot = best_sigma_tot;
        myvert.community = best_comm;
    }
    output.push(((myvert, mycount + 1, !myeven, mym), 1));
})
.map(|((vid, _vstate), (new_vstate, mycount, myeven, m))| {
    (vid, (new_vstate, mycount, myeven, m))
})
.distinct() */
