use difflouvain_utils::shared::{Community, Node, ToEdge};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

pub struct LouvainContext {
    sigma_in: HashMap<Community, u32>,
    sigma_k_i: HashMap<(Node<u32>, Community), u32>,
    sigma_tot: HashMap<Community, u32>,
    k_i: HashMap<Node<u32>, u32>,
    m: u32,
    nodes: HashMap<Node<u32>, Community>,
    _edges: Vec<(Node<u32>, ToEdge<u32>)>,
    node_edges: HashMap<Node<u32>, Vec<ToEdge<u32>>>,
}

impl LouvainContext {
    fn iterate(&self) {
        for (node, com) in &self.nodes {
            let maybe_potential_swaps = self.node_edges.get(node);

            /* let mut best_swap = None;
            let mut best_edge = None; */

            match maybe_potential_swaps {
                Some(potential_swaps) => {
                    let sigma_in = *self.sigma_in.get(com).unwrap_or(&0) as f64;

                    let mut best_score = None;
                    let mut best_swap = None;
                    let k_i = *self.k_i.get(node).unwrap_or(&0) as f64;

                    for edge in potential_swaps {
                        let edge_node = Node { id: edge.to };
                        let other_comm = match self.nodes.get(&edge_node) {
                            Some(other_comm) => other_comm,
                            None => continue,
                        };

                        let sigma_k_i = match self.sigma_k_i.get(&(*node, *other_comm)) {
                            Some(e) => *e as f64,
                            None => continue,
                        };
                        let sigma_tot = match self.sigma_tot.get(other_comm) {
                            Some(e) => *e as f64,
                            None => continue,
                        };
                        let m2 = (2 * self.m) as f64;
                        let delta_q_p1 =
                            ((sigma_in + sigma_k_i) / m2) - ((sigma_tot + k_i) / m2).powf(2.0);
                        let delta_q_p2 =
                            sigma_in / m2 - (sigma_tot / m2).powf(2.0) - (k_i / m2).powf(2.0);
                        let delta_q = delta_q_p1 - delta_q_p2;

                        if delta_q > best_score.unwrap_or(0.0) {
                            best_score = Some(delta_q);
                            best_swap = Some(edge);
                        }
                    }
                    println!(
                        "curr_node: {:?} delta_q: {:?} swap: {:?}",
                        node, best_score, best_swap
                    );
                }
                None => {}
            }
        }
    }

    /* fn add_node() {}

    fn add_edge() {} */
}

fn louvain(
    node_comms: HashMap<Node<u32>, Community>,
    edges: Vec<(Node<u32>, ToEdge<u32>)>,
) -> LouvainContext {
    let mut sigma_in = HashMap::<Community, u32>::new();
    let mut sigma_k_i = HashMap::<(Node<u32>, Community), u32>::new();
    let mut sigma_tot = HashMap::<Community, u32>::new();
    let mut k_i = HashMap::<Node<u32>, u32>::new();

    let mut m = 0;

    let mut node_edges: HashMap<Node<u32>, Vec<ToEdge<u32>>> = HashMap::new();

    for (node, toedge) in &edges {
        node_edges.entry(*node).or_insert(vec![]).push(*toedge);
        m += toedge.weight;

        *k_i.entry(*node).or_insert(0) += toedge.weight;

        let other = Node { id: toedge.to };
        let other_comm = node_comms.get(&other);

        match other_comm {
            Some(com) => {
                *sigma_k_i.entry((*node, *com)).or_insert(0) += toedge.weight;
            }
            None => {}
        }

        let com = node_comms.get(&node);
        if com == other_comm {
            match com {
                Some(com) => {
                    if node > &other {
                        continue;
                    }
                    *sigma_in.entry(*com).or_insert(0) += toedge.weight;
                }
                None => {}
            }
        }
        if com != other_comm && com != None && other_comm != None {
            match com {
                Some(com) => {
                    *sigma_tot.entry(*com).or_insert(0) += toedge.weight;
                }
                None => {}
            }
        }
    }

    m = m / 2;

    LouvainContext {
        sigma_in,
        sigma_k_i,
        sigma_tot,
        k_i,
        m,
        nodes: node_comms,
        _edges: edges,
        node_edges,
    }
}

fn main() {
    let mut node_comms: HashMap<Node<u32>, Community> = HashMap::new();
    let mut edges: Vec<(Node<u32>, ToEdge<u32>)> = Vec::new();

    node_comms.insert(Node { id: 20 }, Community { id: 1, weights: 1 });
    node_comms.insert(Node { id: 1 }, Community { id: 1, weights: 1 });
    node_comms.insert(Node { id: 3 }, Community { id: 1, weights: 1 });

    node_comms.insert(Node { id: 2 }, Community { id: 2, weights: 1 });

    edges.push((Node { id: 1 }, ToEdge { to: 3, weight: 5 }));
    edges.push((Node { id: 3 }, ToEdge { to: 1, weight: 5 }));

    edges.push((Node { id: 3 }, ToEdge { to: 2, weight: 7 }));
    edges.push((Node { id: 2 }, ToEdge { to: 3, weight: 7 }));

    edges.push((Node { id: 1 }, ToEdge { to: 20, weight: 11 }));
    edges.push(((Node { id: 20 }), ToEdge { to: 1, weight: 11 }));

    node_comms.insert(Node { id: 5 }, Community { id: 5, weights: 1 });
    node_comms.insert(Node { id: 6 }, Community { id: 5, weights: 1 });
    node_comms.insert(Node { id: 7 }, Community { id: 5, weights: 1 });
    node_comms.insert(Node { id: 8 }, Community { id: 5, weights: 1 });

    edges.push((Node { id: 5 }, ToEdge { to: 6, weight: 13 }));
    edges.push((Node { id: 6 }, ToEdge { to: 5, weight: 13 }));

    edges.push((Node { id: 5 }, ToEdge { to: 1, weight: 1 }));
    edges.push((Node { id: 1 }, ToEdge { to: 5, weight: 1 }));

    edges.push((Node { id: 8 }, ToEdge { to: 20, weight: 23 }));
    edges.push((Node { id: 20 }, ToEdge { to: 8, weight: 23 }));

    edges.push((Node { id: 5 }, ToEdge { to: 7, weight: 17 }));
    edges.push((Node { id: 7 }, ToEdge { to: 5, weight: 17 }));

    let lc = louvain(node_comms, edges);
    lc.iterate();
}
