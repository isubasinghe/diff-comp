use difflouvain_utils::shared::{Community, Node, ToEdge};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::Included;

macro_rules! somec {
    ($x: expr) => {
        match $x {
            Some(res) => res,
            None => continue,
        }
    };
}

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
    edges: BTreeMap<Node<u32>, ToEdge<u32>>,
}

impl LouvainContext {
    fn iterate(&mut self) {
        let mut new_nodes = Vec::new();

        for (node, old_comm) in &self.nodes {
            let mut delta_q: Option<f64> = None;
            let mut best_community = None;
            for (_, toedge) in self.edges.range((Included(node), Included(node))) {
                let next_comm = somec!(self.nodes.get(&Node { id: toedge.to }));
                let sigma_in = (*somec!(self.sigma_in.get(next_comm))) as f64;
                let sigma_k_i = (*somec!(self.sigma_k_i.get(&(*node, *next_comm)))) as f64;
                let sigma_tot = (*somec!(self.sigma_tot.get(next_comm))) as f64;
                let k_i = (*somec!(self.k_i.get(node))) as f64;
                let m = self.m as f64;
                let part1 =
                    (sigma_in + sigma_k_i) / (m * 2.0) + ((sigma_tot + k_i) / (2.0 * m)).powf(2.0);
                let part2 = (sigma_in / (2.0 * m)) + (sigma_tot / 2.0 * m).powf(2.0)
                    - (k_i / (2.0 * m)).powf(2.0);
                let curr_delta_q = part1 - part2;
                match delta_q {
                    Some(inner_delta_q) => {
                        if curr_delta_q > inner_delta_q {
                            delta_q = Some(curr_delta_q);
                            best_community = Some(next_comm);
                        }
                    }
                    None => {
                        delta_q = Some(curr_delta_q);
                        best_community = Some(next_comm);
                    }
                };
            }

            let delta_q = somec!(delta_q);
            let comm = somec!(best_community);
            if delta_q <= 0.0 || comm == old_comm {
                continue;
            }
            new_nodes.push((*node, *comm));

            for (_, toedge) in self.edges.range((Included(node), Included(node))) {
                let other_comm = somec!(self.nodes.get(&Node { id: toedge.to }));
                if other_comm == old_comm {
                    *self.sigma_in.entry(*old_comm).or_insert(0) -= toedge.weight;
                } else {
                    *self.sigma_tot.entry(*old_comm).or_insert(0) -= toedge.weight;
                }
                if other_comm == comm {
                    *self.sigma_in.entry(*comm).or_insert(0) += toedge.weight;
                } else {
                    *self.sigma_tot.entry(*comm).or_insert(0) += toedge.weight;
                }
                *self
                    .sigma_k_i
                    .entry((Node { id: toedge.to }, *old_comm))
                    .or_insert(0) -= toedge.weight;
                *self
                    .sigma_k_i
                    .entry((Node { id: toedge.to }, *comm))
                    .or_insert(0) += toedge.weight;
            }
        }

        for (node, comm) in new_nodes {
            *self.nodes.entry(node).or_insert(comm) = comm;
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

    let mut indexed_edges = BTreeMap::new();

    for (node, toedge) in &edges {
        indexed_edges.insert(*node, *toedge);
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
        edges: indexed_edges,
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

    let mut lc = louvain(node_comms, edges);
    lc.iterate();
}
