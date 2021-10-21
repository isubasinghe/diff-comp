use difflouvain_utils::shared::{Community, Node, ToEdge};
use difflouvain_utils::utils::read_file;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::Included;
use std::time::Instant;
macro_rules! somec {
    ($x: expr) => {
        match $x {
            Some(res) => res,
            None => continue,
        }
    };
}

fn modularity(
    nodes: &HashMap<Node<u32>, Community>,
    edges: &BTreeMap<Node<u32>, ToEdge<u32>>,
    m: u32,
) -> f64 {
    let m = m as f64;
    let mut q = 0.0;
    let mut k_n = HashMap::new();
    for (node, edge) in edges {
        *k_n.entry(node).or_insert(0) += edge.weight;
    }
    for (node, edge) in edges {
        let comm = somec!(nodes.get(node));
        let other_comm = somec!(nodes.get(&Node { id: edge.to }));
        if comm != other_comm {
            continue;
        }
        let k_i = *somec!(k_n.get(node)) as f64;
        let k_j = *somec!(k_n.get(&Node { id: edge.to })) as f64;
        q += (edge.weight as f64) - (k_i * k_j) / (2.0 * m);
    }
    q / (2.0 * m)
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
    fn print_modularity(&self) {
        let q = modularity(&self.nodes, &self.edges, self.m);
        println!("Modularity {}", q);
    }
    fn modularity(&self) -> f64 {
        modularity(&self.nodes, &self.edges, self.m)
    }
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
                let part2 = (sigma_in / (2.0 * m))
                    - (sigma_tot / (2.0 * m)).powf(2.0)
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

            println!("{:?}", delta_q);

            let delta_q = somec!(delta_q);
            let comm = somec!(best_community);
            if delta_q <= 0.0 || comm == old_comm {
                continue;
            }

            println!("{}", delta_q);
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
    let mut nodes: HashMap<Node<u32>, Community> = HashMap::new();
    let mut edges: Vec<(Node<u32>, ToEdge<u32>)> = Vec::new();

    let file = std::env::args().nth(1).expect("file needed");

    let data_map = read_file(&file, 1);

    let (node_reader, edge_reader) = data_map.get(&0).unwrap();
    let mut fin_node = false;
    let mut fin_edge = false;
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
                    nodes.insert(
                        node,
                        Community {
                            id: node.id,
                            weights: 1,
                        },
                    );
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
                    edges.push(edge);
                    let flipped_node = Node { id: edge.1.to };
                    let flipped_edge = ToEdge {
                        to: edge.0.id,
                        weight: edge.1.weight,
                    };
                    edges.push((flipped_node, flipped_edge));
                }
                None => {}
            }
        }
    }
    println!("LOADED {:?} NODES AND {:?} EDGES", nodes.len(), edges.len());
    let mut lc = louvain(nodes, edges);
    lc.iterate();
}
