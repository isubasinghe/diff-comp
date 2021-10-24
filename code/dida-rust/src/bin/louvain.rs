use difflouvain_utils::shared::{Community, Node, ToEdge};
use difflouvain_utils::utils::read_file;
use petgraph::graph::{NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
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

macro_rules! somez {
    ($x: expr) => {
        match $x {
            Some(res) => res,
            None => &0,
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

pub fn read_file_h(path: &str) -> (HashMap<Node<u32>, Community>, Vec<(Node<u32>, ToEdge<u32>)>) {
    let mut nodes = HashMap::new();
    let mut redges = Vec::new();
    let file = File::open(path).expect("unable to open file");
    let reader = BufReader::new(file);

    let edges = reader.lines().flat_map(|line_res| {
        let line: Result<Vec<u32>, _> = line_res.map(|line| {
            line.split_whitespace()
                .filter_map(|s| s.parse::<u32>().ok())
                .collect()
        });
        line
    });

    for edge in edges {
        // 2 is the number of nodes we expect on a line
        // obviously
        if edge.len() != 2 {
            panic!("Invalid number of edges {}", edge.len());
        }
        let node1 = Node { id: edge[0] };

        let node2 = Node { id: edge[1] };

        let myedge = (
            node1,
            ToEdge {
                to: node2.id,
                weight: 1,
            },
        );

        let flipped_edge = (
            node2,
            ToEdge {
                to: node1.id,
                weight: 1,
            },
        );

        nodes.insert(
            node1,
            Community {
                id: node1.id,
                weights: 1,
            },
        );
        nodes.insert(
            node2,
            Community {
                id: node2.id,
                weights: 1,
            },
        );
        redges.push(myedge);
        redges.push(flipped_edge);
    }
    (nodes, redges)
}

#[derive(Debug, Default, Hash, PartialEq, Eq, Copy, Clone)]
pub struct LouvainNode<N, G>
where
    N: Ord + Eq + PartialEq + PartialOrd,
    G: Ord + Eq + PartialEq + PartialOrd,
{
    id: N,
    community: G,
}

pub fn read_file_petgraph(path: &str) -> UnGraph<LouvainNode<u32, u32>, u32> {
    let file = File::open(path).expect("unable to open file");
    let reader = BufReader::new(file);
    let mut nodes = HashMap::new();
    let edges = reader.lines().flat_map(|line_res| {
        let line: Result<Vec<u32>, _> = line_res.map(|line| {
            line.split_whitespace()
                .filter_map(|s| s.parse::<u32>().ok())
                .collect()
        });
        line
    });

    let mut g = UnGraph::<LouvainNode<u32, u32>, u32>::new_undirected();

    for edge in edges {
        // 2 is the number of nodes we expect on a line
        // obviously
        if edge.len() != 2 {
            panic!("Invalid number of edges {}", edge.len());
        }
        let node = LouvainNode {
            id: edge[0],
            community: edge[0],
        };
        let other_node = LouvainNode {
            id: edge[1],
            community: edge[1],
        };
        let node_idx = match nodes.get(&node) {
            Some(idx) => *idx,
            None => {
                let idx = g.add_node(node);
                nodes.insert(node, idx);
                idx
            }
        };
        let other_node_idx = match nodes.get(&other_node) {
            Some(idx) => *idx,
            None => {
                let idx = g.add_node(other_node);
                nodes.insert(other_node, idx);
                idx
            }
        };
        g.add_edge(node_idx, other_node_idx, 1);
    }
    g
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
        let mut i = 0;
        for (node, old_comm) in &self.nodes {
            let mut delta_q: Option<f64> = None;
            let mut best_community = None;
            let mut best_edge = None;

            for (_, toedge) in self.edges.range((Included(node), Included(node))) {
                let next_comm = self.nodes.get(&Node { id: toedge.to }).unwrap();
                let sigma_in = (*somez!(self.sigma_in.get(next_comm))) as f64;
                let sigma_k_i = (*somez!(self.sigma_k_i.get(&(*node, *next_comm)))) as f64;
                let sigma_tot = (*somez!(self.sigma_tot.get(next_comm))) as f64;
                let k_i = (*somez!(self.k_i.get(node))) as f64;
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
                            best_edge = Some(toedge);
                        }
                    }
                    None => {
                        delta_q = Some(curr_delta_q);
                        best_community = Some(next_comm);
                        best_edge = Some(toedge);
                    }
                };
            }

            let delta_q = somec!(delta_q);
            let comm = somec!(best_community);
            if delta_q <= 0.0 || comm == old_comm {
                continue;
            }
            let best_edge = best_edge.unwrap();

            new_nodes.push((*node, *comm));

            // *self.sigma_in.entry(*old_comm).or_insert(0) -= best_edge.weight;
            // *self.sigma_tot.entry(*old_comm).or_insert(0) += best_edge.weight;

            // *self.sigma_in.entry(*comm).or_insert(0) += best_edge.weight;
            // *self.sigma_tot.entry(*comm).or_insert(0) -= best_edge.weight;

            for (_, toedge) in self.edges.range((Included(node), Included(node))) {
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
    let file = std::env::args().nth(1).expect("file needed");
    let (nodes, edges) = read_file_h(&file);

    println!("LOADED {:?} NODES AND {:?} EDGES", nodes.len(), edges.len());
    let mut lc = louvain(nodes, edges);
    let start = Instant::now();
    lc.print_modularity();
    lc.iterate();
    println!("COMPUTED IN {:?}", start.elapsed());
}
