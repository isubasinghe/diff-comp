use difflouvain_utils::shared::{Community, Node, ToEdge};
use std::collections::{HashMap, HashSet};

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
    edges: Vec<(Node<u32>, ToEdge<u32>)>,
}

impl LouvainContext {
    fn iterate() -> Either<LouvainContext, LouvainContext> {
        unimplemented!();
    }

    fn add_node() {}

    fn add_edge() {}
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

    // let mut nodeedges: HashMap<Node<u32>, Vec<ToEdge<u32>>> = HashMap::new();

    for (node, toedge) in &edges {
        /* nodeedges
        .entry(*node)
        .or_insert(vec![*toedge])
        .push(*toedge); */
        m += toedge.weight;
        match node_comms.get(&node) {
            Some(com) => {
                *sigma_k_i
                    .entry((Node { id: toedge.to }, *com))
                    .or_insert(toedge.weight) += toedge.weight;
            }
            None => {}
        };

        *k_i.entry(*node).or_insert(toedge.weight) += toedge.weight;

        let other = Node { id: toedge.to };
        let com = node_comms.get(&node);
        if com == node_comms.get(&other) {
            match com {
                Some(com) => {
                    *sigma_in.entry(*com).or_insert(toedge.weight) += toedge.weight;
                }
                None => {}
            }
        } else {
            match com {
                Some(com) => {
                    *sigma_tot.entry(*com).or_insert(toedge.weight) += toedge.weight;
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
        edges,
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

    louvain(node_comms, edges);
}
