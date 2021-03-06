pub mod graphtrans;
pub mod kafka;
pub mod ordered_float;

use crossbeam::channel::{unbounded, Receiver};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Error},
    thread,
};

use crate::shared::{Node, ToEdge};
use petgraph::graph::UnGraph;
use petgraph_graphml::GraphMl;

type PipedNode<G> = Option<Node<G>>;

type PipedEdge<G> = Option<(Node<G>, ToEdge<G>)>;

pub fn read_file(
    path: &str,
    peers: usize,
) -> HashMap<usize, (Receiver<PipedNode<u32>>, Receiver<PipedEdge<u32>>)> {
    let file = File::open(path).expect("unable to open file");
    let reader = BufReader::new(file);

    let mut hashmap = HashMap::new();

    let mut sender_hashmap = HashMap::new();

    for i in 0..peers {
        let (edges_sender, edges_reader) = unbounded();
        let (node_sender, node_reader) = unbounded();
        hashmap.insert(i, (node_reader, edges_reader));
        sender_hashmap.insert(i, (node_sender, edges_sender));
    }

    thread::spawn(move || {
        let edges = reader.lines().flat_map(|line_res| {
            let line: Result<Vec<u32>, Error> = line_res.map(|line| {
                line.split_whitespace()
                    .filter_map(|s| s.parse::<u32>().ok())
                    .collect()
            });
            line
        });

        let mut node_set: HashSet<Node<u32>> = HashSet::new();

        let mut i = 0;

        for edge in edges {
            // 2 is the number of nodes we expect on a line
            // obviously
            if edge.len() != 2 {
                panic!("Invalid number of edges {}", edge.len());
            }

            let index = i % peers;

            let (node_sender, edges_sender) = sender_hashmap.get(&index).unwrap();

            let node1 = Node { id: edge[0] };
            if !node_set.contains(&node1) {
                node_set.insert(node1);
                node_sender.send(Some(node1)).unwrap();
            }

            let node2 = Node { id: edge[1] };
            if !node_set.contains(&node2) {
                node_set.insert(node2);
                node_sender.send(Some(node2)).unwrap();
            }

            let myedge = (
                node1,
                ToEdge {
                    to: node2.id,
                    weight: 1,
                },
            );

            edges_sender.send(Some(myedge)).unwrap();

            i += 1;
        }

        for i in 0..peers {
            let (n, e) = sender_hashmap.get(&i).unwrap();
            n.send(None).unwrap();
            e.send(None).unwrap();
        }
    });

    hashmap
}

pub fn read_file_processing(path: &str, outpath: &str) {
    let file = File::open(path).expect("unable to open file");
    let reader = BufReader::new(file);

    let edges = reader.lines().flat_map(|line_res| {
        let line: Result<Vec<u32>, Error> = line_res.map(|line| {
            line.split_whitespace()
                .filter_map(|s| s.parse::<u32>().ok())
                .collect()
        });
        line
    });

    let mut sanitized_edges = Vec::new();

    for edge in edges {
        if edge.len() != 2 {
            continue;
        }
        sanitized_edges.push((edge[0], edge[1]));
    }

    let g = UnGraph::<i32, ()>::from_edges(sanitized_edges);
    let graphml = GraphMl::new(&g);

    let out_file = File::create(outpath).expect("unable to open file to write");
    let f = BufWriter::new(out_file);
    graphml.to_writer(f).unwrap();
}
