
use std::{collections::HashSet, fs::File, io::{ BufRead, BufReader, Error}, sync::{Arc, Mutex}, thread};
use crossbeam::channel::{unbounded};

use crate::shared::{Edge, Node};



pub fn parse_uri(uri: &str) {

}

pub fn read_file(path: &str, peers: usize) {

    let file = File::open(path).expect("unable to open file");
    let reader = BufReader::new(file);


    for i in 1..peers {

    }

    let (edges_sender, edges_reader) = unbounded();
    let (node_sender, node_reader) = unbounded();




    let edges = reader.lines().flat_map(|line_res| {
    
        let line: Result<Vec<u32>, Error> = line_res.map(|line| line.split_whitespace().filter_map(|s| s.parse::<u32>().ok() ).collect());
        line 

    });

    let mut node_set: HashSet<Node<u32>> = HashSet::new();

    for edge in edges {

        if edge.len() != 2 {
            panic!("Invalid number of edges {}", edge.len());
        }
        

        let node1 = Node{ id: edge[0] };
        if !node_set.contains(&node1) {
            node_set.insert(node1);
            node_sender.send(node1).unwrap();
        }

        let node2 = Node{ id: edge[1] };
        if !node_set.contains(&node2) {
            node_set.insert(node2);
            node_sender.send(node2).unwrap();
        }

        let edge: Edge<u32> = (edge[0], edge[1]);
        edges_sender.send(edge).unwrap();
    }
}