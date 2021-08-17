
use std::{collections::{HashMap, HashSet}, fs::File, io::{ BufRead, BufReader, Error}, thread};
use crossbeam::channel::{Receiver,  unbounded};

use crate::shared::{Edge, Node};



pub fn parse_uri(uri: &str) {

}

pub fn read_file(path: &str, peers: usize) -> HashMap<usize, (Receiver<Node<u32>>, Receiver<Edge<u32>>)> {

    let file = File::open(path).expect("unable to open file");
    let reader = BufReader::new(file);


    

    let mut hashmap = HashMap::new();

    let mut sender_hashmap = HashMap::new();
    
    for i in 1..peers {
        let (edges_sender, edges_reader) = unbounded();
        let (node_sender, node_reader) = unbounded();
        hashmap.insert(i, (node_reader, edges_reader));
        sender_hashmap.insert(i, (node_sender, edges_sender));
    }

    thread::spawn(move || {
        let edges = reader.lines().flat_map(|line_res| {
            let line: Result<Vec<u32>, Error> = line_res.map(|line| line.split_whitespace().filter_map(|s| s.parse::<u32>().ok() ).collect());
            line 
        });
    
        let mut node_set: HashSet<Node<u32>> = HashSet::new();
    
        let mut i = 0;
    
        for edge in edges {
    
            
            if edge.len() != 2 {
                panic!("Invalid number of edges {}", edge.len());
            }
            
            let index = i % peers;
    
            let (node_sender, edges_sender) = sender_hashmap.get(&index).unwrap();
    
            let node1 = Node{ id: edge[0] };
            if !node_set.contains(&node1) {
                node_set.insert(node1);
                node_sender.send(node1).unwrap();
            }
    
            let node2 = Node{ id: edge[1] };
            if !node_set.contains(&node2) {
                node_set.insert(node2);
                node_sender.send(node1).unwrap();
            }
    
            let edge: Edge<u32> = (edge[0], edge[1]);
    
            edges_sender.send(edge).unwrap();
    
    
            i += 1;
    
        }
    });

    hashmap
    
}