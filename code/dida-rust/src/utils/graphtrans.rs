// Utility to write into a petgraph file
use crate::shared::{Node, ToEdge};
use petgraph::dot::{Config, Dot};
use petgraph::graph::{Graph, NodeIndex, UnGraph};
use petgraph_graphml::GraphMl;
pub fn load_graph() {
    let g = UnGraph::<i32, ()>::from_edges(&[(1, 2), (4, 1), (2, 3), (3, 4), (1, 4)]);
    println!("{}", GraphMl::new(&g).pretty_print(true).to_string());
    println!("{:?}", Dot::with_config(&g, &[Config::EdgeNoLabel]));
}

pub fn load_edges<T>(edges: Vec<(Node<T>, ToEdge<T>)>) -> Graph<T, (), petgraph::Undirected>
where
    T: Ord + Default + Copy + Clone,
    NodeIndex: From<T>,
{
    UnGraph::<T, ()>::from_edges(edges.iter().map(|(n, e)| (n.id, e.to)))
}

pub fn write_graph<T>(file: String, graph: Graph<T, (), petgraph::Undirected>) {}
