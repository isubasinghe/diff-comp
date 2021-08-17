use std::hash::Hash;

pub type Edge<G> = (G, G);

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
pub struct Node<G> {
    pub id: G
}