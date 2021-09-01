use std::hash::Hash;
use abomonation_derive::Abomonation;


pub trait Unbind<T> {
    fn unbind(&self) -> T;
}

pub trait Bind<T> {
    fn bind(&self) -> T;
}


#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Abomonation)]
pub struct Node<G> {
    pub id: G
}

impl<G> Node<G>
where
    G: Ord + Copy + Clone
{
    fn to_outgoing_edge(&self) ->  FromEdge<G> {
        self.unbind().bind()
    }
}

impl<G> Bind<Node<G>> for G
where
    G: Copy + Clone
{
    fn bind(&self) -> Node<G> {
        Node{id: *self}
    }
}

impl<G> Unbind<G> for Node<G>
where
    G: Copy + Clone
{
    fn unbind(&self) -> G {
        (*self).id
    }
}


#[derive(Debug, Default, Hash, Copy, Clone, Abomonation)]
pub struct FromEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    pub from: G
}

#[derive(Debug, Default, Hash, Copy, Clone, Abomonation)]
pub struct ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    pub to: G,
    pub weight: u32
}


pub type Edge<G> = (FromEdge<G>, ToEdge<G>);

// Impls for ToEdge
impl<G> PartialEq for ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn eq(&self, other: &Self) -> bool {
        self.to.eq(&other.to)
    }
}

impl<G> PartialEq<Node<G>> for ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn eq(&self, other: &Node<G>) -> bool {
        self.to.eq(&other.id)
    }
}

impl<G> Eq for ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{}


impl<G> PartialOrd for ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.to.partial_cmp(&other.to)
    }
}

impl<G> PartialOrd<Node<G>> for ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn partial_cmp(&self, other: &Node<G>) -> Option<std::cmp::Ordering> {
        return self.to.partial_cmp(&other.id)
    }
}

impl<G> Ord for ToEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to.cmp(&other.to)
    }
}
// End Impls for ToEdge


// Impls for FromEdge
impl<G> Bind<FromEdge<G>> for G
where
    G: Ord + Copy + Clone
{
    fn bind(&self) -> FromEdge<G> {
        FromEdge{from: *self}
    }
}

impl<G> Unbind<G> for FromEdge<G>
where
    G: Ord + Copy + Clone
{
    fn unbind(&self) -> G {
        (*self).from
    }
}


impl<G> PartialEq for FromEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn eq(&self, other: &Self) -> bool {
        self.from.eq(&other.from)
    }
}

impl<G> Eq for FromEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{}

impl<G> PartialOrd for FromEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.from.partial_cmp(&other.from)
    }
}

impl<G> Ord for FromEdge<G>
where
    G: PartialEq + Eq + PartialOrd + Ord
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.from.cmp(&other.from)
    }
}
// End impls for FromEdge

#[derive(Debug, Copy, Clone, Hash, Abomonation)]
pub struct Community {
    pub id: u32,
    pub weights: u32,
}

impl PartialEq for Community {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Community {}

impl PartialOrd for Community {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Community {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Community {
    fn new(id: u32) -> Community {
        Community{id, weights: 1}
    }
}


fn example<G: Ord + Copy + Clone>(n: Node<G>) -> FromEdge<G> {
    n.unbind().bind()
}
