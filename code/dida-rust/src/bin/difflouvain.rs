use core::cmp::Ordering;
use core::hash::{Hash, Hasher};
use core::mem;
use core::ops::{
    Add, AddAssign, Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Neg, Rem, RemAssign, Sub,
    SubAssign,
};
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::*;
use differential_dataflow::operators::Iterate;
use differential_dataflow::{AsCollection, Collection};
use difflouvain_utils::cli;
use difflouvain_utils::shared::config::TimelyConfig;
use difflouvain_utils::shared::{CommEdge, Community, Node, ToEdge};
use num_traits::Float;
use std::collections::{HashMap, HashSet};
use std::{fs::File, io::Read};

use abomonation_derive::Abomonation;
// masks for the parts of the IEEE 754 float
const SIGN_MASK: u64 = 0x8000000000000000u64;
const EXP_MASK: u64 = 0x7ff0000000000000u64;
const MAN_MASK: u64 = 0x000fffffffffffffu64;

// canonical raw bit patterns (for hashing)
const CANONICAL_NAN_BITS: u64 = 0x7ff8000000000000u64;
const CANONICAL_ZERO_BITS: u64 = 0x0u64;

#[derive(Debug, Default, Clone, Copy, Abomonation)]
struct OrderedFloat<T>(pub T);

impl<T: Float> OrderedFloat<T> {
    #[inline]
    fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Float> AsRef<T> for OrderedFloat<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T: Float> AsMut<T> for OrderedFloat<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Float> PartialEq for OrderedFloat<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match self.is_nan() {
            true => other.0.is_nan(),
            false => self.0 == other.0,
        }
    }
}

impl<T: Float> PartialEq<T> for OrderedFloat<T> {
    #[inline]
    fn eq(&self, other: &T) -> bool {
        self.0 == *other
    }
}

impl<T: Float> PartialOrd for OrderedFloat<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Float> Eq for OrderedFloat<T> {}

impl<T: Float> Ord for OrderedFloat<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = &self.0;
        let rhs = &other.0;
        match lhs.partial_cmp(rhs) {
            Some(ordering) => ordering,
            None => {
                if lhs.is_nan() {
                    if rhs.is_nan() {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    }
                } else {
                    Ordering::Less
                }
            }
        }
    }
}

impl<T: Float> Deref for OrderedFloat<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Float> DerefMut for OrderedFloat<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Float> Hash for OrderedFloat<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.is_nan() {
            true => hash_float(&T::nan(), state),
            false => hash_float(&self.0, state),
        }
    }
}
#[inline]
fn hash_float<F: Float, H: Hasher>(f: &F, state: &mut H) {
    raw_double_bits(f).hash(state);
}

#[inline]
fn raw_double_bits<F: Float>(f: &F) -> u64 {
    if f.is_nan() {
        return CANONICAL_NAN_BITS;
    }

    let (man, exp, sign) = f.integer_decode();
    if man == 0 {
        return CANONICAL_ZERO_BITS;
    }

    let exp_u64 = unsafe { mem::transmute::<i16, u16>(exp) } as u64;
    let sign_u64 = if sign > 0 { 1u64 } else { 0u64 };
    (man & MAN_MASK) | ((exp_u64 << 52) & EXP_MASK) | ((sign_u64 << 63) & SIGN_MASK)
}

macro_rules! impl_ordered_float_binop {
    ($imp:ident, $method:ident, $assign_imp:ident, $assign_method:ident) => {
        impl<T: $imp> $imp for OrderedFloat<T> {
            type Output = OrderedFloat<T::Output>;

            #[inline]
            fn $method(self, other: Self) -> Self::Output {
                OrderedFloat((self.0).$method(other.0))
            }
        }

        impl<T: $imp> $imp<T> for OrderedFloat<T> {
            type Output = OrderedFloat<T::Output>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a T> for OrderedFloat<T>
        where
            T: $imp<&'a T>,
        {
            type Output = OrderedFloat<<T as $imp<&'a T>>::Output>;

            #[inline]
            fn $method(self, other: &'a T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a Self> for OrderedFloat<T>
        where
            T: $imp<&'a T>,
        {
            type Output = OrderedFloat<<T as $imp<&'a T>>::Output>;

            #[inline]
            fn $method(self, other: &'a Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<'a, T> $imp for &'a OrderedFloat<T>
        where
            &'a T: $imp,
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<'a, T> $imp<OrderedFloat<T>> for &'a OrderedFloat<T>
        where
            &'a T: $imp<T>,
        {
            type Output = OrderedFloat<<&'a T as $imp<T>>::Output>;

            #[inline]
            fn $method(self, other: OrderedFloat<T>) -> Self::Output {
                OrderedFloat((self.0).$method(other.0))
            }
        }

        impl<'a, T> $imp<T> for &'a OrderedFloat<T>
        where
            &'a T: $imp<T>,
        {
            type Output = OrderedFloat<<&'a T as $imp<T>>::Output>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a T> for &'a OrderedFloat<T>
        where
            &'a T: $imp,
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: &'a T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        #[doc(hidden)] // Added accidentally; remove in next major version
        impl<'a, T> $imp<&'a Self> for &'a OrderedFloat<T>
        where
            &'a T: $imp,
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: &'a Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<T: $assign_imp> $assign_imp<T> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: T) {
                (self.0).$assign_method(other);
            }
        }

        impl<'a, T: $assign_imp<&'a T>> $assign_imp<&'a T> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: &'a T) {
                (self.0).$assign_method(other);
            }
        }

        impl<T: $assign_imp> $assign_imp for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: Self) {
                (self.0).$assign_method(other.0);
            }
        }

        impl<'a, T: $assign_imp<&'a T>> $assign_imp<&'a Self> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: &'a Self) {
                (self.0).$assign_method(&other.0);
            }
        }
    };
}

impl_ordered_float_binop! {Add, add, AddAssign, add_assign}
impl_ordered_float_binop! {Sub, sub, SubAssign, sub_assign}
impl_ordered_float_binop! {Mul, mul, MulAssign, mul_assign}
impl_ordered_float_binop! {Div, div, DivAssign, div_assign}
impl_ordered_float_binop! {Rem, rem, RemAssign, rem_assign}

fn read_timely_config(path: &str) -> (timely::Config, usize) {
    let mut file = File::open(path).unwrap();
    let mut s = String::new();
    file.read_to_string(&mut s).unwrap();
    let config: TimelyConfig = ron::from_str(&s).unwrap();

    println!("LOADED CONFIG: {:?}", config);

    let num_peers = config.num_peers();

    let config = config.into_timely();

    let worker_config = timely::WorkerConfig::default();

    let config = timely::Config {
        communication: config,
        worker: worker_config,
    };

    (config, num_peers)
}

fn main() {
    let opts = cli::parse_opts();
    let (config, _num_peers) = read_timely_config(&opts.timely_config);

    timely::execute(config, move |worker| {
        // let receiver = hashmap.get(&index).unwrap();

        let (mut nodes, mut edges) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection();
            let (edge_handle, edges) = scope.new_collection();

            // Find every node in C
            // find all edges of node
            // This is simply all the paths that exist
            // (node, (community, edge))
            let paths = nodes.join(&edges);

            let m_times_2 = paths
                .explode(|(_, (_, e)): (_, (_, ToEdge<u32>))| Some(((), e.weight as isize)))
                .count();

            let k_i = paths.reduce(
                |_key, input: &[(&(Community, ToEdge<u32>), isize)], output| {
                    let mut sum = 0;

                    for ((_c, e), _) in input {
                        sum += e.weight;
                    }
                    output.push((sum, 1));
                },
            );
            // Simple mapping to obtain the node from n1
            // (n1's edge_node,  (n1,n1's community, n1's edge))
            let paths_by_edge_node = paths.map(
                |(node, (community, edge)): (Node<u32>, (Community, ToEdge<u32>))| {
                    (Node { id: edge.to }, (node, community, edge))
                },
            );

            // Obtain the community of the edge
            // (n1's edge node: n2, ((n1, n1's community, n'1 edge), (n2's community, n2's edge to n1))
            let paths_by_edge_node_comm = paths_by_edge_node
                .join_map(&nodes, |n2, (n1, n1c, edge), n2c| {
                    (*n1c, (*n1, *n2, *edge, *n2c))
                });

            let sigma_k_in = paths_by_edge_node_comm
                .map(|(n1c, (n1, n2, edge, n2c))| ((n1, n2c), (n1c, n2, edge)))
                .filter(|((_n1, n2c), (n1c, _n2, _edge))| n1c != n2c)
                .reduce(|(n1, n2c), input, output| {
                    let mut sum: i64 = 0;
                    for ((n1c, n2, edge), _) in input {
                        sum += edge.weight as i64;
                    }
                    output.push((sum, 1));
                });

            // (n2,n1) and (n1,n2) present, this will double weights
            // so we sort and take the lowest essentially n1 < n2 does this
            // index by community as well
            let communities =
                paths_by_edge_node_comm.filter(|(n1c, (_n1, _n2, _edge, n2c))| n1c != n2c);

            communities.inspect(|(x, _, _)| println!("COMMUNITIES: {:?}", x));

            let sigma_total = paths_by_edge_node_comm
                .filter(|(n1c, (_n1, _n2, _edge, n2c))| n1c != n2c)
                .reduce(|_key, input, output| {
                    let mut sum: i64 = 0;
                    for ((_n1, _n2, edge, _), _) in input {
                        sum += edge.weight as i64;
                    }
                    output.push((sum, 1));
                });

            // find all the weights for a given community
            let sigma_in = communities.reduce(|_key, input, output| {
                let mut sum: i64 = 0;

                for ((n1, n2, edge, _n2c), _) in input {
                    if n1 < n2 {
                        sum += edge.weight as i64;
                    }
                }
                output.push((sum, 1));
            });

            let aggreg = paths_by_edge_node_comm
                .join_map(&sigma_in, |n1c, (n1, n2, edge, n2c), sigma_in| {
                    // sigma_in is for n1c
                    (*n1c, (*n1, *n2, *edge, *n2c, *sigma_in))
                })
                .join_map(
                    &sigma_total,
                    |n1c, (n1, n2, edge, n2c, sigma_in), sigma_tot| {
                        // sigma_total is for n1c
                        ((*n2, *n1c), (*n1, *edge, *n2c, *sigma_in, *sigma_tot))
                    },
                )
                .join_map(
                    &sigma_k_in,
                    |(n2, n1c), (n1, edge, n2c, sigma_in, sigma_tot), sigma_k_in| {
                        (
                            // sigma_k_in is for n2 -> n1c
                            *n2,
                            (*n1, *n1c, *edge, *n2c, *sigma_in, *sigma_tot, *sigma_k_in),
                        )
                    },
                )
                .join_map(
                    &k_i,
                    |n2, (n1, n1c, edge, n2c, sigma_in, sigma_tot, sigma_k_in), k_in| {
                        (
                            (),
                            (
                                *n1,
                                *n1c,
                                *edge,
                                *n2,
                                *n2c,
                                *sigma_in,
                                *sigma_tot,
                                *sigma_k_in,
                                *k_in,
                            ),
                        )
                    },
                )
                .join_map(
                    &m_times_2,
                    |_, (n1, n1c, edge, n2, n2c, sigma_in, sigma_tot, sigma_k_in, k_in), m| {
                        let sigma_in = OrderedFloat(*sigma_in as f64);
                        let sigma_tot = OrderedFloat(*sigma_tot as f64);
                        let sigma_k_in = OrderedFloat(*sigma_k_in as f64);
                        let k_in = OrderedFloat(*k_in as f64);
                        let m = OrderedFloat(*m as f64);
                        let delta_q_p1 =
                            (sigma_in + sigma_k_in) / m - ((sigma_tot + k_in) / m).powf(2.0);
                        let delta_q_p2 =
                            (sigma_in / m) - (sigma_tot / m).powf(2.0) - (k_in / m).powf(2.0);

                        // delta q for moving n2c to n1
                        let delta_q = delta_q_p1 - delta_q_p2;

                        ((*n2, *n1c), (*n1, *n1c, *edge, *n2, *n2c, delta_q))
                    },
                );

            let a = aggreg.map(
                |((n2, n1c), (n1, _n1c_copy, _edge, _n2_copy, n2c, delta_q))| ((n1, n2c), delta_q),
            );

            a.join_map(&aggreg, |(k1, k2), v1, v2| ());

            // aggreg.join_map(&a, |key, v1, v2| ());
            aggreg.inspect(|(x, _, _)| println!("{:?}", x));

            (node_handle, edge_handle)
        });

        nodes.advance_to(1);
        nodes.flush();
        edges.advance_to(1);
        edges.flush();

        nodes.insert((Node { id: 20 }, Community { id: 1, weights: 1 }));
        nodes.insert((Node { id: 1 }, Community { id: 1, weights: 1 }));
        nodes.insert((Node { id: 3 }, Community { id: 1, weights: 1 }));

        nodes.insert((Node { id: 2 }, Community { id: 2, weights: 1 }));

        edges.insert((Node { id: 1 }, ToEdge { to: 3, weight: 5 }));
        edges.insert((Node { id: 3 }, ToEdge { to: 1, weight: 5 }));

        edges.insert((Node { id: 3 }, ToEdge { to: 2, weight: 7 }));
        edges.insert((Node { id: 2 }, ToEdge { to: 3, weight: 7 }));

        edges.insert((Node { id: 1 }, ToEdge { to: 20, weight: 11 }));
        edges.insert(((Node { id: 20 }), ToEdge { to: 1, weight: 11 }));

        nodes.insert((Node { id: 5 }, Community { id: 5, weights: 1 }));
        nodes.insert((Node { id: 6 }, Community { id: 5, weights: 1 }));
        nodes.insert((Node { id: 7 }, Community { id: 5, weights: 1 }));
        nodes.insert((Node { id: 8 }, Community { id: 5, weights: 1 }));

        edges.insert((Node { id: 5 }, ToEdge { to: 6, weight: 13 }));
        edges.insert((Node { id: 6 }, ToEdge { to: 5, weight: 13 }));

        edges.insert((Node { id: 5 }, ToEdge { to: 1, weight: 1 }));
        edges.insert((Node { id: 1 }, ToEdge { to: 5, weight: 1 }));

        edges.insert((Node { id: 8 }, ToEdge { to: 20, weight: 23 }));
        edges.insert((Node { id: 20 }, ToEdge { to: 8, weight: 23 }));

        edges.insert((Node { id: 5 }, ToEdge { to: 7, weight: 17 }));
        edges.insert((Node { id: 7 }, ToEdge { to: 5, weight: 17 }));
    })
    .expect("timely failed to start");
}
