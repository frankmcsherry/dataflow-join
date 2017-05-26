//! Construction of dataflow for general graph motifs.
//!
//! A graph motif is a small graph on nodes which each correspond to variables. An instance of a motif in a larger graph
//! is a binding of graph nodes to these variables, so that for each edge in motif the corresponding edge between the bound
//! graph nodes exists in the graph.
//! 
//! For example, we can describe a "triangle" as a motif on three variables, `x0`, `x1`, and `x2` with motif edges 
//! `(x0, x1)`, `(x0,x2)`, and `(x1, x2)`. In a larger graph, each setting of these three variables so that the three edges
//! exist is an instance of the triangle motif.

use std::rc::Rc;
use std::cell::RefCell;

use timely::{Data, ExchangeData};
use timely::dataflow::*;
use timely::dataflow::operators::*;

use index::Index;
use extender::Indexable;
use ::{IndexStream, StreamPrefixExtender, GenericJoin};

/// Handles to the forward and reverse graph indices.
pub struct GraphStreamIndexHandle<T> {
    forward: Rc<RefCell<Index<u32, T>>>,
    reverse: Rc<RefCell<Index<u32, T>>>,
}

impl<T: Ord+Clone+::std::fmt::Debug> GraphStreamIndexHandle<T> {
    /// Merges both handles up to the specified time, compacting their representations.
    pub fn merge_to(&self, time: &T) {
        self.forward.borrow_mut().merge_to(time);
        self.reverse.borrow_mut().merge_to(time);
    }
}

/// Indices and updates for a graph stream.
pub struct GraphStreamIndex<G: Scope> 
    where G::Timestamp: Ord+::std::hash::Hash {
    updates: Stream<G, ((u32, u32), i32)>,
    forward: IndexStream<G>,
    reverse: IndexStream<G>,
}

impl<G: Scope> GraphStreamIndex<G> where G::Timestamp: Ord+::std::hash::Hash {

    /// Constructs a new graph stream index from initial edges and an update stream.
    pub fn from(initially: Stream<G, (u32, u32)>, 
                updates: Stream<G, ((u32, u32), i32)>) -> (Self, GraphStreamIndexHandle<G::Timestamp>) {
        let (forward, f_handle) = updates.index_from(&initially);
        let (reverse, r_handle) = updates.map(|((src,dst),wgt)| ((dst,src),wgt))
                                         .index_from(&initially.map(|(src,dst)| (dst,src)));
        let index = GraphStreamIndex {
            updates: updates,
            forward: forward,
            reverse: reverse,
        };
        let handles = GraphStreamIndexHandle {
            forward: f_handle,
            reverse: r_handle,
        };
        (index, handles)
    }

    /// Constructs a dataflow subgraph to track a described motif.
    pub fn track_motif<'a>(&self, description: &[(usize, usize)]) -> Stream<G, (Vec<u32>, i32)> where G: 'a {
        let mut result = self.updates.filter(|_| false).map(|_| (Vec::new(), 0));
        for relation in 0 .. description.len() {
            result = result.concat(&self.relation_update(relation, &description));
        }
        result
    }
}


trait Indexable64 {
    fn index(&self, index: usize) -> u64;
}

impl Indexable64 for Vec<u32> {
    fn index(&self, index: usize) -> u64 { self[index] as u64 }
}
impl Indexable64 for [u32; 2] { fn index(&self, index: usize) -> u64 { self[index] as u64 } }
impl Indexable64 for [u32; 3] { fn index(&self, index: usize) -> u64 { self[index] as u64 } }
impl Indexable64 for [u32; 4] { fn index(&self, index: usize) -> u64 { self[index] as u64 } }
impl Indexable64 for [u32; 5] { fn index(&self, index: usize) -> u64 { self[index] as u64 } }

impl<G: Scope> GraphStreamIndex<G> where G::Timestamp: Ord+::std::hash::Hash {

    // produces updates for changes in the indicated relation only.
    fn relation_update<'a>(&self, relation: usize, relations: &[(usize, usize)]) -> Stream<G, (Vec<u32>, i32)> 
        where G: 'a {

        // we need to determine an order on the attributes that ensures that each are bound by preceding attributes. 
        let (attrs, _remap, relations) = order_attributes(relation, &relations);
        let query_plan = plan_query(&relations, relation);

        let source = self.updates.map(|((x,y),w)| ([x, y], w));
        let stream = if query_plan.len() > 0 {

            // we do the first extension using arrays rather than vecs, to prove a point.
            let mut stream = self.extend_attribute(&source, &query_plan[0])
                                 .flat_map(|(p, es, w)| es.into_iter().map(move |e| (vec![p[0], p[1], e], w)));

            // now stream contains vecs, and so we use vec extensions4.
            for stage in &query_plan[1..] { 
                stream = self.extend_attribute(&stream, &stage)
                             .flat_map(|(p, es, w)|
                                    es.into_iter().map(move |e|  {
                                       let mut clone = p.clone();
                                       clone.push(e);
                                       (clone, w)
                                    }));
            }

            stream
        }
        else {
            source.map(|p| (vec![p.0[0], p.0[1]], p.1))
        };

        // undo the attribute re-ordering.
        stream.map(move |(vec, w)| {
            let mut new_vec = vec![0; vec.len()];
            for (index, &val) in vec.iter().enumerate() {
                new_vec[attrs[index]] = val;
            }
            (new_vec, w)
        })
    }
    /// Extends an indexable prefix, using a plan described by several (attr, is_forward, is_prior) cues.
    fn extend_attribute<'a, P>(&self, stream: &Stream<G, (P, i32)>, plan: &[(usize, bool, bool)]) -> Stream<G, (P, Vec<u32>, i32)> 
        where G: 'a,
              P: ::std::fmt::Debug+ExchangeData+Indexable64 {
        let mut extenders: Vec<Box<StreamPrefixExtender<G, Prefix=P, Extension=u32>+'a>> = vec![];
        for &(attribute, is_forward, prior) in plan {
            extenders.push(match (is_forward, prior) {
                (true, true)    => Box::new(self.forward.extend_using(move |x: &P| x.index(attribute), |t1, t2| t1.le(t2))),
                (true, false)   => Box::new(self.forward.extend_using(move |x: &P| x.index(attribute), |t1, t2| t1.lt(t2))),
                (false, true)   => Box::new(self.reverse.extend_using(move |x: &P| x.index(attribute), |t1, t2| t1.le(t2))),
                (false, false)  => Box::new(self.reverse.extend_using(move |x: &P| x.index(attribute), |t1, t2| t1.lt(t2))),
            })
        }
        stream.extend(extenders)
    }
}

// orders the numbers 0 .. so that each has at least one relation binding it to a prior attribute, 
// starting from those found in `query`.
fn order_attributes(relation_index: usize, relations: &[(usize, usize)]) -> (Vec<usize>, Vec<usize>, Vec<(usize, usize)>) {

	// 1. Determine an order on the attributes. 
	//    The order may not introduce an attribute until it is are constrained by at least one relation to an existing attribute.
	//    The order may otherwise be arbitrary, for example selecting the most constrained attribute first.
	//    Presently, we just pick attributes arbitrarily.
    let mut active = vec![];
    active.push(relations[relation_index].0);
    active.push(relations[relation_index].1);

    let mut done = false;
    while !done {
        done = true;
        for &(src, dst) in relations {
            if active.contains(&src) && !active.contains(&dst) {
                active.push(dst);
                done = false;
            }
            if active.contains(&dst) && !active.contains(&src) {
                active.push(src);
                done = false;
            }
        }
    }

    // 2. Re-map each of the relations to treat attributes in order, avoiding weird re-indexing later on.
    let mut relabel = vec![0; active.len()];
    for (position, &attribute) in active.iter().enumerate() {
    	relabel[attribute] = position;
    }

    let relations = relations.iter().map(|&(src,dst)| (relabel[src], relabel[dst])).collect::<Vec<_>>();

    // 3. Return the attribute order, the relabeling, and the relabeled relations
    (active, relabel, relations)
}

/// Determines constraints on each of a sequence of attributes.
///
/// Given relations on attributes, presumed to be introduced in increasing order, this method identifies 
/// for each attribute the constraints on it in terms of triples of
///    1. prior attributes, 
///    2. which index is required (forward: true, reverse: false), and 
///    3. whether the relation comes before or after `source_index`.
fn plan_query(relations: &[(usize, usize)], source_index: usize) -> Vec<Vec<(usize, bool, bool)>> {

	let mut attributes = 0;
	for &(src,dst) in relations {
		if attributes < src { attributes = src; }
		if attributes < dst { attributes = dst; }
	}
	attributes += 1;

	// for each attribute, determine relations constraining that attribute.
	let mut plan = vec![];
	for attribute in 2 .. attributes {
		let mut constraints = vec![];
		for (index, &(src, dst)) in relations.iter().enumerate() {
			// if src is our attribute and dst is already bound ...
			if src == attribute && dst < attribute {
				constraints.push((dst, false, index < source_index));
			}
			// if dst is our attribute and src is already bound ...
			if dst == attribute && src < attribute {
				constraints.push((src, true, index < source_index));
			}
		}
		plan.push(constraints);
	}

	plan
}