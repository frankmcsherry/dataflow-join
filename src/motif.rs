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

use timely::ExchangeData;
use timely::dataflow::*;
use timely::dataflow::operators::*;

use index::Index;
use ::{IndexStream, StreamPrefixExtender, GenericJoin};

pub type Node = u32;
pub type Edge = (Node, Node);

/// Handles to the forward and reverse graph indices.
pub struct GraphStreamIndexHandle<T> {
    forward: Rc<RefCell<Index<Node, Node, T>>>,
    reverse: Rc<RefCell<Index<Node, Node, T>>>,
}

impl<T: Ord+Clone+::std::fmt::Debug> GraphStreamIndexHandle<T> {
    /// Merges both handles up to the specified time, compacting their representations.
    pub fn merge_to(&self, time: &T) {
        self.forward.borrow_mut().merge_to(time);
        self.reverse.borrow_mut().merge_to(time);
    }
}

/// Indices and updates for a graph stream.
pub struct GraphStreamIndex<G: Scope, H1: Fn(Node)->u64, H2: Fn(Node)->u64> 
    where G::Timestamp: Ord+::std::hash::Hash {
    updates: Stream<G, (Edge, i32)>,
    pub forward: IndexStream<Node, Node, H1, G::Timestamp>,
    pub reverse: IndexStream<Node, Node, H2, G::Timestamp>,
}

impl<G: Scope, H1: Fn(Node)->u64+'static, H2: Fn(Node)->u64+'static> GraphStreamIndex<G, H1, H2> where G::Timestamp: Ord+::std::hash::Hash {

    /// Constructs a new graph stream index from initial edges and an update stream.
    pub fn from(initially: Stream<G, Edge>, 
                updates: Stream<G, (Edge, i32)>, hash1: H1, hash2: H2) -> (Self, GraphStreamIndexHandle<G::Timestamp>) {

        let forward = IndexStream::from(hash1, &initially, &updates);
        let reverse = IndexStream::from(hash2, &initially.map(|(src,dst)| (dst,src)),
                                               &updates.map(|((src,dst),wgt)| ((dst,src),wgt)));
        let index = GraphStreamIndex {
            updates: updates,
            forward: forward,
            reverse: reverse,
        };
        let handles = GraphStreamIndexHandle {
            forward: index.forward.index.clone(),
            reverse: index.reverse.index.clone(),
        };
        (index, handles)
    }

    /// Constructs a new graph stream index from initial edges and an update stream.
    pub fn from_separately(initially_f: Stream<G, Edge>, initially_r: Stream<G, Edge>, 
                updates: Stream<G, (Edge, i32)>, hash1: H1, hash2: H2) -> (Self, GraphStreamIndexHandle<G::Timestamp>) {

        let forward = IndexStream::from(hash1, &initially_f, &updates);
        let reverse = IndexStream::from(hash2, &initially_r.map(|(src,dst)| (dst,src)),
                                               &updates.map(|((src,dst),wgt)| ((dst,src),wgt)));
        let index = GraphStreamIndex {
            updates: updates,
            forward: forward,
            reverse: reverse,
        };
        let handles = GraphStreamIndexHandle {
            forward: index.forward.index.clone(),
            reverse: index.reverse.index.clone(),
        };
        (index, handles)
    }

    /// Constructs a dataflow subgraph to track a described motif.
    pub fn track_motif<'a>(&self, description: &[(usize, usize)]) -> Stream<G, (Vec<Node>, i32)> where G: 'a {
        let mut result = self.updates.filter(|_| false).map(|_| (Vec::new(), 0));
        for relation in 0 .. description.len() {
            result = result.concat(&self.relation_update(relation, &description));
        }
        result
    }
}


trait IndexNode {
    fn index(&self, index: usize) -> Node;
}

impl IndexNode for Vec<Node> {
    #[inline(always)] fn index(&self, index: usize) -> Node { self[index] }
}
impl IndexNode for [Node; 2] { #[inline(always)] fn index(&self, index: usize) -> Node { self[index] } }
impl IndexNode for [Node; 3] { #[inline(always)] fn index(&self, index: usize) -> Node { self[index] } }
impl IndexNode for [Node; 4] { #[inline(always)] fn index(&self, index: usize) -> Node { self[index] } }
impl IndexNode for [Node; 5] { #[inline(always)] fn index(&self, index: usize) -> Node { self[index] } }

impl<G: Scope, H1: Fn(Node)->u64+'static, H2: Fn(Node)->u64+'static> GraphStreamIndex<G, H1, H2> where G::Timestamp: Ord+::std::hash::Hash {

    // produces updates for changes in the indicated relation only.
    fn relation_update<'a>(&self, relation: usize, relations: &[(usize, usize)]) -> Stream<G, (Vec<Node>, i32)> 
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
              P: ::std::fmt::Debug+ExchangeData+IndexNode {
        let mut extenders: Vec<Box<StreamPrefixExtender<G, Prefix=P, Extension=Node>+'a>> = vec![];
        for &(attribute, is_forward, prior) in plan {
            extenders.push(match (is_forward, prior) {
                (true, true)    => Box::new(self.forward.extend_using(move |x: &P| x.index(attribute), <_ as PartialOrd>::le)),
                (true, false)   => Box::new(self.forward.extend_using(move |x: &P| x.index(attribute), <_ as PartialOrd>::lt)),
                (false, true)   => Box::new(self.reverse.extend_using(move |x: &P| x.index(attribute), <_ as PartialOrd>::le)),
                (false, false)  => Box::new(self.reverse.extend_using(move |x: &P| x.index(attribute), <_ as PartialOrd>::lt)),
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