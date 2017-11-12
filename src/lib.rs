//! An incremental implementation of worst-case optimal joins.
//!
//! This crate contains functionality to construct timely dataflow computations to compute and maintain 
//! the results of complex relational joins under changes to the relations, with worst-case optimality 
//! guarantees about the running time.
//! 
//! As an example, consider a stream of directed graph edges `(src, dst)` where we would like to find all 
//! directed cycles of length three. That is, node indentifiers `x0`, `x1`, and `x2` where the graph contains
//! edges `(x0, x1)`, `(x1, x2)`, and `(x2, x0)`. We can write this query as a relational join on the edge
//! relation `edge(x,y)`, as
//!
//! cycle_3(x0, x1, x2) := edge(x0, x1), edge(x1, x2), edge(x2, x0)
//!
//! To determine the set of three-cycles, we could use standard techniques from the database literature to 
//! perform the join, typically first picking one attribute (`x0`, `x1`, or `x2`) and performing the join on
//! the two relations containing that attribute, then joining (intersecting) with the remaining relation.
//! 
//! This has the defect that it may perform an amount of work quadratic in the size of `edges`. Recent work 
//! on "worst-case optimal join processing" shows how to get around this problem, by considering multiple 
//! relations at the same time.
//!
//! This crate is a streaming implementation of incremental worst-case optimal join processing. You may 
//! indicate a relational query like above, and the crate with synthesize a timely dataflow computation which
//! reports all changes to the occurrences of satisfying assignments to the values. The amount of work performed
//! is no more than the worst-case optimal bound.

extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::Data;

mod index;
mod extender;
pub mod motif;

pub use index::Index;
pub use extender::IndexStream;

/// Functionality used by GenericJoin to extend prefixes with new attributes.
///
/// These methods are used in `GenericJoin`'s `extend` method, and may not be broadly useful elsewhere.
pub trait StreamPrefixExtender<G: Scope, W: Data> {
    /// The type of data to extend.
    type Prefix: Data;
    /// The type of the extentions.
    type Extension: Data;
    /// Updates each prefix with an upper bound on the number of extensions for this relation.
    fn count(&self, Stream<G, (Self::Prefix, u64, u64, W)>, u64) -> Stream<G, (Self::Prefix, u64, u64, W)>;
    /// Proposes each extension from this relation.
    fn propose(&self, Stream<G, (Self::Prefix, W)>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>, W)>;
    /// Restricts proposals by those this relation would propose.
    fn intersect(&self, Stream<G, (Self::Prefix, Vec<Self::Extension>, W)>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>, W)>;
}

/// Extension method for generic join functionality.
pub trait GenericJoin<G:Scope, P:Data, W: Data> {
    /// Extends a stream of prefixes using the supplied prefix extenders.
    fn extend<'a, E: Data>(&self, extenders: Vec<Box<StreamPrefixExtender<G, W, Prefix=P, Extension=E>+'a>>)
        -> Stream<G, (P, Vec<E>, W)>;
}

// A layer of GenericJoin, in which a collection of prefixes are extended by one attribute
impl<G: Scope, P:Data, W: Data> GenericJoin<G, P, W> for Stream<G, (P, W)> {
    fn extend<'a, E>(&self, extenders: Vec<Box<StreamPrefixExtender<G, W, Prefix=P, Extension=E>+'a>>) -> Stream<G, (P, Vec<E>, W)> 
    where E: Data {

        if extenders.len() == 1 {
            extenders[0].propose(self.clone())
        }
        else {
            let mut counts = self.map(|(p,s)| (p, 1 << 31, 0, s));
            for (index,extender) in extenders.iter().enumerate() {
                counts = extender.count(counts, index as u64);
            }

            let parts = counts.partition(extenders.len() as u64, |(p, _, i, w)| (i, (p, w)));

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(nominations);
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].intersect(extensions);
                }

                results.push(extensions);    // save extensions
            }

            self.scope().concatenate(results).map(|(p,es,w)| (p,es,w))
        }
    }
}

/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to 
/// count the number of elements in time logarithmic in the result.
// #[inline(never)]
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {

        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step = step << 1;
        }

        // advance in exponentially shrinking steps.
        step = step >> 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step = step >> 1;
        }

        index += 1;
    }   

    index
}
