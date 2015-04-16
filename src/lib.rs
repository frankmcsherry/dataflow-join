#![allow(dead_code)]
#![feature(core)]

extern crate columnar;
extern crate timely;
extern crate core;
extern crate time;
extern crate mmap;

use std::rc::Rc;
use std::cell::RefCell;

use timely::progress::Graph;
use timely::example::*;
use timely::example::partition::PartitionExt;
use timely::communication::pact::Exchange;
use timely::communication::*;

use columnar::Columnar;

pub mod graph;
mod typedrw;

pub use typedrw::TypedMemoryMap;

// Algorithm 3 is an implementation of an instance of GenericJoin, a worst-case optimal join algorithm.

// The algorithm orders the attributes of the resulting relation, and for each prefix of these attributes
// produces the set of viable prefixes of output relations. The set of prefixes is updated by a new attribute
// by having each relation with that attribute propose extensions for each prefix, based on matching existing
// attributes within their relation. Proposals are then intersected, and surviving extended prefixes form the
// basis of the next iteration


// Informally, the algorithm looks like:
// 0. Let X be an empty relation over 0 attributes
// 1. For each output attribute A:
//     0. Let T be an initially empty set.
//     a. For each relation R containing A:
//         i. For each element x of X, let p(R, x) be the set of distinct values of A in pi_A(R join x),
//            that is, the distinct symbols R would propose to extend x.
//     b. For each element x of X, let r(x) be the relation R with the smallest p(R, x).
//     c. For each relation R containing A:
//         i. For each element x of X with r(x) = R, add (x join p(R, x)) to T.
//     d. For each relation R containing A:
//         i. For each element (x, y) of T, remove (x, y) if y is not in p(R, x).
//
// The important part of this algorithm is that step d.i should take roughly constant time.


// record-by-record prefix extension functionality
pub trait PrefixExtender<Prefix, Extension> {
    // these are needed to tell timely dataflow how to route prefixes
    type RoutingFunction: Fn(&Prefix)->u64+'static;
    fn route(&self) -> Self::RoutingFunction;
    // these are the parts required for the join algorithm
    fn count(&self, &Prefix) -> u64;
    fn propose(&self, &Prefix) -> Vec<Extension>;
    fn intersect(&self, &Prefix, &mut Vec<Extension>);
}

// functionality required by the GenericJoin layer
pub trait StreamPrefixExtender<'a, 'b: 'a, G: Graph+'b, Prefix: Data+Columnar, Extension: Data+Columnar> {
    fn count(&self, &mut Stream<'a, 'b, G, (Prefix, u64, u64)>, u64) -> Stream<'a, 'b, G, (Prefix, u64, u64)>;
    fn propose(&self, &mut Stream<'a, 'b, G, Prefix>) -> Stream<'a, 'b, G, (Prefix, Vec<Extension>)>;
    fn intersect(&self, &mut Stream<'a, 'b, G, (Prefix, Vec<Extension>)>) -> Stream<'a, 'b, G, (Prefix, Vec<Extension>)>;
}

// implementation of StreamPrefixExtender for any (wrapped) PrefixExtender
impl<'a, 'b: 'a, G, P, E, PE> StreamPrefixExtender<'a, 'b, G, P, E> for Rc<RefCell<PE>>
where G: Graph+'b,
      P: Data+Columnar,
      E: Data+Columnar,
      PE: PrefixExtender<P, E>+'static {
    fn count(&self, stream: &mut Stream<'a, 'b, G, (P, u64, u64)>, ident: u64) -> Stream<'a, 'b, G, (P, u64, u64)> {
        let func = self.borrow().route();
        let clone = self.clone();
        let exch = Exchange::new(move |&(ref x,_,_)| func(x));
        stream.unary(exch, format!("Count"), move |handle| {
            let extender = clone.borrow();
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().filter_map(|(p,c,i)| {
                    let nc = extender.count(&p);
                    if nc > c { Some((p,c,i)) }
                    else      { if nc > 0 { Some((p,nc,ident)) } else { None } }
                }));
            }
        })
    }

    fn propose(&self, stream: &mut Stream<'a, 'b, G, P>) -> Stream<'a, 'b, G, (P, Vec<E>)> {
        let func = self.borrow().route();
        let clone = self.clone();
        let exch = Exchange::new(move |x| func(x));
        stream.unary(exch, format!("Propose"), move |handle| {
            let extender = clone.borrow();
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().map(|p| {
                    let x = extender.propose(&p);
                    (p, x)
                }));
            }
        })
    }
    fn intersect(&self, stream: &mut Stream<'a, 'b, G, (P, Vec<E>)>) -> Stream<'a, 'b, G, (P, Vec<E>)> {
        let func = self.borrow().route();
        let clone = self.clone();
        let exch = Exchange::new(move |&(ref x,_)| func(x));
        stream.unary(exch, format!("Intersect"), move |handle| {
            let extender = clone.borrow();
            while let Some((time, data)) = handle.input.pull() {
                handle.output.give_at(&time, data.into_iter().filter_map(|(prefix, mut extensions)| {
                    extender.intersect(&prefix, &mut extensions);
                    if extensions.len() > 0 { Some((prefix, extensions)) } else { None }
                }));
            }
        })
    }
}

pub trait GenericJoinExt<'a, 'b: 'a, G:Graph+'b, P:Data+Columnar, E:Data+Columnar> {
    fn extend(&mut self, extenders: Vec<&StreamPrefixExtender<'a, 'b, G, P, E>>) -> Stream<'a, 'b, G, (P, Vec<E>)>;
}

// A layer of GenericJoin, in which a collection of prefixes are extended by one attribute
impl<'a, 'b: 'a, G: Graph+'b, P:Data+Columnar, E:Data+Columnar> GenericJoinExt<'a, 'b, G, P, E> for Stream<'a, 'b, G, P> {
    fn extend(&mut self, extenders: Vec<&StreamPrefixExtender<'a, 'b, G, P, E>>) -> Stream<'a, 'b, G, (P, Vec<E>)> {

        // improve the counts using each extender
        let mut counts = self.map(|p| (p, 1 << 31, 0));
        for index in (0..extenders.len()) {
            counts = extenders[index].count(&mut counts, index as u64);
        }

        let mut results = Vec::new();
        let parts = counts.partition(extenders.len() as u64, |&(_, _, i)| i);
        for (index, mut part) in parts.into_iter().enumerate() {
            let mut nominations = part.map(|(x, _, _)| x);
            let mut extensions = extenders[index].propose(&mut nominations);
            for other in (0..extenders.len()).filter(|&x| x != index) {
                extensions = extenders[other].intersect(&mut extensions);
            }
            results.push(extensions);
        }

        results.concatenate()
    }
}
