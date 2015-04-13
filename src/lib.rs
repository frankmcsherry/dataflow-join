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
use timely::example::stream::Stream;
use timely::example::unary::UnaryExt;
use timely::example::select::SelectExt;
use timely::example::filter::FilterExt;
use timely::example::concat::ConcatExtensionTrait;
use timely::example::partition::PartitionExt;
use timely::communication::exchange::{Pipeline, Exchange};
use timely::communication::observer::ObserverSessionExt;
use timely::communication::{Data, Pullable};

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


pub trait PrefixExtender<Prefix, Extension> {
    // these are needed to tell timely dataflow how to route prefixes
    type RoutingFunction: Fn(&Prefix)->u64+'static;
    fn route(&self) -> Self::RoutingFunction;
    // these are the parts required for the join algorithm
    fn count(&self, &Prefix) -> u64;
    fn propose(&self, &Prefix) -> Vec<Extension>;
    fn intersect(&self, &Prefix, &mut Vec<Extension>);
}

impl<G, P, E, PE> StreamPrefixExtender<G, P, E> for Rc<RefCell<PE>>
where G: Graph,
      P: Data+Columnar,
      E: Data+Columnar,
      PE: PrefixExtender<P, E>+'static {
    fn count(&self, stream: &mut Stream<G, (P, u64, u64)>, ident: u64) -> Stream<G, (P, u64, u64)> {
        let func = self.borrow().route();
        let clone = self.clone();
        let exch = Exchange::new(move |&(ref x,_,_)| func(x));
        stream.unary(exch, format!("Count"), move |handle| {
            let extender = clone.borrow();
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for (prefix, old_count, old_index) in data {
                    let new_count = extender.count(&prefix);
                    let (count, index) = if new_count < old_count {
                        (new_count, ident)
                    }
                    else {
                        (old_count, old_index)
                    };
                    if count > 0 {
                        session.give((prefix, count, index));
                    }
                }
            }
        })
    }

    fn propose(&self, stream: &mut Stream<G, P>) -> Stream<G, (P, Vec<E>)> {
        let func = self.borrow().route();
        let clone = self.clone();
        let exch = Exchange::new(move |x| func(x));
        stream.unary(exch, format!("Propose"), move |handle| {
            let extender = clone.borrow();
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for prefix in data {
                    let extensions = extender.propose(&prefix);
                    if extensions.len() > 0 {
                        session.give((prefix, extensions));
                    }
                }
            }
        })
    }
    fn intersect(&self, stream: &mut Stream<G, (P, Vec<E>)>) -> Stream<G, (P, Vec<E>)> {
        let func = self.borrow().route();
        let clone = self.clone();
        let exch = Exchange::new(move |&(ref x,_)| func(x));
        stream.unary(exch, format!("Intersect"), move |handle| {
            let extender = clone.borrow();
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for (prefix, mut extensions) in data {
                    extender.intersect(&prefix, &mut extensions);
                    session.give((prefix, extensions));
                }
            }
        })
    }
}

pub trait StreamPrefixExtender<G: Graph, Prefix: Data+Columnar, Extension: Data+Columnar> {
    fn count(&self, &mut Stream<G, (Prefix, u64, u64)>, u64) -> Stream<G, (Prefix, u64, u64)>;
    fn propose(&self, &mut Stream<G, Prefix>) -> Stream<G, (Prefix, Vec<Extension>)>;
    fn intersect(&self, &mut Stream<G, (Prefix, Vec<Extension>)>) -> Stream<G, (Prefix, Vec<Extension>)>;
}

pub trait GenericJoinExt<G, P:Data+Columnar, E:Data+Columnar> {
    fn extend(&mut self, extenders: Vec<&StreamPrefixExtender<G, P, E>>) -> Stream<G, (P, Vec<E>)>;
}

// A layer of GenericJoin, in which a collection of prefixes are extended by one attribute
impl<G: Graph, P:Data+Columnar, E:Data+Columnar> GenericJoinExt<G, P, E> for Stream<G, P> {
    fn extend(&mut self, extenders: Vec<&StreamPrefixExtender<G, P, E>>) -> Stream<G, (P, Vec<E>)> {

        // improve the counts using each extender
        let mut counts = self.select(|p| (p, 1 << 31, 0));
        for index in (0..extenders.len()) {
            counts = extenders[index].count(&mut counts, index as u64)
                                     .filter(|x| x.1 > 0);
        }

        let mut results = Vec::new();
        let mut parts = counts.partition(extenders.len() as u64, |&(_, _, i)| i);
        for index in (0..extenders.len()) {
            let mut nominations = parts[index].select(|(x, _, _)| x);
            let mut extensions = extenders[index].propose(&mut nominations);
            for other in (0..extenders.len()).filter(|&x| x != index) {
                extensions = extenders[other].intersect(&mut extensions);
            }
            results.push(extensions);
        }

        if let Some(mut output) = results.pop() {
            while let Some(mut result) = results.pop() {
                output = output.concat(&mut result);
            }

            output
        }
        else { panic!("extenders.len() == 0"); }
    }
}

pub trait FlattenExt<G: Graph, P: Data+Columnar, E: Data+Columnar> {
    fn flatten(&mut self) -> Stream<G, (P, E)>;
}

impl<G: Graph, P: Data+Columnar, E: Data+Columnar> FlattenExt<G, P, E> for Stream<G, (P, Vec<E>)> {
    fn flatten(&mut self) -> Stream<G, (P, E)> {
        self.unary(Pipeline, format!("Flatten"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                for (prefix, extensions) in data {
                    for extension in extensions {
                        session.give((prefix.clone(), extension));
                    }
                }
            }
        })
    }
}
