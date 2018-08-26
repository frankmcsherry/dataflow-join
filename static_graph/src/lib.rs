// #![allow(dead_code)]

extern crate timely;
extern crate time;
extern crate mmap;

use std::rc::Rc;

use timely::dataflow::*;
use timely::dataflow::operators::{Operator, Map, Partition, Concatenate};
use timely::dataflow::channels::pact::Exchange;
use timely::ExchangeData as Data;

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
pub trait PrefixExtender {
    type Prefix;
    type Extension;

    // these are the parts required for the join algorithm
    fn count(&self, &Self::Prefix) -> u64;
    fn propose(&self, &Self::Prefix, &mut Vec<Self::Extension>);
    fn intersect(&self, &Self::Prefix, &mut Vec<Self::Extension>);

    // these are needed to tell timely dataflow how to route prefixes.
    // this object will be shared under an Rc<RefCell<...>> so we want
    // to give back a function, rather than provide a method ourself.
    type RoutingFunction: Fn(&Self::Prefix)->u64+'static;
    fn logic(&self) -> Rc<Self::RoutingFunction>;
}

// functionality required by the GenericJoin layer
pub trait StreamPrefixExtender<G: Scope> {
    type Prefix: Data;
    type Extension: Data;

    fn count(&self, Stream<G, (Self::Prefix, u64, u64)>, u64) -> Stream<G, (Self::Prefix, u64, u64)>;
    fn propose(&self, Stream<G, Self::Prefix>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>)>;
    fn intersect(&self, Stream<G, (Self::Prefix, Vec<Self::Extension>)>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>)>;
}

// implementation of StreamPrefixExtender for any (wrapped) PrefixExtender
// TODO : Add a Rc<RefCell<Vec<Vec<Self::Extension>>>> to recycle allocations
impl<G: Scope, PE: PrefixExtender+'static> StreamPrefixExtender<G> for Rc<PE>
where PE::Prefix: Data,
      PE::Extension: Data, {
    type Prefix = PE::Prefix;
    type Extension = PE::Extension;

    fn count(&self, stream: Stream<G, (Self::Prefix, u64, u64)>, ident: u64) -> Stream<G, (Self::Prefix, u64, u64)> {
        let clone = self.clone();
        let logic = self.logic();
        let exch = Exchange::new(move |&(ref x,_,_)| (*logic)(x));
        let mut buffer = Vec::new();
        let mut vector = Vec::new();
        stream.unary_notify(exch, "Count", Vec::new(), move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                buffer.extend(vector.drain(..));
                notificator.notify_at(time.retain());
            }

            notificator.for_each(|time, _cnt, _not| {
                let mut session = output.session(&time);
                for (p, mut c, mut i) in buffer.drain(..) {
                    let nc = (*clone).count(&p);
                    if nc < c {
                        c = nc;
                        i = ident;
                    }
                    if c > 0 {
                        session.give((p, c, i));
                    }
                }
            });
        })
    }

    fn propose(&self, stream: Stream<G, Self::Prefix>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>)> {
        let clone = self.clone();
        let logic = self.logic();
        let exch = Exchange::new(move |x| (*logic)(x));
        let mut buffer = Vec::new();
        let mut vector = Vec::new();
        stream.unary_notify(exch, "Propose", Vec::new(), move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                buffer.extend(vector.drain(..));
                notificator.notify_at(time.retain());
            }

            notificator.for_each(|time, _cnt, _not| {
                let mut session = output.session(&time);
                for prefix in buffer.drain(..) {
                    let mut vec = Vec::new();
                    (*clone).propose(&prefix, &mut vec);
                    session.give((prefix, vec));
                }
            });
        })
    }
    fn intersect(&self, stream: Stream<G, (Self::Prefix, Vec<Self::Extension>)>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>)> {
        let logic = self.logic();
        let clone = self.clone();
        let exch = Exchange::new(move |&(ref x,_)| (*logic)(x));
        let mut buffer = Vec::new();
        let mut vector = Vec::new();
        stream.unary_notify(exch, "Intersect", Vec::new(), move |input, output, notificator| {
            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                buffer.extend(vector.drain(..));
                notificator.notify_at(time.retain());
            }

            notificator.for_each(|time, _cnt, _not| {
                let mut session = output.session(&time);

                for (prefix, mut extensions) in buffer.drain(..) {
                    (*clone).intersect(&prefix, &mut extensions);
                    if extensions.len() > 0 {
                        session.give((prefix, extensions));
                    }
                }
            });
        })
    }
}

pub trait GenericJoinExt<G:Scope, P:Data> {
    fn extend<E: Data>(self, extenders: Vec<&StreamPrefixExtender<G, Prefix=P, Extension=E>>)
        -> Stream<G, (P, Vec<E>)>;
}

// A layer of GenericJoin, in which a collection of prefixes are extended by one attribute
impl<G: Scope, P:Data> GenericJoinExt<G, P> for Stream<G, P> {
    fn extend<E: Data>(self, extenders: Vec<&StreamPrefixExtender<G, Prefix=P, Extension=E>>)
        -> Stream<G, (P, Vec<E>)> {

        let mut counts = self.map(|p| (p, 1 << 31, 0));
        for (index,extender) in extenders.iter().enumerate() {
            counts = extender.count(counts, index as u64);
        }

        let parts = counts.partition(extenders.len() as u64, |(p, _, i)| (i, p));

        let mut results = Vec::new();
        for (index, nominations) in parts.into_iter().enumerate() {
            let mut extensions = extenders[index].propose(nominations);
            for other in (0..extenders.len()).filter(|&x| x != index) {
                extensions = extenders[other].intersect(extensions);
            }

            results.push(extensions);    // save extensions
        }

        self.scope().concatenate(results)
    }
}
