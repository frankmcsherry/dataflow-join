use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::fmt::Debug;

use timely::Data;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::progress::Timestamp;

use {Index, StreamPrefixExtender};

/// A pair of update stream and update index.
///
/// The stream itself is empty, but its frontier can be used as a guarantee that the 
/// index contents are locked down for certain times. The index is only available through
/// the `get_index()` method, to ensure that you don't write to it here.
pub struct IndexStream<G: Scope> where G::Timestamp: Ord {
    pub stream: Stream<G, (u32, (u32, i32))>,
    index: Rc<RefCell<Index<u32, G::Timestamp>>>,
}

/// A wrapper for an index and a function turning prefixes into index keys.
pub struct IndexExtender<G: Scope, P, L: Fn(&P)->u64, F: Fn(&G::Timestamp, &G::Timestamp)->bool> where G::Timestamp: Ord {
    stream: Stream<G, (u32,(u32,i32))>,
    helper: Rc<Helper<G::Timestamp, P, L, F>>,
}

pub struct Helper<T: Timestamp+Ord, P, L: Fn(&P)->u64, F: Fn(&T, &T)->bool> {
    index: Rc<RefCell<Index<u32, T>>>,
    logic: Rc<L>,
    phant: PhantomData<P>,
    func: F,
}

// Implementations wrapping the underlying `Index` struct.
impl<T: Timestamp+Ord, P: ::std::fmt::Debug, L: Fn(&P)->u64+'static, F: Fn(&T, &T)->bool> Helper<T, P, L, F> {
    /// Counts extensions for a prefix.
    fn count(&self, data: &mut Vec<(P, u64, u64, i32)>, time: &T, ident: u64) {
        let logic = self.logic.clone();
        self.index.borrow_mut().count(data, &|p| logic(p) as u32, &|t| (self.func)(t, time), ident);
    }
    /// Proposes extensions for a prefix.
    fn propose(&self, data: &mut Vec<(P, Vec<u32>, i32)>, time: &T) {
        let logic = self.logic.clone();
        self.index.borrow_mut().propose(data, &|p| logic(p) as u32, &|t| (self.func)(t, time));
    }
    /// Intersects proposed extensions for a prefix.
    fn intersect(&self, data: &mut Vec<(P, Vec<u32>, i32)>, time: &T) {
        let logic = self.logic.clone();
        self.index.borrow_mut().intersect(data, &|p| logic(p) as u32, &|t| (self.func)(t, time));
    }
    /// returns a copy of the prefix-mapping logic.
    fn logic(&self) -> Rc<L> { self.logic.clone() }
}



impl<G, P, L, F> StreamPrefixExtender<G> for Rc<IndexExtender<G, P, L, F>> 
where G: Scope, 
      G::Timestamp: ::std::hash::Hash+Ord,
      P: Data+Debug, 
      L: Fn(&P)->u64+'static, 
      F: Fn(&G::Timestamp, &G::Timestamp)->bool+'static {
    type Prefix = P;
    type Extension = u32;

    fn count(&self, prefixes: Stream<G, (Self::Prefix, u64, u64, i32)>, ident: u64) 
    -> Stream<G, (Self::Prefix, u64, u64, i32)> {
        
        let clone = self.helper.clone();
        let logic = self.helper.logic();
        let exch = Exchange::new(move |&(ref x,_,_,_)| (*logic)(x));

        let mut blocked = vec![];

        prefixes.binary_notify(&self.stream, exch, Pipeline, "Count", vec![], 
            move |input1, input2, output, notificator| {

            // The logic in this operator should only be applied to data inputs at `time` once we are 
            // certain that the second input has also advanced to `time`. The shared index `clone` is
            // only guaranteed to be up to date once that has happened. So, if we receive data inputs
            // for a time that has not also been achieved in the other input, we must delay it.
            //
            // The same structure also applies to `propose` and `intersect`, so these comments apply too.

            // put all (time, data) pairs into a temporary list 
            input1.for_each(|time, data| blocked.push((time, data.take())));
            input2.for_each(|_,_| {});

            // scan each stashed element and see if it is time to process it.
    		for &mut (ref time, ref mut data) in &mut blocked {

                // ok to process if no further updates less or equal to `time`.
    			if !notificator.frontier(1).iter().any(|t| t.le(&time.time())) {

                    // pop the data out of the list; we'll clean up the entry later.
                    let mut data = data.take();
                    (*clone).count(&mut data, &time.time(), ident);
                    data.retain(|x| x.1 > 0);

                    output.session(time).give_content(&mut data);
    			}
    		}

            // discard any data we processed up above.
            blocked.retain(|&(_, ref data)| data.len() > 0);
    	})
    }

    fn propose(&self, stream: Stream<G, (Self::Prefix, i32)>) 
    -> Stream<G, (Self::Prefix, Vec<Self::Extension>, i32)> {

        let clone = self.helper.clone();
        let logic = self.helper.logic();
        let exch = Exchange::new(move |&(ref x,_)| (*logic)(x));

        let mut blocked = vec![];

        stream.binary_notify(&self.stream, exch, Pipeline, "Managed", vec![], 
            move |input1, input2, output, notificator| {

            input1.for_each(|time, data| blocked.push((time, data.take())));
            input2.for_each(|_,_| {});

            let mut todo = Vec::new();

            for &mut (ref time, ref mut data) in &mut blocked {
                if !notificator.frontier(1).iter().any(|t| t.le(&time.time())) {

                    let mut data = data.take();
                    todo.extend(data.drain(..).map(|(p,s)| (p,vec![],s)));
                }
            }

            if todo.len() > 0 {
                (*clone).propose(&mut todo, &blocked[0].0.time());
                let mut session = output.session(&blocked[0].0);
                for x in todo.drain(..) { 
                    if x.1.len() > 0 {
                        session.give(x); 
                    }
                }
            }

            blocked.retain(|&(_, ref data)| data.len() > 0);
    	})
    }

    fn intersect(&self, stream: Stream<G, (Self::Prefix, Vec<Self::Extension>, i32)>) 
        -> Stream<G, (Self::Prefix, Vec<Self::Extension>, i32)> {

        let logic = self.helper.logic();
        let clone = self.helper.clone();

        let mut blocked = Vec::new();
        let exch = Exchange::new(move |&(ref x,_,_)| (*logic)(x));

        stream.binary_notify(&self.stream, exch, Pipeline, "Intersect", vec![], 
            move |input1, input2, output, notificator| {
    
            input1.for_each(|time, data| blocked.push((time, data.take())));
            input2.for_each(|_,_| {});

            for &mut (ref time, ref mut data) in &mut blocked {
                if !notificator.frontier(1).iter().any(|t| t.le(&time.time())) {
                    let mut data = data.take();
                    let mut session = output.session(&time);

                    (*clone).intersect(&mut data, &time.time());
                    for x in data.drain(..) { session.give(x); }
                }
            }

            blocked.retain(|&(_, ref data)| data.len() > 0);
        })
    }
}

impl<G: Scope> IndexStream<G> where G::Timestamp: Ord {
    /// Extends an `IndexStream` using the supplied functions.
    ///
    /// The `logic` function maps prefixes to index keys.
    /// The `func` function compares timestamps, acting as either `lt` or `le` depending 
    /// on the need.
	pub fn extend_using<P, L, F>(&self, logic: L, func: F) -> Rc<IndexExtender<G, P, L, F>> 
    where 
        L: Fn(&P)->u64+'static, 
        F: Fn(&G::Timestamp, &G::Timestamp)->bool+'static
    {
        let logic = Rc::new(logic);

		Rc::new(IndexExtender {
            stream: self.stream.clone(),
            helper: Rc::new(Helper {
                index: self.index.clone(),
                logic: logic.clone(),
                phant: PhantomData,
                func: func,
            }),
		})
	}
} 

/// Arranges something as an `IndexStream`.
pub trait Indexable<G: Scope> where G::Timestamp : Ord {
    /// Returns an `IndexStream` and a handle through which the index may be mutated.
    fn index_from(&self, initially: &Stream<G, (u32, u32)>) -> (IndexStream<G>, Rc<RefCell<Index<u32, G::Timestamp>>>); 
}

impl<G: Scope> Indexable<G> for Stream<G, ((u32, u32), i32)> where G::Timestamp: ::std::hash::Hash+Ord {
    // returns a container for the streams and indices, as well as handles to the two indices.
    fn index_from(&self, initially: &Stream<G, (u32, u32)>) -> (IndexStream<G>, Rc<RefCell<Index<u32, G::Timestamp>>>) {

    	let index_a1 = Rc::new(RefCell::new(Index::new()));
    	let index_a2 = index_a1.clone();
        let index_a3 = index_a1.clone();

        let mut map = HashMap::new();
        let mut initial = Vec::new();

        let exch1 = Exchange::new(|&((x,_y),_w)| x as u64);
        let exch2 = Exchange::new(|&(x,_y)| x as u64);
    	let stream = self.binary_notify(initially, exch1, exch2, "Index", vec![], move |input1, input2,_output,notificator| {
    		let mut index = index_a2.borrow_mut();

            // extract, enqueue updates.
            input1.for_each(|time, data| {
                map.entry(time.time()).or_insert(Vec::new()).extend(data.drain(..).map(|((s,d),w)| (s,(d,w))));
                notificator.notify_at(time);
            });

            // populate initial collection
            input2.for_each(|time, data| {
                initial.extend(data.drain(..));
                notificator.notify_at(time);
            });

            notificator.for_each(|time,_,_| {
                // initialize if this is the first time
                if initial.len() > 0 {
                    index.initialize(&mut initial);
                }
                // push updates if updates exist
                if let Some(mut list) = map.remove(&time.time()) {
                    index.update(time.time(), &mut list);
                }
            });
    	});

        let index_stream = IndexStream { 
            stream: stream, 
            index: index_a1, 
        };
        
        (index_stream, index_a3)
    }
}