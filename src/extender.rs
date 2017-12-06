use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::DerefMut;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::{Unary, Binary, Probe};
use timely::dataflow::channels::pact::Exchange;
use timely::progress::Timestamp;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use {Index, StreamPrefixExtender};

/// An index materialized from streamed updates.
///
/// The data are updates to (key, value) pairs, indicated by an associated signed integer.
/// The index stream provides information about update times it has completely accepted via
/// its `handle` field, which is a timely dataflow probe handle, and can be interrogated
/// about whether outstanding times might still exist less than any query time.
/// There is also a function `hash` from the key type `K` to `u64` values to indicate how 
/// the data are partitioned, so that users can align their query streams.
pub struct IndexStream<K: Ord+Hash+Clone, V: Ord+Clone, H: Fn(K)->u64, T: Timestamp> {
    /// Times completely absorded into the index.
    pub handle: ProbeHandle<T>,
    /// The index itself.
    pub index: Rc<RefCell<Index<K, V, T>>>,
    /// A map from keys to u64 values, for distribution.
    pub hash: Rc<H>,
}


impl<K: Ord+Hash+Clone, V: Ord+Clone, H: Fn(K)->u64, T: Timestamp+Ord> IndexStream<K, V, H, T> {
    /// Extends an `IndexStream` using the supplied functions.
    ///
    /// The `logic` function maps prefixes to index keys.
    /// The `func` function compares timestamps, acting as either `lt` or `le` depending 
    /// on the need.
    pub fn extend_using<P, L, F>(&self, logic: L, func: F) -> Rc<IndexExtender<K, V, T, P, L, H, F>> 
    where 
        L: Fn(&P)->K+'static,
        F: Fn(&T, &T)->bool+'static
    {
        Rc::new(IndexExtender {
            handle: self.handle.clone(),
            index: self.index.clone(),
            hash: self.hash.clone(),
            logic: Rc::new(logic),
            valid: Rc::new(func),
            phantom: PhantomData,
        })
    }

    /// Constructs an `IndexStream` from initial data and update stream.
    ///
    /// Neither the initial stream nor the update stream are required to produce data.
    /// The index can be static with no changes, or wholy dynamic with no starting data,
    /// or a mix of both. If neither stream has any data, you are probably using the wrong
    /// abstraction (though it will still work correctly).
    pub fn from<G>(hash: H, initially: &Stream<G, (K, V)>, updates: &Stream<G, ((K, V), i32)>) -> Self 
    where
        G: Scope<Timestamp=T>,
        K: ExchangeData,
        V: ExchangeData,
        T: Hash,
        H: 'static
    {
        use self::merge_sorter::MergeSorter;

        let worker_index = initially.scope().index();

        let index_1 = Rc::new(RefCell::new(Index::new()));  // held by operator
        let index_2 = index_1.clone();                      // returned in `IndexStream`.

        let hash_1 = Rc::new(hash);     // used by exchange pact 1.
        let hash_2 = hash_1.clone();    // used by exchange pact 2.
        let hash_3 = hash_1.clone();    // returned in `IndexStream`.

        let mut map = HashMap::new();
        let mut sorter = Some(MergeSorter::new(|x: &(K,V)| x.clone()));

        let exch1 = Exchange::new(move |x: &((K,V),i32)| (*hash_1)((x.0).0.clone()));
        let exch2 = Exchange::new(move |x: &(K,V)| (*hash_2)(x.0.clone()));
        let handle = updates.binary_notify::<_,(),_,_,_>(initially, exch1, exch2, "Index", vec![], 

            move |input1, input2,_output,notificator| {

                // extract, enqueue updates.
                input1.for_each(|time, data| {
                    map.entry(time.time().clone())
                       .or_insert(Vec::new())
                       .extend(data.drain(..));
                    notificator.notify_at(time);
                });

                // populate initial collection
                input2.for_each(|time, data| {
                    if let Some(ref mut sorter) = sorter {
                        sorter.push(data.deref_mut());
                        notificator.notify_at(time);
                    }
                });

                notificator.for_each(|time,_,_| {
                    // initialize if this is the first time
                    if let Some(mut sorter) = sorter.take() {
                        let mut sorted = Vec::new();
                        sorter.finish_into(&mut sorted);
                        let sum: usize = sorted.iter().map(|x| x.len()).sum();
                        println!("worker {}: index built with {} elements", worker_index, sum);
                        index_1.borrow_mut().initialize(&mut sorted);
                    }
                    // push updates if updates exist
                    if let Some(mut list) = map.remove(time.time()) {
                        index_1.borrow_mut().update(time.time().clone(), &mut list);
                    }
                });
            }
        ).probe();

        IndexStream { 
            handle: handle,
            index: index_2,
            hash: hash_3,
        }
    }

} 


/// An `IndexStream` wrapper adding key selectors and time validators.
///
/// The `IndexExtender` wraps an index so that different types `P` can gain access to the
/// index simply by specifying how to extract a `&K` from a `&P`. In addition, we wrap up
/// a "time validator" that indicates for times t1 and t2 whether updates at t1 should be 
/// included in answers for time t2.
pub struct IndexExtender<K, V, T, P, L, H, F>
where 
    K: Ord+Hash+Clone,
    V: Ord+Clone,
    T: Timestamp,
    L: Fn(&P)->K,
    H: Fn(K)->u64,
    F: Fn(&T, &T)->bool,
{
    handle: ProbeHandle<T>,
    index: Rc<RefCell<Index<K, V, T>>>,
    hash: Rc<H>,
    logic: Rc<L>,
    valid: Rc<F>,
    phantom: PhantomData<P>,
}

impl<K, V, G, P, L, H, F, W> StreamPrefixExtender<G, W> for Rc<IndexExtender<K, V, G::Timestamp, P, L, H, F>> 
where 
    K: Ord+Hash+Clone+ExchangeData,
    V: Ord+Clone+ExchangeData,
    G: Scope,
    G::Timestamp: Timestamp+Ord+Clone,//+::std::hash::Hash+Ord,
    P: ExchangeData+Debug,
    L: Fn(&P)->K+'static,
    H: Fn(K)->u64+'static,
    F: Fn(&G::Timestamp, &G::Timestamp)->bool+'static,
    W: ExchangeData,
{
    type Prefix = P;
    type Extension = V;

    fn count(&self, prefixes: Stream<G, (Self::Prefix, u64, u64, W)>, ident: u64) -> Stream<G, (Self::Prefix, u64, u64, W)> {
        
        let hash = self.hash.clone();
        let index = self.index.clone();
        let logic1 = self.logic.clone();
        let logic2 = self.logic.clone();
        let valid = self.valid.clone();

        let handle = self.handle.clone();
        let mut blocked = HashMap::new();//vec![];

        let exch = Exchange::new(move |&(ref x,_,_,_)| (*hash)((*logic1)(x)));

        prefixes.unary_stream(exch, "Count", move |input, output| {

            // The logic in this operator should only be applied to data inputs at `time` once we are 
            // certain that the second input has also advanced to `time`. The shared index `clone` is
            // only guaranteed to be up to date once that has happened. So, if we receive data inputs
            // for a time that has not also been achieved in the other input, we must delay it.
            //
            // The same structure also applies to `propose` and `intersect`, so these comments apply too.

            // put all (time, data) pairs into a temporary list 
            input.for_each(|time, data| blocked.entry(time).or_insert(Vec::new()).extend(data.drain(..)));

            // scan each stashed element and see if it is time to process it.
           for (time, data) in blocked.iter_mut() {
                // ok to process if no further updates less or equal to `time`.
                if !handle.less_equal(time.time()) {
                    // pop the data out of the list; we'll clean up the entry later.
                    (*index).borrow_mut().count(data, &*logic2, &|t| (*valid)(t, time.time()), ident);
                    output.session(time).give_iterator(data.drain(..).filter(|x| x.1 > 0));
                }
            }

            // discard any data we processed up above.
            blocked.retain(|_, data| data.len() > 0);
        })
    }

    fn propose(&self, stream: Stream<G, (Self::Prefix, W)>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>, W)> {

        let hash = self.hash.clone();
        let logic1 = self.logic.clone();
        let logic2 = self.logic.clone();
        let valid = self.valid.clone();
        let handle = self.handle.clone();
        let exch = Exchange::new(move |&(ref x,_)| (*hash)((*logic1)(x)));

        let index = self.index.clone();

        let mut blocked = HashMap::new();//vec![];

        stream.unary_stream(exch, "Propose", move |input, output| {

            input.for_each(|time, data|
                blocked
                    .entry(time)
                    .or_insert(Vec::new())
                    // .extend(data.drain(..).map(|(p,s)| (p,vec![],s)))
                    .push(::std::mem::replace(data.deref_mut(), Vec::new()))
            );


            // scan each stashed element and see if it is time to process it.
            for (time, data) in blocked.iter_mut() {

                // ok to process if no further updates less or equal to `time`.
                if !handle.less_equal(time.time()) {

                    let mut effort = 4096;
                    while data.len() > 0 && effort > 0 {
                        let mut list = data.pop().unwrap();
                        effort = if list.len() > effort { 0 } else { effort - list.len() };

                        let mut data = list.drain(..).map(|(p,s)| (p,vec![],s)).collect::<Vec<_>>();
                        (*index).borrow_mut().propose(&mut data, &*logic2, &|t| (*valid)(t, time.time()));
                        let mut session = output.session(&time);
                        for x in data.drain(..) { 
                            if x.1.len() > 0 {
                                session.give(x); 
                            }
                        }
                    }
                }
            }

            blocked.retain(|_, data| data.len() > 0);
    	})
    }

    fn intersect(&self, stream: Stream<G, (Self::Prefix, Vec<Self::Extension>, W)>) -> Stream<G, (Self::Prefix, Vec<Self::Extension>, W)> {

        let hash = self.hash.clone();
        let logic1 = self.logic.clone();
        let logic2 = self.logic.clone();
        let valid = self.valid.clone();
        let index = self.index.clone();
        let handle = self.handle.clone();

        let mut blocked = HashMap::new();
        let exch = Exchange::new(move |&(ref x,_,_)| (*hash)((*logic1)(x)));

        stream.unary_stream(exch, "Intersect", move |input, output| {
    
            input.for_each(|time, data| blocked.entry(time).or_insert(Vec::new()).extend(data.drain(..)));

            for (time, data) in blocked.iter_mut() {

                // ok to process if no further updates less or equal to `time`.
                if !handle.less_equal(time.time()) {
                    (*index).borrow_mut().intersect(data, &*logic2, &|t| (*valid)(t, time.time()));
                    output.session(&time).give_iterator(data.drain(..));
                }
            }

            blocked.retain(|_, data| data.len() > 0);
        })
    }
}


mod merge_sorter {

    use std::slice::{from_raw_parts};

    pub struct VecQueue<T> {
        list: Vec<T>,
        head: usize,
        tail: usize,
    }

    impl<T> VecQueue<T> {
        #[inline(always)]
        pub fn new() -> Self { VecQueue::from(Vec::new()) }
        #[inline(always)]
        pub fn pop(&mut self) -> T {
            debug_assert!(self.head < self.tail);
            self.head += 1;
            unsafe { ::std::ptr::read(self.list.as_mut_ptr().offset(((self.head as isize) - 1) )) }
        }
        #[inline(always)]
        pub fn peek(&self) -> &T {
            debug_assert!(self.head < self.tail);
            unsafe { self.list.get_unchecked(self.head) }
        }
        #[inline(always)]
        pub fn _peek_tail(&self) -> &T {
            debug_assert!(self.head < self.tail);
            unsafe { self.list.get_unchecked(self.tail-1) }
        }
        #[inline(always)]
        pub fn _slice(&self) -> &[T] {
            debug_assert!(self.head < self.tail);
            unsafe { from_raw_parts(self.list.get_unchecked(self.head), self.tail - self.head) }
        }
        #[inline(always)]
        pub fn from(mut list: Vec<T>) -> Self {
            let tail = list.len();
            unsafe { list.set_len(0); }
            VecQueue {
                list: list,
                head: 0,
                tail: tail,
            }
        }
        // could leak, if self.head != self.tail.
        #[inline(always)]
        pub fn done(self) -> Vec<T> {
            debug_assert!(self.head == self.tail);
            self.list
        }
        #[inline(always)]
        pub fn len(&self) -> usize { self.tail - self.head }
        #[inline(always)]
        pub fn is_empty(&self) -> bool { self.head == self.tail }
    }

    #[inline(always)]
    unsafe fn push_unchecked<T>(vec: &mut Vec<T>, element: T) {
        debug_assert!(vec.len() < vec.capacity());
        let len = vec.len();
        ::std::ptr::write(vec.get_unchecked_mut(len), element);
        vec.set_len(len + 1);
    }

    pub struct MergeSorter<D, K: Ord, F: Fn(&D)->K> {
        queue: Vec<Vec<Vec<D>>>,    // each power-of-two length list of allocations.
        stash: Vec<Vec<D>>,
        logic: F,
        phant: ::std::marker::PhantomData<K>,
    }

    impl<D, K: Ord, F: Fn(&D)->K> MergeSorter<D, K, F> {

        #[inline]
        pub fn new(logic: F) -> Self { MergeSorter { queue: Vec::new(), stash: Vec::new(), logic: logic, phant: ::std::marker::PhantomData } }

        #[inline]
        pub fn _empty(&mut self) -> Vec<D> {
            self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024))
        }

        #[inline(never)]
        pub fn _sort(&mut self, list: &mut Vec<Vec<D>>) {
            for mut batch in list.drain(..) {
                self.push(&mut batch);
            }
            self.finish_into(list);
        }

        #[inline]
        pub fn push(&mut self, batch: &mut Vec<D>) {

            let mut batch = if self.stash.len() > 2 {
                ::std::mem::replace(batch, self.stash.pop().unwrap())
            }
            else {
                ::std::mem::replace(batch, Vec::new())
            };
            
            if batch.len() > 0 {
                batch.sort_unstable_by(|x,y| (self.logic)(x).cmp(&(self.logic)(y)));
                self.queue.push(vec![batch]);
                while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                    let list1 = self.queue.pop().unwrap();
                    let list2 = self.queue.pop().unwrap();
                    let merged = self.merge_by(list1, list2);
                    self.queue.push(merged);
                }
            }
        }

        // This is awkward, because it isn't a power-of-two length any more, and we don't want 
        // to break it down to be so.
        pub fn _push_list(&mut self, list: Vec<Vec<D>>) {
            while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);            
            }
            self.queue.push(list);
        }
        
        #[inline(never)]
        pub fn finish_into(&mut self, target: &mut Vec<Vec<D>>) {
            while self.queue.len() > 1 {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }

            if let Some(mut last) = self.queue.pop() {
                ::std::mem::swap(&mut last, target);
            }
        }

        // merges two sorted input lists into one sorted output list.
        #[inline(never)]
        fn merge_by(&mut self, list1: Vec<Vec<D>>, list2: Vec<Vec<D>>) -> Vec<Vec<D>> {
            
            // use std::cmp::Ordering;

            // TODO: `list1` and `list2` get dropped; would be better to reuse?
            let mut output = Vec::with_capacity(list1.len() + list2.len());
            let mut result = Vec::with_capacity(1024);

            let mut list1 = VecQueue::from(list1);
            let mut list2 = VecQueue::from(list2);

            let mut head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() }; 
            let mut head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() }; 

            // while we have valid data in each input, merge.
            while !head1.is_empty() && !head2.is_empty() {

                while (result.capacity() - result.len()) > 0 && head1.len() > 0 && head2.len() > 0 {
                    
                    // let cmp = {
                    //     let x = head1.peek();
                    //     let y = head2.peek();
                    //     x.cmp(&y) 
                    // };
                    if (self.logic)(head1.peek()) < (self.logic)(head2.peek()) {
                        unsafe { push_unchecked(&mut result, head1.pop()); }
                    }
                    else {
                        unsafe { push_unchecked(&mut result, head2.pop()); }
                    }
                    // match cmp {
                    //     Ordering::Less    => { unsafe { push_unchecked(&mut result, head1.pop()); } }
                    //     Ordering::Greater => { unsafe { push_unchecked(&mut result, head2.pop()); } }
                    //     Ordering::Equal   => {
                    //         let (data1, diff1) = head1.pop();
                    //         let (_data2, diff2) = head2.pop();
                    //         let diff = diff1 + diff2;
                    //         if diff != 0 {
                    //             unsafe { push_unchecked(&mut result, (data1, diff)); }
                    //         }
                    //     }           
                    // }
                }
                
                if result.capacity() == result.len() {
                    output.push(result);
                    result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024)); 
                }

                if head1.is_empty() { 
                    let done1 = head1.done(); 
                    if done1.capacity() == 1024 { self.stash.push(done1); }
                    head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() }; 
                }
                if head2.is_empty() { 
                    let done2 = head2.done(); 
                    if done2.capacity() == 1024 { self.stash.push(done2); }
                    head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() }; 
                }
            }

            if result.len() > 0 { output.push(result); }
            else if result.capacity() > 0 { self.stash.push(result); }

            if !head1.is_empty() {
                let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
                for _ in 0 .. head1.len() { result.push(head1.pop()); }
                output.push(result);
            }
            while !list1.is_empty() { 
                output.push(list1.pop()); 
            }

            if !head2.is_empty() {
                let mut result = self.stash.pop().unwrap_or(Vec::with_capacity(1024));
                for _ in 0 .. head2.len() { result.push(head2.pop()); }
                output.push(result);
            }
            while !list2.is_empty() { 
                output.push(list2.pop()); 
            }

            output
        }
    }
}