use advance;

use std::hash::Hash;
use std::collections::HashMap;

/// A multiversion multimap from `Key` to `Val`.
///
/// An `Index` represents a multiversion `(Key, Val)` relation keyed on the first field. 
/// It presently assumes that the keys are dense, and so uses a `Vec<State>` to maintain 
/// per-key state. This could be generalized (and may need to be) to index structures
/// such as e.g. `HashMap`.
pub struct Index<Key: Ord+Hash, T> {
    /// Optionally, a pair of (key, end) and (val) lists, representing compacted accumulation.
    compact: (Vec<(Key, usize)>, Vec<u32>),
    /// An index of committed but un-compacted updates.
    edges: HashMap<Key, EdgeList>,
    /// A sorted list of un-committed updates.
    diffs: Vec<(Key, u32, T, i32)>,
}

/// A list of somewhat organized edges from a vertex.
///
/// The `edge_list` member is a sequence of sorted runs of the form
/// 
/// [(e1,w1), (e2,w2), ... (len, 0)]^*
///
/// where `len` is the number of edge entries. There may be multiple runs, which can be
/// found by starting from the last entry and stepping forward guided by `len` entries.
struct EdgeList {
    pub edge_list: Vec<(u32, i32)>,
    pub effort: u32,
    pub count: i32,     // accumulated diffs; could be negative
}

impl EdgeList {

    /// Allocates a new empty `EdgeList`.
    pub fn new() -> EdgeList { 
        EdgeList { 
            edge_list: vec![], 
            effort: 0,
            count: 0,
        } 
    }

    /// Consolidates all edges from position `index` onward.
    ///
    /// This method reduces the complexity of the edge list, in the most significant case
    /// when called with `index` equal to zero, in which case the entire edge list is 
    /// consolidated into one run.
    pub fn consolidate_from(&mut self, index: usize) {
        if self.messy() {
            self.edge_list[index..].sort();

            let mut cursor = index;
            for i in (index+1) .. self.edge_list.len() {
                if self.edge_list[i].0 == self.edge_list[cursor].0 {
                    self.edge_list[cursor].1 += self.edge_list[i].1;
                }
                else {
                    if self.edge_list[cursor].1 != 0 {
                        cursor += 1;
                    }
                    self.edge_list[cursor] = self.edge_list[i];
                }
            }
            if self.edge_list[cursor].1 != 0 {
                cursor += 1;
            }

            self.edge_list.truncate(cursor);
            if cursor - index > 0 {
                self.edge_list.push(((cursor - index) as u32, 0));
            }
        }
    }

    /// Indicates whether there is more than one run of edges.
    pub fn messy(&self) -> bool {
        if self.edge_list.len() > 0 {
            let last = self.edge_list.len() - 1;
            self.edge_list.len() > 1 && self.edge_list[last] != (last as u32, 0)
        }
        else {
            false
        }
    }

    /// Indicate that a certain amount of effort will be expended.
    ///
    /// This gives the `EdgeList` a chance to simplify its representation in response to work
    /// that is about to be done. If a great deal of work will be done, it may make sense to
    /// consolidate the edge list to simplify that work.
    #[inline(never)]
    pub fn expend(&mut self, effort: u32) {
        if self.messy() {
            self.effort += effort;
            if self.effort > self.edge_list.len() as u32 {
                self.consolidate_from(0);
            }
            self.effort = 0;
        }
    }

    /// Populates `temp` with accumulated counts for corresponding elements in `values`.
    ///
    /// This method is used to assist with intersection testing, by reporting accumulated
    /// counts for each element of the supplied `values`.
    #[inline(never)]
    pub fn intersect(&self, values: &[u32], temp: &mut Vec<i32>) {
        
        // init counts.
        temp.clear();
        for _ in 0 .. values.len() { 
            temp.push(0); 
        }
        
        let mut slice = &self.edge_list[..];
        while slice.len() > 0 {
            let len = slice.len();
            let run = slice[len-1].0;

            // want to intersect `edges` and `slice`.
            let edges = &slice[(len - (run as usize + 1)).. (len-1)];

            let mut e_cursor = 0;
            let mut v_cursor = 0;

            // merge by galloping
            while edges.len() > e_cursor && values.len() > v_cursor {
                match edges[e_cursor].0.cmp(&values[v_cursor]) {
                    ::std::cmp::Ordering::Less => {
                        let step = advance(&edges[e_cursor..], |x| x.0 < values[v_cursor]);
                        assert!(step > 0);
                        e_cursor += step;
                    },
                    ::std::cmp::Ordering::Equal => {
                        temp[v_cursor] += edges[e_cursor].1;
                        e_cursor += 1;
                        v_cursor += 1;
                    },
                    ::std::cmp::Ordering::Greater => {
                        let step = advance(&values[v_cursor..], |&x| x < edges[e_cursor].0);
                        assert!(step > 0);
                        v_cursor += step;
                    },
                }
            } 

            // trim off the run and the footer.
            slice = &slice[..(len - (run as usize + 1))];
        }
    }
}

impl<Key: Ord+Hash+Copy, T: Ord+Clone> Index<Key, T> {

    /// Allocates a new empty index.
    pub fn new() -> Index<Key, T> { 
        Index { 
            compact: (Vec::new(), Vec::new()), 
            edges: HashMap::new(), 
            diffs: Vec::new(), 
        } 
    }

    /// Updates entries of `data` to reflect counts in the index.
    #[inline(never)]
    pub fn count<P,K,V>(&mut self, data: &mut Vec<(P, u64, u64, i32)>, func: &K, _valid: &V, ident: u64) 
    where K:Fn(&P)->Key, V:Fn(&T)->bool {

        // sort data by key, to share work for the same key.
        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        // cursors into `self.compact` and `self.diffs`.
        let mut c_cursor = 0;
        let mut d_cursor = 0;

        let mut index = 0;
        while index < data.len() {

            let key = func(&data[index].0);
            let mut count = 0u64;

            // (ia) update `count` by values in `self.compact`.
            c_cursor += advance(&self.compact.0[c_cursor ..], |x| x.0 < key);
            if c_cursor < self.compact.0.len() && self.compact.0[c_cursor].0 == key {
                let lower = if c_cursor == 0 { 0 } else { self.compact.0[c_cursor - 1].1 };
                let upper = self.compact.0[c_cursor].1;
                count += (upper - lower) as u64;
                c_cursor += 1; // <-- can advance, as we service all instances of `key`. 
            }

            // (ib) update `count` by values in `self.edges`.
            if let Some(entry) = self.edges.get(&key) {
                count += entry.count as u64;
            }

            // (ic) update `count` by values in `self.diffs`.
            // This is an over-estimate because we do not consult times with `_valid`, but 
            // it is important that we stay "constant-time", which is hard if we look at each
            // update individually.
            d_cursor += advance(&self.diffs[d_cursor ..], |x| x.0 < key);
            let d_step = advance(&self.diffs[d_cursor ..], |x| x.0 <= key);
            d_cursor += d_step;
            count += d_step as u64;

            // (ii) we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) == key {

                // if the count improves, retain the count and the identifier of the index.
                if count < data[index].1 {
                    data[index].1 = count;
                    data[index].2 = ident;
                }

                index += 1;
            }
        }
    }

    /// Proposes extensions for prefixes based on the index.
    #[inline(never)]
    pub fn propose<P, K, V>(&mut self, data: &mut Vec<(P, Vec<u32>, i32)>, func: &K, valid: &V) 
    where K:Fn(&P)->Key, V:Fn(&T)->bool {

        // sorting allows us to re-use computation for the same key, and simplifies the searching 
        // of self.compact and self.diffs.
        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        // fingers into compacted data and uncommited updates.
        let mut offset_cursor = 0;
        let mut diffs = &self.diffs[..];

        // temporary array to stage proposals
        let mut proposals = Vec::new();

        // current position in `data`.
        let mut index = 0;  
        while index < data.len() {

            // for each key, we (i) determine the proposals and then (ii) supply them to each 
            // entry of `data` with the same key.

            let key = func(&data[index].0);
            proposals.clear();

            // println!("a");
            // (ia): incorporate updates from `self.compact`.
            let steps = advance(&self.compact.0[offset_cursor..], |x| x.0 < key);
            offset_cursor += steps;
            if offset_cursor < self.compact.0.len() && self.compact.0[offset_cursor].0 == key {
                let lower = if offset_cursor == 0 { 0 } else { self.compact.0[offset_cursor-1].1 };
                let upper = self.compact.0[offset_cursor].1;
                for &val in &self.compact.1[lower .. upper] {
                    proposals.push((val, 1));
                }
            }
        

            // println!("b");
            // (ib): incorporate updates from `self.edges`.
            if let Some(entry) = self.edges.get_mut(&key) {
                entry.consolidate_from(0);
                if entry.edge_list.len() > 0 {
                    for &(val, cnt) in &entry.edge_list[..entry.edge_list.len()-1] {
                        proposals.push((val, cnt));
                    }
                }
            }

            // println!("c");
            // (ic): incorporate updates from `self.diffs`.
            let lower = advance(diffs, |x| x.0 < key);
            diffs = &diffs[lower..];
            let upper = advance(diffs, |x| x.0 <= key);
            for &(_key, val, ref time, wgt) in &diffs[..upper] {
                if valid(time) {
                    proposals.push((val, wgt));
                }
            }

            // println!("d");
            // (id): consolidate all the counts that we added in, keep positive counts.
            if proposals.len() > 0 {
                proposals.sort();
                for cursor in 0 .. proposals.len() - 1 {
                    if proposals[cursor].0 == proposals[cursor + 1].0 {
                        proposals[cursor + 1].1 += proposals[cursor].1;
                        proposals[cursor].1 = 0;
                    }
                }
                proposals.retain(|x| x.1 > 0);
            }

            // println!("i");
            // (ii): we may have multiple records with the same key, propose for them all.
            while index < data.len() && func(&data[index].0) == key {
                for &(val, cnt) in &proposals {
                    for _ in 0 .. cnt {
                        data[index].1.push(val);
                    }
                }
                index += 1;
            }
        }
    }

    /// Restricts extensions for prefixes to those found in the index.
    #[inline(never)]
    pub fn intersect<P, F, V>(&mut self, data: &mut Vec<(P, Vec<u32>, i32)>, func: &F, valid: &V) 
    where F: Fn(&P)->Key, V: Fn(&T)->bool {

        // sorting data by key allows us to re-use some work / compact representations.
        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        // counts for each value to validate
        let mut temp = Vec::new();

        // fingers into compacted data and uncommited updates.
        let mut offset_cursor = 0;
        let mut diffs = &self.diffs[..];

        let mut index = 0;
        while index < data.len() {

            // println!("start");

            let key = func(&data[index].0);

            // consider the amount of effort we are about to invest:
            let mut effort = 0;
            let mut temp_index = index;
            while temp_index < data.len() && func(&data[temp_index].0)  == key {
                effort += data[temp_index].1.len();
                temp_index += 1;
            }

            // println!("i");
            // (i) position `self.compact` cursor so that we can re-use it.
            let steps = advance(&self.compact.0[offset_cursor..], |x| x.0 < key);
            offset_cursor += steps;
            let compact_slice = if offset_cursor < self.compact.0.len() && self.compact.0[offset_cursor].0 == key {
                let lower = if offset_cursor == 0 { 0 } else { self.compact.0[offset_cursor-1].1 };
                let upper = self.compact.0[offset_cursor].1;
                &self.compact.1[lower .. upper]
            }
            else { &[] };

            // println!("ii");
            // (ii) prepare non-compact updates. if our effort level is large, consolidate. 
            if let Some(e) = self.edges.get_mut(&key) {
                e.expend(effort as u32);
            }

            // println!("iii");
            // (iii) position `self.diffs` cursor so that we can re-use it.
            let d_lower = advance(diffs, |x| x.0 < key);
            diffs = &diffs[d_lower..];
            let d_upper = advance(diffs, |x| x.0 <= key);
            let diffs_slice = &diffs[..d_upper];
            diffs = &diffs[d_upper..];

            let entry = self.edges.get(&key);

            // println!("while");
            // we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) == key {

                // set `temp` to be a vector of initially zero counts.
                temp.clear(); 
                temp.extend(data[index].1.iter().map(|_| 0));

                // println!("  ia");
                // (ia) update `temp` counts based on `self.edges[key]`.
                if let Some(entry) = entry {
                    entry.intersect(&data[index].1, &mut temp);
                }

                // println!("  ib");
                // (ib) update `temp` counts based on `self.compact` and `self.diffs`.
                let mut c_cursor = 0;
                let mut d_cursor = 0;

                // walk proposals linearly (could gallop, if we felt strongly enough).
                for (index2, proposal) in data[index].1.iter().enumerate() {

                    // move c_cursor to where `proposal` would start ..
                    c_cursor += advance(&compact_slice[c_cursor..], |x| x < proposal);
                    while c_cursor < compact_slice.len() && &compact_slice[c_cursor] == proposal {
                        temp[index2] += 1;
                        c_cursor += 1;
                    }

                    // move d_cursor to where `proposal` would start ..
                    d_cursor += advance(&diffs_slice[d_cursor..], |x| &x.1 < proposal);
                    while d_cursor < diffs_slice.len() && &diffs_slice[d_cursor].1 == proposal {
                        if valid(&diffs_slice[d_cursor].2) {
                            temp[index2] += diffs_slice[d_cursor].3;
                        }
                        d_cursor += 1;
                    }
                }

                // println!("  ii");
                // (ii) remove elements whose count is not strictly positive.
                let mut cursor = 0;
                for i in 0 .. temp.len() {
                    if temp[i] > 0 {
                        data[index].1[cursor] = data[index].1[i];
                        cursor += 1;
                    }
                }
                data[index].1.truncate(cursor);

                index += 1;
            }

            // println!("end");
        }
    }

    /// Commits updates up to and including `time`.
    ///
    /// This merges any differences with time less or equal to `time`, and should probably only be called
    /// once the user is certain to never require such a distinction again. These differences are not yet 
    /// compacted, they've just had their times stripped off.
    ///
    /// This operation is important to ensure that `self.diffs` doesn't grow too large, as our strategy
    /// for keeping it sorted is to re-sort it whenever we add data. If it grew without bound this would
    /// be pretty horrible. In principe, this operation also allows us to consolidate the representation, 
    /// if we have updates which update the same value (potentially cancelling).
    #[inline(never)]
    pub fn merge_to(&mut self, time: &T) {

        let mut index = 0;
        while index < self.diffs.len() {

            let key = self.diffs[index].0;
            let entry = self.edges.entry(key).or_insert(EdgeList::new());

            // move all timely updates into edge_list
            let len = entry.edge_list.len();
            while index < self.diffs.len() && self.diffs[index].0 == key {
                if self.diffs[index].2.le(time) {
                    entry.edge_list.push((self.diffs[index].1, self.diffs[index].3));
                    entry.count += self.diffs[index].3;
                    self.diffs[index].3 = 0;
                }
                index += 1;
            }

            // We may have appended enough entries that the lengths no longer decrease geometrically.
            // We need to walk back until the tail is at most half the size of the next region, and then
            // consolidate everything from that point backwards.
            let new_len = entry.edge_list.len();
            if new_len - len > 0 {
                entry.edge_list.push(((new_len - len) as u32, 0));
                if len > 0 {
                    // we now have from len .. now as new data.
                    let mut mess = (new_len - len) as u32;
                    while (new_len - 1) > mess as usize 
                       && mess > entry.edge_list[(new_len - 1) - mess as usize].0 / 2 {
                        mess += entry.edge_list[(new_len - 1) - mess as usize].0 + 1;
                    }

                    entry.consolidate_from(new_len - mess as usize);
                }
                else {
                    entry.consolidate_from(0);
                }
            }
        }

        // remove committed updates
        self.diffs.retain(|x| x.3 != 0);
    }

    /// Introduces a collection of updates at various times.
    /// 
    /// These updates will now be reflected in all queries against the index, at or after the 
    /// indicated logical time.
    #[inline(never)]
    pub fn update(&mut self, time: T, updates: &mut Vec<(Key, (u32, i32))>) {
        self.diffs.extend(updates.drain(..).map(|(key,(val,wgt))| (key, val, time.clone(), wgt)));
        self.diffs.sort();
    }

    /// Sets an initial collection of positive counts, which we can compact.
    #[inline(never)]
    pub fn initialize(&mut self, initial: &mut Vec<Vec<(Key, u32)>>) {

        if self.compact.0.len() > 0 || self.edges.len() > 0 || self.diffs.len() > 0 {
            panic!("re-initializing active index");
        }

        // perhaps we should use a radix sort here, to avoid extra memory allocation?
        // initial.sort();

        let length = initial.iter().map(|x| x.len()).sum();
        self.compact.1 = Vec::with_capacity(length);

        for batch in initial.drain(..) {
            for (key, val) in batch {
                self.compact.1.push(val);
                let idx = self.compact.0.len();
                if idx == 0 || key != self.compact.0[idx-1].0 {
                    self.compact.0.push((key, self.compact.1.len()));
                }
                else {
                    self.compact.0[idx-1].1 = self.compact.1.len();
                }
            }
        }

        // self.compact.1 = Vec::with_capacity(initial.len());
        // self.compact.1.push(initial[0].1);
        // for index in 1 .. initial.len() {
        //     if initial[index].0 != initial[index-1].0 {
        //         self.compact.0.push((initial[index-1].0, index));
        //     }
        //     self.compact.1.push(initial[index].1)
        // }
        // self.compact.0.push((initial[initial.len()-1].0, initial.len()));

        *initial = Vec::new();

        // println!("index initialized: ({}, {})", self.compact.0.len(), self.compact.1.len());
    }
}
