use advance;

/// A multiversion multimap from `u32` to `u32`.
///
/// An index represents a multiversion relation keyed on `u32` entries. It presently 
/// assumes that the keys are dense, and uses a `Vec<State>` to maintain per-key state.
/// This could pretty easily be generalized (and may need to be) to other index structures
/// such as e.g. `HashMap`.
pub struct Index<T> {
    edges: Vec<EdgeList>,
    diffs: Vec<(u32, u32, T, i32)>,
    peers: usize,
}

/// A list of somewhat organized edges from a vertex.
///
/// The `edge_list` member is a sequence of sorted runs of the form
/// 
///     [(e1,w1), (e2,w2), ... (len, 0)]^*
///
/// where `len` is the number of edge entries. There may be multiple runs, which can be
/// found by starting from the last entry and stepping forward guided by `len` entries.
struct EdgeList {
    pub elsewhere: u32,
    pub edge_list: Vec<(u32, i32)>,
    pub effort: u32,
}

impl EdgeList {

    /// Allocates a new empty `EdgeList`.
    pub fn new() -> EdgeList { 
        EdgeList { 
            elsewhere: 0, 
            edge_list: vec![], 
            effort: 0 
        } 
    }

    /// An upper bound on the number of edges.
    pub fn count_approx(&self) -> u64 {
        self.elsewhere as u64 + self.edge_list.len() as u64
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

impl<T: Ord+Clone+::std::fmt::Debug> Index<T> {

    /// Allocates a new empty index.
    pub fn new(peers: usize) -> Index<T> { Index { edges: vec![], diffs: vec![], peers: peers } }

    /// Updates entries of `data` to reflect counts in the index.
    #[inline(never)]
    pub fn count<P,K,V>(&mut self, data: &mut Vec<(P, u64, u64, i32)>, func: &K, _valid: &V, ident: u64) 
    where K:Fn(&P)->u64, V:Fn(&T)->bool {

        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        let mut index = 0;
        while index < data.len() {

            // determine the count 
            let key = func(&data[index].0) as usize / self.peers;
            while self.edges.len() <= key { self.edges.push(EdgeList::new()); }

            // this approximation is not great for structurally empty queries, like dA and dB.
            // it would be better if we could confirm that the count is zero for some time.
            let count = self.edges[key].count_approx();
            
            // println!("count[{}]: {}", func(&data[index].0), count);

            // we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) as usize / self.peers == key {

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
    where K:Fn(&P)->u64, V:Fn(&T)->bool {

        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        let mut diffs = &self.diffs[..];

        let mut index = 0;
        while index < data.len() {

            let key = func(&data[index].0) as usize / self.peers;
            let key2 = func(&data[index].0);
            while self.edges.len() <= key { self.edges.push(EdgeList::new()); }

            // might as well consolidate the edges.
            self.edges[key].consolidate_from(0);

            // need to merge with diffs
            let mut proposals = vec![];
            let mut edge_slice = if self.edges[key].edge_list.len() > 0 {
                &self.edges[key].edge_list[..self.edges[key].edge_list.len()-1]
            }
            else {
                &[]
            };

            let lower = advance(diffs, |x| x.0 < key2 as u32);
            diffs = &diffs[lower..];
            let upper = advance(diffs, |x| x.0 <= key2 as u32);
            let mut diff_slice = &diffs[..upper];
            diffs = &diffs[upper..];

            // println!("proposing for key: {}", func(&data[index].0));
            // println!("  edge_slice: {:?}", edge_slice);
            // println!("  diff_slice: {:?}", diff_slice);

            // merge between edges and diffs
            while edge_slice.len() > 0 && diff_slice.len() > 0 {
                let extension = ::std::cmp::min(edge_slice[0].0, diff_slice[0].1);
                let mut count = 0;
                if edge_slice[0].0 == extension {
                    count += edge_slice[0].1;
                    edge_slice = &edge_slice[1..];
                }

                // there may be multiple diffs with the same extension ...
                while diff_slice.len() > 0 && diff_slice[0].1 == extension {
                    if valid(&diff_slice[0].2) {
                        count += diff_slice[0].3;
                    }
                    diff_slice = &diff_slice[1..];
                }               
                
                if count > 0 {
                    proposals.push(extension);
                }
            }

            // drain remaining edges/diffs
            while edge_slice.len() > 0 {
                if edge_slice[0].1 > 0 { 
                    proposals.push(edge_slice[0].0); 
                }
                edge_slice = &edge_slice[1..];
            }
            while diff_slice.len() > 0 {
                let extension = diff_slice[0].1;
                let mut count = 0;
                while diff_slice.len() > 0 && diff_slice[0].1 == extension {
                    if valid(&diff_slice[0].2) { 
                        count += diff_slice[0].3;
                    }
                    diff_slice = &diff_slice[1..];
                }
                if count > 0 {
                    proposals.push(extension);
                }
            }

            // println!("proposals[{}]: {:?}", func(&data[index].0), proposals);

            // we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) as usize  / self.peers == key {
                data[index].1.extend_from_slice(&proposals);
                index += 1;
            }
        }
    }

    /// Restricts extensions for prefixs to those found in the index.
    #[inline(never)]
    pub fn intersect<P, F, V>(&mut self, data: &mut Vec<(P, Vec<u32>, i32)>, func: &F, valid: &V) 
    where F: Fn(&P)->u64, V: Fn(&T)->bool {

        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        let mut temp = Vec::new();
        let mut diffs = &self.diffs[..];

        let mut index = 0;
        while index < data.len() {

            let key = func(&data[index].0) as usize / self.peers;
            let key2 = func(&data[index].0) as usize;
            while self.edges.len() <= key { self.edges.push(EdgeList::new()); }

            // consider the amount of effort we are about to invest:
            let mut effort = 0;
            let mut temp_index = index;
            while temp_index < data.len() && func(&data[temp_index].0) as usize / self.peers  == key {
                effort += data[temp_index].1.len();
                temp_index += 1;
            }

            // if our effort level is large, consolidate.
            self.edges[key].expend(effort as u32);

            let d_lower = advance(diffs, |x| x.0 < key2 as u32);
            diffs = &diffs[d_lower..];
            let d_upper = advance(diffs, |x| x.0 <= key2 as u32);
            let edges = &diffs[..d_upper];
            diffs = &diffs[d_upper..];

            // we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) as usize / self.peers == key {

                self.edges[key].intersect(&data[index].1, &mut temp);

                // scope to let `values` drop.
                {
                    let values = &data[index].1;

                    let mut e_cursor = 0;
                    let mut v_cursor = 0;

                    // merge by galloping
                    while edges.len() > e_cursor && values.len() > v_cursor {
                        match edges[e_cursor].1.cmp(&values[v_cursor]) {
                            ::std::cmp::Ordering::Less => {
                                let step = advance(&edges[e_cursor..], |x| x.1 < values[v_cursor]);
                                assert!(step > 0);
                                e_cursor += step;
                            },
                            ::std::cmp::Ordering::Equal => {
                                if valid(&edges[e_cursor].2) {
                                    temp[v_cursor] += edges[e_cursor].3;
                                }
                                e_cursor += 1;
                                v_cursor += 1;
                            },
                            ::std::cmp::Ordering::Greater => {
                                let step = advance(&values[v_cursor..], |&x| x < edges[e_cursor].1);
                                assert!(step > 0);
                                v_cursor += step;
                            },
                        }
                    } 
                }

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
        }
    }

    /// Merges differences up to `time` into the main index.
    ///
    /// This merges any differences with time less or equal to `time`, and should probably only be called
    /// once the user is certain to never require such a distinction again.
    #[inline(never)]
    pub fn merge_to(&mut self, time: &T) {

        let mut index = 0;
        while index < self.diffs.len() {

            // ensure self.edges[key] exists.
            let key = self.diffs[index].0 as usize / self.peers;
            while self.edges.len() <= key as usize {
                self.edges.push(EdgeList::new());
            }

            // move all timely updates into edge_list
            let len = self.edges[key].edge_list.len();
            while index < self.diffs.len() && (self.diffs[index].0 as usize) / self.peers == key {
                if self.diffs[index].2.le(time) {
                    self.edges[key].elsewhere -= 1;
                    self.edges[key].edge_list.push((self.diffs[index].1, self.diffs[index].3));
                    self.diffs[index].3 = 0;
                }
                index += 1;
            }

            // worry about consolidating edge_list
            let new_len = self.edges[key].edge_list.len();
            if new_len - len > 0 {
                self.edges[key].edge_list.push(((new_len - len) as u32, 0));
                if len > 0 {
                    // we now have from len .. now as new data.
                    let mut mess = (new_len - len) as u32;
                    while (new_len - 1) > mess as usize 
                       && mess > self.edges[key].edge_list[(new_len - 1) - mess as usize].0 / 2 {
                        mess += self.edges[key].edge_list[(new_len - 1) - mess as usize].0 + 1;
                    }

                    self.edges[key].consolidate_from(new_len - mess as usize);
                }
                else {
                    self.edges[key].consolidate_from(0);
                }
            }
        }

        // remove committed updates
        self.diffs.retain(|x| x.3 != 0);
	// // we do not need to free the memory if we are going to load with batch, because this memory will be allocated again any way!
	//self.diffs.shrink_to_fit();
    }

    /// Introduces a collection of updates at various times.
    #[inline(never)]
    pub fn update(&mut self, time: T, updates: &mut Vec<(u32, (u32, i32))>) {
        for (src, (dst, wgt)) in updates.drain(..) {
            self.diffs.push((src, dst, time.clone(), wgt));
            while self.edges.len() <= src as usize / self.peers {
                self.edges.push(EdgeList::new());
            }
            self.edges[src as usize / self.peers].elsewhere += 1;
        }
        self.diffs.sort();
	// we do not need to free the memory if we are going to load with batch, because this memory will be allocated again any way!
	//updates.shrink_to_fit();
    }
}
