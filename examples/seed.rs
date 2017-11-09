extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::*;

use timely::dataflow::operators::*;

use graph_map::GraphMMap;

#[allow(non_snake_case)]
fn main () {

    let start = ::std::time::Instant::now();

    let send = Arc::new(Mutex::new(0));
    let send2 = send.clone();

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.clone();

        // used to partition graph loading
        let index = root.index();
        let peers = root.peers();

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input, probe, forward, reverse) = root.dataflow::<u32,_,_>(|builder| {

            // A stream of changes to the set of *triangles*, where a < b < c.
            let (graph, dT) = builder.new_input::<((u32, u32, u32), i32)>();

            // Our query is K4(w,x,y,z) := T(w,x,y), T(w,x,z), T(w,y,z), T(x,y,z)
            //
            // This query is technically redundant, because the middle two constraints imply the fourth,
            // so let's slim it down to
            //
            //    K4(w,x,y,z) := T(w,x,y), T(w,x,z), T(w,y,z)
            //
            // This seems like it could be a bit more complicated than triangles, in determining the rules
            // for incremental updates. I'm going to write them down first, and we'll see which indices we
            // actually need. I'll use A, B, and C for the instances of T above.
            //
            //    dK4dA(w,x,y,z) := dA(w,x,y), B(w,x,z), C(w,y,z)
            //    dK4dB(w,x,y,z) := dB(w,x,z), A(w,x,y), C(w,y,z)
            //    dK4dC(w,x,y,z) := dC(w,y,z), A(w,x,y), B(w,x,z)
            //
            // Looking at this, it seems like we will need
            //
            //    dK4dA : indices on (w,x,_) and (w,_,y)
            //    dK4dB : indices on (w,x,_) and (w,_,z)
            //    dK4dC : indices on (w,_,y) and (w,_,z)
            //
            // All of this seems to boil down to a "forward" and a "reverse" index, just as for triangles,
            // but where `w` is always present as part of the key. We just might want the first or second
            // field that follows it.

            // create two indices, one "forward" from (a,b) to c, and one "reverse" from (a,c) to b.
            let forward = IndexStream::from(|(a,b)| (a + b) as u64, &Vec::new().to_stream(builder), &dT.map(|((a,b,c),wgt)| (((a,b),c),wgt)));
            let reverse = IndexStream::from(|(a,c)| (a + c) as u64, &Vec::new().to_stream(builder), &dT.map(|((a,b,c),wgt)| (((a,c),b),wgt)));

            // dK4dA(w,x,y,z) := dA(w,x,y), B(w,x,z), C(w,y,z)
            let dK4dA = dT.extend(vec![Box::new(forward.extend_using(|&(w,x,y)| (w,x), <_ as PartialOrd>::lt)),
                                       Box::new(forward.extend_using(|&(w,x,y)| (w,y), <_ as PartialOrd>::lt))])
                          .flat_map(|((w,x,y), zs, wgt)| zs.into_iter().map(move |z| ((w,x,y,z),wgt)));

            // dK4dB(w,x,y,z) := dB(w,x,z), A(w,x,y), C(w,y,z)
            let dK4dB = dT.extend(vec![Box::new(forward.extend_using(|&(w,x,z)| (w,x), <_ as PartialOrd>::le)),
                                       Box::new(reverse.extend_using(|&(w,x,z)| (w,z), <_ as PartialOrd>::lt))])
                          .flat_map(|((w,x,z), ys, wgt)| ys.into_iter().map(move |y| ((w,x,y,z),wgt)));

            // dK4dC(w,x,y,z) := dC(w,y,z), A(w,x,y), B(w,x,z)
            let dK4dC = dT.extend(vec![Box::new(reverse.extend_using(|&(w,y,z)| (w,y), <_ as PartialOrd>::le)),
                                       Box::new(reverse.extend_using(|&(w,y,z)| (w,z), <_ as PartialOrd>::le))])
                          .flat_map(|((w,y,z), xs, wgt)| xs.into_iter().map(move |x| ((w,x,y,z),wgt)));

            let dK4 = dK4dA.concat(&dK4dB).concat(&dK4dC);

            // if the third argument is "inspect", report triangle counts.
            if inspect {
                dK4.exchange(|x| (x.0).0 as u64)
                       // .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                       .count()
                       .inspect_batch(move |t,x| println!("{:?}: {:?}", t, x))
                       .inspect_batch(move |_,x| { 
                            if let Ok(mut bound) = send.lock() {
                                *bound += x[0];
                            }
                        });
            }

            (graph, dK4.probe(), forward, reverse)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
        let graph = GraphMap::new(&filename);

        let nodes = graph.nodes();
        let mut triangles = Vec::new();

        let mut v1 = root.index() as u32;
        while v1 < graph.nodes() {
            let v1f = graph.forward(v1);
            for (index_v2, &v2) in v1f.iter().enumerate() {
                intersect_and(&v1f[(index_v2+1)..], graph.forward(v2), |v3| triangles.push((v1 as u32, v2, v3)));
            }
            v1 += root.peers() as u32;
        }

        drop(graph);

        println!("{:?}\tworker {} computed {} triangles", start.elapsed(), root.index(), triangles.len());

        // synchronize with other workers.
        let prev = input.time().clone();
        input.advance_to(prev.inner + 1);
        root.step_while(|| probe.less_than(input.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = ::std::time::Instant::now();

        let mut sent = 0;
        while sent < triangles.len() {
            let to_send = std::cmp::min(batch / root.peers(), triangles.len() - sent);
            for off in 0 .. to_send {
                input.send((triangles[sent + off], 1));
            }
            sent += to_send;

            let prev = input.time().clone();
            input.advance_to(prev.inner + 1);
            while probe.less_than(input.time()) { root.step(); }

            // merge all of the indices we maintain.
            forward.index.borrow_mut().merge_to(&prev);
            reverse.index.borrow_mut().merge_to(&prev);
        }

        input.close();
        while root.step() { }

        if inspect { 
            println!("worker {} elapsed: {:?}", index, start.elapsed()); 
        }

    }).unwrap();

    let total = if let Ok(lock) = send2.lock() {
        *lock
    }
    else { 0 };

    if inspect { 
        println!("elapsed: {:?}\ttotal triangles at this process: {:?}", start.elapsed(), total); 
    }
}


struct GraphMap {
    map: GraphMMap,
    reverse: Vec<u32>,
}


impl GraphMap {
    pub fn new(filename: &str) -> Self {

        let map = GraphMMap::new(filename);

        let mut reverse = vec![0; map.nodes()];
        for node in 0 .. map.nodes() {
            for &neighbor in map.edges(node) {
                if (neighbor as usize) < node {
                    reverse[node] += 1;
                }
                if (neighbor as usize) == node {
                    panic!("self-loop");
                }
            }
        }

        GraphMap {
            map: map,
            reverse: reverse,
        }
    }

    #[inline(always)]
    pub fn nodes(&self) -> u32 { self.map.nodes() as u32 }
    #[inline(always)]
    pub fn edges(&self, node: u32) -> &[u32] { self.map.edges(node as usize) }
    #[inline(always)]
    pub fn forward(&self, node: u32) -> &[u32] { 
        &self.edges(node)[(self.reverse[node as usize] as usize)..]
    }
}

fn intersect_and<F: FnMut(u32)>(aaa: &[u32], mut bbb: &[u32], mut func: F) {

    if aaa.len() > bbb.len() {
        intersect_and(bbb, aaa, func);
    }
    else {
        if aaa.len() < bbb.len() / 16 {
            for &a in aaa.iter() {
                bbb = gallop_ge(bbb, &a);
                if bbb.len() > 0 && bbb[0] == a {
                    func(a)
                }
            }
        }
        else {
            for &a in aaa.iter() {
                while bbb.len() > 0 && bbb[0] < a {
                    bbb = &bbb[1..];
                }
                if bbb.len() > 0 && a == bbb[0] {
                    func(a);
                }
            }
        }
    }
}

#[inline(always)]
pub fn gallop_ge<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already >= element, return
    if slice.len() > 0 && &slice[0] < value {
        let mut step = 1;
        while step < slice.len() && &slice[step] < value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] < value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < value
    }

    return slice;
}

#[inline(always)]
pub fn gallop_gt<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already > element, return
    if slice.len() > 0 && &slice[0] <= value {
        let mut step = 1;
        while step < slice.len() && &slice[step] <= value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] <= value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed <= value
    }

    return slice;
}