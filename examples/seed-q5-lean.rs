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

    let send = Arc::new(Mutex::new(0usize));
    let send2 = send.clone();

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.clone();

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input, mut query, probe, forward) = root.dataflow::<u32,_,_>(|builder| {

            // A stream of changes to the set of *triangles*, where a < b < c.
            let (graph, dT) = builder.new_input::<((u32, u32, u32), i32)>();

            // A stream of changes to the set of *triangles*, where a < b < c.
            let (query, dQ) = builder.new_input::<((u32, u32, u32), ())>();

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

            let index = IndexStream::from(
                |(a,b)| (a + b) as u64,             // distribute triangles by a + b.
                &dT.map(|((a,b,c),_)| ((a,b),c)),   // initialize with (a,b) keys and c values.
                &Vec::new().to_stream(builder)      // empty update stream.
            );

            // we start with dQ = (v1, v3, v4)
            let dQ5dA = dQ
                .extend(vec![Box::new(index.extend_using(|&(v1,v3,_v4)| min_max(v1,v3), <_ as PartialOrd>::le))])
                .map(|((v1,v3,v4),v2s,w)| ((v1,v2s,v3,v4),w))
                .extend(vec![Box::new(index.extend_using(|&(v1,_,_v3,v4)| min_max(v1,v4), <_ as PartialOrd>::le))])
                .flat_map(|((v1,v2s,v3,v4),v5s,w)| 
                    v5s.into_iter()
                       .filter(move |&v5| v3 < v5)
                       .map(move|v5| ((v1,v2s.clone(),v3,v4,v5),w))
                )
                .extend(vec![Box::new(index.extend_using(|&(v1,_,_,_,v5)| min_max(v1,v5), <_ as PartialOrd>::le))])
                .map(|((v1, mut v2s, v3, v4, v5), mut v6s, w)| {
                    v2s.retain(|&v2| v2 != v4 && v2 != v5);
                    v6s.retain(|&v6| v6 != v3 && v6 != v4);
                    ((v1,v2s,v3,v4,v5,v6s),w)
                });                          

            // if the third argument is "inspect", report triangle counts.
            if inspect {
                dQ5dA.inspect_batch(move |_,x| {
                    
                    let mut sum = 0;
                    for &(ref tuple,_) in x.iter() {
                        let v2s = &tuple.1;
                        let v6s = &tuple.5;
                        for &v2 in v2s.iter() {
                            for &v6 in v6s.iter() {
                                if v2 != v6 {
                                    sum += 1;
                                }
                            }
                        }
                    }

                    if let Ok(mut bound) = send.lock() {
                        *bound += sum;
                    }
                });
            }

            (graph, query, dQ5dA.probe(), index)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
        let graph = GraphMMap::new(&filename);

        let mut triangles = Vec::new();

        let mut v1 = root.index();
        while v1 < graph.nodes() {
            let v1f = graph.edges(v1);
            for &v2 in v1f.iter() {
                intersect_and(v1f, graph.edges(v2 as usize), |v3| triangles.push((v1 as u32, v2, v3)));
            }
            v1 += root.peers();
        }

        drop(graph);

        println!("{:?}\tworker {} computed {} triangles", start.elapsed(), root.index(), triangles.len());

        for &(a,b,c) in triangles.iter() {
            // send each extension
            input.send(((a,b,c), 1));
            input.send(((a,c,b), 1));
            input.send(((b,c,a), 1));
        }

        // synchronize with other workers.
        let prev = query.time().clone();
        input.close();
        query.advance_to(prev.inner + 1);
        while probe.less_than(query.time()) { root.step(); }
        forward.index.borrow_mut().merge_to(&prev);

        println!("{:?}\tworker {} loaded index", start.elapsed(), root.index());

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        let mut node = 0; 
        let mut sent = 0;

        while sent < triangles.len() {
            node += batch as u32;
            while sent < triangles.len() && triangles[sent].0 < node {
                let (a,b,c) = triangles[sent];
                query.send(((a,b,c), ()));
                query.send(((a,c,b), ()));
                query.send(((b,a,c), ()));
                query.send(((b,c,a), ()));
                query.send(((c,a,b), ()));
                query.send(((c,b,a), ()));
                sent += 1;
            }

            // advance input and synchronize.
            let prev = query.time().clone();
            query.advance_to(prev.inner + 1);
            while probe.less_than(query.time()) { root.step(); }

            // merge all of the indices we maintain.
            forward.index.borrow_mut().merge_to(&prev);
        }

        query.close();
        while root.step() { }

        if inspect { 
            println!("{:?}\tworker {} complete", start.elapsed(), root.index()); 
        }

    }).unwrap();

    let total = if let Ok(lock) = send2.lock() {
        *lock
    }
    else { 0 };

    if inspect { 
        println!("elapsed: {:?}\ttotal instances at this process: {:?}", start.elapsed(), total); 
    }
}

fn min_max<T: Ord>(a: T, b: T) -> (T, T) {
    if a < b { (a, b) } else { (b, a) }
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

// #[inline(always)]
// pub fn gallop_gt<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
//     // if empty slice, or already > element, return
//     if slice.len() > 0 && &slice[0] <= value {
//         let mut step = 1;
//         while step < slice.len() && &slice[step] <= value {
//             slice = &slice[step..];
//             step = step << 1;
//         }

//         step = step >> 1;
//         while step > 0 {
//             if step < slice.len() && &slice[step] <= value {
//                 slice = &slice[step..];
//             }
//             step = step >> 1;
//         }

//         slice = &slice[1..]; // advance one, as we always stayed <= value
//     }

//     return slice;
// }