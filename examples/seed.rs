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

        // // load fragment of input graph into memory to avoid io while running.
        // let filename = std::env::args().nth(1).unwrap();
        // let graph = GraphMMap::new(&filename);

        // let nodes = graph.nodes();
        // let mut triangles = Vec::new();

        // for node in 0 .. graph.nodes() {
        //     if node % peers == index {
        //         edges.push(graph.edges(node).to_vec());
        //     }
        // }

        // drop(graph);

        // // synchronize with other workers.
        // let prev = input.time().clone();
        // input.advance_to(prev.inner + 1);
        // root.step_while(|| probe.less_than(input.time()));

        // // number of nodes introduced at a time
        // let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // // start the experiment!
        // let start = ::std::time::Instant::now();
        // for node in 0 .. nodes {

        //     // introduce the node if it is this worker's responsibility
        //     if node % peers == index {
        //         for &edge in &edges[node / peers] {
        //             input.send(((node as u32, edge), 1));
        //         }
        //     }

        //     // if at a batch boundary, advance time and do work.
        //     if node % batch == (batch - 1) {
        //         let prev = input.time().clone();
        //         input.advance_to(prev.inner + 1);
        //         root.step_while(|| probe.less_than(input.time()));

        //         // merge all of the indices we maintain.
        //         forward.index.borrow_mut().merge_to(&prev);
        //         reverse.index.borrow_mut().merge_to(&prev);
        //     }
        // }

        // input.close();
        // while root.step() { }

        // if inspect { 
        //     println!("worker {} elapsed: {:?}", index, start.elapsed()); 
        // }

    }).unwrap();

    let total = if let Ok(lock) = send2.lock() {
        *lock
    }
    else { 0 };

    if inspect { 
        println!("elapsed: {:?}\ttotal triangles at this process: {:?}", start.elapsed(), total); 
    }
}
