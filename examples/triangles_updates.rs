extern crate rand;
extern crate time;
extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::*;

use timely::dataflow::operators::*;

use graph_map::GraphMMap;

#[allow(non_snake_case)]
fn main () {

    let start = time::precise_time_s();

    let send = Arc::new(Mutex::new(0));
    let send2 = send.clone();

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.clone();

        // used to partition graph loading
        let index = root.index();
        let peers = root.peers();

        // handles to input and probe, but also both indices so we can compact them.
        let (mut inputG, mut inputQ, probe, forward, reverse) = root.dataflow::<u32,_,_>(|builder| {

            // A dynamic graph is a stream of updates: `((src, dst), wgt)`.
            // Each triple indicates a change to the count of the number of arcs from
            // `src` to `dst`. Typically this change would be +/-1, but whatever.
            let (graph, dG) = builder.new_input::<(u32, u32)>();
            let (query, dQ) = builder.new_input::<((u32, u32), i32)>();

            // Our query is K3 = A(x,y) B(x,z) C(y,z): triangles.
            //
            // The dataflow determines how to update this query with respect to changes in each
            // of the input relations: A, B, and C. Each partial derivative will use the other
            // relations, but the order in which attributes are added may (will) be different.
            //
            // The updates also use the other relations with slightly stale data: updates to each
            // relation must not see updates for "later" relations (under some order on relations).

            // we will index the data both by src and dst.
            let (forward, f_handle) = dQ.index_from(&dG);
            let (reverse, r_handle) = { 
                let dGr = dG.map(|(src,dst)| (dst,src));
                let dQr = dQ.map(|((src,dst),wgt)| ((dst,src),wgt));
                dQr.index_from(&dGr)
            };

            // dA(x,y) extends to z first through C(x,z) then B(y,z), both using forward indices.
            let dK3dA = dQ//.filter(|_| false)
                          .extend(vec![Box::new(forward.extend_using(|&(ref x,_)| x, |&k| k as u64, |t1, t2| t1.lt(t2))),
                                       Box::new(forward.extend_using(|&(_,ref y)| y, |&k| k as u64, |t1, t2| t1.lt(t2)))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((p.0,p.1,e), w)));

            // dB(x,z) extends to y first through A(x,y) then C(y,z), using forward and reverse indices, respectively.
            let dK3dB = dQ//.filter(|_| false)
                          .extend(vec![Box::new(forward.extend_using(|&(ref x,_)| x, |&k| k as u64, |t1, t2| t1.le(t2))),
                                       Box::new(reverse.extend_using(|&(_,ref z)| z, |&k| k as u64, |t1, t2| t1.lt(t2)))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((p.0,e,p.1), w)));

            // dC(y,z) extends to x first through A(x,y) then B(x,z), both using reverse indices.
            let dK3dC = dQ.extend(vec![Box::new(reverse.extend_using(|&(ref y,_)| y, |&k| k as u64, |t1, t2| t1.le(t2))),
                                       Box::new(reverse.extend_using(|&(_,ref z)| z, |&k| k as u64, |t1, t2| t1.le(t2)))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((e,p.0,p.1), w)));

            // accumulate all changes together
            let cliques = dK3dC.concat(&dK3dB).concat(&dK3dA);

            // if the third argument is "inspect", report triangle counts.
            if inspect {
                cliques.exchange(|x| (x.0).0 as u64)
                       // .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                       .count()
                       // .inspect_batch(move |t,x| println!("{:?}: {:?}", t, x))
                       .inspect_batch(move |_,x| { 
                            if let Ok(mut bound) = send.lock() {
                                *bound += x[0];
                            }
                        });
            }
            (graph, query, cliques.probe(), f_handle, r_handle)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
        let graph = GraphMMap::new(&filename);

        let nodes = graph.nodes();
        let mut edges = Vec::new();

        for node in 0 .. graph.nodes() {
            if node % peers == index {
                edges.push(graph.edges(node).to_vec());
            }
        }

        drop(graph);

        // synchronize with other workers.
        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = ::std::time::Instant::now();
	    let limit = (95 * nodes /100) as usize ;

        for node in 0 .. limit {
            if node % peers == index {
                for &edge in &edges[node / peers] {
                    inputG.send((node as u32, edge));
                }
            }
        }

        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));

        if inspect { 
            println!("{:?}\t[worker {}]\tdata loaded", start.elapsed(), index);
        }

        // merge all of the indices we maintain.
        let prevG = inputG.time().clone();
        forward.borrow_mut().merge_to(&prevG);
        reverse.borrow_mut().merge_to(&prevG);

        if inspect { 
            println!("{:?}\t[worker {}]\tindices merged", start.elapsed(), index);
        }

        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));

        for node in limit .. nodes {

            if node % peers == index {
    		    for &edge in &edges[node / peers] {
                   inputQ.send(((node as u32, edge), 1));
    		    }
            }

    		// advance the graph stream (only useful in the first time)
    		let prevG = inputG.time().clone();
            inputG.advance_to(prevG.inner + 1);

            if node % batch == (batch - 1) {
                let prev = inputQ.time().clone();
                inputQ.advance_to(prev.inner + 1);
                root.step_while(|| probe.less_than(inputQ.time()));

                // merge all of the indices we maintain.
                forward.borrow_mut().merge_to(&prev);
                reverse.borrow_mut().merge_to(&prev);
            }
        }

        inputG.close();
	      inputQ.close();
        while root.step() { }

        if inspect { 
            println!("{:?}\t[worker {}]\tcomplete", start.elapsed(), index); 
        }

    }).unwrap();

    let total = if let Ok(lock) = send2.lock() {
        *lock
    }
    else { 0 };

    if inspect { 
        println!("elapsed: {:?}\ttotal triangles at this process: {:?}", time::precise_time_s() - start, total); 
    }
}
