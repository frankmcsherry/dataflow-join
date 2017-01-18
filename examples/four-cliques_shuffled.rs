extern crate rand;
extern crate time;
extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};
use std::io::BufReader;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use alg3_dynamic::*;

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
        let (mut input_graph, mut input_query, probe, forward, reverse) = root.scoped::<u32,_,_>(|builder| {

            // Please see triangles for more information on `graph_input` and `graph`.
            let (graph_input, graph) = builder.new_input::<((u32, u32), i32)>();
            let (query_input, query) = builder.new_input::<((u32, u32), i32)>();
            
            // index the graph relation by first and second fields.
            let (forward, forward_handle) = graph.concat(&query).index();
            let (reverse, reverse_handle) = graph.concat(&query).map(|((src,dst),wgt)| ((dst,src),wgt)).index();

            // construct the four_cliques dataflow subgraph.
            let cliques = cliques_4(&query, &forward, &reverse);

            // if "inspect", report 4-clique counts.
            if inspect {
                cliques
                    .count()
                    .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .inspect_batch(move |_,x| { 
                        if let Ok(mut bound) = send.lock() {
                            *bound += x[0];
                        }
                    });
            }

            (graph_input, query_input, cliques.probe().0, forward_handle, reverse_handle)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
        let pre_load = std::env::args().nth(2).unwrap().parse().unwrap();
        let load_batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
        let query_batch: usize = std::env::args().nth(4).unwrap().parse().unwrap();

        // let limit = (percent * graph_size / max) as usize ;
        // if index==0 {
        //     println!("worker {}: graph edges {} , query edges {}",index, graph_size, graph_size-limit);
        // }

        // start the experiment!
        let start = ::std::time::Instant::now();

        // Open the path in read-only mode, returns `io::Result<File>`
        let mut lines = match File::open(&Path::new(&filename)) {
            Ok(file) => BufReader::new(file).lines(),
            Err(why) => {
                panic!("EXCEPTION: couldn't open {}: {}",
                       Path::new(&filename).display(),
                       Error::description(&why))
            },
        };

        // load up the graph, using the first `limit` lines in the file.
        for (counter, line) in lines.by_ref().take(pre_load).enumerate() {

            // each worker is responsible for a fraction of the queries
            if counter % peers == index {
                let good_line = line.ok().expect("EXCEPTION: read error");
                if !good_line.starts_with('#') && good_line.len() > 0 {
                    let mut elements = good_line[..].split_whitespace();
                    let src: u32 = elements.next().unwrap().parse().ok().expect("malformed src");
                    let dst: u32 = elements.next().unwrap().parse().ok().expect("malformed dst");
                    input_graph.send(((src, dst), 1));
                }
            }

            // synchronize and merge indices, to keep buffers in check.
            if counter % load_batch == (load_batch - 1) {
               let prev_time = input_graph.time().clone();
               input_graph.advance_to(prev_time.inner + 1);
               input_query.advance_to(prev_time.inner + 1);
               root.step_while(|| probe.lt(input_query.time()));
               forward.borrow_mut().merge_to(&prev_time);
               reverse.borrow_mut().merge_to(&prev_time);                
            }
        }

        // synchronize with other workers before reporting data loaded.
        let prev_time = input_graph.time().clone();
        input_graph.advance_to(prev_time.inner + 1);
        input_query.advance_to(prev_time.inner + 1);
        root.step_while(|| probe.lt(input_graph.time()));
        println!("{:?}\t[worker {}]\tdata loaded", start.elapsed(), index);

        // merge all of the indices the worker maintains.
        let prev_time = input_graph.time().clone();
        forward.borrow_mut().merge_to(&prev_time);
        reverse.borrow_mut().merge_to(&prev_time);

        // synchronize with other workers before reporting indices merged.
        let prev_time = input_graph.time().clone();
        input_graph.advance_to(prev_time.inner + 1);
        input_query.advance_to(prev_time.inner + 1);
        root.step_while(|| probe.lt(input_graph.time()));
        println!("{:?}\t[worker {}]\tindices merged", start.elapsed(), index);

        // issue queries and updates, using the remaining lines in the file.
        for (query_counter, line) in lines.enumerate() {

            // each worker is responsible for a fraction of the queries
            if query_counter % peers == index {
                let good_line = line.ok().expect("EXCEPTION: read error");
                if !good_line.starts_with('#') && good_line.len() > 0 {
                    let mut elements = good_line[..].split_whitespace();
                    let src: u32 = elements.next().unwrap().parse().ok().expect("malformed src");
                    let dst: u32 = elements.next().unwrap().parse().ok().expect("malformed dst");
                    input_query.send(((src, dst), 1));
                }
            }

            // synchronize and merge indices.
            if query_counter % query_batch == (query_batch - 1) {
                let prev_time = input_graph.time().clone();
                input_graph.advance_to(prev_time.inner + 1);
                input_query.advance_to(prev_time.inner + 1);
                root.step_while(|| probe.lt(input_query.time()));
                forward.borrow_mut().merge_to(&prev_time);
                reverse.borrow_mut().merge_to(&prev_time);
            }
        }

        input_graph.close();
        input_query.close();

        while root.step() { }

        if inspect { 
            println!("worker {} elapsed: {:?}", index, start.elapsed()); 
        }

    }).unwrap();

    let total = if let Ok(lock) = send2.lock() {
        *lock
    }
    else { 1 };

    if inspect { 
        println!("elapsed: {:?}\ttotal 4-cliques at this process: {:?}", time::precise_time_s() - start, total); 
    }
}

// constructs a stream of four clique changes 
#[allow(non_snake_case)]
fn cliques_4<G: Scope>(
    queries: &Stream<G, ((u32, u32), i32)>, 
    forward: &IndexStream<G>,
    reverse: &IndexStream<G>) -> Stream<G, ((u32, u32, u32, u32), i32)> 

    where G::Timestamp : Ord+::std::hash::Hash {

    // Our query is K4 = Q(a1,a2,a3,a4) = A(a1,a2) B(a1, a3) C(a1,a4) D(a2,a3) E(a2,a4) F(a3,a4)
    //
    // The dataflow has 6 derivatives, with respect to each relation:
    // dQdA := dA x B x C x D x E x F
    // dQdB := dB x A x C x D x E x F
    // dQdC := dC x A x B x D x E x F
    // dQdD := dD x A x B x C x E x F
    // dQdE := dE x A x B x C x D x F
    // dQdF := dF x A x B x C x D x E

    let dQ = queries.clone();

    let dK4dA1 = dQ.extend(vec![Box::new(forward.extend_using(|&( a1,_a2)| a1 as u64, |t1, t2| t1.lt(t2))),
                                Box::new(forward.extend_using(|&(_a1, a2)| a2 as u64, |t1, t2| t1.lt(t2)))])
                  .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,e), wght)));
    let dK4dA =  dK4dA1.extend(vec![Box::new(forward.extend_using(|&( a1,_a2,_a3)| a1 as u64, |t1, t2| t1.lt(t2))),
                                    Box::new(forward.extend_using(|&(_a1, a2,_a3)| a2 as u64, |t1, t2| t1.lt(t2))),
                                    Box::new(forward.extend_using(|&(_a1,_a2, a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
                  .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,p.2,e), wght)));

    // dQdB(a1,a3): Similar to above first extend (a1, a3) to (a1, a2, a3). Then to (a1, a2, a3, a4).
    let dK4dB1 = dQ.extend(vec![Box::new(forward.extend_using(|&( a1,_a3)| a1 as u64, |t1, t2| t1.le(t2))),
                                Box::new(reverse.extend_using(|&(_a1, a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
    let dK4dB =  dK4dB1.extend(vec![Box::new(forward.extend_using(|&( a1,_a2,_a3)| a1 as u64, |t1, t2| t1.lt(t2))),
                                    Box::new(forward.extend_using(|&(_a1, a2,_a3)| a2 as u64, |t1, t2| t1.lt(t2))),
                                    Box::new(forward.extend_using(|&(_a1,_a2, a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,p.2,e), wght)));
    
    // dQdC(a1,a4): Similar to above first extend (a1, a4) to (a1, a2, a4). Then to (a1, a2, a3, a4).
    let dK4dC1 = dQ.extend(vec![Box::new(forward.extend_using(|&( a1,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                Box::new(reverse.extend_using(|&(_a1, a4)| a4 as u64, |t1, t2| t1.lt(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
    let dK4dC =  dK4dC1.extend(vec![Box::new(forward.extend_using(|&( a1,_a2,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                    Box::new(forward.extend_using(|&(_a1, a2,_a4)| a2 as u64, |t1, t2| t1.lt(t2))),
                                    Box::new(reverse.extend_using(|&(_a1,_a2, a4)| a4 as u64, |t1, t2| t1.lt(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,e, p.2), wght)));
    
    // dQdD(a2,a3): Similar to above first extend (a2, a3) to (a1, a2, a3). Then to (a1, a2, a3, a4).
    let dK4dD1 = dQ.extend(vec![Box::new(reverse.extend_using(|&( a2,_a3)| a2 as u64, |t1, t2| t1.le(t2))),
                                Box::new(reverse.extend_using(|&(_a2, a3)| a3 as u64, |t1, t2| t1.le(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
    let dK4dD =  dK4dD1.extend(vec![Box::new(forward.extend_using(|&( a1,_a2,_a3)| a1 as u64, |t1, t2| t1.le(t2))),
                                    Box::new(forward.extend_using(|&(_a1, a2,_a3)| a2 as u64, |t1, t2| t1.lt(t2))),
                                    Box::new(forward.extend_using(|&(_a1,_a2, a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,p.2,e), wght)));
    
    // dQdE(a2,a4): Similar to above first extend (a2, a4) to (a1, a2, a4). Then to (a1, a2, a3, a4).
    let dK4dE1 = dQ.extend(vec![Box::new(reverse.extend_using(|&( a2,_a4)| a2 as u64, |t1, t2| t1.le(t2))),
                                Box::new(reverse.extend_using(|&(_a2, a4)| a4 as u64, |t1, t2| t1.le(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
    let dK4dE =  dK4dE1.extend(vec![Box::new(forward.extend_using(|&( a1,_a2,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                    Box::new(forward.extend_using(|&(_a1, a2,_a4)| a2 as u64, |t1, t2| t1.le(t2))),
                                    Box::new(reverse.extend_using(|&(_a1,_a2, a4)| a4 as u64, |t1, t2| t1.lt(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1, e, p.2), wght)));
    
    // dQdF(a3,a4): Similar to above first extend (a3, a4) to (a1, a3, a4). Then to (a1, a2, a3, a4).
    let dK4dF1 = dQ.extend(vec![Box::new(reverse.extend_using(|&( a3,_a4)| a3 as u64, |t1, t2| t1.le(t2))),
                                Box::new(reverse.extend_using(|&(_a3, a4)| a4 as u64, |t1, t2| t1.le(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
    let dK4dF =  dK4dF1.extend(vec![Box::new(forward.extend_using(|&( a1,_a3,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                    Box::new(reverse.extend_using(|&(_a1, a3,_a4)| a3 as u64, |t1, t2| t1.le(t2))),
                                    Box::new(reverse.extend_using(|&(_a1,_a3, a4)| a4 as u64, |t1, t2| t1.le(t2)))])
        .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1, p.2), wght)));
    

    // accumulate all changes together into a single dataflow.
    dK4dF.concat(&dK4dE).concat(&dK4dD).concat(&dK4dC).concat(&dK4dB).concat(&dK4dA)
}