extern crate rand;
extern crate time;
extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::*;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::Extract;

use graph_map::GraphMMap;

#[allow(non_snake_case)]
fn main () {

    let start = time::precise_time_s();

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.lock().unwrap().clone();

        // used to partition graph loading
        let index = root.index();
        let peers = root.peers();

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input, probe, forward, reverse) = root.scoped::<u32,_,_>(|builder| {

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<((u32, u32), i32)>();

            // Our query is K4 = Q(a1,a2,a3,a4) = A(a1,a2) B(a1, a3) C(a1,a4) D(a2,a3) E(a2,a4) F(a3,a4)
            //
            // The dataflow has 6 derivatives, with respect to each relation:
            // dQdA := dA x B x C x D x E x F
            // dQdB := dB x A x C x D x E x F
            // dQdC := dC x A x B x D x E x F
            // dQdD := dD x A x B x C x E x F
            // dQdE := dE x A x B x C x D x F
            // dQdF := dF x A x B x C x D x E

            // we will index the data both by src and dst.
            let (forward, f_handle) = dG.index_from(&dG.filter(|_| false).map(|_| (0,0)));
            let (reverse, r_handle) = dG.map(|((src,dst),wgt)| ((dst,src),wgt)).index_from(&dG.filter(|_| false).map(|_| (0,0)));

            // We then pick an ordering of attributes for each derivative:
            // dQdA: we start with dA(a1, a2) and extend to a3 and then to a4. So there will be 2 extensions:
            // (1) Extending (a1, a2) prefixes to (a1, a2, a3) prefixes: a3 is included in B(a1, a3), D(a2, a3), and F(a3, a4).
            //     However we ignore F for now becase a3 is not bounded by a1 or a2 yet (the two variables bounded by
            //     dA(a1, a2)). We will use F when we extend to a4 in the 2nd extensions below.
            //     1st extension involves two ``extenders'', who will offer proposals and do intersections:
            //     (i) B(a1, a3) using the forward index because a1 is bounded and is the 1st variable in B(a1, a3).
            //     We'd pick the reverse index if a1 was the 2nd variable. We will also use a1 as exchange; and
            //     (ii) D(a2, a3) also using the forward index and now using a2 as the exchange by the same reasons.
            // (2) Extending (a1, a2, a3) to (a1, a2, a3, a4): a4 appears in C, E, and F. We will use 3 extenders.
            //     (i)  C(a1, a4): Use forward index and use a1 as exchange;
            //     (ii) E(a2, a4): Use forward index and use a2 as exchange;
            //     (ii) F(a3, a4): Use forward index and use a3 as exchange;
            let dK4dA1 = dG.extend(vec![Box::new(forward.extend_using(|&(a1,_a2)| a1 as u64, |t1, t2| t1.lt(t2))),
                                        Box::new(forward.extend_using(|&(_a1,a2)| a2 as u64, |t1, t2| t1.lt(t2)))])
                          .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,e), wght)));
            let dK4dA =  dK4dA1.extend(vec![Box::new(forward.extend_using(|&(a1,_a2,_a3)| a1 as u64, |t1, t2| t1.lt(t2))),
                                            Box::new(forward.extend_using(|&(_a1,a2,_a3)| a2 as u64, |t1, t2| t1.lt(t2))),
                                            Box::new(forward.extend_using(|&(_a1,_a2,a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
                          .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,p.2,e), wght)));

            // dQdB(a1,a3): Similar to above first extend (a1, a3) to (a1, a2, a3). Then to (a1, a2, a3, a4).
            let dK4dB1 = dG.extend(vec![Box::new(forward.extend_using(|&(a1,_a3)| a1 as u64, |t1, t2| t1.le(t2))),
                                        Box::new(reverse.extend_using(|&(_a1,a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
            let dK4dB =  dK4dB1.extend(vec![Box::new(forward.extend_using(|&(a1,_a2,_a3)| a1 as u64, |t1, t2| t1.lt(t2))),
                                            Box::new(forward.extend_using(|&(_a1,a2,_a3)| a2 as u64, |t1, t2| t1.lt(t2))),
                                            Box::new(forward.extend_using(|&(_a1,_a2,a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,p.2,e), wght)));
            
            // dQdC(a1,a4): Similar to above first extend (a1, a4) to (a1, a2, a4). Then to (a1, a2, a3, a4).
            let dK4dC1 = dG.extend(vec![Box::new(forward.extend_using(|&(a1,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                        Box::new(reverse.extend_using(|&(_a1,a4)| a4 as u64, |t1, t2| t1.lt(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
            let dK4dC =  dK4dC1.extend(vec![Box::new(forward.extend_using(|&(a1,_a2,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                            Box::new(forward.extend_using(|&(_a1,a2,_a4)| a2 as u64, |t1, t2| t1.lt(t2))),
                                            Box::new(reverse.extend_using(|&(_a1,_a2,a4)| a4 as u64, |t1, t2| t1.lt(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,e, p.2), wght)));
            
            // dQdD(a2,a3): Similar to above first extend (a2, a3) to (a1, a2, a3). Then to (a1, a2, a3, a4).
            let dK4dD1 = dG.extend(vec![Box::new(reverse.extend_using(|&(a2,_a3)| a2 as u64, |t1, t2| t1.le(t2))),
                                        Box::new(reverse.extend_using(|&(_a2,a3)| a3 as u64, |t1, t2| t1.le(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK4dD =  dK4dD1.extend(vec![Box::new(forward.extend_using(|&(a1,_a2,_a3)| a1 as u64, |t1, t2| t1.le(t2))),
                                            Box::new(forward.extend_using(|&(_a1,a2,_a3)| a2 as u64, |t1, t2| t1.lt(t2))),
                                            Box::new(forward.extend_using(|&(_a1,_a2,a3)| a3 as u64, |t1, t2| t1.lt(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,p.2,e), wght)));
            
            // dQdE(a2,a4): Similar to above first extend (a2, a4) to (a1, a2, a4). Then to (a1, a2, a3, a4).
            let dK4dE1 = dG.extend(vec![Box::new(reverse.extend_using(|&(a2,_a4)| a2 as u64, |t1, t2| t1.le(t2))),
                                        Box::new(reverse.extend_using(|&(_a2,a4)| a4 as u64, |t1, t2| t1.le(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK4dE =  dK4dE1.extend(vec![Box::new(forward.extend_using(|&(a1,_a2,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                            Box::new(forward.extend_using(|&(_a1,a2,_a4)| a2 as u64, |t1, t2| t1.le(t2))),
                                            Box::new(reverse.extend_using(|&(_a1,_a2,a4)| a4 as u64, |t1, t2| t1.lt(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1, e, p.2), wght)));
            
            // dQdF(a3,a4): Similar to above first extend (a3, a4) to (a1, a3, a4). Then to (a1, a2, a3, a4).
            let dK4dF1 = dG.extend(vec![Box::new(reverse.extend_using(|&(a3,_a4)| a3 as u64, |t1, t2| t1.le(t2))),
                                        Box::new(reverse.extend_using(|&(_a3,a4)| a4 as u64, |t1, t2| t1.le(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK4dF =  dK4dF1.extend(vec![Box::new(forward.extend_using(|&(a1,_a3,_a4)| a1 as u64, |t1, t2| t1.le(t2))),
                                            Box::new(reverse.extend_using(|&(_a1,a3,_a4)| a3 as u64, |t1, t2| t1.le(t2))),
                                            Box::new(reverse.extend_using(|&(_a1,_a3,a4)| a4 as u64, |t1, t2| t1.le(t2)))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1, p.2), wght)));
            

            // accumulate all changes together into a single dataflow.
            let cliques = dK4dF.concat(&dK4dE).concat(&dK4dD).concat(&dK4dC).concat(&dK4dB).concat(&dK4dA);

            // if the third argument is "inspect", report 4-clique counts.
            if inspect {
                cliques.exchange(|x| (x.0).0 as u64)
                // .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .count()
                    .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .capture_into(send);
            }
            (graph, cliques.probe().0, f_handle, r_handle)
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
        let prev = input.time().clone();
        input.advance_to(prev.inner + 1);
        root.step_while(|| probe.lt(input.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = time::precise_time_s();
        for node in 0 .. nodes {

            // introduce the node if it is this worker's responsibility
            if node % peers == index {
                for &edge in &edges[node / peers] {
                    input.send(((node as u32, edge), 1));
                }
            }

            // if at a batch boundary, advance time and do work.
            if node % batch == (batch - 1) {
                let prev = input.time().clone();
                input.advance_to(prev.inner + 1);
                root.step_while(|| probe.lt(input.time()));

                // merge all of the indices we maintain.
                forward.borrow_mut().merge_to(&prev);
                reverse.borrow_mut().merge_to(&prev);
            }
        }

        input.close();
        while root.step() { }

        if inspect { 
            println!("worker {} elapsed: {:?}", index, time::precise_time_s() - start); 
        }

    }).unwrap();

    let result = recv.extract();

    let mut total = 0;
    for &(_, ref counts) in &result {
        for &count in counts {
            total += count;
        }
    } 

    if inspect { 
        println!("elapsed: {:?}\ttotal 4-cliques at this process: {:?}", time::precise_time_s() - start, total); 
    }
}
