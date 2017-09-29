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

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<((u32, u32), i32)>();

            // Our query is K3 = A(x,y) B(x,z) C(y,z): triangles.
            //
            // The dataflow determines how to update this query with respect to changes in each
            // of the input relations: A, B, and C. Each partial derivative will use the other
            // relations, but the order in which attributes are added may (will) be different.
            //
            // The updates also use the other relations with slightly stale data: updates to each
            // relation must not see updates for "later" relations (under some order on relations).

            let forward = IndexStream::from(|&k| k as u64, &Vec::new().to_stream(builder), &dG);
            let reverse = IndexStream::from(|&k| k as u64, &Vec::new().to_stream(builder), &dG.map(|((src,dst),wgt)| ((dst,src),wgt)));

            // dA(x,y) extends to z first through C(x,z) then B(y,z), both using forward indices.
            let dK3dA = dG.extend(vec![Box::new(forward.extend_using(|&(ref x,_)| x, <_ as PartialOrd>::lt)),
                                       Box::new(forward.extend_using(|&(_,ref y)| y, <_ as PartialOrd>::lt))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((p.0,p.1,e), w)));

            // dB(x,z) extends to y first through A(x,y) then C(y,z), using forward and reverse indices, respectively.
            let dK3dB = dG.extend(vec![Box::new(forward.extend_using(|&(ref x,_)| x, <_ as PartialOrd>::le)),
                                       Box::new(reverse.extend_using(|&(_,ref z)| z, <_ as PartialOrd>::lt))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((p.0,e,p.1), w)));

            // dC(y,z) extends to x first through A(x,y) then B(x,z), both using reverse indices.
            let dK3dC = dG.extend(vec![Box::new(reverse.extend_using(|&(ref y,_)| y, <_ as PartialOrd>::le)),
                                       Box::new(reverse.extend_using(|&(_,ref z)| z, <_ as PartialOrd>::le))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((e,p.0,p.1), w)));

            // accumulate all changes together
            let cliques = dK3dC.concat(&dK3dB).concat(&dK3dA);

            // if the third argument is "inspect", report triangle counts.
            if inspect {
                cliques.exchange(|x| (x.0).0 as u64)
                       // .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                       .count()
                       .inspect_batch(move |t,x| println!("{:?}: {:?}", t, x))
                       .inspect_batch(move |_,x| { 
                            if let Ok(mut bound) = send.lock() {
                                *bound += x[0];
                            }
                        });
            }

            (graph, cliques.probe(), forward, reverse)
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
        root.step_while(|| probe.less_than(input.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = ::std::time::Instant::now();
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
                root.step_while(|| probe.less_than(input.time()));

                // merge all of the indices we maintain.
                forward.index.borrow_mut().merge_to(&prev);
                reverse.index.borrow_mut().merge_to(&prev);
            }
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
