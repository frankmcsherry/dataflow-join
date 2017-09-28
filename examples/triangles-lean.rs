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
        let (mut input, mut query, probe, /*forward,*/ reverse) = root.dataflow::<u32,_,_>(|builder| {

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<((u32, u32), i32)>();
            // Please see triangles for more information on "graph" and dG.
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
            // let (forward, f_handle) = dG.index_from(&dG.filter(|_| false).map(|_| (0,0)));
            let (reverse, r_handle) = dG.filter(|_|false).map(|((src,dst),wgt)| ((dst,src),wgt)).index_from(&dG.map(|((x,y),_)| (y,x)));

            // dC(y,z) extends to x first through A(x,y) then B(x,z), both using reverse indices.
            let cliques = dQ.extend(vec![Box::new(reverse.extend_using(|&(ref y,_)| y, |&k| k as u64, |t1, t2| t1.le(t2))),
                                       Box::new(reverse.extend_using(|&(_,ref z)| z, |&k| k as u64, |t1, t2| t1.le(t2)))]);

            // if the third argument is "inspect", report triangle counts.
            if inspect {
                cliques
                       .inspect_batch(move |_,x| { 
                            if let Ok(mut bound) = send.lock() {
                                for xx in x.iter() {
                                   *bound += xx.1.len();
                               }
                            }
                        });
            }

            (graph, query, cliques.probe(), /*f_handle,*/ r_handle)
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
        query.advance_to(prev.inner + 1);
        root.step_while(|| probe.less_than(input.time()));

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
        }

        let prev = input.time().clone();
        input.advance_to(prev.inner + 1);
        query.advance_to(prev.inner + 1);
        root.step_while(|| probe.less_than(query.time()));
        reverse.borrow_mut().merge_to(&prev);
        input.close();

        println!("{:?}: index built", time::precise_time_s() - start);

        for node in 0 .. nodes {

            // introduce the node if it is this worker's responsibility
            if node % peers == index {
                for &edge in &edges[node / peers] {
                    query.send(((node as u32, edge), 1));
                }
            }

            // if at a batch boundary, advance time and do work.
            if node % batch == (batch - 1) {
                let prev = query.time().clone();
                query.advance_to(prev.inner + 1);
                root.step_while(|| probe.less_than(query.time()));

                // merge all of the indices we maintain.
                // forward.borrow_mut().merge_to(&prev);
                reverse.borrow_mut().merge_to(&prev);
            }
        }

        query.close();
        while root.step() { }

        if inspect { 
            println!("worker {} elapsed: {:?}", index, time::precise_time_s() - start); 
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
