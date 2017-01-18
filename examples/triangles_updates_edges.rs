extern crate rand;
extern crate time;
extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::*;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use std::io::BufReader;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

#[allow(non_snake_case)]
fn main () {

    let start = time::precise_time_s();

    let send = Arc::new(Mutex::new(0));
    let send2 = send.clone();

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.clone();

        // used to partition graph loading
        let index = root.index() as u32;
        let peers = root.peers() as u32;

        // handles to input and probe, but also both indices so we can compact them.
        let (mut inputG, mut inputQ, probe, forward, reverse) = root.scoped::<u32,_,_>(|builder| {

            // A dynamic graph is a stream of updates: `((src, dst), wgt)`.
            // Each triple indicates a change to the count of the number of arcs from
            // `src` to `dst`. Typically this change would be +/-1, but whatever.
            let (graph, dG) = builder.new_input::<((u32, u32), i32)>();
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
            let (forward, f_handle) = dG.concat(&dQ).index();
            let (reverse, r_handle) = dG.concat(&dQ).map(|((src,dst),wgt)| ((dst,src),wgt)).index();


            // dA(x,y) extends to z first through C(x,z) then B(y,z), both using forward indices.
            let dK3dA = dQ//.filter(|_| false)
                          .extend(vec![Box::new(forward.extend_using(|&(x,_)| x as u64, |t1, t2| t1.lt(t2))),
                                       Box::new(forward.extend_using(|&(_,y)| y as u64, |t1, t2| t1.lt(t2)))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((p.0,p.1,e), w)));

            // dB(x,z) extends to y first through A(x,y) then C(y,z), using forward and reverse indices, respectively.
            let dK3dB = dQ//.filter(|_| false)
                          .extend(vec![Box::new(forward.extend_using(|&(x,_)| x as u64, |t1, t2| t1.le(t2))),
                                       Box::new(reverse.extend_using(|&(_,z)| z as u64, |t1, t2| t1.lt(t2)))])
                          .flat_map(|(p,es,w)| es.into_iter().map(move |e| ((p.0,e,p.1), w)));

            // dC(y,z) extends to x first through A(x,y) then B(x,z), both using reverse indices.
            let dK3dC = dQ.extend(vec![Box::new(reverse.extend_using(|&(y,_)| y as u64, |t1, t2| t1.le(t2))),
                                       Box::new(reverse.extend_using(|&(_,z)| z as u64, |t1, t2| t1.le(t2)))])
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
            (graph, query, cliques.probe().0, f_handle, r_handle)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
	// load percentage out of 100
        let percent: usize = std::env::args().nth(3).unwrap().parse().unwrap();


	let input_graph = read_edges(&filename, peers, index);
	let graphSize: usize = std::env::args().nth(4).unwrap().parse().unwrap();
	let limit = (percent * graphSize /100 / peers as usize) as usize ;
	/*
        let graph = GraphMMap::new(&filename);
        let nodes = graph.nodes();
        let mut edges = Vec::new();
        for node in 0 .. graph.nodes() {
            if node % peers == index {
                edges.push(graph.edges(node).to_vec());
            }
        }

        drop(graph);
	*/

	let mut edges = Vec::new();
	let mut edgesQ = Vec::new();
	for e in 0 .. input_graph.len() {
	    // keep edges related to this worker only
	    //if input_graph[e].0 % peers == index {
		if e <= limit {
		   edges.push(input_graph[e]);
		}
		else {
		   edgesQ.push(input_graph[e]);
		}
	    //}
	}
	
	drop(input_graph);

        // synchronize with other workers.
        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.lt(inputG.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = ::std::time::Instant::now();

	// load graph to data flow
        for e in 0 .. edges.len() {
            inputG.send((edges[e], 1));
        }

        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.lt(inputG.time()));

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
        root.step_while(|| probe.lt(inputG.time()));

	let mut counter = 0 as usize;
        for e in 0 .. edgesQ.len() {
            
	    inputQ.send((edgesQ[e], 1));
	    counter += 1;

    	    // advance the graph stream (only useful in the first time)
	    // should I check if counter == 1 before we do this step !
    	    let prevG = inputG.time().clone();
            inputG.advance_to(prevG.inner + 1);

            if counter % batch == (batch - 1) {
                let prev = inputQ.time().clone();
                inputQ.advance_to(prev.inner + 1);
                root.step_while(|| probe.lt(inputQ.time()));

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
    else { 5 };

    if inspect { 
        println!("elapsed: {:?}\ttotal triangles at this process: {:?}", time::precise_time_s() - start, total); 
    }
}


/// Reads a list of edges from a file and puts them into an array of (u32, u32).
fn read_edges(filename: &str, peers: u32, index:u32) -> Vec<(u32, u32)> {
    // Create a path to the desired file
    let path = Path::new(filename);
    let display = path.display();

    // Open the path in read-only mode, returns `io::Result<File>`
    let file = match File::open(&path) {
        // The `description` method of `io::Error` returns a string that describes the error
        Err(why) => {
            panic!("EXCEPTION: couldn't open {}: {}",
                   display,
                   Error::description(&why))
        }
        Ok(file) => file,
    };

    // Collect all lines into a vector
    let reader = BufReader::new(file);
    // graph is a vector of tuples.
    let mut graph = Vec::new();
    for line in reader.lines() {
        let good_line = line.ok().expect("EXCEPTION: read error");
        if !good_line.starts_with('#') && good_line.len() > 0 {
            let elts: Vec<&str> = good_line[..].split_whitespace().collect();
            let src: u32 = elts[0].parse().ok().expect("malformed src");
            let dst: u32 = elts[1].parse().ok().expect("malformed dst");
	    if src % peers == index {
               graph.push((src, dst));
            }
        }
    }
    return graph;
}
