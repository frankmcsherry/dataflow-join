extern crate rand;
extern crate time;
extern crate timely;
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

type Node = u32;

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

        let mut motif = vec![];
        let query_size: usize = std::env::args().nth(1).unwrap().parse().unwrap();
        for query in 0 .. query_size {
            let attr1: usize = std::env::args().nth(2 * (query + 1) + 0).unwrap().parse().unwrap();
            let attr2: usize = std::env::args().nth(2 * (query + 1) + 1).unwrap().parse().unwrap();
            motif.push((attr1, attr2));
        }

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(2 * (query_size) + 2).unwrap();
        let pre_load = std::env::args().nth(2 * (query_size) + 3).unwrap().parse().unwrap();
        let load_batch: usize = std::env::args().nth(2 * (query_size) + 4).unwrap().parse().unwrap();
        let query_batch: usize = std::env::args().nth(2 * (query_size) + 5).unwrap().parse().unwrap();

        println!("motif:\t{:?}", motif);
        println!("filename:\t{:?}", filename);

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input_graph, mut input_delta, probe, handles) = root.scoped::<Node,_,_>(move |builder| {

            // inputs for initial edges and changes to the edge set, respectively.
            let (graph_input, graph) = builder.new_input::<((Node, Node), i32)>();
            let (delta_input, delta) = builder.new_input::<((Node, Node), i32)>();
            
            // create indices and handles from the initial edges plus updates.
            let (graph_index, handles) = motif::GraphStreamIndex::from(graph, delta);

            // construct the motif dataflow subgraph.
            let motifs = graph_index.track_motif(&motif);

            // if "inspect", report motif counts.
            if inspect {
                motifs
                    .count()
                    .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .inspect_batch(move |_,x| { 
                        if let Ok(mut bound) = send.lock() {
                            *bound += x[0];
                        }
                    });
            }

            (graph_input, delta_input, motifs.probe().0, handles)
        });

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
                    let src: Node = elements.next().unwrap().parse().ok().expect("malformed src");
                    let dst: Node = elements.next().unwrap().parse().ok().expect("malformed dst");
                    input_graph.send(((src, dst), 1));
                }
            }

            // synchronize and merge indices, to keep buffers in check.
            if counter % load_batch == (load_batch - 1) {
               let prev_time = input_graph.time().clone();
               input_graph.advance_to(prev_time.inner + 1);
               input_delta.advance_to(prev_time.inner + 1);
               root.step_while(|| probe.lt(input_delta.time()));
               handles.merge_to(&prev_time);
            }
        }

        // synchronize with other workers before reporting data loaded.
        let prev_time = input_graph.time().clone();
        input_graph.advance_to(prev_time.inner + 1);
        input_delta.advance_to(prev_time.inner + 1);
        root.step_while(|| probe.lt(input_graph.time()));
        println!("{:?}\t[worker {}]\tdata loaded", start.elapsed(), index);

        // merge all of the indices the worker maintains.
        let prev_time = input_graph.time().clone();
        handles.merge_to(&prev_time);

        // synchronize with other workers before reporting indices merged.
        let prev_time = input_graph.time().clone();
        input_graph.advance_to(prev_time.inner + 1);
        input_delta.advance_to(prev_time.inner + 1);
        root.step_while(|| probe.lt(input_graph.time()));
        println!("{:?}\t[worker {}]\tindices merged", start.elapsed(), index);

        // issue queries and updates, using the remaining lines in the file.
        for (query_counter, line) in lines.enumerate() {

            // each worker is responsible for a fraction of the queries
            if query_counter % peers == index {
                let good_line = line.ok().expect("EXCEPTION: read error");
                if !good_line.starts_with('#') && good_line.len() > 0 {
                    let mut elements = good_line[..].split_whitespace();
                    let src: Node = elements.next().unwrap().parse().ok().expect("malformed src");
                    let dst: Node = elements.next().unwrap().parse().ok().expect("malformed dst");
                    input_delta.send(((src, dst), 1));
                }
            }

            // synchronize and merge indices.
            if query_counter % query_batch == (query_batch - 1) {
                let prev_time = input_graph.time().clone();
                input_graph.advance_to(prev_time.inner + 1);
                input_delta.advance_to(prev_time.inner + 1);
                root.step_while(|| probe.lt(input_delta.time()));
                handles.merge_to(&prev_time);
            }
        }
    }).unwrap();

    let total = send2.lock().map(|x| *x).unwrap_or(0);
    println!("elapsed: {:?}\ttotal motifs at this process: {:?}", time::precise_time_s() - start, total); 
}