extern crate timely;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};
use std::io::BufReader;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use timely::dataflow::operators::*;

use alg3_dynamic::*;

type Node = u32;

fn main () {

    let start = std::time::Instant::now();

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
        let number_files: usize = std::env::args().nth(2 * (query_size) + 3).unwrap().parse().unwrap();
        let pre_load = std::env::args().nth(2 * (query_size) + 4).unwrap().parse().unwrap();
        let query_filename = std::env::args().nth(2 * (query_size) + 5).unwrap();
        let query_batch: usize = std::env::args().nth(2 * (query_size) + 6).unwrap().parse().unwrap();

        if index==0 {
            println!("motif:\t{:?}", motif);
            println!("filename:\t{:?} , {:?}", filename, number_files);
        }

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input_graph1, mut input_graph2, mut input_delta, probe, load_probe1, load_probe2, handles) = root.dataflow::<Node,_,_>(move |builder| {

            // inputs for initial edges and changes to the edge set, respectively.
            let (graph_input1, graph1) = builder.new_input::<(Node, Node)>();
            let (graph_input2, graph2) = builder.new_input::<(Node, Node)>();
            let (delta_input, delta) = builder.new_input::<((Node, Node), i32)>();
            
            // // create indices and handles from the initial edges plus updates.
            let (graph_index, handles) = motif::GraphStreamIndex::from_separately_static(graph1, graph2, delta, |k| k as u64, |k| k as u64);

            // construct the motif dataflow subgraph.
            let motifs = graph_index.build_motif(&motif);

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

            let load_probe1 = graph_index.forward.handle.clone();
            let load_probe2 = graph_index.reverse.handle.clone();

            (graph_input1, graph_input2, delta_input, motifs.probe(), load_probe1, load_probe2, handles)
        });

        // start the experiment!
        let start = ::std::time::Instant::now();
        let mut remaining = pre_load;
        let max_vertex = pre_load as Node;
        let prev_time = input_delta.time().clone();
        input_delta.advance_to(prev_time.inner + 1);


    if number_files == 1 {
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
                  input_graph1.send((src, dst));
                }
            }
        }
          // synchronize with other workers before reporting data loaded.
        input_graph1.close();
        root.step_while(|| load_probe1.less_than(input_delta.time()));
        println!("{:?}\t[worker {}]\tforward index loaded", start.elapsed(), index);
        //
        // REPEAT ABOVE
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
                    input_graph2.send((src, dst));
                }
            }
        }

        // synchronize with other workers before reporting data loaded.
        input_graph2.close();
        root.step_while(|| load_probe2.less_than(input_delta.time()));
        println!("{:?}\t[worker {}]\treverse index loaded", start.elapsed(), index);


        // END REPEAT
    } else {

    println!("Multiple files...");

      for p in 0..number_files {
      if p % peers != index {
            // each partition will be handeled by one worker only.
            continue;
           }
 
        let mut p_str = filename.clone().to_string();
        if p / 10 == 0{
           p_str = p_str + "0000"+ &(p.to_string());
        }
        else if p / 100 == 0{
           p_str = p_str + "000"+ &(p.to_string());
        }
        else if p / 1000 == 0{
           p_str = p_str + "00"+ &(p.to_string());
        }
        else if p / 10000 == 0{
           p_str = p_str + "0"+ &(p.to_string());
        }
        else {
           p_str = p_str + &(p.to_string());
        }
    
        println!("worker{:?} --> filename: {:?} {:?}", index,p, p_str);
 
          let mut lines = match File::open(&Path::new(&p_str)) {
            Ok(file) => BufReader::new(file).lines(),
            Err(why) => {
                panic!("EXCEPTION: couldn't open {}: {}",
                       Path::new(&p_str).display(),
                       Error::description(&why))
            },
        };

          remaining = 0;

        // load up all lines in the file.
        for (counter, line) in lines.by_ref().enumerate() {
          // count edges 
          remaining = remaining +1 ;
           // each worker should load all available edges. Note that each partition is handled by one worker only.
           let good_line = line.ok().expect("EXCEPTION: read error");
           if !good_line.starts_with('#') && good_line.len() > 0 {
               let mut elements = good_line[..].split_whitespace();
               let src: Node = elements.next().unwrap().parse().ok().expect("malformed src");
               let dst: Node = elements.next().unwrap().parse().ok().expect("malformed dst");
               input_graph1.send((src, dst)); // send each edge to its responsible worker;
            }
        }

      } // end loop on files for forward 


        // synchronize with other workers before reporting data loaded.
        input_graph1.close();
        root.step_while(|| load_probe1.less_than(input_delta.time()));
        println!("{:?}\t[worker {}]\tforward index loaded", start.elapsed(), index);



        // REPEAT ABOVE

    for p in 0..number_files {
      if p % peers != index {
            // each partition will be handeled by one worker only.
            continue;
           }
 
        let mut p_str = filename.clone().to_string();
        if p / 10 == 0{
           p_str = p_str + "0000"+ &(p.to_string());
        }
        else if p / 100 == 0{
           p_str = p_str + "000"+ &(p.to_string());
        }
        else if p / 1000 == 0{
           p_str = p_str + "00"+ &(p.to_string());
        }
        else if p / 10000 == 0{
           p_str = p_str + "0"+ &(p.to_string());
        }
        else {
           p_str = p_str + &(p.to_string());
        }
  
        println!("worker{:?} --> filename: {:?} {:?}", index,p, p_str);
 
        let mut lines = match File::open(&Path::new(&p_str)) {
            Ok(file) => BufReader::new(file).lines(),
            Err(why) => {
                panic!("EXCEPTION: couldn't open {}: {}",
                       Path::new(&p_str).display(),
                       Error::description(&why))
            },
        };

        remaining = 0;


        // Open the path in read-only mode, returns `io::Result<File>`
        let mut lines = match File::open(&Path::new(&p_str)) {
            Ok(file) => BufReader::new(file).lines(),
            Err(why) => {
                panic!("EXCEPTION: couldn't open {}: {}",
                       Path::new(&p_str).display(),
                       Error::description(&why))
            },
        };

        // load up the graph, using the first `limit` lines in the file.
        for (counter, line) in lines.by_ref().enumerate() {
            // each worker is responsible for a fraction of the queries
                let good_line = line.ok().expect("EXCEPTION: read error");
                if !good_line.starts_with('#') && good_line.len() > 0 {
                   let mut elements = good_line[..].split_whitespace();
                   let src: Node = elements.next().unwrap().parse().ok().expect("malformed src");
                   let dst: Node = elements.next().unwrap().parse().ok().expect("malformed dst");
                   input_graph2.send((src, dst));
                }
        }




    }//end loop on files

    // synchronize with other workers before reporting data loaded.
    input_graph2.close();
    root.step_while(|| load_probe2.less_than(input_delta.time()));
    println!("{:?}\t[worker {}]\treverse index loaded", start.elapsed(), index);


    // END REPEAT



    }// end if there are multi files


        // loop { }

        // merge all of the indices the worker maintains.
        let prev_time = input_delta.time().clone();
        handles.merge_to(&prev_time);

        // synchronize with other workers before reporting indices merged.
        let prev_time = input_delta.time().clone();
        // input_graph.advance_to(prev_time.inner + 1);
        input_delta.advance_to(prev_time.inner + 1);
        root.step_while(|| probe.less_than(input_delta.time()));
        println!("{:?}\t[worker {}]\tindices merged", start.elapsed(), index);


        let lines = match File::open(&Path::new(&query_filename)) {
            Ok(file) => BufReader::new(file).lines(),
            Err(why) => {
                panic!("EXCEPTION: couldn't open {}: {}",
                       Path::new(&query_filename).display(),
                       Error::description(&why))
            },
        };


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
                let prev_time = input_delta.time().clone();
                // input_graph.advance_to(prev_time.inner + 1);
                input_delta.advance_to(prev_time.inner + 1);
                root.step_while(|| probe.less_than(input_delta.time()));
                handles.merge_to(&prev_time);
            }
        }
    }).unwrap();

    let total = send2.lock().map(|x| *x).unwrap_or(0);
    println!("elapsed: {:?}\ttotal motifs at this process: {:?}", start.elapsed(), total); 
}