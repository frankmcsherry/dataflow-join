// #![feature(scoped)]
// #![feature(collections)]

extern crate mmap;
extern crate time;
extern crate timely;
extern crate columnar;
extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use std::thread;
use std::mem;

use dataflow_join::graph::{GraphTrait, GraphMMap};

use timely::progress::timestamp::RootTimestamp;
use timely::progress::scope::Scope;
use timely::progress::nested::Summary::Local;
use timely::example_static::*;
use timely::communication::*;
use timely::communication::pact::Exchange;

use timely::networking::initialize_networking;
use timely::networking::initialize_networking_from_file;

use timely::drain::DrainExt;

static USAGE: &'static str = "
Usage: pagerank <source> [options] [<arguments>...]

Options:
    -w <arg>, --workers <arg>    number of workers per process [default: 1]
    -p <arg>, --processid <arg>  identity of this process      [default: 0]
    -n <arg>, --processes <arg>  number of processes involved  [default: 1]
    -h <arg>, --hosts <arg>      list of host:port for workers
";


fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    // let workers = if let Ok(threads) = args.get_str("<workers>").parse() { threads }
    //               else { panic!("invalid setting for workers: {}", args.get_str("<workers>")) };
    // println!("starting pagerank dataflow with {:?} worker{}", workers, if workers == 1 { "" } else { "s" });
    let source = args.get_str("<source>").to_owned();

    let workers: u64 = if let Ok(threads) = args.get_str("-w").parse() { threads }
                       else { panic!("invalid setting for --workers: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                          else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Starting pagerank dataflow with");
    println!("\tworkers:\t{}", workers);
    println!("\tprocesses:\t{}", processes);
    println!("\tprocessid:\t{}", process_id);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        println!("Initializing BinaryCommunicator");

        let hosts = args.get_str("-h");
        let communicators = if hosts != "" {
            initialize_networking_from_file(hosts, process_id, workers).ok().expect("error initializing networking")
        }
        else {
            let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
            initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking")
        };

        pagerank_multi(communicators, source);
    }
    else if workers > 1 {
        println!("Initializing ProcessCommunicator");
        pagerank_multi(ProcessCommunicator::new_vector(workers), source);
    }
    else {
        println!("Initializing ThreadCommunicator");
        pagerank_multi(vec![ThreadCommunicator], source);
    };
}

fn pagerank_multi<C>(communicators: Vec<C>, filename: String)
where C: Communicator+Send {
    let mut guards = Vec::new();
    let workers = communicators.len();
    for communicator in communicators.into_iter() {
        let filename = filename.clone();
        guards.push(thread::Builder::new().name(format!("timely worker {}", communicator.index()))
                                          .spawn(move || pagerank(communicator, filename, workers))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn pagerank<C>(communicator: C, filename: String, workers: usize)
where C: Communicator {
    let index = communicator.index() as usize;
    let peers = communicator.peers() as usize;

    let mut root = GraphRoot::new(communicator);

    {   // new scope avoids long borrow on root
        let mut builder = root.new_subgraph();

        // establish the beginnings of a loop,
        // 20 iterations, each time around += 1.
        let (helper, stream) = builder.loop_variable::<(u32, f32)>(RootTimestamp::new(20), Local(1));

        let graph = GraphMMap::<u32>::new(&filename);

        let nodes = graph.nodes();

        let mut src = vec![1.0; 1 + (nodes / peers as usize)];  // local rank accumulation
        let mut tmp = vec![1.0; 1 + (nodes / peers as usize)];  // local rank accumulation
        // let mut dst = vec![0.0; nodes];                         // local rank accumulation

        let mut buf = vec![];

        let mut start = time::precise_time_s();

        // from feedback, place an operator that
        // aggregates and broadcasts ranks along edges.
        let ranks = stream.enable(builder).unary_notify(

            Exchange::new(|x: &(u32, f32)| x.0 as u64),     // 1. how data should be exchanged
            format!("PageRank"),                            // 2. a tasteful, descriptive name
            vec![RootTimestamp::new(0)],                    // 3. indicate an initial capability
            move |input, output, iterator| {                // 4. provide the operator logic

                while let Some((iter, _)) = iterator.next() {

                    mem::swap(&mut src, &mut tmp);


                    // /---- should look familiar! ----\
                    for node in 0..src.len() {
                        src[node] = 0.15 + 0.85 * src[node];
                    }

                    let mut node = 0;
                    let mut read = 0;
                    let mut counter = 0;

                    while node < src.len() {

                        let mut session = output.session(&iter);
                        for _ in 0 .. std::cmp::min(1_000, src.len() - node) {

                            let edges = graph.edges(index + peers * node);
                            let value = src[node] / edges.len() as f32;
                            for &b in edges {
                                session.give((b, value));
                            }

                            counter += edges.len();
                            node += 1;
                        }

                        while let Some((iter, data)) = input.pull() {
                            iterator.notify_at(&iter);
                            read += data.len();
                            buf.extend(data.drain_temp());
                            if read > counter { break; }
                        }

                        for (node, rank) in buf.drain_temp() {
                            tmp[node as usize / peers] += rank;
                        }

                        // if (node % 100_000) == 0 {
                        //     println!("status: {} node, {} counter, {} read, \tdefecit: {}", node, counter, read, counter as i64 - read as i64);
                        // }
                    }
                    // \------ end familiar part ------/

                    println!("iteration {:?}: {}s", iter, time::precise_time_s() - start);
                    start = time::precise_time_s();
                }

                while let Some((iter, data)) = input.pull() {
                    iterator.notify_at(&iter);
                    for (node, rank) in data.drain_temp() {
                        tmp[node as usize / peers] += rank;
                    }
                }
            }
        );

        // let local_index = index as usize % workers;
        // let mut acc = vec![0.0; 1 + (nodes / workers)];

        ranks
        // .unary_notify(
        //     Exchange::new(move |x: &(u32, f32)| (workers * (index / workers)) as u64 + (x.0 as u64 % workers as u64)),
        //     format!("Aggregation"),
        //     vec![],
        //     move |input, output, iterator| {
        //         while let Some((iter, data)) = input.pull() {
        //             iterator.notify_at(&iter);
        //             for (node, rank) in data.drain_temp() {
        //                 acc[node as usize / workers] += rank;
        //             }
        //         }
        //
        //         while let Some((item, _)) = iterator.next() {
        //
        //             output.give_at(&item, acc.drain_temp().enumerate().filter(|x| x.1 != 0.0)
        //                                      .map(|(u,f)| (((u * workers + local_index) as u32), f)));
        //
        //             for _ in 0..(1 + (nodes/workers)) { acc.push(0.0); }
        //             assert!(acc.len() == (1 + (nodes/workers)));
        //         }
        //     }
        // )
        .connect_loop(helper);
    }

    while root.step() { }
}
