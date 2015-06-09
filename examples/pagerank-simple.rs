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

use dataflow_join::graph::{GraphTrait, GraphMMap};

use timely::progress::timestamp::RootTimestamp;
use timely::progress::scope::Scope;
use timely::progress::nested::Summary::Local;
use timely::example_static::*;
use timely::communication::*;
use timely::communication::pact::Exchange;

use timely::drain::DrainExt;

static USAGE: &'static str = "
Usage: pagerank <source> <workers>
";

fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let workers = if let Ok(threads) = args.get_str("<workers>").parse() { threads }
                  else { panic!("invalid setting for workers: {}", args.get_str("<workers>")) };
    println!("starting pagerank dataflow with {:?} worker{}", workers, if workers == 1 { "" } else { "s" });
    let source = args.get_str("<source>").to_owned();

    pagerank_multi(ProcessCommunicator::new_vector(workers), source);
}

fn pagerank_multi<C>(communicators: Vec<C>, filename: String)
where C: Communicator+Send {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        let filename = filename.clone();
        guards.push(thread::Builder::new().name(format!("timely worker {}", communicator.index()))
                                          .spawn(move || pagerank(communicator, filename))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn pagerank<C>(communicator: C, filename: String)
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
        let mut src = vec![1.0; graph.nodes() / peers as usize];    // local rank accumulation
        let mut dst = vec![0.0; graph.nodes()];                     // local rank accumulation

        let mut start = time::precise_time_s();

        // from feedback, place an operator that
        // aggregates and broadcasts ranks along edges.
        stream.enable(builder).unary_notify(

            Exchange::new(|x: &(u32, f32)| x.0 as u64),     // 1. how data should be exchanged
            format!("PageRank"),                            // 2. a tasteful, descriptive name
            vec![RootTimestamp::new(0)],                    // 3. indicate an initial capability
            move |input, output, iterator| {                // 4. provide the operator logic

                while let Some((iter, _)) = iterator.next() {
                    // /---- should look familiar! ----\
                    for node in 0..src.len() {
                        src[node] = 0.15 + 0.85 * src[node];
                    }

                    for node in 0..src.len() {
                        let edges = graph.edges(index + peers * node);
                        let value = src[node] / edges.len() as f32;
                        for &b in edges {
                            dst[b as usize] += value;
                        }
                    }
                    // \------ end familiar part ------/
                    output.give_at(&iter, dst.drain_temp()
                                             .enumerate()
                                             .filter(|&(_,f)| f != 0.0)
                                             .map(|(u,f)| (u as u32, f)));

                    // dst.resize(graph.nodes(), 0.0);
                    for _ in 0..graph.nodes() { dst.push(0.0); }

                    println!("{}s", time::precise_time_s() - start);
                    start = time::precise_time_s();
                }

                while let Some((iter, data)) = input.pull() {
                    iterator.notify_at(&iter);
                    for (node, rank) in data.drain_temp() {
                        src[node as usize / peers] += rank;
                    }
                }
            }
        )
        .connect_loop(helper);
    }

    while root.step() { }
}
