extern crate mmap;
extern crate time;
extern crate timely;
extern crate dataflow_join;

use std::io::{BufRead, stdin};
use std::rc::Rc;

use dataflow_join::*;
use dataflow_join::graph::{GraphTrait, GraphMMap, GraphExtenderExt};

use timely::example_shared::*;
use timely::example_shared::operators::*;

use timely::communication::*;

fn main () {

    let source = std::env::args().skip(1).next().unwrap();
    let interactive = std::env::args().skip(2).next().unwrap() == "interactive";
    let inspect = std::env::args().skip(3).next().unwrap() == "inspect";

    timely::initialize(std::env::args().skip(4), move |communicator| {
        triangles(communicator, source.clone(), inspect, interactive, 1000);
    });
}

fn triangles<C>(communicator: C, filename: String, inspect: bool, interactive: bool, step_size: u64)
where C: Communicator {
    let index = communicator.index();
    let peers = communicator.peers();

    let graph = Rc::new(GraphMMap::<u32>::new(&filename));

    let mut root = GraphRoot::new(communicator);
    let mut input = root.subcomputation(|builder| {

        let (input, stream) = builder.new_input::<u32>();

        // extend u32s to pairs, then pairs to triples.
        let triangles = stream.extend(vec![&graph.extend_using(|&a| a as u64)])
                              .flat_map(|(p, es)| es.into_iter().map(move |e| (p, e)))
                              .extend(vec![&graph.extend_using(|&(a,_)| a as u64),
                                           &graph.extend_using(|&(_,b)| b as u64)]);

        // // Quads
        // triangles.flat_map(|(p,es)| es.into_iter().map(move |e| (p, e)))
        //          .extend(vec![&graph.extend_using(|&((a,_),_)| a as u64),
        //                       &graph.extend_using(|&((_,b),_)| b as u64),
        //                       &graph.extend_using(|&((_,_),c)| c as u64)]);

        if inspect { triangles.inspect(|x| println!("triangles: {:?}", x)); }

        input
    });

    if interactive {
        let mut stdinput = stdin();
        let mut line = String::new();
        for round in (0..) {
            stdinput.read_line(&mut line).unwrap();
            let start = time::precise_time_ns();
            if let Some(word) = line.split(" ").next() {
                let read = word.parse();
                if let Ok(number) = read {
                    input.send_at(round, vec![number].into_iter());
                }
                else {
                    println!("failed to parse: <{}>", word);
                }
            }
            line.clear();

            input.advance_to(round + 1);
            for _ in (0..5) { root.step(); }

            println!("elapsed: {:?}us", (time::precise_time_ns() - start)/1000);
        }
    }
    else {
        let nodes = graph.nodes() as u64 - 1;
        let limit = (nodes / step_size) + 1;
        for round in (0..limit) {
            input.send_at(round, (0..step_size).map(|x| x + round * step_size)
                                               .filter(|&x| x % peers == index)
                                               .filter(|&x| x < nodes)
                                               .map(|x| x as u32));
            input.advance_to(round + 1);
            root.step();
        }

        input.close();
        while root.step() { }
    }
}


// fn triangles_auto_multi<C>(communicators: Vec<C>, filename: String, step_size: u64)
// where C: Communicator+Send {
//     let mut guards = Vec::new();
//     for communicator in communicators.into_iter() {
//         let filename = filename.clone();
//         guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
//                                           .spawn(move || triangles_auto(communicator, filename, step_size))
//                                           .unwrap());
//     }
//
//     for guard in guards { guard.join().unwrap(); }
// }
//
// fn triangles_auto<C>(communicator: C, filename: String, step_size: u64)
// where C: Communicator {
//     let index = communicator.index();
//     let peers = communicator.peers();
//
//     let batch = step_size;
//     let graph = Rc::new(GraphMMap::<u32>::new(&filename));
//     let nodes = graph.nodes() as u64;
//
//     let mut root = GraphRoot::new(communicator);
//
//     {   // new scope to avoid long borrow on root
//
//         let mut builder = root.new_subgraph::<u64>();
//
//         let (feedback, stream) = builder.loop_variable(RootTimestamp::new(nodes + 1), Local(1));
//
//         stream.enable(&mut builder)
//               .unary_notify(Pipeline, format!("Input"), vec![RootTimestamp::new(0)], move |_, output, notificator| {
//                   if let Some((time, _count)) = notificator.next() {
//                       if time.inner < (nodes / batch) {
//                           notificator.notify_at(&RootTimestamp::new(time.inner + 1));
//                       }
//                       let mut session = output.session(&time);
//                       for next in (0..(batch/peers)) {
//                           session.give(time.inner * batch + next * peers + index);
//                       }
//                   }})
//               .extend(vec![&graph.extend_using(|| { |&a| a as u64 } )])
//               .flat_map(|(p, es)| es.into_iter().map(move |e| (p, e)))
//               .extend(vec![&graph.extend_using(|| { |&(a,_)| a as u64 }),
//                            &graph.extend_using(|| { |&(_,b)| b as u64 })])
//               .filter(|_| false)
//               .connect_loop(feedback);
//     }
//
//     while root.step() { }
// }
