extern crate mmap;
extern crate time;
extern crate timely;
extern crate columnar;
extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use std::io::{BufRead, stdin};
use std::rc::Rc;
// use std::cell::RefCell;
use std::thread;

use std::cmp::Ordering::*;

use dataflow_join::*;
use dataflow_join::graph::{GraphTrait, GraphMMap, GraphExtenderExt, gallop};

use timely::progress::timestamp::RootTimestamp;
use timely::progress::scope::Scope;
use timely::progress::nested::Summary::Local;
use timely::example_static::*;

use timely::communication::*;
use timely::communication::pact::Pipeline;


static USAGE: &'static str = "
Usage: triangles dataflow <source> <workers> <stepsize> [--inspect] [--interactive] [--alt]
       triangles autorun <source> <workers> <stepsize>
       triangles compute <source>
       triangles help
";



fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    if args.get_bool("dataflow") {
        let inspect = args.get_bool("--inspect");
        let interactive = args.get_bool("--interactive");
        let alt = args.get_bool("--alt");
        let workers = if let Ok(threads) = args.get_str("<workers>").parse() { threads }
                      else { panic!("invalid setting for workers: {}", args.get_str("<workers>")) };
        let stepsize = if let Ok(size) = args.get_str("<stepsize>").parse() { size }
                      else { panic!("invalid setting for stepsize: {}", args.get_str("<stepsize>")) };
        println!("starting triangles dataflow with {:?} worker{}; inspection: {:?}, interactive: {:?}",
                    workers, if workers == 1 { "" } else { "s" }, inspect, interactive);
        let source = args.get_str("<source>").to_owned();

        triangles_multi(ProcessCommunicator::new_vector(workers), source, inspect, interactive, stepsize, alt);
    }
    if args.get_bool("autorun") {
        let workers = if let Ok(threads) = args.get_str("<workers>").parse() { threads }
                      else { panic!("invalid setting for workers: {}", args.get_str("-t")) };
      let stepsize = if let Ok(size) = args.get_str("<stepsize>").parse() { size }
                    else { panic!("invalid setting for stepsize: {}", args.get_str("<stepsize>")) };
        println!("starting triangles dataflow with {:?} worker{}; autorun",
                    workers, if workers == 1 { "" } else { "s" });
        let source = args.get_str("<source>").to_owned();
        triangles_auto_multi(ProcessCommunicator::new_vector(workers), source, stepsize);
    }
    if args.get_bool("compute") {
        let source = args.get_str("<source>");
        let graph = GraphMMap::new(&source);
        println!("triangles: {:?}", raw_triangles(&graph));
    }

    if args.get_bool("help") {
        println!("the code presently assumes you have access to the livejournal graph, from:");
        println!("   https://snap.stanford.edu/data/soc-LiveJournal1.html");
        println!("");
        println!("before you can use it you will need to \"digest\" it using example digest.rs");
        println!("digest will overwrite <target>.targets and <target>.offsets, so be careful!");
        println!("at least, it will once you edit the code to uncomment the line.");
        println!("Once you grok the binary format, you can totally use other graphs too!");
    }
}

fn raw_triangles<G: GraphTrait<Target=u32>>(graph: &G) -> u64 {
    let mut count = 0;
    for a in (0..graph.nodes()) {
        let aaa = graph.edges(a);
        for &b in aaa {
            let bbb = graph.edges(b as usize);
            count += if aaa.len() < bbb.len() { intersect(aaa.clone(), bbb) }
                     else                     { intersect(bbb, aaa.clone()) };
        }
    }
    count
}

fn intersect<E: Ord>(mut aaa: &[E], mut bbb: &[E]) -> u64 {
    let mut count = 0;
    // magic gallop overhead # is 4
    if aaa.len() < bbb.len() / 4 {
        for a in aaa {
            bbb = gallop(bbb, a);
            if bbb.len() > 0 && &bbb[0] == a { count += 1; }
        }
    }
    else {
        while aaa.len() > 0 && bbb.len() > 0 {
            match aaa[0].cmp(&bbb[0]) {
                Greater => { bbb = &bbb[1..]; },
                Less    => { aaa = &aaa[1..]; },
                Equal   => { aaa = &aaa[1..];
                             bbb = &bbb[1..];
                             count += 1;
                           },
            }
        }
    }
    count
}


fn triangles_multi<C>(communicators: Vec<C>, filename: String, inspect: bool, interactive: bool, step_size: u64, alt: bool)
where C: Communicator+Send {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        let filename = filename.clone();
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .spawn(move || triangles(communicator, filename, inspect, interactive, step_size, alt))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn triangles<C>(communicator: C, filename: String, inspect: bool, interactive: bool, step_size: u64, alt: bool)
where C: Communicator {
    let index = communicator.index();
    let peers = communicator.peers();

    let graph = Rc::new(GraphMMap::<u32>::new(&filename));

    let mut root = GraphRoot::new(communicator);
    let mut input = { // new scope to avoid long borrow on root

        let mut builder = root.new_subgraph();
        let (input, stream) = builder.new_input::<u32>();

        if !alt {
            // extend u32s to pairs, then pairs to triples.
            let triangles = builder.enable(&stream)
                                   .extend(vec![&graph.extend_using(|| { |&a| a as u64 } )])
                                   .flat_map(|(p, es)| es.into_iter().map(move |e| (p, e)))
                                   .extend(vec![&graph.extend_using(|| { |&(a,_)| a as u64 }),
                                                &graph.extend_using(|| { |&(_,b)| b as u64 })]);

            triangles.flat_map(|(p,es)| es.into_iter().map(move |e| (p, e)))
                     .extend(vec![&graph.extend_using(|| { |&((a,_),_)| a as u64 }),
                               &graph.extend_using(|| { |&((_,b),_)| b as u64 }),
                               &graph.extend_using(|| { |&((_,_),c)| c as u64 })]);


            // if inspect { triangles.inspect(|x| println!("triangles: {:?}", x)); }
        }
        else {
            // extend u32s to pairs, then pairs to triples.
            let triangles = builder.enable(&stream)
                                   .extend2(vec![&graph.extend_using(|| { |&a| a as u64 } )])
                                   .extend2(vec![&graph.extend_using(|| { |&(a,_)| a as u64 }),
                                                 &graph.extend_using(|| { |&(_,b)| b as u64 })]);

             if inspect { triangles.inspect(|x| println!("triangles: {:?}", x)); }
        }

        input
    };

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


fn triangles_auto_multi<C>(communicators: Vec<C>, filename: String, step_size: u64)
where C: Communicator+Send {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        let filename = filename.clone();
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .spawn(move || triangles_auto(communicator, filename, step_size))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn triangles_auto<C>(communicator: C, filename: String, step_size: u64)
where C: Communicator {
    let index = communicator.index();
    let peers = communicator.peers();

    let batch = step_size;
    let graph = Rc::new(GraphMMap::<u32>::new(&filename));
    let nodes = graph.nodes() as u64;

    let mut root = GraphRoot::new(communicator);

    {   // new scope to avoid long borrow on root

        let mut builder = root.new_subgraph::<u64>();

        let (feedback, stream) = builder.loop_variable(RootTimestamp::new(nodes + 1), Local(1));

        stream.enable(&mut builder)
              .unary_notify(Pipeline, format!("Input"), vec![RootTimestamp::new(0)], move |_, output, notificator| {
                  if let Some((time, _count)) = notificator.next() {
                      if time.inner < (nodes / batch) {
                          notificator.notify_at(&RootTimestamp::new(time.inner + 1));
                      }
                      let mut session = output.session(&time);
                      for next in (0..(batch/peers)) {
                          session.give(time.inner * batch + next * peers + index);
                      }
                  }})
              .extend(vec![&graph.extend_using(|| { |&a| a as u64 } )])
              .flat_map(|(p, es)| es.into_iter().map(move |e| (p, e)))
              .extend(vec![&graph.extend_using(|| { |&(a,_)| a as u64 }),
                           &graph.extend_using(|| { |&(_,b)| b as u64 })])
              .filter(|_| false)
              .connect_loop(feedback);
    }

    while root.step() { }
}

// fn _quads<'a, G, G2>(stream: &mut Stream<'a, G, ((u32, u32), u32)>, graph: &Rc<RefCell<G2>>) ->
//                                                             Stream<'a, G, (((u32, u32), u32), u32)>
// where G: GraphBuilder+'a, G2: GraphTrait<Target=u32> {
//     //
//     stream.extend(vec![&graph.extend_using(|| { |&((a,_),_)| a as u64 }),
//                        &graph.extend_using(|| { |&((_,b),_)| b as u64 }),
//                        &graph.extend_using(|| { |&((_,_),c)| c as u64 })])
//           .flat_map(|(p,es)| es.into_iter().map(move |e| (p.clone(), e)))
//     //
//     //  let mut fives = quads.extend(vec![Box::new(fragment.extend_using(|| { |&(((a,_),_),_)| a as u64 })),
//     //                                    Box::new(fragment.extend_using(|| { |&(((_,b),_),_)| b as u64 })),
//     //                                    Box::new(fragment.extend_using(|| { |&(((_,_),c),_)| c as u64 })),
//     //                                    Box::new(fragment.extend_using(|| { |&(((_,_),_),d)| d as u64 }))]).flatten();
//     //
//     // let mut sixes = fives.extend(vec![Box::new(fragment.extend_using(|| { |&((((a,_),_),_),_)| a as u64 })),
//     //                                   Box::new(fragment.extend_using(|| { |&((((_,b),_),_),_)| b as u64 })),
//     //                                   Box::new(fragment.extend_using(|| { |&((((_,_),c),_),_)| c as u64 })),
//     //                                   Box::new(fragment.extend_using(|| { |&((((_,_),_),d),_)| d as u64 })),
//     //                                   Box::new(fragment.extend_using(|| { |&((((_,_),_),_),e)| e as u64 }))]).flatten();
//     //
//     // sixes.observe();
// }
