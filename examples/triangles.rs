#![feature(core)]
#![feature(str_words)]

extern crate mmap;
extern crate time;
extern crate core;
extern crate timely;
extern crate columnar;
extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use std::io::{BufRead, BufReader, BufWriter, Write, stdin};
use std::fs::File;
use std::rc::Rc;
use std::cell::RefCell;
use std::thread;
use core::raw::Slice as RawSlice;

use dataflow_join::GenericJoinExt;
use dataflow_join::graph::{GraphTrait, GraphVector, GraphMMap, GraphExtenderExt, gallop};

use timely::progress::Graph;
use timely::progress::scope::Scope;
use timely::progress::graph::Root;
use timely::progress::nested::subgraph::SubgraphBuilder;
use timely::progress::nested::product::Product;
use timely::example::*;
use timely::communication::*;


static USAGE: &'static str = "
Usage: triangles dataflow (text | binary) <source> <workers> [--inspect] [--interactive]
       triangles compute (text | binary) <source>
       triangles digest <source> <target>
       triangles help
";

fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    if args.get_bool("dataflow") {
        let inspect = args.get_bool("--inspect");
        let interactive = args.get_bool("--interactive");
        let workers = if let Ok(threads) = args.get_str("<workers>").parse() { threads }
                      else { panic!("invalid setting for workers: {}", args.get_str("-t")) };;
        println!("starting triangles dataflow with {:?} worker{}; inspection: {:?}, interactive: {:?}", workers, if workers == 1 { "" } else { "s" }, inspect, interactive);
        let source = args.get_str("<source>");
        if args.get_bool("text") {
            let mut graph = livejournal(&source);
            organize_graph(&mut graph);
            triangles_multi(ProcessCommunicator::new_vector(workers), |index, peers| extract_fragment(&graph, index, peers), inspect, interactive);
        }
        if args.get_bool("binary") {
            triangles_multi(ProcessCommunicator::new_vector(workers), |_, _| GraphMMap::new(&source), inspect, interactive);
        }
    }
    if args.get_bool("compute") {
        let source = args.get_str("<source>");
        if args.get_bool("text") {
            let mut graph = livejournal(&source);
            organize_graph(&mut graph);
            let graph = extract_fragment(&graph, 0, 1);
            println!("triangles: {:?}", raw_triangles(&graph));
        }
        if args.get_bool("binary") {
            let graph = GraphMMap::new(&source);
            println!("triangles: {:?}", raw_triangles(&graph));
        }
    }
    if args.get_bool("digest") {
        println!("digest will overwrite <target>.targets and <target>.offsets, so careful");
        println!("at least, it will once you edit the code to uncomment the line.");
        let source = args.get_str("<source>");
        let _target = args.get_str("<target>");
        let mut graph = livejournal(&source);
        organize_graph(&mut graph);
        // _digest_graph_vector(&extract_fragment(&graph, 0, 1), _target); // will overwrite "prefix.offsets" and "prefix.targets"
    }
    if args.get_bool("help") {
        println!("the code presently assumes you have access to the livejournal graph, from:");
        println!("   https://snap.stanford.edu/data/soc-LiveJournal1.html");
        println!("");
        println!("stash that somewhere, and use it as text source for compute or digest.");
        println!("digest will overwrite <target>.targets and <target>.offsets, so careful");
        println!("at least, it will once you edit the code to uncomment the line.")
    }
}

fn intersect<E: Ord>(aaa: &[E], mut bbb: &[E]) -> u64 {
    let mut count = 0;
    for a in aaa {
        bbb = gallop(bbb, a);
        if bbb.len() > 0 && &bbb[0] == a { count += 1; }
    }
    count
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

fn triangles_multi<C: Communicator+Send, G: GraphTrait<Target=u32>, F: Fn(u64, u64)->G+Send+Sync>(communicators: Vec<C>, loader: F, inspect: bool, interactive: bool) {
    let mut guards = Vec::new();
    let loader = &loader;
    for communicator in communicators.into_iter() {
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .scoped(move || triangles(communicator, loader, inspect, interactive))
                                          .unwrap());
    }
}

fn triangles<C: Communicator, G: GraphTrait<Target=u32>, F: Fn(u64, u64)->G>(communicator: C, loader: &F, inspect: bool, interactive: bool) {
    let comm_index = communicator.index();
    let comm_peers = communicator.peers();

    let graph = Rc::new(RefCell::new(loader(comm_index, comm_peers)));

    let mut root = Root::new(communicator);
    let mut input = {
        let borrow = root.builder();
        let mut computation = SubgraphBuilder::new(&borrow);

        let input = {
            let builder = &computation.builder();
            let (input, mut stream) = builder.new_input::<u32>();

            // // extend u32s to pairs, then pairs to triples.
            let mut triangles = stream.extend(vec![&graph.extend_using(|| { |&a| a as u64 } )])
                                      .flat_map(|(p,es)| es.into_iter().map(move |e| (p.clone(), e)))
                                      .extend(vec![&graph.extend_using(|| { |&(a,_)| a as u64 }),
                                                   &graph.extend_using(|| { |&(_,b)| b as u64 })]);
                                    //   .flat_map(|(p,es)| es.into_iter().map(move |e| (p.clone(), e)));

            if inspect { triangles.inspect(|x| println!("triangles: {:?}", x)); }

            input
        };

        computation.seal();
        input
    };

    // computation.0.get_internal_summary();
    // computation.0.set_external_summary(Vec::new(), &mut[]);

    if interactive {
        let mut stdinput = stdin();
        let mut line = String::new();
        for index in (0..) {
            stdinput.read_line(&mut line).unwrap();
            let start = time::precise_time_ns();
            if let Some(word) = line.words().next() {
                let read = word.parse();
                if let Ok(number) = read {
                    input.send_messages(&Product::new((), index as u64), vec![number]);
                }
            }
            line.clear();

            input.advance(&Product::new((), index as u64), &Product::new((), index as u64 + 1));
            for _ in (0..5) { root.step(); }

            println!("elapsed: {:?}us", (time::precise_time_ns() - start)/1000);
        }
    }
    else {
        let step = 1000;
        let nodes = graph.borrow().nodes() - 1;
        let limit = (nodes / step) + 1;
        for index in (0..limit) {
            let index = index as usize;
            let data = ((index * step) as u32 .. ((index + 1) * step) as u32).filter(|&x| x as u64 % comm_peers == comm_index)
                                                                             .filter(|&x| x < nodes as u32)
                                                                             .collect();
            input.send_messages(&Product::new((), index as u64), data);
            input.advance(&Product::new((), index as u64), &Product::new((), index as u64 + 1));
            // computation.0.pull_internal_progress(&mut[], &mut[], &mut[]);
            root.step();
        }
        input.close_at(&Product::new((), limit as u64));
        while root.step() { }
    }
}

fn _quads<'a, 'b: 'a, G: Graph+'b, G2: GraphTrait<Target=u32>>(stream: &mut Stream<'a, 'b, G, ((u32, u32), u32)>, graph: &Rc<RefCell<G2>>) -> Stream<'a, 'b, G, (((u32, u32), u32), u32)> {
    //
    stream.extend(vec![&graph.extend_using(|| { |&((a,_),_)| a as u64 }),
                       &graph.extend_using(|| { |&((_,b),_)| b as u64 }),
                       &graph.extend_using(|| { |&((_,_),c)| c as u64 })])
          .flat_map(|(p,es)| es.into_iter().map(move |e| (p.clone(), e)))
    //
    //  let mut fives = quads.extend(vec![Box::new(fragment.extend_using(|| { |&(((a,_),_),_)| a as u64 })),
    //                                    Box::new(fragment.extend_using(|| { |&(((_,b),_),_)| b as u64 })),
    //                                    Box::new(fragment.extend_using(|| { |&(((_,_),c),_)| c as u64 })),
    //                                    Box::new(fragment.extend_using(|| { |&(((_,_),_),d)| d as u64 }))]).flatten();
    //
    // let mut sixes = fives.extend(vec![Box::new(fragment.extend_using(|| { |&((((a,_),_),_),_)| a as u64 })),
    //                                   Box::new(fragment.extend_using(|| { |&((((_,b),_),_),_)| b as u64 })),
    //                                   Box::new(fragment.extend_using(|| { |&((((_,_),c),_),_)| c as u64 })),
    //                                   Box::new(fragment.extend_using(|| { |&((((_,_),_),d),_)| d as u64 })),
    //                                   Box::new(fragment.extend_using(|| { |&((((_,_),_),_),e)| e as u64 }))]).flatten();
    //
    // sixes.observe();
}

fn redirect(edges: &mut Vec<(u32, u32)>) {
    let mut degrees = Vec::new();
    for &(src, dst) in edges.iter() {
        while src as usize >= degrees.len() { degrees.push(0); }
        while dst as usize >= degrees.len() { degrees.push(0); }
        degrees[src as usize] += 1;
        degrees[dst as usize] += 1;
    }

    for index in (0..edges.len()) {
        if degrees[edges[index].0 as usize] > degrees[edges[index].1 as usize] {
            edges[index] = (edges[index].1, edges[index].0);
        }
    }
}

// loads the livejournal file available at https://snap.stanford.edu/data/soc-LiveJournal1.html
fn livejournal(filename: &str) -> Vec<(u32, u32)> {
    let mut graph = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let elts: Vec<&str> = line[..].words().collect();
            let src: u32 = elts[0].parse().ok().expect("malformed src");
            let dst: u32 = elts[1].parse().ok().expect("malformed dst");
            if src < dst { graph.push((src, dst)) }
            if src > dst { graph.push((dst, src)) }
        }
    }

    println!("graph data loaded; {:?} edges", graph.len());
    return graph;
}

fn organize_graph(graph: &mut Vec<(u32, u32)>) {

    for index in (0..graph.len()) {
        if graph[index].1 < graph[index].0 {
            graph[index] = (graph[index].1, graph[index].0);
        }
    }

    graph.sort();
    println!("graph data sorted; {:?} edges", graph.len());

    graph.dedup();
    println!("graph data uniqed; {:?} edges", graph.len());

    // speeds things up a bit by tweaking edge directions.
    // loses the src < dst property, which can be helpful.
    redirect(graph);
    graph.sort();
    println!("graph data dirctd; {:?} edges", graph.len());
}

fn extract_fragment(graph: &Vec<(u32, u32)>, index: u64, parts: u64) -> GraphVector<u32> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for &(src,dst) in graph {
        if src as u64 % parts == index {
            while src + 1 >= nodes.len() as u32 { nodes.push(0); }
            while dst + 1 >= nodes.len() as u32 { nodes.push(0); } // allows unsafe access to nodes

            nodes[src as usize + 1] += 1;
            edges.push(dst);
        }
    }

    for index in (1..nodes.len()) {
        nodes[index] += nodes[index - 1];
    }

    return GraphVector { nodes: nodes, edges: edges };
}


fn _digest_graph_vector<E: Ord+Copy>(graph: &GraphVector<E>, output_prefix: &str) {
    let mut edge_writer = BufWriter::new(File::create(format!("{}.targets", output_prefix)).unwrap());
    let mut node_writer = BufWriter::new(File::create(format!("{}.offsets", output_prefix)).unwrap());
    node_writer.write_all(unsafe { _typed_as_byte_slice(&graph.nodes[..]) }).unwrap();

    let mut slice = unsafe { _typed_as_byte_slice(&graph.edges[..]) };
    while slice.len() > 0 {
        let to_write = if slice.len() < 1000000 { slice.len() } else { 1000000 };
        edge_writer.write_all(&slice[..to_write]).unwrap();
        println!("wrote some; remaining: {}", slice.len());
        slice = &slice[to_write..];
    }
}

unsafe fn _typed_as_byte_slice<T>(slice: &[T]) -> &[u8] {
    std::mem::transmute(RawSlice {
        data: slice.as_ptr() as *const u8,
        len: slice.len() * std::mem::size_of::<T>(),
    })
}
