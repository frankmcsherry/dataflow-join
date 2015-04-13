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

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::rc::Rc;
use std::cell::RefCell;
use std::thread;
use core::raw::Slice as RawSlice;

use dataflow_join::{GenericJoinExt, FlattenExt};
use dataflow_join::graph::{GraphTrait, GraphVector, GraphMMap, GraphExtenderExt, gallop};
// use dataflow_join::{PrefixExtender, GenericJoinExt, FlattenExt, TypedMemoryMap};

use timely::progress::scope::Scope;
use timely::progress::subgraph::new_graph;
use timely::example::input::InputExtensionTrait;
use timely::communication::{Data, Communicator, ProcessCommunicator};

use timely::progress::Graph;
use timely::example::stream::Stream;
use timely::example::unary::UnaryExt;
use timely::communication::exchange::Pipeline;
use timely::communication::observer::ObserverSessionExt;
use timely::communication::Pullable;
use columnar::Columnar;

static USAGE: &'static str = "
Usage: triangles compute (text | binary) <source>
       triangles digest <source> <target>
       triangles help
";

fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    if args.get_bool("compute") {
        let workers = 4;
        let source = args.get_str("<source>");
        if args.get_bool("text") {
            let mut graph = livejournal(&source);
            organize_graph(&mut graph);
            triangles_multi(ProcessCommunicator::new_vector(workers), |index, peers| extract_fragment(&graph, index, peers));
        }
        if args.get_bool("binary") {
            triangles_multi(ProcessCommunicator::new_vector(workers), |index, peers| GraphMMap::new(&source));
        }
    }
    if args.get_bool("digest") {
        println!("digest will overwrite <target>.targets and <target>.offsets, so careful");
        println!("at least, it will once you edit the code to uncomment the line.")
        // let source = args.get_str("<source>");
        // let target = args.get_str("<target>");
        // digest_livejournal(&source, &target); // will overwrite "prefix.offsets" and "prefix.targets"
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

fn raw_triangles<G: GraphTrait<Target=u32>>(graph: G) -> u64 {
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

fn triangles_multi<C: Communicator+Send, G: GraphTrait<Target=u32>, F: Fn(u64, u64)->G+Send+Sync>(communicators: Vec<C>, loader: F) {
    let mut guards = Vec::new();
    let loader = &loader;
    for communicator in communicators.into_iter() {
        // let graph = loader(communicator.index(), communicator.peers());
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .scoped(move || triangles(communicator, loader))
                                          .unwrap());
    }
}

fn triangles<C: Communicator, G: GraphTrait<Target=u32>, F: Fn(u64, u64)->G>(communicator: C, loader: &F) {
    let comm_index = communicator.index();
    let comm_peers = communicator.peers();

    let graph = Rc::new(RefCell::new(loader(comm_index, comm_peers)));
    let mut computation = new_graph(communicator);
    let (mut input, mut stream) = computation.new_input::<u32>();

    // extend u32s to pairs, then pairs to triples.
    let mut triangles = stream.extend(vec![&graph.extend_using(|| { |&a| a as u64 } )]).flatten()
                              .extend(vec![&graph.extend_using(|| { |&(a,_)| a as u64 }),
                                           &graph.extend_using(|| { |&(_,b)| b as u64 })]);

    // panic!("triangles: {:?}", graph.borrow().triangles());

    // triangles.observe();
   //  triangles.observe(|&((a,b), c)| println!("triangle: ({:?}, {:?}, {:?})", a, b, c));
   //
   //  let mut quads = triangles.extend(vec![Box::new(fragment.extend_using(|| { |&((a,_),_)| a as u64 })),
   //                                        Box::new(fragment.extend_using(|| { |&((_,b),_)| b as u64 })),
   //                                        Box::new(fragment.extend_using(|| { |&((_,_),c)| c as u64 }))]).flatten();
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
   // sixes.observe(|&x| println!("observed: {:?}", x));

    computation.0.borrow_mut().get_internal_summary();
    computation.0.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());

    // println!("nodes: {:?}", graph.borrow().nodes());
    let mut time = time::precise_time_ns();
    let step = 1000;
    let limit = (graph.borrow().nodes() / step) + 1;
    for index in (0..limit) {
        let index = index as usize;
        let data = ((index * step) as u32 .. ((index + 1) * step) as u32).filter(|&x| x as u64 % comm_peers == comm_index).collect();
        input.send_messages(&((), index as u64), data);
        input.advance(&((), index as u64), &((), index as u64 + 1));
        computation.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());
        // println!("enumerated triangles from ({:?}..{:?}) in {:?}ns", step * index, step * (index + 1), (time::precise_time_ns() - time));
        time = time::precise_time_ns();
    }

    input.close_at(&((), limit as u64));
    while computation.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
}

fn redirect(edges: &mut Vec<(u32, u32)>) {
    let mut degrees = vec![0u64; 5000000];
    for &(src, dst) in edges.iter() {
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
            while src + 1 >= nodes.len() as u32 {
                nodes.push(0);
            }

            nodes[src as usize + 1] += 1;
            edges.push(dst);
        }
    }

    for index in (1..nodes.len()) {
        nodes[index] += nodes[index - 1];
    }

    return GraphVector { nodes: nodes, edges: edges };
}


fn digest_graph_vector<E: Ord+Copy>(graph: &GraphVector<E>, output_prefix: &String) {
    let mut edge_writer = BufWriter::new(File::create(format!("{}.targets", output_prefix)).unwrap());
    let mut node_writer = BufWriter::new(File::create(format!("{}.offsets", output_prefix)).unwrap());
    node_writer.write_all(unsafe { typed_as_byte_slice(&graph.nodes[..]) }).unwrap();
    edge_writer.write_all(unsafe { typed_as_byte_slice(&graph.edges[..]) }).unwrap();
}

unsafe fn typed_as_byte_slice<T>(slice: &[T]) -> &[u8] {
    std::mem::transmute(RawSlice {
        data: slice.as_ptr() as *const u8,
        len: slice.len() * std::mem::size_of::<T>(),
    })
}



// TODO : This should probably be in core timely dataflow
pub trait ObserveExt<G: Graph, D: Data+Columnar> {
    fn observe(&mut self) -> Self;
}

impl<G: Graph, D: Data+Columnar> ObserveExt<G, D> for Stream<G, D> {
    fn observe(&mut self) -> Stream<G, D> {
        let mut counter = 0u64;
        self.unary(Pipeline, format!("Observe"), move |handle| {
            while let Some((time, data)) = handle.input.pull() {
                let mut session = handle.output.session(&time);
                counter += data.len() as u64;
                println!("seen: {:?} records", counter);
                for datum in data {
                    session.give(datum);
                }
            }
        })
    }
}
