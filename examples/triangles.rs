#![feature(core)]
#![feature(str_words)]

extern crate time;
extern crate core;
extern crate timely;
extern crate columnar;
extern crate dataflow_join;

use std::io::{BufRead, BufReader};
use std::fs::File;
use std::rc::Rc;
use std::cell::RefCell;
use core::marker::PhantomData;

use dataflow_join::{PrefixExtender, GenericJoinExt, FlattenExt, ObserveExt};

use timely::progress::scope::Scope;
use timely::progress::subgraph::new_graph;
use timely::example::input::InputExtensionTrait;
use timely::communication::{Data, Communicator, ThreadCommunicator};

use columnar::Columnar;

trait GraphExtenderExt<E: Data+Columnar+Ord> {
    fn extend_using<P:Data+Columnar, L: Fn(&P)->u64+'static, F: Fn()->L+'static>(&self, func: F) -> Rc<RefCell<GraphFragmentExtender<E,P,L,F>>>;
}

impl<E: Data+Columnar+Ord> GraphExtenderExt<E> for Rc<RefCell<GraphFragment<E>>> {
    fn extend_using<P:Data+Columnar, L: Fn(&P)->u64+'static, F: Fn()->L+'static>(&self, func: F) -> Rc<RefCell<GraphFragmentExtender<E,P,L,F>>> {
        let logic = func();

        Rc::new(RefCell::new(GraphFragmentExtender {
            graph:           self.clone(),
            logic:           logic,
            logic_generator: func,
            phant:           PhantomData,
        }))
    }
}

pub struct GraphFragment<E: Data+Columnar+Ord> {
    nodes: Vec<usize>,
    edges: Vec<E>,
}

impl<E: Data+Columnar+Ord> GraphFragment<E> {
    fn edges(&self, node: usize) -> &[E] {
        if node + 1 < self.nodes.len() {
            &self.edges[self.nodes[node]..self.nodes[node+1]]
        }
        else { &[] }
    }
}

pub struct GraphFragmentExtender<E: Data+Columnar+Ord, P, L: Fn(&P)->u64, F:Fn()->L> {
    graph: Rc<RefCell<GraphFragment<E>>>,
    logic: L,
    logic_generator: F,
    phant: PhantomData<P>,
}

impl<E: Data+Columnar+Ord, P:Data+Columnar, L: Fn(&P)->u64+'static, F:Fn()->L+'static> PrefixExtender<P, E> for GraphFragmentExtender<E, P, L, F> {
    type RoutingFunction = L;
    fn route(&self) -> L { (self.logic_generator)() }

    fn count(&self, (prefix, p_count, p_index): (P, u64, u64), id: u64) -> (P, u64, u64) {
        let node = (self.logic)(&prefix) as usize;
        let graph = self.graph.borrow();
        let count = graph.edges(node).len() as u64;
        if count < p_count { (prefix, count, id) }
        else               { (prefix, p_count, p_index) }
    }

    fn propose(&self, prefix: P) -> (P, Vec<E>) {
        let node = (self.logic)(&prefix) as usize;
        let graph = self.graph.borrow();
        (prefix, graph.edges(node).to_vec())
    }

    fn intersect(&self, (prefix, mut list): (P, Vec<E>)) -> (P, Vec<E>) {
        let node = (self.logic)(&prefix) as usize;
        let graph = self.graph.borrow();
        let mut slice = graph.edges(node);
        list.retain(move |value| {
            slice = gallop(slice, value);
            slice.len() > 0 && &slice[0] == value
        });

        (prefix, list)
    }
}

// intended to advance slice to start at the first element >= value.
pub fn gallop<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already >= element, return
    if slice.len() > 0 && &slice[0] < value {
        let mut step = 1;
        // while step < slice.len() && &slice[step] < value {
        while step < slice.len() && &slice[step] < value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] < value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < value
    }

    return slice;
}

fn main () {
    println!("using a bogus graph for now; check out the source to point it at real data");
    let graph = GraphFragment { nodes: vec![0, 4, 7, 9, 10, 10], edges: vec![1, 2, 3, 4, 2, 3, 4, 3, 4, 4] };
    // let graph = livejournal(format!("path/to/soc-LiveJournal1.txt"), 100000000);
    let graph = Rc::new(RefCell::new(graph));

    triangles(ThreadCommunicator, |_index, _peers| graph.clone());
}

fn triangles<C: Communicator, F: Fn(u64, u64)->Rc<RefCell<GraphFragment<u32>>>>(communicator: C, loader: F) {
    let fragment = loader(communicator.index(), communicator.peers());
    let mut graph = new_graph(communicator);
    let (mut input, mut stream) = graph.new_input::<u32>();

    // extend u32s to pairs, then pairs to triples.
    let mut triangles = stream.generic_join_layer(vec![Box::new(fragment.extend_using(|| { |&a| a as u64 }))]).flatten()
                              .generic_join_layer(vec![Box::new(fragment.extend_using(|| { |&(a,_)| a as u64 })),
                                                       Box::new(fragment.extend_using(|| { |&(_,b)| b as u64 }))]).flatten();

    triangles.observe(|&((a,b), c)| println!("triangle: ({:?}, {:?}, {:?})", a, b, c));

   //  let mut quads = triangles.generic_join_layer(vec![Box::new(fragment.extend_using(|| { |&((a,_),_)| a as u64 })),
   //                                                    Box::new(fragment.extend_using(|| { |&((_,b),_)| b as u64 })),
   //                                                    Box::new(fragment.extend_using(|| { |&((_,_),c)| c as u64 }))]).flatten();
   //
   //  let mut fives = quads.generic_join_layer(vec![Box::new(fragment.extend_using(|| { |&(((a,_),_),_)| a as u64 })),
   //                                                Box::new(fragment.extend_using(|| { |&(((_,b),_),_)| b as u64 })),
   //                                                Box::new(fragment.extend_using(|| { |&(((_,_),c),_)| c as u64 })),
   //                                                Box::new(fragment.extend_using(|| { |&(((_,_),_),d)| d as u64 }))]).flatten();
   //
   // let mut sixes = next.generic_join_layer(vec![Box::new(fragment.extend_using(|| { |&((((a,_),_),_),_)| a as u64 })),
   //                                              Box::new(fragment.extend_using(|| { |&((((_,b),_),_),_)| b as u64 })),
   //                                              Box::new(fragment.extend_using(|| { |&((((_,_),c),_),_)| c as u64 })),
   //                                              Box::new(fragment.extend_using(|| { |&((((_,_),_),d),_)| d as u64 })),
   //                                              Box::new(fragment.extend_using(|| { |&((((_,_),_),_),e)| e as u64 }))]).flatten();

    graph.0.borrow_mut().get_internal_summary();
    graph.0.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());

    println!("nodes: {:?}", fragment.borrow().nodes.len());
    let mut time = time::precise_time_ns();
    let step = 1;
    let limit = fragment.borrow().nodes.len() / step;
    for index in (0..limit) {
        let index = index as usize;
        let data = ((index * step) as u32 .. ((index + 1) * step) as u32).collect();
        input.send_messages(&((), index as u64), data);
        input.advance(&((), index as u64), &((), index as u64 + 1));
        graph.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());
        println!("enumerated triangles from ({:?}..{:?}) in {:?}ns", step * index, step * (index + 1), (time::precise_time_ns() - time));
        time = time::precise_time_ns();
    }

    input.close_at(&((), limit as u64));
    while graph.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
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
fn livejournal(filename: String, limit: u64) -> GraphFragment<u32> {
    let mut temp = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let elts: Vec<&str> = line[..].words().collect();
            let src: u32 = elts[0].parse().ok().expect("malformed src");
            let dst: u32 = elts[1].parse().ok().expect("malformed dst");

            if src < dst { temp.push((src, dst)) }
            if src > dst { temp.push((dst, src)) }
            if temp.len() >= limit as usize {
                break;
            }
        }
    }

    println!("graph data loaded; {:?} edges", temp.len());

    temp.sort();
    println!("graph data sorted; {:?} edges", temp.len());

    temp.dedup();
    println!("graph data uniqed; {:?} edges", temp.len());

    redirect(&mut temp);
    temp.sort();
    println!("graph data dirctd; {:?} edges", temp.len());

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    nodes.push(0);
    for &(src,dst) in &temp {
        while src + 1 >= nodes.len() as u32 {
            nodes.push(0);
        }

        nodes[src as usize + 1] += 1;
        edges.push(dst);
    }

    for index in (1..nodes.len()) {
        nodes[index] += nodes[index - 1];
    }

    return GraphFragment { nodes: nodes, edges: edges };
}
