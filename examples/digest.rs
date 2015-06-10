extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::slice;
use std::mem;

use dataflow_join::graph::{GraphTrait, GraphVector};

static USAGE: &'static str = "
Usage: digest <source> <target>
";

fn main() {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    println!("digest will overwrite <target>.targets and <target>.offsets, so careful");
    println!("at least, it will once you edit the code to uncomment the line.");
    let source = args.get_str("<source>");
    let _target = args.get_str("<target>");
    let mut graph = livejournal(&source);
    organize_graph(&mut graph);
    _digest_graph_vector(&_extract_fragment(graph.iter().map(|x| *x), 0, 1), _target); // will overwrite "prefix.offsets" and "prefix.targets"

}

// loads the livejournal file available at https://snap.stanford.edu/data/soc-LiveJournal1.html
fn livejournal(filename: &str) -> Vec<(u32, u32)> {
    let mut graph = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let elts: Vec<&str> = line[..].split(" ").collect();
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


fn _extract_fragment<I: Iterator<Item=(u32, u32)>>(graph: I, index: u64, parts: u64) -> GraphVector<u32> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for (src, dst) in graph {
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
    slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * mem::size_of::<T>())
}
