extern crate mmap;
extern crate time;
extern crate timely;
extern crate columnar;
extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use dataflow_join::graph::{GraphTrait, GraphMMap};

static USAGE: &'static str = "
Usage: pagerank <source>
";

fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());
    let source = args.get_str("<source>");

    pagerank(&GraphMMap::new(&source), 20);
}

fn pagerank<G: GraphTrait<Target=u32>>(graph: &G, iterations: usize) {

    let mut src = vec![1.0f32; graph.nodes()];
    let mut dst = vec![0.0f32; graph.nodes()];

    let mut start = time::precise_time_s();

    for iteration in 0..iterations {
        for node in 0..src.len() {
            src[node] = 0.15 + 0.85 * src[node];
        }
        for node in 0..src.len() {
            let edges = graph.edges(node);
            let value = src[node] / edges.len() as f32;
            for &edge in edges {
                dst[edge as usize] += value;
            }
        }
        for node in 0..src.len() { src[node] = dst[node]; dst[node] = 0.0; }

        println!("{}s", time::precise_time_s() - start);
        start = time::precise_time_s();
    }
}
