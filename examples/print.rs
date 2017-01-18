extern crate rand;
extern crate graph_map;

use rand::Rng;
use graph_map::GraphMMap;

fn main() {

    let filename = std::env::args().skip(1).next().unwrap();
    let graph = GraphMMap::new(&filename);
    let mut edges = vec![];
    for node in 0 .. graph.nodes() {
        for &edge in graph.edges(node) {
        	edges.push((node, edge));
        }
    }

	let mut rng = rand::thread_rng();
    rng.shuffle(&mut edges);

    for (src, dst) in edges {
    	println!("{}\t{}", src, dst);
    }
}