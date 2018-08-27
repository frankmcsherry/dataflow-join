extern crate timely;
extern crate naive;

use naive::{GraphMap, intersect_and};

fn main () {

    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args(), move |root| {

        let timer = std::time::Instant::now();
        let index = root.index() as u32;
        let peers = root.peers() as u32;
        let graph = GraphMap::new(&filename);

        let count: usize = 0;
        let mut tris = Vec::new();
        let mut v1 = index;
        while v1 < graph.nodes() {
            let v1e = graph.edges(v1);
            for (index_b, &b) in v1e.iter().enumerate() {
                intersect_and(&v1e[(index_b + 1)..], graph.forward(b), |c| tris.push((b, c)));
            }
            // enumerate 4-paths in `tris`.
            tris.clear();
            v1 += peers;
        }
        println!("{:?}\tworker {:?}/{:?}:\tcount: {:?}", timer.elapsed(), index, peers, count);

    }).unwrap();
}