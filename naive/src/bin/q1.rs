extern crate timely;
extern crate naive;

use naive::{GraphMap, gallop_gt, intersect_and};

fn main () {

    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |root| {

        let timer = std::time::Instant::now();
        let index = root.index() as u32;
        let peers = root.peers() as u32;
        let graph = GraphMap::new(&filename);

        let mut count: usize = 0;
        let mut v1 = index;
        while v1 < graph.nodes() {
            let v1f = graph.forward(v1);
            for (index_v2, &v2) in v1f.iter().enumerate() {
                let v2e = gallop_gt(graph.edges(v2), &v1);
                for &v4 in v1f[(index_v2 + 1)..].iter() {
                    let v4e = gallop_gt(graph.edges(v4), &v1);
                    intersect_and(v2e, v4e, |v3| if v3 != u32::max_value() { count += 1 });
                }
            }
            v1 += peers;
        }
        println!("{:?}\tworker {:?}/{:?}:\tcount: {:?}", timer.elapsed(), index, peers, count);

    }).unwrap();
}