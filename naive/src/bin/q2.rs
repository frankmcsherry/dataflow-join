extern crate timely;
extern crate naive;

use naive::{GraphMap, intersect_and};

fn main () {

    let filename = std::env::args().nth(1).unwrap();
    let optimized = std::env::args().any(|x| x == "optimized");

    timely::execute_from_args(std::env::args(), move |root| {

        let timer = std::time::Instant::now();
        let index = root.index() as u32;
        let peers = root.peers() as u32;
        let graph = GraphMap::new(&filename);

        let mut count: usize = 0;
        let mut prefix = Vec::new();
        let mut v1 = index;
        while v1 < graph.nodes() {
            let v1f = graph.forward(v1);
            for &v3 in v1f.iter() {
                intersect_and(graph.edges(v1), graph.edges(v3), |x| prefix.push(x));
                if optimized {
                    count += (prefix.len() * (prefix.len() - 1)) / 2;
                }
                else {
                    for (index_v2, &_v2) in prefix.iter().enumerate() {
                        for &v4 in prefix[(index_v2 + 1)..].iter() {
                            if _v2 != u32::max_value() && v4 != u32::max_value() { count += 1 }
                         }
                    }
                }
                prefix.clear();
            }
            v1 += peers;
        }
        println!("{:?}\tworker {:?}/{:?}:\tcount: {:?}", timer.elapsed(), index, peers, count);

    }).unwrap();
}