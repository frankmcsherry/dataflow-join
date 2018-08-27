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

        let mut count: usize = 0;
        let mut prefix = Vec::new();
        let mut v2 = index;
        while v2 < graph.nodes() {
            let v2f = graph.forward(v2);
            for &v5 in v2f.iter() {
                intersect_and(graph.edges(v2), graph.edges(v5), |x| prefix.push(x));
                for (index_v3, &v3) in prefix.iter().enumerate() {
                    intersect_and(&prefix[(index_v3 + 1)..], graph.forward(v3), |v4| {
                        for &v5 in prefix.iter() {
                            if v5 != v3 && v5 != v4 {
                                count += 1;
                            }
                        }
                    });
                }
                prefix.clear();
            }
            v2 += peers;
        }
        println!("{:?}\tworker {:?}/{:?}:\tcount: {:?}", timer.elapsed(), index, peers, count);

    }).unwrap();
}