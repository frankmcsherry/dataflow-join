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
        let mut prefix1 = Vec::new();
        let mut prefix2 = Vec::new();
        let mut v1 = index;
        while v1 < graph.nodes() {
            let v1f = graph.forward(v1);
            for (index_v2, &v2) in v1f.iter().enumerate() {
                intersect_and(&v1f[(index_v2 + 1)..], graph.forward(v2), |v3| prefix1.push(v3));
                for (index_v3, &v3) in prefix1.iter().enumerate() {
                    intersect_and(&prefix1[(index_v3 + 1)..], graph.forward(v3), |v4| prefix2.push(v4));
                    for (index_v4, &v4) in prefix2.iter().enumerate() {
                        intersect_and(&prefix2[(index_v4 + 1)..], graph.forward(v4), |v5| if v5 != u32::max_value() { count += 1 });
                    }
                    prefix2.clear();
                }
                prefix1.clear();
            }
            v1 += peers;
        }
        println!("{:?}\tworker {:?}/{:?}:\tcount: {:?}", timer.elapsed(), index, peers, count);

    }).unwrap();
}