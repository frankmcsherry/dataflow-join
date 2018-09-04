extern crate rand;
extern crate core_affinity;
extern crate timely;
extern crate dataflow_join;

use std::rc::Rc;

use dataflow_join::*;
use dataflow_join::graph::{GraphTrait, GraphMMap, GraphExtenderExt};

use timely::dataflow::operators::*;
use timely::dataflow::operators::aggregation::Aggregate;

fn main () {

    let filename = std::env::args().nth(1).unwrap();
    let inspect = std::env::args().nth(2).unwrap() == "inspect";
    let step_size = std::env::args().nth(3).unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args().skip(4), move |root| {

        let index = root.index();
        let peers = root.peers();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index % core_ids.len()]);

        let graph = Rc::new(GraphMMap::<u32>::new(&filename));

        let (mut input, probe) = root.dataflow(|builder| {

            let (input, edges) = builder.new_input::<(u32, u32)>();

            // // pairs
            // let cliques = cliques.extend(vec![&graph.extend_using(|&a| a as u64)]);

            // triangles
            let cliques = edges.extend(vec![&graph.extend_using(|&(a,_)| a as u64)]);

            // quadrangles?
            let cliques = cliques.flat_map(|(p,es)| es.into_iter().map(move |e| (p, e)))
                                 .filter(|&((_,b),c)| b < c)
                                 .extend(vec![&graph.extend_using(|&((_,b),_)| b as u64),
                                              &graph.extend_using(|&((_,_),c)| c as u64)]);

            // // 5 cliques?
            // let cliques = cliques.flat_map(|(p,es)| es.into_iter().map(move |e| (p, e)))
            //                      .extend(vec![&graph.extend_using(|&(((a,_),_),_)| a as u64),
            //                                   &graph.extend_using(|&(((_,b),_),_)| b as u64),
            //                                   &graph.extend_using(|&(((_,_),c),_)| c as u64),
            //                                   &graph.extend_using(|&(((_,_),_),d)| d as u64)]);

            let mut count = 0;
            if inspect {
                cliques
                    .map(|x| ((), x.1.len()))
                    .aggregate(|_k, v: usize, a| *a += v, |_k, a: usize| a, |_k| 0)
                    .inspect_batch(move |_t, b| {
                        for x in b { count += *x; }
                        println!("count: {}", count);
                    });
            }

            (input, cliques.probe())
        });

        let mut edges = Vec::new();
        for node in 0 .. graph.nodes() {
            for &edge in graph.edges(node) {
                if (node + (edge as usize)) % peers == index {
                    edges.push((node as u32, edge));
                }
            }
        }
        use rand::prelude::*;
        let mut rng = thread_rng();
        rng.shuffle(&mut edges);

        let mut round = 0;
        while !edges.is_empty() {
            let to_take = std::cmp::min(edges.len(), step_size / peers);
            for _index in 0 .. to_take {
                input.send(edges.pop().unwrap());
            }
            input.advance_to(round as u64 + 1);
            round += 1;
            while probe.less_than(input.time()) {
                root.step();
            }
        }

    }).unwrap();
}
