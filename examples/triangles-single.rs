extern crate mmap;
extern crate time;
extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use std::cmp::Ordering::*;

use dataflow_join::graph::{GraphTrait, GraphMMap, gallop};

static USAGE: &'static str = "
Usage: triangles dataflow <source> <workers> <stepsize> [--inspect] [--interactive] [--alt]
       triangles autorun <source> <workers> <stepsize>
       triangles compute <source>
       triangles help
";



fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let source = args.get_str("<source>");
    let graph = GraphMMap::new(&source);
    println!("triangles: {:?}", raw_triangles(&graph));
}

fn raw_triangles<G: GraphTrait<Target=u32>>(graph: &G) -> u64 {
    let mut count = 0;
    for a in (0..graph.nodes()) {
        let aaa = graph.edges(a);
        for &b in aaa {
            let bbb = graph.edges(b as usize);
            count += if aaa.len() < bbb.len() { intersect(aaa, bbb) }
                     else                     { intersect(bbb, aaa) };
        }
    }
    count
}

fn intersect<E: Ord>(mut aaa: &[E], mut bbb: &[E]) -> u64 {
    let mut count = 0;
    // magic gallop overhead # is 4
    if aaa.len() < bbb.len() / 4 {
        for a in aaa {
            bbb = gallop(bbb, a);
            if bbb.len() > 0 && &bbb[0] == a { count += 1; }
        }
    }
    else {
        while aaa.len() > 0 && bbb.len() > 0 {
            match aaa[0].cmp(&bbb[0]) {
                Greater => { bbb = &bbb[1..]; },
                Less    => { aaa = &aaa[1..]; },
                Equal   => { aaa = &aaa[1..];
                             bbb = &bbb[1..];
                             count += 1;
                           },
            }
        }
    }
    count
}
