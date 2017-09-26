extern crate graph_map;

use std::sync::Arc;
use graph_map::GraphMMap;

struct GraphMap {
    map: GraphMMap,
    reverse: Vec<u32>,
}

impl GraphMap {
    pub fn new(filename: &str) -> Self {

        let map = GraphMMap::new(filename);

        let mut reverse = vec![0; map.nodes()];
        for node in 0 .. map.nodes() {
            for &neighbor in map.edges(node) {
                if (neighbor as usize) < node {
                    reverse[node] += 1;
                }
                if (neighbor as usize) == node {
                    panic!("self-loop");
                }
            }
        }

        GraphMap {
            map: map,
            reverse: reverse,
        }
    }

    #[inline(always)]
    pub fn nodes(&self) -> u32 { self.map.nodes() as u32 }
    #[inline(always)]
    pub fn edges(&self, node: u32) -> &[u32] { self.map.edges(node as usize) }
    #[inline(always)]
    pub fn forward(&self, node: u32) -> &[u32] { 
        &self.edges(node)[(self.reverse[node as usize] as usize)..]
    }
}

fn sum_sequential<F: Fn(&GraphMap, u32, u32)->usize>(graph: &Arc<GraphMap>, peers: u32, logic: F) -> (usize, ::std::time::Duration) {
    let mut sum = 0;
    let mut max_time = Default::default();
    for index in 0 .. peers {
        let timer = ::std::time::Instant::now();
        sum += logic(&*graph, index, peers);
        let elapsed = timer.elapsed();
        if max_time < elapsed { max_time = elapsed; }
    }
    (sum, max_time)
}

fn sum_parallel<F: Fn(&GraphMap, u32, u32)->usize+Sync+Send+'static>(graph: &Arc<GraphMap>, peers: u32, logic: F) -> (usize, ::std::time::Duration) {
    let logic = Arc::new(logic);
    let timer = ::std::time::Instant::now();
    let mut handles = Vec::new();
    for index in 0 .. peers {
        let graph = graph.clone();
        let logic = logic.clone();
        handles.push(::std::thread::spawn(move || { (*logic)(&*graph, index, peers) }));
    }
    let sum: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    (sum, timer.elapsed())
}

fn main () {

    let source = std::env::args().nth(1).unwrap();
    let threads = std::env::args().nth(2).unwrap().parse::<u32>().unwrap();

    let graph = ::std::sync::Arc::new(GraphMap::new(&source));

    let edges: usize = (0 .. graph.nodes()).map(|i| graph.edges(i).len()).sum();
    println!("graph loaded: {:?} nodes, {:?} edges", graph.nodes(), edges);

    // Triange measurements
    let (count, time) = sum_parallel(&graph, threads, q0);
    // let (count, time) = sum_sequential(&graph, threads, q0);
    println!("{:?}\tq0: {:?}", time, count);

    // Four-cycle measurements
    let (count, time) = sum_parallel(&graph, threads, q1);
    // let (count, time) = sum_sequential(&graph, threads, q1);
    println!("{:?}\tq1: {:?}", time, count);

    // 2-strut measurements
    let (count, time) = sum_parallel(&graph, threads, q2);
    // let (count, time) = sum_sequential(&graph, threads, q2);
    println!("{:?}\tq2: {:?}", time, count);

    // 2-strut measurements
    let (count, time) = sum_parallel(&graph, threads, q2_star);
    // let (count, time) = sum_sequential(&graph, threads, q2_star);
    println!("{:?}\tq2*: {:?}", time, count);

    // Four-clique measurements
    let (count, time) = sum_parallel(&graph, threads, q3);
    // let (count, time) = sum_sequential(&graph, threads, q3);
    println!("{:?}\tq3: {:?}", time, count);

    // Fan measurements
    let (count, time) = sum_parallel(&graph, threads, q5_star);
    // let (count, time) = sum_sequential(&graph, threads, q5_star);
    println!("{:?}\tq5*: {:?}", time, count);

    // Four-clique with hat measurements.
    let (count, time) = sum_parallel(&graph, threads, q6);
    // let (count, time) = sum_sequential(&graph, threads, q6);
    println!("{:?}\tq6: {:?}", time, count);

    // Four-clique with hat measurements.
    let (count, time) = sum_parallel(&graph, threads, q6_star);
    // let (count, time) = sum_sequential(&graph, threads, q6_star);
    println!("{:?}\tq6*: {:?}", time, count);

    // Five-clique measurements.
    let (count, time) = sum_parallel(&graph, threads, q7);
    // let (count, time) = sum_sequential(&graph, threads, q7);
    println!("{:?}\tq7: {:?}", time, count);
}

/// Q0: Triangle
///
/// Q0(a,b,c) := a ~ b, b ~ c, a ~ c, a < b, b < c
fn q0(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
    let mut v1 = index;
    while v1 < graph.nodes() {
        let v1f = graph.forward(v1);
        for (index_v2, &v2) in v1f.iter().enumerate() {
            intersect_and(&v1f[(index_v2+1)..], graph.forward(v2), |v3| if v3 != u32::max_value() { count += 1 });
        }
        v1 += peers;
    }
    count
}

/// Q1: Four-cycle
fn q1(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
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
    count
}


/// Q2: 2-Strut
fn q2(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
    let mut prefix = Vec::new();
    let mut v1 = index;
    while v1 < graph.nodes() {
        let v1f = graph.forward(v1);
        for &v3 in v1f.iter() {
            intersect_and(graph.edges(v1), graph.edges(v3), |x| prefix.push(x));
            // count += (prefix.len() * (prefix.len() - 1)) / 2;
            for (index_v2, &_v2) in prefix.iter().enumerate() {
                for &v4 in prefix[(index_v2 + 1)..].iter() {
                    if _v2 != u32::max_value() && v4 != u32::max_value() { count += 1 }
                 }
            }
            prefix.clear();
        }
        v1 += peers;
    }
    count
}

/// Q2*: 2-Strut
fn q2_star(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
    let mut prefix = Vec::new();
    let mut v1 = index;
    while v1 < graph.nodes() {
        let v1f = graph.forward(v1);
        for &v3 in v1f.iter() {
            intersect_and(graph.edges(v1), graph.edges(v3), |x| prefix.push(x));
            count += (prefix.len() * (prefix.len() - 1)) / 2;
            prefix.clear();
        }
        v1 += peers;
    }
    count
}

// Q3: Four-clique
fn q3(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
    let mut prefix = Vec::new();
    let mut v1 = index;
    while v1 < graph.nodes() {
        let v1f = graph.forward(v1);
        for (index_v2, &v2) in v1f.iter().enumerate() {
            intersect_and(&v1f[(index_v2 + 1)..], graph.forward(v2), |v3| prefix.push(v3));
            for (index_v3, &v3) in prefix.iter().enumerate() {
                intersect_and(&prefix[(index_v3 + 1)..], graph.forward(v3), |v4| if v4 != u32::max_value() { count += 1 });
            }
            prefix.clear();
        }
        v1 += peers;
    }
    count
}

/// Q5*: Fan
fn q5_star(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let count = 0;
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
    count
}

/// Q6: Four-clique with hat
fn q6(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
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
    count
}

/// Q6: Four-clique with hat
fn q6_star(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
    let mut prefix = Vec::new();
    let mut v2 = index;
    while v2 < graph.nodes() {
        let v2f = graph.forward(v2);
        for &v5 in v2f.iter() {
            intersect_and(graph.edges(v2), graph.edges(v5), |x| prefix.push(x));
            for (index_v3, &v3) in prefix.iter().enumerate() {
                intersect_and(&prefix[(index_v3 + 1)..], graph.forward(v3), |_v4| {
                    count += prefix.len() - 2;
                });
            }
            prefix.clear();
        }
        v2 += peers;
    }
    count
}

// Q7: Five-clique
fn q7(graph: &GraphMap, index: u32, peers: u32) -> usize {
    let mut count = 0;
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
    count
}

// fn intersect_many_into(lists: &mut [&[u32]], dest: &mut Vec<u32>) {
//     if lists.len() > 0 {
//         lists.sort_by(|x,y| x.len().cmp(&y.len()));
//         dest.extend_from_slice(lists[0]);
//         let mut lists = &lists[1..];
//         while lists.len() > 0 {
//             let (head, tail) = lists.split_at(1);
//             restrict(dest, head[0]);
//             lists = tail;
//         }
//     }
// }

// fn restrict(aaa: &mut Vec<u32>, mut bbb: &[u32]) {
//
//     let mut cursor = 0;
//     if aaa.len() < bbb.len() / 16 {
//         for index in 0 .. aaa.len() {
//             bbb = gallop(bbb, &aaa[index]);
//             if bbb.len() > 0 && bbb[0] == aaa[index] {
//                 aaa[cursor] = aaa[index];
//                 cursor += 1;
//             }
//         }
//     }
//     else {
//         let mut index = 0;
//         while index < aaa.len() {
//             while bbb.len() > 0 && bbb[0] < aaa[index] {
//                 bbb = &bbb[1..];
//             }
//             if bbb.len() > 0 && aaa[index] == bbb[0] {
//                 aaa[cursor] = aaa[index];
//                 cursor += 1;
//             }
//             index += 1;
//         }
//     }
//     aaa.truncate(cursor);
// }

fn intersect_and<F: FnMut(u32)>(aaa: &[u32], mut bbb: &[u32], mut func: F) {

    if aaa.len() > bbb.len() {
        intersect_and(bbb, aaa, func);
    }
    else {
        if aaa.len() < bbb.len() / 16 {
            for &a in aaa.iter() {
                bbb = gallop_ge(bbb, &a);
                if bbb.len() > 0 && bbb[0] == a {
                    func(a)
                }
            }
        }
        else {
            for &a in aaa.iter() {
                while bbb.len() > 0 && bbb[0] < a {
                    bbb = &bbb[1..];
                }
                if bbb.len() > 0 && a == bbb[0] {
                    func(a);
                }
            }
        }
    }
}

#[inline(always)]
pub fn gallop_ge<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already >= element, return
    if slice.len() > 0 && &slice[0] < value {
        let mut step = 1;
        while step < slice.len() && &slice[step] < value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] < value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < value
    }

    return slice;
}

#[inline(always)]
pub fn gallop_gt<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already > element, return
    if slice.len() > 0 && &slice[0] <= value {
        let mut step = 1;
        while step < slice.len() && &slice[step] <= value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] <= value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed <= value
    }

    return slice;
}
