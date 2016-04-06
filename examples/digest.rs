extern crate dataflow_join;
extern crate timely_sort;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::slice;
use std::mem;

use dataflow_join::graph::{GraphTrait, GraphVector};

fn main() {
    // println!("Usage: digest <source> <target>");
    let source = std::env::args().skip(1).next().unwrap();
    let target = std::env::args().skip(2).next().unwrap();


    let mut graph = read_from_text(&source);
    _digest_graph_vector(&_extract_fragment(graph.into_iter().flat_map(|x| x.into_iter())), &target); // will overwrite "prefix.offsets" and "prefix.targets"

}

fn read_from_text(filename: &str) -> Vec<Vec<(u32, u32)>> {
    let mut sorter = timely_sort::LSBRadixSorter::new();
    let file = BufReader::new(File::open(filename).unwrap());

    let mut chunks = Vec::new();
    let mut chunk = Vec::with_capacity(1024);

    let mut max = 0;

    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let mut elts = line[..].split_whitespace();
            let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
            let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");

            let (src, dst) = if src < dst { (src, dst) } else { (dst, src) };
            if src != dst {
                chunk.push((src, dst));
            }
            if chunk.len() == chunk.capacity() {
                chunks.push(mem::replace(&mut chunk, Vec::with_capacity(1024)));
            }
        }
    }

    chunks.push(chunk);

    // let mut map = HashMap::new();
    // for chunk in &chunks {
    //     for &(src, dst) in chunk {
    //         let len = map.len();
    //         map.entry(src).or_insert(len as u32);
    //         let len = map.len();
    //         map.entry(dst).or_insert(len as u32);
    //     }
    // }

    // for chunk in &mut chunks {
    //     for src_dst in chunk {
    //         *src_dst = (map[&src_dst.0], map[&src_dst.1]);
    //     }
    // }

    // determine the maximum node identifier.
    // let mut max_node = 0;
    // for chunk in &chunks {
    //     for &(src,dst) in chunk {
    //         if max_node < src { max_node = src; }
    //         if max_node < dst { max_node = dst; }
    //     }
    // }

    // // determine the undirected degree of each node.
    // let mut degrees = vec![0; max_node as usize + 1];
    // for chunk in &chunks {
    //     for &(src, dst) in chunk {
    //         degrees[src as usize] += 1;
    //         degrees[dst as usize] += 1;
    //     }
    // }

    // // swing edges from low degree to high degree.
    // for chunk in &mut chunks {
    //     for src_dst in chunk {
    //         if degrees[src_dst.0 as usize] > degrees[src_dst.1 as usize] {
    //             *src_dst = (src_dst.1, src_dst.0);
    //         }
    //     }
    // }

    // sort the edges by source then destination, and deduplicate them.
    sorter.sort(&mut chunks, &|&(x,y)| ((x as u64) << 32) + (y as u64));
    let mut prev = (u32::max_value(), u32::max_value());
    for chunk in &mut chunks {
        chunk.retain(|&(x,y)| if (x,y) != prev { prev = (x,y); true } else { false });
    }

    return chunks;
}

fn _extract_fragment<I: Iterator<Item=(u32, u32)>>(graph: I) -> GraphVector<u32> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    nodes.push(0);
    let mut max_dst = 0;
    for (src, dst) in graph {
        // println!("{:?}", (src, dst));
        if src == dst { println!("early error?"); }
        while nodes.len() <= src as usize {
            nodes.push(edges.len() as u64);
        }

        if max_dst < dst { max_dst = dst; }
        edges.push(dst);
    }

    while nodes.len() <= (max_dst + 1) as usize { nodes.push(edges.len() as u64); }

    println!("nodes.len(): {}", nodes.len());

    let result = GraphVector { nodes: nodes, edges: edges };

    for node in 0..(result.nodes.len() - 1) {
        for &edge in &result.edges[result.nodes[node] as usize .. result.nodes[node + 1] as usize] {
            if node == edge as usize {
                println!("error? {} == {}", node, edge);
            }
        }
    }

    return result;
}

fn _print(graph: &GraphVector<u32>, _output: &str) {
    // let mut edge_writer = BufWriter::new(File::create(output).unwrap());

    let mut largest = 0;
    for node in 0..graph.nodes() {
        let edges = graph.edges(node);
        if edges.len() > 0 {
            let mut prev = edges[0];
            print!("{}", prev);
            for &edge in &edges[1..] {
                if edge > largest { largest = edge; }
                if edge > prev {
                    print!(" {}", edge);
                    prev = edge;
                }
            }
            println!("");
        }
        else {
            println!("");
        }
    }
    for _ in graph.nodes()..(largest as usize) {
        println!("");
    }
}

fn _digest_graph_vector<E: Ord+Copy>(graph: &GraphVector<E>, output_prefix: &str) {
    let mut edge_writer = BufWriter::new(File::create(format!("{}.targets", output_prefix)).unwrap());
    let mut node_writer = BufWriter::new(File::create(format!("{}.offsets", output_prefix)).unwrap());
    node_writer.write_all(unsafe { _typed_as_byte_slice(&graph.nodes[..]) }).unwrap();

    let mut slice = unsafe { _typed_as_byte_slice(&graph.edges[..]) };
    while slice.len() > 0 {
        let to_write = if slice.len() < 1000000 { slice.len() } else { 1000000 };
        edge_writer.write_all(&slice[..to_write]).unwrap();
        println!("wrote some; remaining: {}", slice.len());
        slice = &slice[to_write..];
    }
}

unsafe fn _typed_as_byte_slice<T>(slice: &[T]) -> &[u8] {
    slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * mem::size_of::<T>())
}
