extern crate dataflow_join;

use std::io::{BufWriter, Write};
use std::fs::File;
use std::slice;
use std::mem;

use dataflow_join::graph::{GraphTrait, GraphVector, GraphMMap};

fn main() {
    println!("Usage: layout <source> <target>");
    let source = std::env::args().skip(1).next().unwrap();
    let target = std::env::args().skip(2).next().unwrap();
    let graph = layout(&source);

    let graph = _extract_fragment(&graph, 0, 1);
    _digest_graph_vector(&graph, &target);
}

fn layout(prefix: &str) -> Vec<(u32, u32)> {
    let graph = GraphMMap::<u32>::new(&prefix);

    let mut degree = Vec::new();
    for node in 0..graph.nodes() {
        for &edge in graph.edges(node) {
            while degree.len() <= node { degree.push((0,0)); }
            while degree.len() <= edge as usize { degree.push((0,0)); }
            degree[node as usize].0 += 1u32;
            degree[edge as usize].0 += 1u32;
        }
    }

    for node in 0..degree.len() {
        degree[node].1 = node as u32;
    }

    degree.sort();
    for node in 0..degree.len() {
        degree[node] = (degree[node].1, node as u32);
    }

    let mut result = Vec::new();
    for node in 0..graph.nodes() {
        for &edge in graph.edges(node) {
            result.push((degree[node as usize].1, degree[edge as usize].1));
        }
    }

    organize_graph(&mut result);
    result
}


fn organize_graph(graph: &mut Vec<(u32, u32)>) {

    for index in (0..graph.len()) {
        if graph[index].1 < graph[index].0 {
            graph[index] = (graph[index].1, graph[index].0);
        }
    }

    graph.sort();
    println!("graph data sorted; {:?} edges", graph.len());

    graph.dedup();
    println!("graph data uniqed; {:?} edges", graph.len());
}

fn _extract_fragment(graph: &Vec<(u32, u32)>, index: u64, parts: u64) -> GraphVector<u32> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for &(src,dst) in graph {
        if src as u64 % parts == index {
            while src + 1 >= nodes.len() as u32 { nodes.push(0); }
            while dst + 1 >= nodes.len() as u32 { nodes.push(0); } // allows unsafe access to nodes

            nodes[src as usize + 1] += 1;
            edges.push(dst);
        }
    }

    for index in (1..nodes.len()) {
        nodes[index] += nodes[index - 1];
    }

    return GraphVector { nodes: nodes, edges: edges };
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
