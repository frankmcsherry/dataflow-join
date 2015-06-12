// #![feature(scoped)]
// #![feature(collections)]

extern crate mmap;
extern crate time;
extern crate timely;
extern crate columnar;
extern crate dataflow_join;

extern crate docopt;
use docopt::Docopt;

use std::thread;
use std::mem;
use std::ptr;

use dataflow_join::graph::{GraphTrait, GraphMMap};

use timely::progress::timestamp::RootTimestamp;
use timely::progress::scope::Scope;
use timely::progress::nested::Summary::Local;
use timely::example_static::*;
use timely::communication::*;
use timely::communication::pact::Exchange;

use timely::networking::initialize_networking;
use timely::networking::initialize_networking_from_file;

use timely::drain::DrainExt;

static USAGE: &'static str = "
Usage: pagerank <source> [options] [<arguments>...]

Options:
    -w <arg>, --workers <arg>    number of workers per process [default: 1]
    -p <arg>, --processid <arg>  identity of this process      [default: 0]
    -n <arg>, --processes <arg>  number of processes involved  [default: 1]
    -h <arg>, --hosts <arg>      list of host:port for workers
";


fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    // let workers = if let Ok(threads) = args.get_str("<workers>").parse() { threads }
    //               else { panic!("invalid setting for workers: {}", args.get_str("<workers>")) };
    // println!("starting pagerank dataflow with {:?} worker{}", workers, if workers == 1 { "" } else { "s" });
    let source = args.get_str("<source>").to_owned();

    let workers: u64 = if let Ok(threads) = args.get_str("-w").parse() { threads }
                       else { panic!("invalid setting for --workers: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                          else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Starting pagerank dataflow with");
    println!("\tworkers:\t{}", workers);
    println!("\tprocesses:\t{}", processes);
    println!("\tprocessid:\t{}", process_id);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        println!("Initializing BinaryCommunicator");

        let hosts = args.get_str("-h");
        let communicators = if hosts != "" {
            initialize_networking_from_file(hosts, process_id, workers).ok().expect("error initializing networking")
        }
        else {
            let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
            initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking")
        };

        pagerank_multi(communicators, source);
    }
    else if workers > 1 {
        println!("Initializing ProcessCommunicator");
        pagerank_multi(ProcessCommunicator::new_vector(workers), source);
    }
    else {
        println!("Initializing ThreadCommunicator");
        pagerank_multi(vec![ThreadCommunicator], source);
    };
}

fn pagerank_multi<C>(communicators: Vec<C>, filename: String)
where C: Communicator+Send {
    let mut guards = Vec::new();
    let workers = communicators.len();
    for communicator in communicators.into_iter() {
        let filename = filename.clone();
        guards.push(thread::Builder::new().name(format!("timely worker {}", communicator.index()))
                                          .spawn(move || pagerank(communicator, filename, workers))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn transpose(filename: String, index: usize, peers: usize) -> (Vec<u32>, Vec<(u32, u32)>, Vec<u32>)  {

    let graph = GraphMMap::<u32>::new(&filename);

    let mut src = vec![];
    let mut dst = vec![];
    let mut deg = vec![];

    for node in 0..graph.nodes() {
        if node % peers == index {
            deg.push(graph.edges(node).len() as u32);
            for &b in graph.edges(node) {
                src.push((node / peers) as u32);
                dst.push(b as u32);
            }
        }
    }

    // println!("slice {} of {} extracted {} edges", index, peers, edges.len());

    qsort_kv(&mut dst[..], &mut src[..]);

    let mut rev = vec![(0,0);0];
    for d in dst.drain_temp() {
        let len = rev.len();
        if (len == 0) || (rev[len-1].0 < d) {
            rev.push((d, 0));
        }

        let len = rev.len();
        rev[len-1].1 += 1;
    }

    return (deg, rev, src);
}

fn pagerank<C>(communicator: C, filename: String, _workers: usize)
where C: Communicator {
    let index = communicator.index() as usize;
    let peers = communicator.peers() as usize;

    let mut root = GraphRoot::new(communicator);

    let mut start = time::precise_time_s();
    let mut going = start;

    {   // new scope avoids long borrow on root
        let mut builder = root.new_subgraph();

        // establish the beginnings of a loop,
        // 20 iterations, each time around += 1.
        let (helper, stream) = builder.loop_variable::<(u32, f32)>(RootTimestamp::new(20), Local(1));

        let (deg, rev, edges) = transpose(filename, index, peers);
        let mut src = vec![0.0; deg.len()];

        // from feedback, place an operator that
        // aggregates and broadcasts ranks along edges.
        let ranks = stream.enable(builder).unary_notify(

            Exchange::new(|x: &(u32, f32)| x.0 as u64),     // 1. how data should be exchanged
            format!("PageRank"),                            // 2. a tasteful, descriptive name
            vec![RootTimestamp::new(0)],                    // 3. indicate an initial capability
            move |input, output, iterator| {                // 4. provide the operator logic

                while let Some((iter, _)) = iterator.next() {

                    if iter.inner == 10 {
                        going = time::precise_time_s();
                    }

                    if iter.inner == 20 {
                        if index == 0 {
                            println!("average over 10 iters: {}", (time::precise_time_s() - going) / 10.0);
                        }
                    }

                    for node in 0..src.len() {
                        src[node] = 0.15 + 0.85 * src[node] / deg[node] as f32;
                    }

                    let mut index = 0;
                    let mut slice = &edges[..];
                    while index < rev.len() {

                        let mut session = output.session(&iter);

                        for _ in 0..std::cmp::min(100_000, rev.len() - index) {

                            let (dst, deg) = rev[index];

                            let mut accum = 0.0;
                            for &s in &slice[..deg as usize] {
                                accum += src[s as usize];
                            }
                            slice = &slice[deg as usize..];
                            session.give((dst, accum));

                            index += 1;
                        }
                    }

                    for s in &mut src { *s = 0.0; }

                    // println!("iteration {:?}: {}s", iter, time::precise_time_s() - start);
                    start = time::precise_time_s();
                }

                while let Some((iter, data)) = input.pull() {
                    iterator.notify_at(&iter);
                    for (node, rank) in data.drain_temp() {
                        src[node as usize / peers] += rank;
                    }
                }
            }
        );

        // let local_index = index as usize % workers;
        // let mut acc = vec![0.0; src.len()];

        ranks
        // .unary_notify(
        //     Exchange::new(move |x: &(u32, f32)| (workers * (index / workers)) as u64 + (x.0 as u64 % workers as u64)),
        //     format!("Aggregation"),
        //     vec![],
        //     move |input, output, iterator| {
        //         while let Some((iter, data)) = input.pull() {
        //             iterator.notify_at(&iter);
        //             for (node, rank) in data.drain_temp() {
        //                 acc[node as usize / workers] += rank;
        //             }
        //         }
        //
        //         while let Some((item, _)) = iterator.next() {
        //
        //             output.give_at(&item, acc.drain_temp().enumerate().filter(|x| x.1 != 0.0)
        //                                      .map(|(u,f)| (((u * workers + local_index) as u32), f)));
        //
        //             for _ in 0..(1 + (nodes/workers)) { acc.push(0.0); }
        //             assert!(acc.len() == (1 + (nodes/workers)));
        //         }
        //     }
        // )
        .connect_loop(helper);
    }

    while root.step() { }

    if index == 0 {
        println!("elapsed: {}", time::precise_time_s() - start);
    }
}


pub fn qsort_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) {
    let mut work = vec![(keys, vals)];
    while let Some((ks, vs)) = work.pop() {
        if ks.len() < 16 { isort_kv(ks, vs); }
        else {
            let p = partition_kv(ks, vs);
            let (ks1, ks2) = ks.split_at_mut(p);
            let (vs1, vs2) = vs.split_at_mut(p);
            work.push((&mut ks2[1..], &mut vs2[1..]));
            work.push((ks1, vs1));
        }
    }
}

#[inline(always)]
pub fn partition_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) -> usize {

    let pivot = keys.len() / 2;

    let mut lower = 0;
    let mut upper = keys.len() - 1;

    unsafe {
        while lower < upper {
            // NOTE : Pairs are here to insulate against "same key" balance issues
            while lower < upper && (keys.get_unchecked(lower),lower) <= (keys.get_unchecked(pivot),pivot) { lower += 1; }
            while lower < upper && (keys.get_unchecked(pivot),pivot) <= (keys.get_unchecked(upper),upper) { upper -= 1; }
            ptr::swap(keys.get_unchecked_mut(lower), keys.get_unchecked_mut(upper));
            ptr::swap(vals.get_unchecked_mut(lower), vals.get_unchecked_mut(upper));
        }
    }

    // we want to end up with xs[p] near lower.
    if keys[lower] < keys[pivot] && lower < pivot { lower += 1; }
    if keys[lower] > keys[pivot] && lower > pivot { lower -= 1; }
    keys.swap(lower, pivot);
    vals.swap(lower, pivot);
    lower
}


// insertion sort
pub fn isort_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) {
    for i in 1..keys.len() {
        let mut j = i;
        unsafe {
            while j > 0 && keys.get_unchecked(j-1) > keys.get_unchecked(i) { j -= 1; }

            // bulk shift the stuff we skipped over
            let mut tmp_k: K = mem::uninitialized();
            ptr::swap(&mut tmp_k, keys.get_unchecked_mut(i));
            ptr::copy(keys.get_unchecked_mut(j), keys.get_unchecked_mut(j+1), i-j);
            ptr::swap(&mut tmp_k, keys.get_unchecked_mut(j));
            mem::forget(tmp_k);

            let mut tmp_v: V = mem::uninitialized();
            ptr::swap(&mut tmp_v, vals.get_unchecked_mut(i));
            ptr::copy(vals.get_unchecked_mut(j), vals.get_unchecked_mut(j+1), i-j);
            ptr::swap(&mut tmp_v, vals.get_unchecked_mut(j));
            mem::forget(tmp_v);
        }
    }
}
