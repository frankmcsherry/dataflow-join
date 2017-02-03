# dataflow-join

A streaming implementation of Ngo et al's GenericJoin in timely dataflow.

---

Ngo et al presented a very cool join algorithm, some details of which are described in [a blog post](http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html). This project (a collaboration with Khaled Ammar and Semih Salihoglu) extends Ngo et al's algorithm to the case where the underlying relations change, allowing us to track the results of complex join queries as they change.

A [series of posts](https://github.com/frankmcsherry/blog/blob/master/posts/2016-09-17.md) describe the ideas behind this implementation.

## An example: graph motifs

For an example, the [`examples/motif.rs`](https://github.com/frankmcsherry/dataflow-join/blob/master/examples/motif.rs) program takes the description of a directed graph motif (to be explained) and a list of graph edges, and reports the change in the numbers of these motifs as we stream the edges in. To look for directed triangles of the form `(a,b), (a,c), (b,c)`, using the livejournal graph edges in a random order (any text file where each line has the form `src dst`), loading the first 68 million edges, and then swinging over the remaining entries in batches of 1,000, we would type:

	cargo run --release --example motif -- 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 inspect

The motif is expressed clumsily at the moment, but has the form `num_edges [src dst]^num_edges` where you indicate how many edges (3 for triangles) and then repeatedly indicate edges in the motif. There are bugs here, including symmetric edges ((a,b) and (b,a)) not being handled correctly, among others I probably don't know about yet.

If you run the command above you should see something like:

	Echidnatron% cargo run --release --example motif -- 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 inspect
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 inspect`
	motif:	[(0, 1), (0, 2), (1, 2)]
	filename:	"./soc-LiveJournal1.random.txt"
	index initialized: (4295196, 68000000)
	index initialized: (4474513, 68000000)
	Duration { secs: 36, nanos: 995859508 }	[worker 0]	data loaded
	Duration { secs: 36, nanos: 995924311 }	[worker 0]	indices merged
	(Root, 2): [37695]
	(Root, 3): [36745]
	(Root, 4): [42000]
	...

which reports the input reading, index building (forward and reverse), and then starts reporting the number of changes to the directed motif in each batch of 1,000 edges. In this example all changes are edge additions, so all the motif changes are all additions as well.

As the program continues to run, it should result in something like

	...
	(Root, 993): [37462]
	(Root, 994): [44505]
	(Root, 995): [30189]
	elapsed: 67.91147424298106	total motifs at this process: 39772836
	Echidnatron%

This reports the total number of motif changes observed at this process and an elapsed running time.

## Parallelism

Like other timely dataflow programs, we can increase the number of workers to parallelize computation (and printing of things to the screen):

	Echidnatron% cargo run --release --example motif -- 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 inspect -w2
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 inspect -w2`
	motif:	[(0, 1), (0, 2), (1, 2)]
	filename:	"./soc-LiveJournal1.random.txt"
	motif:	[(0, 1), (0, 2), (1, 2)]
	filename:	"./soc-LiveJournal1.random.txt"
	index initialized: (2147644, 34060151)
	index initialized: (2147552, 33939849)
	index initialized: (2237328, 34057137)
	index initialized: (2237185, 33942863)
	Duration { secs: 22, nanos: 149237598 }	[worker 1]	data loaded
	Duration { secs: 22, nanos: 149245453 }	[worker 0]	data loaded
	Duration { secs: 22, nanos: 149295018 }	[worker 0]	indices merged
	Duration { secs: 22, nanos: 149296396 }	[worker 1]	indices merged
	(Root, 2): [15863]
	(Root, 2): [21832]
	(Root, 3): [16715]
	(Root, 3): [20030]
	...
	(Root, 994): [22018]
	(Root, 994): [22487]
	(Root, 995): [16231]
	(Root, 995): [13958]
	elapsed: 40.51301470899489	total motifs at this process: 39772836
	Echidnatron%

Each worker reads the input file, separates lines, but only parses lines whose index equals their worker id modulo the number of workers. For now this means that each machine needs access to a copy of the file, which should be improved.

## Reporting

The `inspect` argument attaches a dataflow fragment to count each of the changes, and by omitting it performance improves somewhat (by shuttling around much less data).

	Echidnatron% cargo run --release --example motif -- 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 -w2
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 3 0 1 0 2 1 2 ./soc-LiveJournal1.random.txt 68000000 1000 -w2`
	motif:	[(0, 1), (0, 2), (1, 2)]
	motif:	[(0, 1), (0, 2), (1, 2)]
	filename:	"./soc-LiveJournal1.random.txt"
	filename:	"./soc-LiveJournal1.random.txt"
	index initialized: (2147552, 33939849)
	index initialized: (2147644, 34060151)
	index initialized: (2237185, 33942863)
	index initialized: (2237328, 34057137)
	Duration { secs: 21, nanos: 904351750 }	[worker 0]	data loaded
	Duration { secs: 21, nanos: 904355535 }	[worker 1]	data loaded
	Duration { secs: 21, nanos: 904398564 }	[worker 1]	indices merged
	Duration { secs: 21, nanos: 904412222 }	[worker 0]	indices merged
	elapsed: 35.086652864003554	total motifs at this process: 0
	Echidnatron%

The total reported is zero, because we detached the counting infrastructure.

## Other example motifs

The infrastructure decouples loading graph data from observing changes in motif counts, allowing us to observe the *change* in motif counts without paying the cost of determining the original count, allowing us to track relatively complex motifs whose computation would otherwise be rather painful.

Here are some examples:

### Directed 3-cycles

These are also triangles, but ones that form a cycle, rather than pointing from `a` to `b` to `c`.

	Echidnatron% cargo run --release --example motif -- 3 0 1 1 2 2 0 ./soc-LiveJournal1.random.txt 68000000 1000 inspect
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 3 0 1 1 2 2 0 ./soc-LiveJournal1.random.txt 68000000 1000 inspect`
	motif:	[(0, 1), (1, 2), (2, 0)]
	filename:	"./soc-LiveJournal1.random.txt"
	...
	(Root, 994): [35917]
	(Root, 995): [24601]
	elapsed: 62.80372881703079	total motifs at this process: 31171871
	Echidnatron%

### Directed 4-cliques

Generalizing directed triangles to 4-cliques, where we have four nodes `a`, `b`, `c`, and `d` that each point to those nodes after them, and pre-loading a bit more data so we don't have to wait as long for the result, 

	Echidnatron% cargo run --release --example motif -- 6 0 1 0 2 0 3 1 2 1 3 2 3 ./soc-LiveJournal1.random.txt 68900000 1000 inspect
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 6 0 1 0 2 0 3 1 2 1 3 2 3 ./soc-LiveJournal1.random.txt 68900000 1000 inspect`
	motif:	[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
	filename:	"./soc-LiveJournal1.random.txt"
	index initialized: (4307203, 68900000)
	index initialized: (4487812, 68900000)
	Duration { secs: 38, nanos: 751447929 }	[worker 0]	data loaded
	Duration { secs: 38, nanos: 751585622 }	[worker 0]	indices merged
	(Root, 2): [6239159]
	(Root, 3): [5386443]
	...
	(Root, 95): [3938957]
	elapsed: 233.18197546800366	total motifs at this process: 550759817
	Echidnatron%

This took longer, but there were also a larger number of the motifs to process.

### Directed 6-cliques

You can imagine where this is going, but I thought I'd do this example because it is something that seems is much to complicated to compute completely and then difference.

	Echidnatron% cargo run --release --example motif -- 15 0 1 0 2 0 3 0 4 0 5 1 2 1 3 1 4 1 5 2 3 2 4 2 5 3 4 3 5 4 5 soc-LiveJournal1.random.txt 68900000 1 inspect
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 15 0 1 0 2 0 3 0 4 0 5 1 2 1 3 1 4 1 5 2 3 2 4 2 5 3 4 3 5 4 5 ./soc-LiveJournal1.random.txt 68900000 1 inspect`
	motif:	[(0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (1, 2), (1, 3), (1, 4), (1, 5), (2, 3), (2, 4), (2, 5), (3, 4), (3, 5), (4, 5)]
	filename:	"./soc-LiveJournal1.random.txt"
	index initialized: (4307203, 68900000)
	index initialized: (4487812, 68900000)
	Duration { secs: 38, nanos: 759079572 }	[worker 0]	data loaded
	Duration { secs: 38, nanos: 760087081 }	[worker 0]	indices merged
	(Root, 2): [473]
	(Root, 3): [94]
	(Root, 5): [8975]
	(Root, 6): [101569]
	(Root, 7): [5]
	(Root, 8): [1554]
	(Root, 9): [128]
	(Root, 10): [33446]
	(Root, 11): [5339]
	Echidnatron%

This program goes to 11. 

But just 11. What happened? As it turns out, the 12th update causes so many changed 6-cliques (or candidates along the way) that the process wanders up to 60GB on my laptop and then shuts itself down. 

For comparison, here are the output numbers for 5-cliques, where the 12th update produces 48,807,868 updates. Each of these updates are produced along the way in the 6-clique dataflow (which just extends the 5-clique dataflow), and each apparently leads to some large number of further candidates.

	Echidnatron% cargo run --release --example motif -- 10 0 1 0 2 0 3 0 4 1 2 1 3 1 4 2 3 2 4 3 4 ./soc-LiveJournal1.random.txt 68900000 1 inspect
	    Finished release [optimized + debuginfo] target(s) in 0.0 secs
	     Running `target/release/examples/motif 10 0 1 0 2 0 3 0 4 1 2 1 3 1 4 2 3 2 4 3 4 ./soc-LiveJournal1.random.txt 68900000 1 inspect`
	motif:	[(0, 1), (0, 2), (0, 3), (0, 4), (1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]
	filename:	"./soc-LiveJournal1.random.txt"
	index initialized: (4307203, 68900000)
	index initialized: (4487812, 68900000)
	Duration { secs: 36, nanos: 387479783 }	[worker 0]	data loaded
	Duration { secs: 36, nanos: 387815357 }	[worker 0]	indices merged
	(Root, 2): [131]
	(Root, 3): [53]
	(Root, 5): [1570]
	(Root, 6): [11081]
	(Root, 7): [4]
	(Root, 8): [574]
	(Root, 9): [67]
	(Root, 10): [6197]
	(Root, 11): [952]
	(Root, 12): [48807868]
	(Root, 13): [3612]
	(Root, 14): [470]
	...

The problem is that, even though we are streaming in single updates, we try and do all of the count, propose, and intersect work for these 48 million tuples at the same time. What we *should* be doing, in a better world, is stream through the 48 million bits of intermediate work as well. We should stage them so that we don't try and do all of the work at once, but rather retire chunks of updates at a time, keeping our resource use in check.

This is an exciting open area for us, where the answer lies in [recent scheduling work](https://people.inf.ethz.ch/zchothia/papers/faucet-beyondmr16.pdf) with Andrea Lattuada and Zaheer Chothia that prioritizes operators further down the dataflow graph, aggressively draining the dataflow rather than producing more work. In principle we should be able to try this out and see what happens!