## naive

Naive parallel implementations of graph pattern mining

---

These are naive [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) implementations of several graph mining queries. To execute them, you will need graphs in the [graph-map](https://github.com/frankmcsherry/graph-map) format, and to get correct answers you will want to make sure that the graphs are symmetric (have the same edges in forward and reverse direction).

For example, using the relatively small [LiveJournal graph](http://snap.stanford.edu/data/soc-LiveJournal1.html) to perform query 3 (which counts four-cliques) one might type:

    frankmc@fdr1:~/dataflow-join/naive$ cargo run --release --bin q3 -- ~/lj
        Finished release [optimized] target(s) in 0.0 secs
         Running `target/release/q3 /home/frankmc/lj`
    Duration { secs: 64, nanos: 839351740 } worker 0/1:     count: 9933532019
    frankmc@fdr1:~/dataflow-join/naive$

You can use standard timely dataflow arguments here, for example increasing the number of worker threads like so:

    frankmc@fdr1:~/dataflow-join/naive$ cargo run --release --bin q3 -- ~/lj -w16
        Finished release [optimized] target(s) in 0.0 secs
         Running `target/release/q3 /home/frankmc/lj -w16`
    Duration { secs: 4, nanos: 202392818 }  worker 1/16:    count: 584181375
    Duration { secs: 4, nanos: 325044206 }  worker 12/16:   count: 564532287
    Duration { secs: 4, nanos: 334431840 }  worker 10/16:   count: 596933184
    Duration { secs: 4, nanos: 384298574 }  worker 15/16:   count: 583504168
    Duration { secs: 4, nanos: 396689252 }  worker 0/16:    count: 593037511
    Duration { secs: 4, nanos: 399859841 }  worker 13/16:   count: 620514577
    Duration { secs: 4, nanos: 427228653 }  worker 11/16:   count: 618154900
    Duration { secs: 4, nanos: 437146125 }  worker 14/16:   count: 597109329
    Duration { secs: 4, nanos: 456128426 }  worker 3/16:    count: 624909578
    Duration { secs: 4, nanos: 464649594 }  worker 9/16:    count: 654328036
    Duration { secs: 4, nanos: 509829896 }  worker 2/16:    count: 586789630
    Duration { secs: 4, nanos: 508653220 }  worker 6/16:    count: 636120221
    Duration { secs: 4, nanos: 560799953 }  worker 5/16:    count: 665648136
    Duration { secs: 4, nanos: 568182112 }  worker 8/16:    count: 646472177
    Duration { secs: 4, nanos: 662570020 }  worker 7/16:    count: 684003005
    Duration { secs: 4, nanos: 768076006 }  worker 4/16:    count: 677293905
    frankmc@fdr1:~/dataflow-join/naive$

You can also involve multiple processes, using standard timely dataflow machinery, like so:

    frankmc@fdr1:~/dataflow-join/naive$ cargo run --release --bin q3 -- ~/lj -w16 -h ~/fdr-hosts.txt -n4 -p0
        Finished release [optimized] target(s) in 0.0 secs
         Running `target/release/q3 /home/frankmc/lj -w16 -h /home/frankmc/fdr-hosts.txt -n4 -p0`
    Duration { secs: 1, nanos: 180489839 }  worker 1/64:    count: 143160851
    Duration { secs: 1, nanos: 194260985 }  worker 15/64:   count: 138804302
    Duration { secs: 1, nanos: 204388927 }  worker 2/64:    count: 131297752
    Duration { secs: 1, nanos: 208143406 }  worker 14/64:   count: 142389947
    Duration { secs: 1, nanos: 212073494 }  worker 0/64:    count: 150057695
    Duration { secs: 1, nanos: 241199365 }  worker 9/64:    count: 146856171
    Duration { secs: 1, nanos: 267122334 }  worker 12/64:   count: 155727439
    Duration { secs: 1, nanos: 281714899 }  worker 5/64:    count: 160704569
    Duration { secs: 1, nanos: 285222571 }  worker 11/64:   count: 164154660
    Duration { secs: 1, nanos: 287690930 }  worker 6/64:    count: 151420814
    Duration { secs: 1, nanos: 298658553 }  worker 10/64:   count: 157484735
    Duration { secs: 1, nanos: 307072383 }  worker 8/64:    count: 151645014
    Duration { secs: 1, nanos: 316065464 }  worker 4/64:    count: 173606961
    Duration { secs: 1, nanos: 320209615 }  worker 13/64:   count: 159364814
    Duration { secs: 1, nanos: 327837654 }  worker 3/64:    count: 164525353
    Duration { secs: 1, nanos: 387238398 }  worker 7/64:    count: 171146505
    frankmc@fdr1:~/dataflow-join/naive$

For that last one to work out, you would have also needed to run

    frankmc@fdr2:~/dataflow-join/naive$ cargo run --release --bin q3 -- ~/lj -w16 -h ~/fdr-hosts.txt -n4 -p1
    frankmc@fdr3:~/dataflow-join/naive$ cargo run --release --bin q3 -- ~/lj -w16 -h ~/fdr-hosts.txt -n4 -p2
    frankmc@fdr4:~/dataflow-join/naive$ cargo run --release --bin q3 -- ~/lj -w16 -h ~/fdr-hosts.txt -n4 -p3

on some other machines.