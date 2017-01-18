# dataflow-join
An streaming implementation of Ngo et al's GenericJoin in timely dataflow.

Ngo et al presented a very cool join algorithm, some details of which are described in [a blog post](http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html). This project (a collaboration with Khaled Ammar and Semih Salihoglu) extends Ngo et al's algorithm to the case where the underlying relations change, allowing us to track the results of complex join queries as they change.

A [series of posts](https://github.com/frankmcsherry/blog/blob/master/posts/2016-09-17.md) describe the ideas behind this implementation.

The simplest example to consider is probably [`examples/triangles.rs`](https://github.com/frankmcsherry/dataflow-join/blob/master/examples/triangles.rs), which shows how we might implement a dataflow for computing and maintaining the triangles in a graph (nodes `a`, `b`, `c` with edges between each).