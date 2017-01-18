extern crate rand;
extern crate time;
extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::*;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::Extract;

use graph_map::GraphMMap;

#[allow(non_snake_case)]
fn main () {

    let start = time::precise_time_s();

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.lock().unwrap().clone();

        // used to partition graph loading
        let index = root.index();
        let peers = root.peers();

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input, probe, forward, reverse) = root.scoped::<u32,_,_>(|builder| {

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<((u32, u32), i32)>();

            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            //
            // The dataflow has 10 derivatives, with respect to each relation.
            // Please see four-cliques.rs for more information.
            
            // we will index the data both by src and dst.
            let (forward, f_handle) = dG.index();
            let (reverse, r_handle) = dG.map(|((src,dst),wgt)| ((dst,src),wgt)).index();

            // Please see four-cliques.rs for more information on how the derivatives are computed.
            // dAdQ
            let dK5dA1 = dG.extend(vec![&forward.extend_using(|&(a1,a2)| a1 as u64, |t1, t2| t1.lt(t2)),
                                       &forward.extend_using(|&(a1,a2)| a2 as u64, |t1, t2| t1.lt(t2))])
                           .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0,p.1,e), wght)));
            let dK5dA2 =  dK5dA1.extend(vec![&forward.extend_using(|&(a1,a2,a3)| a1 as u64, |t1, t2| t1.lt(t2)),
                                        &forward.extend_using(|&(a1,a2,a3)| a2 as u64, |t1, t2| t1.lt(t2)),
                                        &forward.extend_using(|&(a1,a2,a3)| a3 as u64, |t1, t2| t1.lt(t2))])
                                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, e), wght)));

            let dK5dA = dK5dA2.extend(vec![&forward.extend_using(|&(a1,a2,a3, a4)| a1 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3, a4)| a2 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3, a4)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3, a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                              .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, p.3, e), wght)));

            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dBdQ
            let dK5dB1 = dG.extend(vec![&forward.extend_using(|&(a1,a3)| a1 as u64, |t1, t2| t1.le(t2)),
                                       &reverse.extend_using(|&(a1,a3)| a3 as u64, |t1, t2| t1.lt(t2))])
                           .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
            let dK5dB2 =  dK5dB1.extend(vec![&forward.extend_using(|&(a1,a2,a3)| a1 as u64, |t1, t2| t1.lt(t2)),
                                        &forward.extend_using(|&(a1,a2,a3)| a2 as u64, |t1, t2| t1.lt(t2)),
                                        &forward.extend_using(|&(a1,a2,a3)| a3 as u64, |t1, t2| t1.lt(t2))])
                                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, e), wght)));
            let dK5dB = dK5dB2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a4)| a1 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a2 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                             .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, p.3, e), wght)));

            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dCdQ
            let dK5dC1 = dG.extend(vec![&forward.extend_using(|&(a1,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                       &reverse.extend_using(|&(a1,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                           .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
            let dK5dC2 =  dK5dC1.extend(vec![&forward.extend_using(|&(a1,a2,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                        &forward.extend_using(|&(a1,a2,a4)| a2 as u64, |t1, t2| t1.lt(t2)),
                                        &reverse.extend_using(|&(a1,a2,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, e, p.2), wght)));
            let dK5dC = dK5dC2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a4)| a1 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a2 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                             .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, p.3, e), wght)));

            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dDdQ
            let dK5dD1 = dG.extend(vec![&forward.extend_using(|&(a1,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                       &reverse.extend_using(|&(a1,a5)| a5 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1), wght)));
            let dK5dD2 =  dK5dD1.extend(vec![&forward.extend_using(|&(a1,a2,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                        &forward.extend_using(|&(a1,a2,a5)| a2 as u64, |t1, t2| t1.lt(t2)),
                                        &reverse.extend_using(|&(a1,a2,a5)| a5 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, e, p.2), wght)));
            let dK5dD = dK5dD2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a5)| a2 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a5)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &reverse.extend_using(|&(a1,a2,a3,a5)| a5 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, e, p.3), wght)));
            
            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dEdQ
            let dK5dE1 = dG.extend(vec![&reverse.extend_using(|&(a2,a3)| a2 as u64, |t1, t2| t1.le(t2)),
                                       &reverse.extend_using(|&(a2,a3)| a3 as u64, |t1, t2| t1.le(t2))])
                           .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK5dE2 =  dK5dE1.extend(vec![&forward.extend_using(|&(a1,a2,a3)| a1 as u64, |t1, t2| t1.le(t2)),
                                        &forward.extend_using(|&(a1,a2,a3)| a2 as u64, |t1, t2| t1.lt(t2)),
                                        &forward.extend_using(|&(a1,a2,a3)| a3 as u64, |t1, t2| t1.lt(t2))])
                                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, e), wght)));
            let dK5dE = dK5dE2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a2 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                             .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, p.3, e), wght)));           

            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dFdQ
            let dK5dF1 = dG.extend(vec![&reverse.extend_using(|&(a2,a4)| a2 as u64, |t1, t2| t1.le(t2)),
                                        &reverse.extend_using(|&(a2,a4)| a4 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK5dF2 =  dK5dF1.extend(vec![&forward.extend_using(|&(a1,a2,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                             &forward.extend_using(|&(a1,a2,a4)| a2 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a2,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, e, p.2), wght)));
            let dK5dF = dK5dF2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a2 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, p.3, e), wght)));

            
            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dGdQ
            let dK5dG1 = dG.extend(vec![&reverse.extend_using(|&(a2,a5)| a2 as u64, |t1, t2| t1.le(t2)),
                                        &reverse.extend_using(|&(a2,a5)| a5 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK5dG2 =  dK5dG1.extend(vec![&forward.extend_using(|&(a1,a2,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                             &forward.extend_using(|&(a1,a2,a5)| a2 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a2,a5)| a5 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, e, p.2), wght)));
            let dK5dG = dK5dG2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a5)| a2 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a5)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &reverse.extend_using(|&(a1,a2,a3,a5)| a5 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, e, p.3), wght)));

            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dHdQ
            let dK5dH1 = dG.extend(vec![&reverse.extend_using(|&(a3,a4)| a3 as u64, |t1, t2| t1.le(t2)),
                                        &reverse.extend_using(|&(a3,a4)| a4 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK5dH2 =  dK5dH1.extend(vec![&forward.extend_using(|&(a1,a3,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a3,a4)| a3 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a3,a4)| a4 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1, p.2), wght)));
            let dK5dH = dK5dH2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a4)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a2 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a3 as u64, |t1, t2| t1.lt(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a4)| a4 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, p.3, e), wght)));


            // Our query is K5 = Q(a1,a2,a3,a4,a5) = A(a1,a2) B(a1,a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                       F(a2,a4) G(a2,a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dIdQ
            let dK5dI1 = dG.extend(vec![&reverse.extend_using(|&(a3,a5)| a3 as u64, |t1, t2| t1.le(t2)),
                                        &reverse.extend_using(|&(a3,a5)| a5 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK5dI2 =  dK5dI1.extend(vec![&forward.extend_using(|&(a1,a3,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a3,a5)| a3 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a3,a5)| a5 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1, p.2), wght)));
            let dK5dI = dK5dI2.extend(vec![&forward.extend_using(|&(a1,a2,a3,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a5)| a2 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a3,a5)| a3 as u64, |t1, t2| t1.le(t2)),
                                           &reverse.extend_using(|&(a1,a2,a3,a5)| a5 as u64, |t1, t2| t1.lt(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, p.2, e, p.3), wght)));


            
            // Our query is K5 = Q(a1,a2,a3,a4, a5) = A(a1,a2) B(a1, a3) C(a1,a4) D(a1,a5) E(a2,a3)
            //                                        F(a2,a4) G(a2, a5) H(a3,a4) I(a3,a5) J(a4,a5)
            // dJdQ
            let dK5dJ1 = dG.extend(vec![&reverse.extend_using(|&(a4,a5)| a4 as u64, |t1, t2| t1.le(t2)),
                                       &reverse.extend_using(|&(a4,a5)| a5 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((e, p.0, p.1), wght)));
            let dK5dJ2 =  dK5dJ1.extend(vec![&forward.extend_using(|&(a1,a4,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a4,a5)| a4 as u64, |t1, t2| t1.le(t2)),
                                             &reverse.extend_using(|&(a1,a4,a5)| a5 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, e, p.1,p.2), wght)));
            let dK5dJ = dK5dJ2.extend(vec![&forward.extend_using(|&(a1,a2,a4,a5)| a1 as u64, |t1, t2| t1.le(t2)),
                                           &forward.extend_using(|&(a1,a2,a4,a5)| a2 as u64, |t1, t2| t1.le(t2)),
                                           &reverse.extend_using(|&(a1,a2,a4,a5)| a4 as u64, |t1, t2| t1.le(t2)),
                                           &reverse.extend_using(|&(a1,a2,a4,a5)| a5 as u64, |t1, t2| t1.le(t2))])
                .flat_map(|(p,es,wght)| es.into_iter().map(move |e| ((p.0, p.1, e, p.2, p.3), wght)));

            // accumulate all changes together into a single dataflow.
            let cliques = dK5dJ.concat(&dK5dI).concat(&dK5dH).concat(&dK5dG).concat(&dK5dF).concat(&dK5dE).concat(&dK5dD).concat(&dK5dC).concat(&dK5dB).concat(&dK5dA);

            // if the third argument is "inspect", report 5-clique counts.
            if inspect {
                cliques.exchange(|x| (x.0).0 as u64)
                // .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .count()
                    .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .capture_into(send);
            }
            (graph, cliques.probe().0, f_handle, r_handle)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
        let graph = GraphMMap::new(&filename);

        let nodes = graph.nodes();
        let mut edges = Vec::new();

        for node in 0 .. graph.nodes() {
            if node % peers == index {
                edges.push(graph.edges(node).to_vec());
            }
        }

        drop(graph);

        // synchronize with other workers.
        let prev = input.time().clone();
        input.advance_to(prev.inner + 1);
        root.step_while(|| probe.lt(input.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = time::precise_time_s();
        for node in 0 .. nodes {

            // introduce the node if it is this worker's responsibility
            if node % peers == index {
                for &edge in &edges[node / peers] {
                    input.send(((node as u32, edge), 1));
                }
            }

            // if at a batch boundary, advance time and do work.
            if node % batch == (batch - 1) {
                let prev = input.time().clone();
                input.advance_to(prev.inner + 1);
                root.step_while(|| probe.lt(input.time()));

                // merge all of the indices we maintain.
                forward.borrow_mut().merge_to(&prev);
                reverse.borrow_mut().merge_to(&prev);
            }
        }

        input.close();
        while root.step() { }

        if inspect { 
            println!("worker {} elapsed: {:?}", index, time::precise_time_s() - start); 
        }

    }).unwrap();

    let result = recv.extract();

    let mut total = 0;
    for &(_, ref counts) in &result {
        for &count in counts {
            total += count;
        }
    } 

    if inspect { 
        println!("elapsed: {:?}\ttotal 5-cliques at this process: {:?}", time::precise_time_s() - start, total); 
    }
}
