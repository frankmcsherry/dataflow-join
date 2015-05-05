use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use timely::progress::nested::subgraph::Source::ScopeOutput;
use timely::progress::nested::subgraph::Target::ScopeInput;

use timely::communication::*;
use timely::communication::pact::{Pipeline, PactPullable};
use timely::progress::count_map::CountMap;
use timely::progress::{Timestamp, Scope, Antichain};
use timely::communication::channels::ObserverHelper;

use timely::example_static::builder::*;
use timely::example_static::unary::PullableHelper;
use timely::example_static::stream::{ActiveStream, Stream};


pub trait FlattenerExt<G: GraphBuilder, P: Data, E: Data> {
    fn flatten(self) -> (Stream<G::Timestamp, (P, E)>, ActiveStream<G, Vec<E>>);
}

impl<G: GraphBuilder, P: Data, E: Data> FlattenerExt<G, P, E> for ActiveStream<G, (P, Vec<E>)> {
    fn flatten(mut self) -> (Stream<G::Timestamp, (P, E)>, ActiveStream<G, Vec<E>>) {
        let (sender, receiver) = Pipeline.connect(self.builder.communicator());

        let (targets1, registrar1) = OutputPort::<G::Timestamp, (P, E)>::new();
        let (targets2, registrar2) = OutputPort::<G::Timestamp, Vec<E>>::new();

        let scope = FlattenerScope::new(receiver, targets1, targets2);
        let index = self.builder.add_scope(scope);
        self.connect_to(ScopeInput(index, 0), sender);

        (Stream { name: ScopeOutput(index, 0), ports: registrar1 },
         self.transfer_borrow_to(ScopeOutput(index, 1), registrar2) )
    }
}

pub struct FlattenerScope<T: Timestamp, P: Data, E: Data> {
    input:      PullableHelper<T, (P,Vec<E>), Rc<RefCell<VecDeque<(T, Vec<(P, Vec<E>)>)>>>>,
    output1:    ObserverHelper<OutputPort<T, (P, E)>>,
    output2:    ObserverHelper<OutputPort<T, Vec<E>>>,
    stash:      Vec<Vec<E>>,
    counter:    u64,
}

impl<T: Timestamp, P: Data, E: Data> FlattenerScope<T, P, E> {
    pub fn new(receiver: PactPullable<T, (P, Vec<E>), Rc<RefCell<VecDeque<(T, Vec<(P, Vec<E>)>)>>>>,
               output1: OutputPort<T, (P, E)>,
               output2: OutputPort<T, Vec<E>>) -> FlattenerScope<T, P, E> {
        FlattenerScope {
            input:   PullableHelper::new(receiver),
            output1: ObserverHelper::new(output1, Rc::new(RefCell::new(CountMap::new()))),
            output2: ObserverHelper::new(output2, Rc::new(RefCell::new(CountMap::new()))),
            stash:   Vec::new(),
            counter: 0,
        }
    }
}

impl<T: Timestamp, P: Data, E: Data> Scope<T> for FlattenerScope<T, P, E> {
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 2 }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, _frontier: &mut [CountMap<T>]) -> () { }

    fn push_external_progress(&mut self,_external: &mut [CountMap<T>]) -> () { }

    fn pull_internal_progress(&mut self,_internal: &mut [CountMap<T>],
                                         consumed: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, pairs)) = self.input.pull() {
            let mut session = self.output1.session(&time);
            for (prefix, mut extensions) in pairs.drain(..) {
                for extension in extensions.drain(..) {
                    session.give((prefix.clone(), extension));
                }

                self.stash.push(extensions);
            }
            // self.counter += self.stash.len() as u64;
            self.output2.give_at(&time, self.stash.drain(..));
        }
        // println!("stashed: {}", self.counter);

        // extract what we know about progress from the input and output adapters.
        self.input.pull_progress(&mut consumed[0]);
        self.output1.pull_progress(&mut produced[0]);
        self.output2.pull_progress(&mut produced[1]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("Flattener") }
    fn notify_me(&self) -> bool { false }
}
