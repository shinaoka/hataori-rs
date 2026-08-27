mod support;

use hataori_runtime::{Place, Runtime, RuntimeError, RuntimeLimits};
use hataori_runtime_foundation::{
    mpi::MpiTransport,
    protocol::{DomainId, LocalityId, ProtocolLimits, RunId},
};
use mpi::{
    collective::{CommunicatorCollectives, SystemOperation},
    topology::Communicator,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use support::{Counter, CounterAdd, Increment};

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn collective_block_on<C: CommunicatorCollectives, T>(
    world: &C,
    runtime: &mut Runtime,
    future: impl Future<Output = Result<T, RuntimeError>>,
) -> Result<T, RuntimeError> {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let mut result = None;
    loop {
        runtime.progress(64)?;
        if result.is_none() {
            if let Poll::Ready(value) = Future::poll(Pin::as_mut(&mut future), &mut context) {
                result = Some(value);
            }
        }
        let local_done = i32::from(result.is_some());
        let mut all_done = 0;
        world.all_reduce_into(&local_done, &mut all_done, SystemOperation::min());
        if all_done == 1 {
            return result.unwrap();
        }
        std::thread::yield_now();
    }
}

fn run_round<C: Communicator + CommunicatorCollectives>(world: &C, run: u128) {
    let run_id = RunId::new(run).unwrap();
    let mut builder = Runtime::builder(
        run_id,
        ProtocolLimits::default(),
        RuntimeLimits {
            default_deadline: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(2),
            ..RuntimeLimits::default()
        },
    )
    .unwrap();
    support::register(&mut builder).unwrap();
    let (handle, driver) = MpiTransport::connect(world, builder.hello().unwrap()).unwrap();
    let mut runtime = builder.start(handle, driver).unwrap();
    let destination = (world.rank() + 1) % world.size();
    let future = runtime
        .spawn_on(
            Place::new(LocalityId::new(destination as u64), DomainId::DEFAULT),
            Increment(world.rank() as u64),
        )
        .unwrap();
    assert_eq!(
        collective_block_on(world, &mut runtime, future).unwrap(),
        world.rank() as u64 + 1
    );
    let create = runtime
        .client()
        .create_at(
            Place::new(LocalityId::new(destination as u64), DomainId::DEFAULT),
            Counter(run as u64),
        )
        .unwrap();
    let remote = collective_block_on(world, &mut runtime, create).unwrap();
    let call = remote.call_write(CounterAdd(5)).unwrap();
    assert_eq!(
        collective_block_on(world, &mut runtime, call).unwrap(),
        run as u64 + 5
    );
    drop(remote);
    let report = runtime.shutdown().unwrap();
    assert_eq!(report.stats.pending_calls, 0);
    assert_eq!(report.stats.transport.retained_bytes(), 0);
    world.barrier();
}

fn main() {
    let universe = mpi::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    run_round(&world, 1);
    run_round(&world, 2);
}
