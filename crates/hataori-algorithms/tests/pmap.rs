use hataori_algorithms::{
    broadcast, pmap, scatter, AlgorithmRegistryExt, CollectiveRegistryExt, PmapOptions,
};
use hataori_runtime::{Action, ActionError, Runtime, RuntimeLimits, Segments, WireValue};
use hataori_runtime_foundation::{
    memory::MemoryNetwork,
    protocol::{ProtocolLimits, RunId},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

struct Number(u64);
impl WireValue for Number {
    const SCHEMA_ID: u64 = 0xa001;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(value: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(value)?))
    }
}
impl Action for Number {
    const ID: u128 = 0xa001;
    type Output = u64;
}
fn builder(
    run: RunId,
) -> (
    hataori_runtime::RuntimeBuilder,
    hataori_algorithms::BatchActionToken<Number>,
) {
    let mut b = Runtime::builder(run, ProtocolLimits::default(), RuntimeLimits::default()).unwrap();
    let t = b
        .register_pmap_action::<Number, _>(|v| Ok(v.0 * 2))
        .unwrap();
    b.hello().unwrap();
    (b, t)
}

#[test]
fn ordered_batches_use_one_local_typed_dispatch_each() {
    let run = RunId::new(0xa001).unwrap();
    let transport = MemoryNetwork::build(1, run, ProtocolLimits::default(), [])
        .unwrap()
        .pop()
        .unwrap();
    let (b, token) = builder(run);
    let mut runtime = b.start(transport.0, transport.1).unwrap();
    let future = pmap(
        &runtime,
        PmapOptions::default().batch_size(2).unwrap(),
        (0..5).map(Number).collect(),
        token,
    )
    .unwrap();
    let (values, stats) = runtime.block_on(future).unwrap();
    assert_eq!(values, vec![0, 2, 4, 6, 8]);
    assert_eq!(stats.batch_count, 3);
    assert_eq!(runtime.stats().local_typed_dispatches, 3);
    assert_eq!(runtime.stats().local_action_serializations, 0);
    runtime.shutdown().unwrap();
}

struct WakeThread(std::thread::Thread);
impl Wake for WakeThread {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
#[test]
fn action_future_collectives_preserve_membership_order() {
    let run = RunId::new(0xa003).unwrap();
    let transport = MemoryNetwork::build(1, run, ProtocolLimits::default(), [])
        .unwrap()
        .pop()
        .unwrap();
    let mut builder =
        Runtime::builder(run, ProtocolLimits::default(), RuntimeLimits::default()).unwrap();
    let token = builder.register_collective::<u64>().unwrap();
    builder.hello().unwrap();
    let mut runtime = builder.start(transport.0, transport.1).unwrap();
    assert_eq!(
        runtime
            .block_on(broadcast(&runtime, hataori_runtime::DomainId::DEFAULT, 7, token).unwrap())
            .unwrap(),
        vec![7]
    );
    assert_eq!(
        runtime
            .block_on(
                scatter(&runtime, hataori_runtime::DomainId::DEFAULT, vec![9], token).unwrap()
            )
            .unwrap(),
        vec![9]
    );
    runtime.shutdown().unwrap();
}

#[test]
fn remote_batches_are_ordered_and_batch_granular() {
    let run = RunId::new(0xa002).unwrap();
    let transports = MemoryNetwork::build(2, run, ProtocolLimits::default(), []).unwrap();
    let mut runtimes = transports
        .into_iter()
        .map(|transport| {
            let (b, _) = builder(run);
            b.start(transport.0, transport.1).unwrap()
        })
        .collect::<Vec<_>>();
    let token = hataori_algorithms::BatchActionToken::new();
    let mut future = Box::pin(
        pmap(
            &runtimes[0],
            PmapOptions::default().batch_size(2).unwrap(),
            (0..7).map(Number).collect(),
            token,
        )
        .unwrap(),
    );
    let waker = Waker::from(Arc::new(WakeThread(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let (values, stats) = loop {
        for runtime in &mut runtimes {
            runtime.progress(64).unwrap();
        }
        if let Poll::Ready(result) = Future::poll(Pin::as_mut(&mut future), &mut cx) {
            break result.unwrap();
        }
    };
    assert_eq!(values, vec![0, 2, 4, 6, 8, 10, 12]);
    assert_eq!(stats.batch_count, 4);
    assert!(stats.peak_in_flight <= 2);
    drop(future);
    for runtime in &mut runtimes {
        runtime.shutdown().unwrap();
    }
}
