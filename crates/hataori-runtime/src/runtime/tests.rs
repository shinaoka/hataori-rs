use super::*;
use crate::{ActionError, WireValue};
use hataori_runtime_foundation::{
    memory::{MemoryFault, MemoryNetwork},
    protocol::ProtocolLimits,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

#[derive(Debug)]
struct Add {
    left: u64,
    right: u64,
}

impl WireValue for Add {
    const SCHEMA_ID: u64 = 100;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(vec![
            self.left.to_le_bytes().to_vec(),
            self.right.to_le_bytes().to_vec(),
        ])
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.len() != 2 {
            return Err(ActionError::codec("Add requires two segments"));
        }
        let left = u64::from_le_bytes(
            segments[0]
                .as_slice()
                .try_into()
                .map_err(|_| ActionError::codec("left operand must contain 8 bytes"))?,
        );
        let right = u64::from_le_bytes(
            segments[1]
                .as_slice()
                .try_into()
                .map_err(|_| ActionError::codec("right operand must contain 8 bytes"))?,
        );
        Ok(Self { left, right })
    }
}

impl Action for Add {
    const ID: u128 = 1;
    type Output = u64;
}

#[derive(Debug)]
struct Fail;

impl WireValue for Fail {
    const SCHEMA_ID: u64 = 101;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("Fail has no payload"))
        }
    }
}

impl Action for Fail {
    const ID: u128 = 2;
    type Output = ();
}

fn builder(run_id: RunId, count: Arc<AtomicUsize>) -> RuntimeBuilder {
    builder_with_limits(
        run_id,
        count,
        RuntimeLimits {
            default_deadline: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(100),
            dedup_ttl: Duration::from_millis(100),
            ..RuntimeLimits::default()
        },
    )
}

fn builder_with_limits(
    run_id: RunId,
    count: Arc<AtomicUsize>,
    limits: RuntimeLimits,
) -> RuntimeBuilder {
    let mut builder = Runtime::builder(run_id, ProtocolLimits::default(), limits).unwrap();
    builder
        .register::<Add, _>(move |add| {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(add.left + add.right)
        })
        .unwrap();
    builder
        .register::<Fail, _>(|_| Err(ActionError::user("expected failure")))
        .unwrap();
    builder
}

fn pair(
    faults: impl IntoIterator<Item = MemoryFault>,
) -> (Runtime, Runtime, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    pair_with_limits(
        faults,
        RuntimeLimits {
            default_deadline: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(100),
            dedup_ttl: Duration::from_millis(100),
            ..RuntimeLimits::default()
        },
    )
}

fn pair_with_limits(
    faults: impl IntoIterator<Item = MemoryFault>,
    limits: RuntimeLimits,
) -> (Runtime, Runtime, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let run_id = RunId::new(1).unwrap();
    let transports = MemoryNetwork::build(2, run_id, ProtocolLimits::default(), faults).unwrap();
    let mut transports = transports.into_iter();
    let (handle0, driver0) = transports.next().unwrap();
    let (handle1, driver1) = transports.next().unwrap();
    let count0 = Arc::new(AtomicUsize::new(0));
    let count1 = Arc::new(AtomicUsize::new(0));
    let mut builder0 = builder_with_limits(run_id, Arc::clone(&count0), limits);
    builder0.hello().unwrap();
    let mut builder1 = builder_with_limits(run_id, Arc::clone(&count1), limits);
    builder1.hello().unwrap();
    (
        builder0.start(handle0, driver0).unwrap(),
        builder1.start(handle1, driver1).unwrap(),
        count0,
        count1,
    )
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn drive<T: WireValue>(
    left: &mut Runtime,
    right: &mut Runtime,
    future: RemoteFuture<T>,
) -> Result<T, RuntimeError> {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    for _ in 0..10_000 {
        if let Poll::Ready(result) = Future::poll(Pin::as_mut(&mut future), &mut context) {
            return result;
        }
        left.progress(64)?;
        right.progress(64)?;
        std::thread::yield_now();
    }
    panic!("runtime test exceeded bounded progress iterations");
}

fn shutdown_pair(left: &mut Runtime, right: &mut Runtime) {
    for _ in 0..32 {
        let _ = left.progress(64);
        let _ = right.progress(64);
    }
    left.shutdown().unwrap();
    right.shutdown().unwrap();
}

#[test]
fn control_ticket_reservation_survives_action_ticket_saturation() {
    let limits = RuntimeLimits {
        max_pending_calls: 1,
        max_action_queue_per_domain: 1,
        max_dedup_entries: 1,
        default_deadline: Duration::from_millis(100),
        shutdown_timeout: Duration::from_millis(100),
        dedup_ttl: Duration::from_millis(100),
        ..RuntimeLimits::default()
    };
    let (mut left, mut right, _, _) = pair_with_limits(
        (0..4).map(|_| MemoryFault::Delay { progress_ticks: 8 }),
        limits,
    );
    for sequence in 1..=3 {
        left.shared
            .send(
                LocalityId::new(1),
                RuntimeMessage {
                    kind: RuntimeMessageKind::Success,
                    request: RequestId::new(LocalityId::new(1), sequence).unwrap(),
                    action: ActionId::new(Add::ID).unwrap(),
                    domain: DomainId::DEFAULT,
                    deadline_ms: 0,
                    payload: 1_u64.encode().unwrap(),
                },
                TicketPurpose::RequiredResponse,
                None,
            )
            .unwrap();
    }
    assert!(matches!(
        left.shared.send(
            LocalityId::new(1),
            RuntimeMessage {
                kind: RuntimeMessageKind::Success,
                request: RequestId::new(LocalityId::new(1), 4).unwrap(),
                action: ActionId::new(Add::ID).unwrap(),
                domain: DomainId::DEFAULT,
                deadline_ms: 0,
                payload: 1_u64.encode().unwrap(),
            },
            TicketPurpose::RequiredResponse,
            None,
        ),
        Err(RuntimeError::ResourceExhausted {
            resource: ResourceKind::SendTickets,
            limit: 3
        })
    ));
    left.shared
        .send(
            LocalityId::new(1),
            RuntimeMessage {
                kind: RuntimeMessageKind::Cancel,
                request: RequestId::new(LocalityId::new(0), 5).unwrap(),
                action: ActionId::new(Add::ID).unwrap(),
                domain: DomainId::DEFAULT,
                deadline_ms: 0,
                payload: Vec::new(),
            },
            TicketPurpose::BestEffort,
            None,
        )
        .unwrap();
    assert_eq!(left.shared.tickets.lock().unwrap().len(), 4);
    for _ in 0..32 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn handshake_seals_registry_before_transport_start() {
    let invalid_protocol = ProtocolLimits {
        max_payload_bytes: 900,
        max_inflight_bytes_per_peer: 1000,
        control_reserved_bytes_per_peer: 100,
        ..ProtocolLimits::default()
    };
    assert!(matches!(
        Runtime::builder(
            RunId::new(8).unwrap(),
            invalid_protocol,
            RuntimeLimits::default()
        ),
        Err(RuntimeError::InvalidLimits(_))
    ));
    let count = Arc::new(AtomicUsize::new(0));
    let mut builder = builder(RunId::new(9).unwrap(), count);
    let first = builder.hello().unwrap();
    let second = builder.hello().unwrap();
    assert_eq!(first.action_registry_hash, second.action_registry_hash);
    assert!(matches!(
        builder.register::<Add, _>(|add| Ok(add.left + add.right)),
        Err(RuntimeError::BuilderSealed)
    ));
}

#[test]
fn local_and_remote_actions_complete_with_distinct_send_completion() {
    let (mut left, mut right, count0, count1) = pair([]);
    let local = left
        .spawn_on(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Add { left: 2, right: 3 },
        )
        .unwrap();
    assert_eq!(left.block_on(local).unwrap(), 5);
    let remote = left
        .spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 7, right: 11 },
        )
        .unwrap();
    assert_eq!(drive(&mut left, &mut right, remote).unwrap(), 18);
    assert_eq!(count0.load(Ordering::Relaxed), 1);
    assert_eq!(count1.load(Ordering::Relaxed), 1);
    assert_eq!(left.stats().completed_calls, 2);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn duplicated_request_executes_once_and_replays_bounded_result() {
    let (mut left, mut right, _, count1) = pair([MemoryFault::Duplicate]);
    let future = left
        .spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add {
                left: 20,
                right: 22,
            },
        )
        .unwrap();
    assert_eq!(drive(&mut left, &mut right, future).unwrap(), 42);
    assert_eq!(count1.load(Ordering::Relaxed), 1);
    assert_eq!(right.stats().duplicate_requests, 1);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn response_backpressure_retries_without_losing_completion() {
    let (mut left, mut right, _, count1) =
        pair([MemoryFault::Pass, MemoryFault::Saturate, MemoryFault::Pass]);
    let future = left
        .spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 40, right: 2 },
        )
        .unwrap();
    assert_eq!(drive(&mut left, &mut right, future).unwrap(), 42);
    assert_eq!(count1.load(Ordering::Relaxed), 1);
    assert_eq!(right.stats().queued_responses, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn dropped_future_removes_waiter_and_cancels_delayed_action() {
    let (mut left, mut right, _, count1) =
        pair([MemoryFault::Delay { progress_ticks: 5 }, MemoryFault::Pass]);
    let future = left
        .spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 1, right: 1 },
        )
        .unwrap();
    drop(future);
    assert_eq!(left.stats().pending_calls, 0);
    for _ in 0..32 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(count1.load(Ordering::Relaxed), 0);
    assert!(left.stats().cancelled_calls >= 1);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn deadline_and_transport_saturation_are_typed() {
    let (mut left, mut right, _, _) = pair([MemoryFault::Loss]);
    let future = left
        .spawn_on_with(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 1, right: 2 },
            SpawnOptions {
                deadline: Duration::from_millis(2),
                trace_id: None,
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(3));
    assert!(matches!(
        left.block_on(future),
        Err(RuntimeError::DeadlineExceeded { .. })
    ));
    shutdown_pair(&mut left, &mut right);

    let (mut left, mut right, _, _) = pair([MemoryFault::Saturate]);
    assert!(matches!(
        left.spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 1, right: 2 }
        ),
        Err(RuntimeError::Transport(_))
    ));
    assert_eq!(left.stats().pending_calls, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn disconnect_fails_the_run_and_pending_call() {
    let (mut left, mut right, _, _) = pair([MemoryFault::Disconnect]);
    let future = left
        .spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 1, right: 2 },
        )
        .unwrap();
    assert!(matches!(
        drive(&mut left, &mut right, future),
        Err(RuntimeError::PeerFailed(peer)) if peer == LocalityId::new(1)
    ));
    assert_eq!(left.state(), RuntimeState::Failed);
}

#[test]
fn remote_user_error_and_scope_cleanup_are_visible() {
    let (mut left, mut right, _, _) = pair([]);
    let failure = left
        .spawn_on(Place::new(LocalityId::new(1), DomainId::DEFAULT), Fail)
        .unwrap();
    assert!(matches!(
        drive(&mut left, &mut right, failure),
        Err(RuntimeError::RemoteAction { .. })
    ));

    left.scope(|scope| {
        let future = scope.spawn_on(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Add { left: 4, right: 5 },
        )?;
        assert_eq!(scope.block_on(future)?, 9);
        Ok(())
    })
    .unwrap();
    assert_eq!(left.stats().pending_calls, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn shutdown_atomically_rejects_concurrent_client_submission() {
    let (mut left, mut right, _, _) = pair([]);
    let client = left.client();
    let started = Arc::new(std::sync::Barrier::new(2));
    let worker_started = Arc::clone(&started);
    let worker = std::thread::spawn(move || {
        worker_started.wait();
        loop {
            match client.spawn_on(
                Place::new(LocalityId::new(0), DomainId::DEFAULT),
                Add { left: 1, right: 1 },
            ) {
                Ok(future) => drop(future),
                Err(RuntimeError::Shutdown) => break,
                Err(RuntimeError::ResourceExhausted { .. }) => std::thread::yield_now(),
                Err(error) => panic!("unexpected concurrent spawn error: {error}"),
            }
        }
    });
    started.wait();
    let report = left.shutdown().unwrap();
    worker.join().unwrap();
    assert_eq!(report.stats.pending_calls, 0);
    assert_eq!(report.stats.queued_responses, 0);
    right.shutdown().unwrap();
}

#[test]
fn explicit_shutdown_is_clean_reusable_and_rejects_clients() {
    let (mut left, mut right, _, _) = pair([]);
    let stale = left.client();
    shutdown_pair(&mut left, &mut right);
    assert_eq!(left.state(), RuntimeState::Stopped);
    assert_eq!(left.stats().transport.retained_bytes(), 0);
    assert!(matches!(
        stale.spawn_on(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Add { left: 1, right: 1 }
        ),
        Err(RuntimeError::Shutdown)
    ));

    let (mut next_left, mut next_right, _, _) = pair([]);
    let future = next_left
        .spawn_on(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Add { left: 40, right: 2 },
        )
        .unwrap();
    assert_eq!(drive(&mut next_left, &mut next_right, future).unwrap(), 42);
    shutdown_pair(&mut next_left, &mut next_right);
}
