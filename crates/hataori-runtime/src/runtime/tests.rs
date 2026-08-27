use super::*;
use crate::{ActionError, DistributedObject, ObjectReadAction, ObjectWriteAction, WireValue};
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

#[derive(Clone, Debug)]
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

struct Counter(u64);
impl WireValue for Counter {
    const SCHEMA_ID: u64 = 200;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}
impl DistributedObject for Counter {
    const TYPE_ID: u128 = 200;
}

struct Increment(u64);
impl WireValue for Increment {
    const SCHEMA_ID: u64 = 201;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}
impl ObjectWriteAction<Counter> for Increment {
    const ID: u128 = 201;
    type Output = u64;
    fn execute(self, state: &mut Counter) -> Result<u64, ActionError> {
        state.0 += self.0;
        Ok(state.0)
    }
}

struct ReadCounter;
impl WireValue for ReadCounter {
    const SCHEMA_ID: u64 = 202;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("read payload"))
        }
    }
}
impl ObjectReadAction<Counter> for ReadCounter {
    const ID: u128 = 202;
    type Output = u64;
    fn execute(self, state: &Counter) -> Result<u64, ActionError> {
        Ok(state.0)
    }
}

struct PanicCounter;
impl WireValue for PanicCounter {
    const SCHEMA_ID: u64 = 203;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("panic payload"))
        }
    }
}
impl ObjectWriteAction<Counter> for PanicCounter {
    const ID: u128 = 203;
    type Output = ();
    fn execute(self, _: &mut Counter) -> Result<(), ActionError> {
        panic!("expected object panic")
    }
}

struct LocalOnly(u64);
impl WireValue for LocalOnly {
    const SCHEMA_ID: u64 = 210;
    fn encode(self) -> Result<Segments, ActionError> {
        Err(ActionError::codec("must not encode local object"))
    }
    fn decode(_: Segments) -> Result<Self, ActionError> {
        Err(ActionError::codec("must not decode local object"))
    }
}
impl DistributedObject for LocalOnly {
    const TYPE_ID: u128 = 210;
}

struct LocalOnlyAdd(u64);
impl WireValue for LocalOnlyAdd {
    const SCHEMA_ID: u64 = 211;
    fn encode(self) -> Result<Segments, ActionError> {
        Err(ActionError::codec("must not encode local action"))
    }
    fn decode(_: Segments) -> Result<Self, ActionError> {
        Err(ActionError::codec("must not decode local action"))
    }
}
impl ObjectWriteAction<LocalOnly> for LocalOnlyAdd {
    const ID: u128 = 211;
    type Output = u64;
    fn execute(self, state: &mut LocalOnly) -> Result<u64, ActionError> {
        state.0 += self.0;
        Ok(state.0)
    }
}

struct CheckDomain;
impl WireValue for CheckDomain {
    const SCHEMA_ID: u64 = 212;
    fn encode(self) -> Result<Segments, ActionError> {
        Err(ActionError::codec("must not encode local action"))
    }
    fn decode(_: Segments) -> Result<Self, ActionError> {
        Err(ActionError::codec("must not decode local action"))
    }
}
impl ObjectWriteAction<LocalOnly> for CheckDomain {
    const ID: u128 = 212;
    type Output = u64;
    fn execute(self, _: &mut LocalOnly) -> Result<u64, ActionError> {
        Ok(u64::from(rayon::current_thread_index().is_some()))
    }
}

struct Blob(Vec<u8>);
impl WireValue for Blob {
    const SCHEMA_ID: u64 = 220;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(vec![self.0])
    }
    fn decode(mut segments: Segments) -> Result<Self, ActionError> {
        if segments.len() != 1 {
            return Err(ActionError::codec("blob payload"));
        }
        Ok(Self(segments.pop().unwrap()))
    }
}
impl DistributedObject for Blob {
    const TYPE_ID: u128 = 220;
}

struct BlobLen;
impl WireValue for BlobLen {
    const SCHEMA_ID: u64 = 221;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("blob len payload"))
        }
    }
}
impl ObjectReadAction<Blob> for BlobLen {
    const ID: u128 = 221;
    type Output = u64;
    fn execute(self, state: &Blob) -> Result<u64, ActionError> {
        Ok(state.0.len() as u64)
    }
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
        .object_limits(ObjectLimits {
            max_placement_tickets: 1,
            ..ObjectLimits::default()
        })
        .unwrap();
    builder
        .register::<Add, _>(move |add| {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(add.left + add.right)
        })
        .unwrap();
    builder
        .register::<Fail, _>(|_| Err(ActionError::user("expected failure")))
        .unwrap();
    builder.register_object_read_write::<Counter>().unwrap();
    builder
        .register_object_read::<Counter, ReadCounter>()
        .unwrap();
    builder
        .register_object_write::<Counter, Increment>()
        .unwrap();
    builder
        .register_object_write::<Counter, PanicCounter>()
        .unwrap();
    builder.register_object_exclusive::<LocalOnly>().unwrap();
    builder
        .register_object_write::<LocalOnly, LocalOnlyAdd>()
        .unwrap();
    builder
        .register_object_write::<LocalOnly, CheckDomain>()
        .unwrap();
    builder.register_object_read_write::<Blob>().unwrap();
    builder.register_object_read::<Blob, BlobLen>().unwrap();
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
    drive_future(left, right, future)
}

fn drive_future<T>(
    left: &mut Runtime,
    right: &mut Runtime,
    future: impl Future<Output = Result<T, RuntimeError>>,
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
    for _ in 0..10_000 {
        let _ = left.progress(64);
        let _ = right.progress(64);
        let settled = [&left.stats(), &right.stats()].into_iter().all(|stats| {
            stats.pending_calls == 0
                && stats.send_tickets == 0
                && stats.queued_responses == 0
                && stats.objects.queued_calls == 0
                && stats.objects.in_flight_calls == 0
                && stats.transport.pending_events == 0
                && stats.domains.iter().all(|(_, domain)| {
                    domain.queued == 0 && domain.running == 0 && domain.pending_completions == 0
                })
        });
        if settled {
            break;
        }
        std::thread::yield_now();
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
fn fixed_remote_object_creation_calls_clone_lease_and_collection() {
    let (mut left, mut right, left_count, right_count) = pair([]);
    let client = left.client();
    let create = client
        .create_at(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Counter(4),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    assert_eq!(remote.observed_location().locality(), LocalityId::new(1));
    let clone = remote.clone();
    let mut stale = remote.clone();
    stale.location = hataori_runtime_foundation::protocol::ObjectLocation::new(
        stale.location.locality(),
        stale.location.domain(),
        stale.location.slot(),
        stale.location.generation(),
        stale.location.epoch() + 1,
    )
    .unwrap();
    left.shared.objects.clear_resolver();
    assert!(matches!(
        drive_future(&mut left, &mut right, stale.call_read(ReadCounter).unwrap()),
        Err(RuntimeError::RemoteObject { .. })
    ));
    assert_eq!(left.state(), RuntimeState::Running);
    left.shared
        .objects
        .cache_location(remote.object, remote.location);
    let weak = remote.downgrade();
    let decoded_weak = WeakRemote::<Counter>::decode(weak.encode().unwrap()).unwrap();
    assert_eq!(decoded_weak.object_id(), remote.object_id());
    let upgraded = drive_future(&mut left, &mut right, weak.upgrade(&client).unwrap()).unwrap();
    assert_eq!(
        drive_future(
            &mut left,
            &mut right,
            remote.call_write(Increment(3)).unwrap()
        )
        .unwrap(),
        7
    );
    assert_eq!(
        drive_future(&mut left, &mut right, clone.call_read(ReadCounter).unwrap()).unwrap(),
        7
    );
    assert_eq!(right.stats().objects.live_objects, 1);
    assert_eq!(right.stats().objects.leases, 1);
    let colocated = client
        .spawn_colocated(&remote, Add { left: 2, right: 5 })
        .unwrap();
    assert_eq!(left.stats().objects.placement_tickets, 1);
    let preferred = client
        .spawn_preferred_colocated(&remote, PlacementFallback::Any, Add { left: 4, right: 5 })
        .unwrap();
    assert_eq!(drive_future(&mut left, &mut right, preferred).unwrap(), 9);
    assert_eq!(left_count.load(Ordering::Relaxed), 1);
    assert_eq!(drive_future(&mut left, &mut right, colocated).unwrap(), 7);
    assert_eq!(right_count.load(Ordering::Relaxed), 1);
    assert_eq!(left.stats().objects.placement_tickets, 0);
    drop(remote);
    drop(stale);
    drop(upgraded);
    assert_eq!(right.stats().objects.leases, 1);
    drop(clone);
    for _ in 0..32 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(right.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn remote_object_panic_releases_admission_and_pin() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Counter(1),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    assert!(matches!(
        drive_future(
            &mut left,
            &mut right,
            remote.call_write(PanicCounter).unwrap()
        ),
        Err(RuntimeError::RemoteAction { .. })
    ));
    assert_eq!(right.stats().objects.in_flight_calls, 0);
    assert_eq!(right.stats().objects.queued_calls, 0);
    drop(remote);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn segmented_megabyte_object_state_is_created_once_and_called_remotely() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Blob(vec![0x5a; 1024 * 1024 + 17]),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    assert_eq!(
        drive_future(&mut left, &mut right, remote.call_read(BlobLen).unwrap()).unwrap(),
        1024 * 1024 + 17
    );
    drop(remote);
    for _ in 0..32 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(right.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn same_locality_object_creation_and_call_skip_codecs_and_transport() {
    let (mut left, mut right, _, _) = pair([]);
    let sent_before = left.stats().sent_by_channel;
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            LocalOnly(5),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    assert_eq!(
        drive_future(
            &mut left,
            &mut right,
            remote.call_write(LocalOnlyAdd(4)).unwrap(),
        )
        .unwrap(),
        9
    );
    assert_eq!(
        drive_future(
            &mut left,
            &mut right,
            remote.call_write(CheckDomain).unwrap(),
        )
        .unwrap(),
        1
    );
    assert_eq!(left.stats().sent_by_channel, sent_before);
    assert_eq!(left.stats().objects.local_calls, 2);
    drop(remote);
    assert_eq!(left.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn lease_transfer_imports_once_and_enables_same_locality_fast_call() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Counter(12),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let transfer = drive_future(
        &mut left,
        &mut right,
        remote.transfer_to(LocalityId::new(1)).unwrap(),
    )
    .unwrap();
    assert_eq!(right.stats().objects.leases, 2);
    let imported = right.client().import_transfer(transfer).unwrap();
    for _ in 0..16 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(right.stats().objects.transfers, 0);
    drop(remote);
    for _ in 0..16 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    let sent_before = right.stats().sent_by_channel;
    assert_eq!(
        drive_future(
            &mut right,
            &mut left,
            imported.call_read(ReadCounter).unwrap()
        )
        .unwrap(),
        12
    );
    assert_eq!(right.stats().sent_by_channel, sent_before);
    assert_eq!(right.stats().objects.local_calls, 1);
    drop(imported);
    assert_eq!(right.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn object_root_retains_after_last_lease_and_releases_mechanically() {
    let (mut left, mut right, _, _) = pair([]);
    let rooted = left
        .client()
        .create_rooted_at(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Counter(9),
        )
        .unwrap();
    let (remote, root) = drive_future(&mut left, &mut right, rooted).unwrap();
    drop(remote);
    for _ in 0..16 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(right.stats().objects.live_objects, 1);
    assert_eq!(right.stats().objects.roots, 1);
    drop(root);
    for _ in 0..16 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(right.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
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
