use super::*;
use crate::{
    ActionError, DistributedObject, ObjectReadAction, ObjectWriteAction, RestoreContext, WireValue,
};
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

struct Slow(u64);
impl WireValue for Slow {
    const SCHEMA_ID: u64 = 102;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}
impl Action for Slow {
    const ID: u128 = 3;
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
impl MobileObject for Counter {
    const MOBILITY: Mobility = Mobility::Migratable;
    const SNAPSHOT_VERSION: u32 = 1;
    type Snapshot = u64;

    fn freeze(&mut self) -> Result<Self::Snapshot, ActionError> {
        Ok(self.0)
    }

    fn restore(snapshot: Self::Snapshot, _: RestoreContext) -> Result<Self, ActionError> {
        Ok(Self(snapshot))
    }
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

struct LocalTyped(u64);
impl WireValue for LocalTyped {
    const SCHEMA_ID: u64 = 209;
    fn encode(self) -> Result<Segments, ActionError> {
        Err(ActionError::codec("must not encode local typed action"))
    }
    fn decode(_: Segments) -> Result<Self, ActionError> {
        Err(ActionError::codec("must not decode local typed action"))
    }
}
impl Action for LocalTyped {
    const ID: u128 = 209;
    type Output = u64;
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

impl MobileObject for Blob {
    const MOBILITY: Mobility = Mobility::Migratable;
    const SNAPSHOT_VERSION: u32 = 1;
    type Snapshot = Vec<u8>;

    fn freeze(&mut self) -> Result<Self::Snapshot, ActionError> {
        Ok(self.0.clone())
    }

    fn restore(snapshot: Self::Snapshot, _: RestoreContext) -> Result<Self, ActionError> {
        Ok(Self(snapshot))
    }
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

struct DeclaredPinned;
impl WireValue for DeclaredPinned {
    const SCHEMA_ID: u64 = 229;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("declared pinned payload"))
        }
    }
}
impl DistributedObject for DeclaredPinned {
    const TYPE_ID: u128 = 229;
}
impl MobileObject for DeclaredPinned {
    const MOBILITY: Mobility = Mobility::Pinned;
    const SNAPSHOT_VERSION: u32 = 1;
    type Snapshot = ();
    fn freeze(&mut self) -> Result<Self::Snapshot, ActionError> {
        Ok(())
    }
    fn restore(_: Self::Snapshot, _: RestoreContext) -> Result<Self, ActionError> {
        Ok(Self)
    }
}

struct RefuseRestore {
    logical: u64,
    provider_locality: u64,
}
impl WireValue for RefuseRestore {
    const SCHEMA_ID: u64 = 230;
    fn encode(self) -> Result<Segments, ActionError> {
        self.logical.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self {
            logical: u64::decode(segments)?,
            provider_locality: u64::MAX,
        })
    }
}
impl DistributedObject for RefuseRestore {
    const TYPE_ID: u128 = 230;
}
impl MobileObject for RefuseRestore {
    const MOBILITY: Mobility = Mobility::Reconstructible;
    const SNAPSHOT_VERSION: u32 = 1;
    type Snapshot = u64;

    fn freeze(&mut self) -> Result<Self::Snapshot, ActionError> {
        Ok(self.logical)
    }

    fn restore(snapshot: Self::Snapshot, context: RestoreContext) -> Result<Self, ActionError> {
        if snapshot == 19 && context.destination().locality == LocalityId::new(1) {
            return Err(ActionError::user("destination provider unavailable"));
        }
        Ok(Self {
            logical: snapshot,
            provider_locality: context.destination().locality.get(),
        })
    }
}

struct ReadRefuse;
impl WireValue for ReadRefuse {
    const SCHEMA_ID: u64 = 231;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("read refuse payload"))
        }
    }
}
impl ObjectReadAction<RefuseRestore> for ReadRefuse {
    const ID: u128 = 231;
    type Output = u64;
    fn execute(self, state: &RefuseRestore) -> Result<u64, ActionError> {
        Ok((state.logical << 8) | state.provider_locality)
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
    builder
        .register::<Slow, _>(|slow| {
            std::thread::sleep(Duration::from_millis(slow.0));
            Ok(())
        })
        .unwrap();
    builder
        .register::<LocalTyped, _>(|action| Ok(action.0 + 1))
        .unwrap();
    builder
        .register_mobile_object_read_write::<Counter>()
        .unwrap();
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
    builder.register_mobile_object_read_write::<Blob>().unwrap();
    builder.register_object_read::<Blob, BlobLen>().unwrap();
    builder
        .register_mobile_object_read_write::<RefuseRestore>()
        .unwrap();
    builder
        .register_object_read::<RefuseRestore, ReadRefuse>()
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
fn same_locality_typed_action_skips_wire_codec_and_transport() {
    let (mut left, mut right, _, _) = pair([]);
    let before = left.stats();
    let future = left
        .spawn_on(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            LocalTyped(4),
        )
        .unwrap();
    assert_eq!(left.block_on(future).unwrap(), 5);
    let after = left.stats();
    assert_eq!(
        after.local_typed_dispatches,
        before.local_typed_dispatches + 1
    );
    assert_eq!(
        after.local_action_serializations,
        before.local_action_serializations
    );
    assert_eq!(
        after.transport.submitted_parcels,
        before.transport.submitted_parcels
    );
    shutdown_pair(&mut left, &mut right);
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
fn explicit_migration_preserves_identity_state_and_epoch() {
    let (mut left, mut right, _, _) = pair([]);
    let client = left.client();
    let remote = drive_future(
        &mut left,
        &mut right,
        client
            .create_at(
                Place::new(LocalityId::new(0), DomainId::DEFAULT),
                Counter(7),
            )
            .unwrap(),
    )
    .unwrap();
    let object = remote.object_id();
    let from = remote.observed_location();
    let transfer = drive_future(
        &mut left,
        &mut right,
        remote.transfer_to(LocalityId::new(1)).unwrap(),
    )
    .unwrap();
    let imported = right.client().import_transfer(transfer).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
        .unwrap();
    let report = drive_future(&mut left, &mut right, migration).unwrap();
    assert_eq!(report.object, object);
    assert_eq!(report.from, from);
    assert_eq!(report.to.locality(), LocalityId::new(1));
    assert_eq!(report.to.epoch(), from.epoch() + 1);
    assert_eq!(report.snapshot_bytes, 8);
    assert_eq!(right.stats().objects.live_objects, 1);
    right.clear_object_resolver();
    let request_before = right.shared.next_request.load(Ordering::Relaxed);
    let value = drive_future(
        &mut left,
        &mut right,
        imported.call_write(Increment(5)).unwrap(),
    )
    .unwrap();
    assert_eq!(value, 12);
    assert_eq!(
        right.shared.next_request.load(Ordering::Relaxed),
        request_before
    );
    let back = remote
        .migrate_to(Place::new(LocalityId::new(0), DomainId::DEFAULT))
        .unwrap();
    let back = drive_future(&mut left, &mut right, back).unwrap();
    assert_eq!(back.to.locality(), LocalityId::new(0));
    assert_eq!(back.to.epoch(), report.to.epoch() + 1);
    assert_eq!(left.stats().objects.forwarding_entries, 0);
    let redirect_request = right.shared.next_request.load(Ordering::Relaxed);
    assert_eq!(
        drive_future(
            &mut left,
            &mut right,
            imported.call_read(ReadCounter).unwrap(),
        )
        .unwrap(),
        12
    );
    assert_eq!(
        right.shared.next_request.load(Ordering::Relaxed),
        redirect_request + 1
    );
    assert_eq!(left.stats().objects.completed_migrations, 2);
    drop(imported);
    drop(remote);
    for _ in 0..16 {
        let _ = left.progress(64);
        let _ = right.progress(64);
    }
    assert_eq!(left.stats().objects.live_objects, 0);
    assert_eq!(right.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn mobile_registration_rejects_declared_pinned_types() {
    let mut builder = Runtime::builder(
        RunId::new(77).unwrap(),
        ProtocolLimits::default(),
        RuntimeLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        builder.register_mobile_object_exclusive::<DeclaredPinned>(),
        Err(RuntimeError::PinnedObjectType(_))
    ));
}

#[test]
fn same_locality_migration_changes_domain_without_transporting_calls() {
    let run_id = RunId::new(78).unwrap();
    let mut transports = MemoryNetwork::build(1, run_id, ProtocolLimits::default(), [])
        .unwrap()
        .into_iter();
    let (handle, driver) = transports.next().unwrap();
    let mut builder = builder(run_id, Arc::new(AtomicUsize::new(0)));
    builder.domain(DomainConfig::default()).unwrap();
    builder
        .domain(DomainConfig {
            id: DomainId::new(1),
            ..DomainConfig::default()
        })
        .unwrap();
    builder.hello().unwrap();
    let mut runtime = builder.start(handle, driver).unwrap();
    let create = runtime
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Counter(3),
        )
        .unwrap();
    let remote = runtime.block_on(create).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(0), DomainId::new(1)))
        .unwrap();
    let report = runtime.block_on(migration).unwrap();
    assert_eq!(report.to.domain(), DomainId::new(1));
    let call = remote.call_write(Increment(4)).unwrap();
    assert_eq!(runtime.block_on(call).unwrap(), 7);
    drop(remote);
    runtime.shutdown().unwrap();
}

#[test]
fn lost_precommit_snapshot_transfer_times_out_and_rolls_back() {
    let (mut left, mut right, _, _) = pair([MemoryFault::Loss]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Counter(8),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
        .unwrap();
    assert!(matches!(
        drive_future(&mut left, &mut right, migration),
        Err(RuntimeError::Migration { message, .. }) if message.contains("DeadlineExceeded")
    ));
    let call = remote.call_write(Increment(1)).unwrap();
    assert_eq!(drive_future(&mut left, &mut right, call).unwrap(), 9);
    drop(remote);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn duplicated_migration_parcels_restore_and_execute_once() {
    let (mut left, mut right, _, _) = pair(std::iter::repeat_n(MemoryFault::Duplicate, 24));
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Counter(4),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
        .unwrap();
    let report = drive_future(&mut left, &mut right, migration).unwrap();
    assert_eq!(report.to.epoch(), 2);
    let call = remote.call_write(Increment(1)).unwrap();
    assert_eq!(drive_future(&mut left, &mut right, call).unwrap(), 5);
    drop(remote);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn migration_waits_for_hard_colocation_ticket_release() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(1), DomainId::DEFAULT),
            Counter(1),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let colocated = left.client().spawn_colocated(&remote, Slow(20)).unwrap();
    for _ in 0..32 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
        if right.stats().objects.placement_tickets == 1 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(right.stats().objects.placement_tickets, 1);
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(0), DomainId::DEFAULT))
        .unwrap();
    for _ in 0..8 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(left.stats().objects.completed_migrations, 0);
    drop(colocated);
    let report = drive_future(&mut left, &mut right, migration).unwrap();
    assert_eq!(report.to.locality(), LocalityId::new(0));
    assert_eq!(right.stats().objects.placement_tickets, 0);
    drop(remote);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn dropped_migration_expires_freeze_and_reopens_source_admission() {
    let run_id = RunId::new(79).unwrap();
    let mut transports = MemoryNetwork::build(1, run_id, ProtocolLimits::default(), [])
        .unwrap()
        .into_iter();
    let (handle, driver) = transports.next().unwrap();
    let mut builder = builder(run_id, Arc::new(AtomicUsize::new(0)));
    builder
        .object_limits(ObjectLimits {
            forwarding_ttl: Duration::from_millis(2),
            ..ObjectLimits::default()
        })
        .unwrap();
    builder.domain(DomainConfig::default()).unwrap();
    builder
        .domain(DomainConfig {
            id: DomainId::new(1),
            ..DomainConfig::default()
        })
        .unwrap();
    builder.hello().unwrap();
    let mut runtime = builder.start(handle, driver).unwrap();
    let create = runtime
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Counter(11),
        )
        .unwrap();
    let remote = runtime.block_on(create).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(0), DomainId::new(1)))
        .unwrap();
    drop(migration);
    for _ in 0..8 {
        let _ = runtime.progress(64);
    }
    std::thread::sleep(Duration::from_millis(3));
    for _ in 0..8 {
        let _ = runtime.progress(64);
    }
    let call = remote.call_write(Increment(1)).unwrap();
    assert_eq!(runtime.block_on(call).unwrap(), 12);
    assert_eq!(runtime.stats().objects.active_migrations, 0);
    drop(remote);
    runtime.shutdown().unwrap();
}

#[test]
fn migration_rolls_back_before_commit_when_restore_fails() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            RefuseRestore {
                logical: 19,
                provider_locality: 0,
            },
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let error = drive_future(
        &mut left,
        &mut right,
        remote
            .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
            .unwrap(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Migration { message, .. } if message.contains("provider"))
    );
    assert_eq!(
        drive_future(&mut left, &mut right, remote.call_read(ReadRefuse).unwrap(),).unwrap(),
        19 << 8
    );
    assert_eq!(left.stats().objects.active_migrations, 0);
    assert_eq!(left.stats().objects.rolled_back_migrations, 1);
    drop(remote);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn migrated_object_root_preserves_remote_resident_until_release() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_rooted_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Counter(6),
        )
        .unwrap();
    let (remote, root) = drive_future(&mut left, &mut right, create).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
        .unwrap();
    drive_future(&mut left, &mut right, migration).unwrap();
    drop(remote);
    for _ in 0..8 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(left.stats().objects.roots, 1);
    assert_eq!(right.stats().objects.live_objects, 1);
    root.release();
    for _ in 0..16 {
        left.progress(64).unwrap();
        right.progress(64).unwrap();
    }
    assert_eq!(left.stats().objects.live_objects, 0);
    assert_eq!(right.stats().objects.live_objects, 0);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn reconstructible_migration_rebuilds_destination_resources() {
    let (mut left, mut right, _, _) = pair([]);
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            RefuseRestore {
                logical: 20,
                provider_locality: 0,
            },
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let migration = remote
        .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
        .unwrap();
    drive_future(&mut left, &mut right, migration).unwrap();
    assert_eq!(
        drive_future(&mut left, &mut right, remote.call_read(ReadRefuse).unwrap()).unwrap(),
        (20 << 8) | 1
    );
    drop(remote);
    shutdown_pair(&mut left, &mut right);
}

#[test]
fn segmented_megabyte_snapshot_migrates_without_state_in_object_calls() {
    let (mut left, mut right, _, _) = pair([]);
    let bytes = 1024 * 1024 + 17;
    let create = left
        .client()
        .create_at(
            Place::new(LocalityId::new(0), DomainId::DEFAULT),
            Blob(vec![7; bytes]),
        )
        .unwrap();
    let remote = drive_future(&mut left, &mut right, create).unwrap();
    let report = drive_future(
        &mut left,
        &mut right,
        remote
            .migrate_to(Place::new(LocalityId::new(1), DomainId::DEFAULT))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(report.snapshot_bytes, bytes);
    assert_eq!(
        drive_future(&mut left, &mut right, remote.call_read(BlobLen).unwrap()).unwrap(),
        bytes as u64
    );
    drop(remote);
    shutdown_pair(&mut left, &mut right);
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
        Err(RuntimeError::Protocol(message)) if message.contains("redirect")
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
    assert_eq!(left.stats().objects.placement_tickets, 0);
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
