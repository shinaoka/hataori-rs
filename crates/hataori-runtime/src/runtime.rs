use crate::{
    action::{Action, ActionRegistry, Segments},
    dedup::{DedupDisposition, DedupTable},
    domain::{ActionCompletion, ActionJob, DomainConfig, DomainRegistry, DomainStats},
    error::{cap, ResourceKind, RuntimeError, RuntimeState},
    pending::{PendingEntry, PendingTable, Promise, RemoteFuture},
    wire::{self, RuntimeMessage, RuntimeMessageKind},
};
use hataori_runtime_foundation::{
    protocol::{
        ActionId, Channel, DomainId, Hello, LocalityId, MessageId, Parcel, ParcelKind,
        ProtocolLimits, RequestId, RunId, RuntimeVersion, TraceId,
    },
    transport::{SendTicket, TransportDriver, TransportEvent, TransportHandle, TransportStats},
};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

const RUNTIME_VERSION: RuntimeVersion = RuntimeVersion {
    major: 0,
    minor: 2,
    patch: 0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Place {
    pub locality: LocalityId,
    pub domain: DomainId,
}

impl Place {
    pub const fn new(locality: LocalityId, domain: DomainId) -> Self {
        Self { locality, domain }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpawnOptions {
    pub deadline: Duration,
    pub trace_id: Option<TraceId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimits {
    pub max_pending_calls: usize,
    pub max_domains: usize,
    pub max_workers_per_domain: usize,
    pub max_action_queue_per_domain: usize,
    pub max_dedup_entries: usize,
    pub max_dedup_bytes: usize,
    pub max_progress_events: usize,
    pub default_deadline: Duration,
    pub dedup_ttl: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_pending_calls: 1024,
            max_domains: 16,
            max_workers_per_domain: 64,
            max_action_queue_per_domain: 256,
            max_dedup_entries: 1024,
            max_dedup_bytes: 16 * 1024 * 1024,
            max_progress_events: 256,
            default_deadline: Duration::from_secs(30),
            dedup_ttl: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl RuntimeLimits {
    pub fn validate(self) -> Result<Self, RuntimeError> {
        if self.max_pending_calls == 0
            || self.max_domains == 0
            || self.max_workers_per_domain == 0
            || self.max_action_queue_per_domain == 0
            || self.max_dedup_entries == 0
            || self.max_dedup_bytes == 0
            || self.max_progress_events == 0
            || self.default_deadline.is_zero()
            || self.dedup_ttl.is_zero()
            || self.shutdown_timeout.is_zero()
        {
            return Err(RuntimeError::InvalidLimits(
                "runtime limits and durations must be nonzero",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeProgress {
    pub events: usize,
    pub made_progress: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeStats {
    pub state: RuntimeState,
    pub pending_calls: usize,
    pub completed_calls: u64,
    pub cancelled_calls: u64,
    pub timed_out_calls: u64,
    pub late_results: u64,
    pub duplicate_requests: u64,
    pub action_failures: u64,
    pub dedup_entries: usize,
    pub dedup_bytes: usize,
    pub sent_by_channel: [u64; 3],
    pub received_by_channel: [u64; 3],
    pub domains: Vec<(DomainId, DomainStats)>,
    pub send_tickets: usize,
    pub queued_responses: usize,
    pub queued_response_bytes: usize,
    pub transport: TransportStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    pub stats: RuntimeStats,
}

pub struct RuntimeBuilder {
    run_id: RunId,
    protocol_limits: ProtocolLimits,
    runtime_limits: RuntimeLimits,
    registry: ActionRegistry,
    domains: Vec<DomainConfig>,
    sealed: bool,
}

impl RuntimeBuilder {
    pub fn register<A, F>(&mut self, handler: F) -> Result<&mut Self, RuntimeError>
    where
        A: Action,
        F: Fn(A) -> Result<A::Output, crate::ActionError> + Send + Sync + 'static,
    {
        if self.sealed {
            return Err(RuntimeError::BuilderSealed);
        }
        self.registry.register::<A, F>(handler)?;
        Ok(self)
    }

    pub fn domain(&mut self, config: DomainConfig) -> Result<&mut Self, RuntimeError> {
        if self.sealed {
            return Err(RuntimeError::BuilderSealed);
        }
        if config.workers == 0
            || config.workers > self.runtime_limits.max_workers_per_domain
            || config.queue_capacity == 0
            || config.queue_capacity > self.runtime_limits.max_action_queue_per_domain
        {
            return Err(RuntimeError::InvalidLimits("invalid domain configuration"));
        }
        if self.domains.len() >= self.runtime_limits.max_domains {
            return Err(RuntimeError::InvalidLimits("too many domains"));
        }
        if self.domains.iter().any(|domain| domain.id == config.id) {
            return Err(RuntimeError::InvalidLimits("duplicate domain id"));
        }
        self.domains.push(config);
        Ok(self)
    }

    pub fn hello(&mut self) -> Result<Hello, RuntimeError> {
        self.sealed = true;
        Ok(Hello {
            run_id: self.run_id,
            runtime_version: RUNTIME_VERSION,
            action_registry_hash: self.registry.fingerprint(),
            object_registry_hash: [0; 32],
            capabilities: 1,
            limits: self.protocol_limits.validate().map_err(|error| {
                RuntimeError::Protocol(format!("invalid protocol limits: {error}"))
            })?,
        })
    }

    pub fn start<H, D>(mut self, handle: H, driver: D) -> Result<Runtime, RuntimeError>
    where
        H: TransportHandle + 'static,
        D: TransportDriver + 'static,
    {
        if !self.sealed {
            return Err(RuntimeError::HandshakeNotPrepared);
        }
        let local_id = driver.local_id();
        if !driver.members().contains(&local_id) {
            return Err(RuntimeError::InvalidMembership(local_id));
        }
        if self.domains.is_empty() {
            self.domains.push(DomainConfig {
                queue_capacity: self.runtime_limits.max_action_queue_per_domain,
                ..DomainConfig::default()
            });
        }
        let completion_capacity = self
            .runtime_limits
            .max_pending_calls
            .checked_add(self.runtime_limits.max_dedup_entries)
            .ok_or(RuntimeError::InvalidLimits("completion capacity overflow"))?;
        let domains = DomainRegistry::new(&self.domains, completion_capacity)?;
        let (local_tx, local_rx) = mpsc::sync_channel(self.runtime_limits.max_pending_calls);
        let state = Arc::new(AtomicU8::new(RuntimeState::Bootstrapping as u8));
        let shared = Arc::new(Shared {
            state: Arc::clone(&state),
            local_id,
            run_id: self.run_id,
            limits: self.runtime_limits,
            protocol_limits: self.protocol_limits,
            transport: Arc::new(handle),
            registry: Arc::new(self.registry),
            pending: Arc::new(PendingTable::new(self.runtime_limits.max_pending_calls)),
            local_tx,
            next_request: AtomicU64::new(1),
            next_message: AtomicU64::new(1),
            tickets: Mutex::new(HashMap::new()),
            work_gate: Mutex::new(()),
            counters: SharedCounters::default(),
        });
        state.store(RuntimeState::Running as u8, Ordering::Release);
        Ok(Runtime {
            shared,
            driver: Box::new(driver),
            domains,
            local_rx,
            dedup: DedupTable::new(
                self.runtime_limits.max_dedup_entries,
                self.runtime_limits.max_dedup_bytes,
                self.runtime_limits.dedup_ttl,
            ),
            responses: VecDeque::new(),
            response_bytes: 0,
        })
    }
}

#[derive(Default)]
struct SharedCounters {
    completed: AtomicU64,
    cancelled: AtomicU64,
    timed_out: AtomicU64,
    late_results: AtomicU64,
    duplicate_requests: AtomicU64,
    action_failures: AtomicU64,
    sent: [AtomicU64; 3],
    received: [AtomicU64; 3],
}

enum TicketPurpose {
    Request(RequestId),
    RequiredResponse,
    BestEffort,
}

struct LocalRequest {
    message: RuntimeMessage,
    cancel_token: Arc<std::sync::atomic::AtomicBool>,
    trace_id: Option<TraceId>,
}

struct Shared {
    state: Arc<AtomicU8>,
    local_id: LocalityId,
    run_id: RunId,
    limits: RuntimeLimits,
    protocol_limits: ProtocolLimits,
    transport: Arc<dyn TransportHandle>,
    registry: Arc<ActionRegistry>,
    pending: Arc<PendingTable>,
    local_tx: SyncSender<LocalRequest>,
    next_request: AtomicU64,
    next_message: AtomicU64,
    tickets: Mutex<HashMap<SendTicket, TicketPurpose>>,
    work_gate: Mutex<()>,
    counters: SharedCounters,
}

impl Shared {
    fn state(&self) -> RuntimeState {
        decode_state(self.state.load(Ordering::Acquire))
    }

    fn ensure_running(&self) -> Result<(), RuntimeError> {
        let state = self.state();
        if state == RuntimeState::Running {
            Ok(())
        } else if matches!(state, RuntimeState::Draining | RuntimeState::Stopped) {
            Err(RuntimeError::Shutdown)
        } else {
            Err(RuntimeError::InvalidState {
                expected: RuntimeState::Running,
                actual: state,
            })
        }
    }

    fn next_request(&self) -> Result<RequestId, RuntimeError> {
        let sequence = self
            .next_request
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1).filter(|next| *next != 0)
            })
            .map_err(|_| RuntimeError::ResourceExhausted {
                resource: ResourceKind::PendingCalls,
                limit: self.limits.max_pending_calls,
            })?;
        RequestId::new(self.local_id, sequence)
            .map_err(|_| RuntimeError::Protocol("request id overflow".into()))
    }

    fn next_message(&self) -> Result<MessageId, RuntimeError> {
        let value = self
            .next_message
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1).filter(|next| *next != 0)
            })
            .map_err(|_| RuntimeError::Protocol("message id overflow".into()))?;
        MessageId::new(u128::from(value))
            .map_err(|_| RuntimeError::Protocol("zero message id".into()))
    }

    fn send(
        &self,
        destination: LocalityId,
        message: RuntimeMessage,
        purpose: TicketPurpose,
        trace_id: Option<TraceId>,
    ) -> Result<SendTicket, RuntimeError> {
        let channel = if message.kind == RuntimeMessageKind::Cancel {
            Channel::Control
        } else {
            Channel::Action
        };
        let parcel_kind = if channel == Channel::Control {
            ParcelKind::Control
        } else {
            ParcelKind::Data
        };
        let parcel = Parcel {
            run_id: self.run_id,
            message_id: self.next_message()?,
            channel,
            kind: parcel_kind,
            source: self.local_id,
            destination,
            trace_id,
            segments: wire::encode(message),
        };
        parcel.validate(self.protocol_limits).map_err(|error| {
            RuntimeError::Protocol(format!("runtime parcel exceeds limits: {error}"))
        })?;
        let mut tickets = self.tickets.lock().unwrap();
        let action_limit = self
            .limits
            .max_pending_calls
            .saturating_add(self.limits.max_dedup_entries)
            .saturating_add(self.limits.max_action_queue_per_domain);
        let total_limit = action_limit.saturating_add(self.limits.max_pending_calls);
        let limit = if channel == Channel::Control {
            total_limit
        } else {
            action_limit
        };
        if tickets.len() >= limit {
            return Err(RuntimeError::ResourceExhausted {
                resource: ResourceKind::SendTickets,
                limit,
            });
        }
        let ticket = self.transport.try_send(parcel)?;
        tickets.insert(ticket, purpose);
        self.counters.sent[channel as usize].fetch_add(1, Ordering::Relaxed);
        Ok(ticket)
    }

    fn cancel(&self, request: RequestId) {
        let Some(entry) = self.pending.remove(request) else {
            return;
        };
        self.counters.cancelled.fetch_add(1, Ordering::Relaxed);
        if let Some(token) = entry.cancel_token {
            token.store(true, Ordering::Release);
        }
        if entry.destination != self.local_id {
            let message = RuntimeMessage {
                kind: RuntimeMessageKind::Cancel,
                request,
                action: entry.action,
                domain: entry.domain,
                deadline_ms: 0,
                payload: Vec::new(),
            };
            let _ = self.send(
                entry.destination,
                message,
                TicketPurpose::BestEffort,
                entry.trace_id,
            );
        }
    }
}

#[derive(Clone)]
pub struct RuntimeClient {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for RuntimeClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeClient")
            .field("local_id", &self.shared.local_id)
            .field("state", &self.shared.state())
            .finish_non_exhaustive()
    }
}

impl RuntimeClient {
    pub fn local_id(&self) -> LocalityId {
        self.shared.local_id
    }

    pub fn spawn_on<A: Action>(
        &self,
        place: Place,
        action: A,
    ) -> Result<RemoteFuture<A::Output>, RuntimeError> {
        self.spawn_on_with(
            place,
            action,
            SpawnOptions {
                deadline: self.shared.limits.default_deadline,
                trace_id: None,
            },
        )
    }

    pub fn spawn_on_with<A: Action>(
        &self,
        place: Place,
        action: A,
        options: SpawnOptions,
    ) -> Result<RemoteFuture<A::Output>, RuntimeError> {
        self.shared.ensure_running()?;
        if options.deadline.is_zero() {
            return Err(RuntimeError::InvalidDeadline);
        }
        let action_id = ActionId::new(A::ID).map_err(|_| RuntimeError::InvalidActionId)?;
        if self.shared.registry.get(action_id).is_none() {
            return Err(RuntimeError::UnknownAction(action_id));
        }
        let payload = action
            .encode()
            .map_err(|error| RuntimeError::Protocol(format!("action encode failed: {error}")))?;
        let deadline_ms = u64::try_from(options.deadline.as_millis().max(1))
            .map_err(|_| RuntimeError::InvalidDeadline)?;
        let _gate = self.shared.work_gate.lock().unwrap();
        self.shared.ensure_running()?;
        let request = self.shared.next_request()?;
        let promise = Arc::new(Promise::new());
        let expires_at = Instant::now()
            .checked_add(options.deadline)
            .ok_or(RuntimeError::InvalidDeadline)?;
        let cancel_token = (place.locality == self.shared.local_id)
            .then(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        self.shared.pending.insert(
            request,
            PendingEntry {
                promise: Arc::clone(&promise),
                destination: place.locality,
                action: action_id,
                domain: place.domain,
                trace_id: options.trace_id,
                expires_at,
                deadline: options.deadline,
                cancel_token: cancel_token.clone(),
            },
        )?;
        let message = RuntimeMessage {
            kind: RuntimeMessageKind::Request,
            request,
            action: action_id,
            domain: place.domain,
            deadline_ms,
            payload,
        };
        let submitted = if place.locality == self.shared.local_id {
            self.shared
                .local_tx
                .try_send(LocalRequest {
                    message,
                    cancel_token: cancel_token.unwrap(),
                    trace_id: options.trace_id,
                })
                .map_err(|error| match error {
                    TrySendError::Full(_) => RuntimeError::ResourceExhausted {
                        resource: ResourceKind::ActionQueue,
                        limit: self.shared.limits.max_pending_calls,
                    },
                    TrySendError::Disconnected(_) => RuntimeError::Shutdown,
                })
                .map(|_| ())
        } else {
            self.shared
                .send(
                    place.locality,
                    message,
                    TicketPurpose::Request(request),
                    options.trace_id,
                )
                .map(|_| ())
        };
        if let Err(error) = submitted {
            self.shared.pending.remove(request);
            return Err(error);
        }
        let shared = Arc::clone(&self.shared);
        Ok(RemoteFuture::new(
            request,
            promise,
            Arc::new(move |request| shared.cancel(request)),
        ))
    }

    pub(crate) fn cancel(&self, request: RequestId) {
        self.shared.cancel(request);
    }
}

struct QueuedResponse {
    destination: LocalityId,
    message: RuntimeMessage,
    trace_id: Option<TraceId>,
    cache: bool,
    bytes: usize,
}

pub struct Runtime {
    shared: Arc<Shared>,
    driver: Box<dyn TransportDriver>,
    domains: DomainRegistry,
    local_rx: Receiver<LocalRequest>,
    dedup: DedupTable,
    responses: VecDeque<QueuedResponse>,
    response_bytes: usize,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        formatter
            .debug_struct("Runtime")
            .field("local_id", &self.shared.local_id)
            .field("state", &stats.state)
            .field("pending_calls", &stats.pending_calls)
            .field("dedup_entries", &stats.dedup_entries)
            .field("send_tickets", &stats.send_tickets)
            .field("queued_responses", &stats.queued_responses)
            .field("queued_response_bytes", &stats.queued_response_bytes)
            .finish_non_exhaustive()
    }
}

impl Runtime {
    pub fn builder(
        run_id: RunId,
        protocol_limits: ProtocolLimits,
        runtime_limits: RuntimeLimits,
    ) -> Result<RuntimeBuilder, RuntimeError> {
        protocol_limits
            .validate()
            .map_err(|error| RuntimeError::Protocol(format!("invalid protocol limits: {error}")))?;
        let largest_runtime_frame = protocol_limits
            .max_payload_bytes
            .checked_add(protocol_limits.max_segments.saturating_mul(8))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(RuntimeError::InvalidLimits(
                "runtime frame capacity overflow",
            ))?;
        let action_byte_limit = protocol_limits
            .max_inflight_bytes_per_peer
            .checked_sub(protocol_limits.control_reserved_bytes_per_peer)
            .ok_or(RuntimeError::InvalidLimits(
                "control reservation exceeds in-flight capacity",
            ))?;
        if largest_runtime_frame > action_byte_limit {
            return Err(RuntimeError::InvalidLimits(
                "one maximum runtime parcel must fit the action byte reservation",
            ));
        }
        Ok(RuntimeBuilder {
            run_id,
            protocol_limits,
            runtime_limits: runtime_limits.validate()?,
            registry: ActionRegistry::default(),
            domains: Vec::new(),
            sealed: false,
        })
    }

    pub fn client(&self) -> RuntimeClient {
        RuntimeClient {
            shared: Arc::clone(&self.shared),
        }
    }

    pub fn local_id(&self) -> LocalityId {
        self.shared.local_id
    }

    pub fn state(&self) -> RuntimeState {
        self.shared.state()
    }

    pub fn spawn_on<A: Action>(
        &self,
        place: Place,
        action: A,
    ) -> Result<RemoteFuture<A::Output>, RuntimeError> {
        self.client().spawn_on(place, action)
    }

    pub fn spawn_on_with<A: Action>(
        &self,
        place: Place,
        action: A,
        options: SpawnOptions,
    ) -> Result<RemoteFuture<A::Output>, RuntimeError> {
        self.client().spawn_on_with(place, action, options)
    }

    pub fn progress(&mut self, max_events: usize) -> Result<RuntimeProgress, RuntimeError> {
        let state = self.state();
        if !matches!(state, RuntimeState::Running | RuntimeState::Draining) {
            return Err(RuntimeError::InvalidState {
                expected: RuntimeState::Running,
                actual: state,
            });
        }
        let limit = max_events.min(self.shared.limits.max_progress_events);
        if limit == 0 {
            return Ok(RuntimeProgress::default());
        }
        self.expire_pending();
        self.dedup.expire(Instant::now());
        let mut events = Vec::new();
        let transport = self.driver.progress(&mut events, limit).map_err(|error| {
            self.fail(RuntimeError::Transport(error.clone()));
            RuntimeError::Transport(error)
        })?;
        let mut handled = 0;
        for event in events {
            if let Err(error) = self.handle_transport_event(event) {
                self.fail(error.clone());
                return Err(error);
            }
            handled += 1;
        }
        while handled < limit {
            let Some(completion) = self.domains.try_completion() else {
                break;
            };
            if let Err(error) = self.handle_completion(completion) {
                self.fail(error.clone());
                return Err(error);
            }
            handled += 1;
        }
        while handled < limit {
            match self.local_rx.try_recv() {
                Ok(request) => {
                    self.dispatch_local(request)?;
                    handled += 1;
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        let sent = match self.drain_responses(limit - handled) {
            Ok(sent) => sent,
            Err(error) => {
                self.fail(error.clone());
                return Err(error);
            }
        };
        handled += sent;
        Ok(RuntimeProgress {
            events: handled,
            made_progress: transport.made_progress || handled > 0,
        })
    }

    pub fn block_on<F, T>(&mut self, future: F) -> Result<T, RuntimeError>
    where
        F: Future<Output = Result<T, RuntimeError>>,
    {
        struct ThreadWake(std::thread::Thread);
        impl Wake for ThreadWake {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            match self.progress(self.shared.limits.max_progress_events) {
                Ok(progress) if progress.made_progress => {}
                Ok(_) => std::thread::park_timeout(Duration::from_millis(1)),
                Err(error) => {
                    self.fail(error.clone());
                    return Err(error);
                }
            }
        }
    }

    pub fn stats(&self) -> RuntimeStats {
        RuntimeStats {
            state: self.state(),
            pending_calls: self.shared.pending.len(),
            completed_calls: self.shared.counters.completed.load(Ordering::Relaxed),
            cancelled_calls: self.shared.counters.cancelled.load(Ordering::Relaxed),
            timed_out_calls: self.shared.counters.timed_out.load(Ordering::Relaxed),
            late_results: self.shared.counters.late_results.load(Ordering::Relaxed),
            duplicate_requests: self
                .shared
                .counters
                .duplicate_requests
                .load(Ordering::Relaxed),
            action_failures: self.shared.counters.action_failures.load(Ordering::Relaxed),
            dedup_entries: self.dedup.len(),
            dedup_bytes: self.dedup.retained_bytes(),
            sent_by_channel: std::array::from_fn(|index| {
                self.shared.counters.sent[index].load(Ordering::Relaxed)
            }),
            received_by_channel: std::array::from_fn(|index| {
                self.shared.counters.received[index].load(Ordering::Relaxed)
            }),
            domains: self.domains.stats(),
            send_tickets: self.shared.tickets.lock().unwrap().len(),
            queued_responses: self.responses.len(),
            queued_response_bytes: self.response_bytes,
            transport: self.driver.stats(),
        }
    }

    pub fn shutdown(&mut self) -> Result<ShutdownReport, RuntimeError> {
        let state = self.state();
        if state == RuntimeState::Stopped {
            return Ok(ShutdownReport {
                stats: self.stats(),
            });
        }
        if !matches!(state, RuntimeState::Running | RuntimeState::Failed) {
            return Err(RuntimeError::InvalidState {
                expected: RuntimeState::Running,
                actual: state,
            });
        }
        let work_gate = self.shared.work_gate.lock().unwrap();
        self.shared
            .state
            .store(RuntimeState::Draining as u8, Ordering::Release);
        for (request, entry) in self.shared.pending.drain() {
            entry.promise.complete(Err(RuntimeError::Shutdown));
            if let Some(token) = entry.cancel_token {
                token.store(true, Ordering::Release);
            }
            if entry.destination != self.shared.local_id {
                let message = RuntimeMessage {
                    kind: RuntimeMessageKind::Cancel,
                    request,
                    action: entry.action,
                    domain: entry.domain,
                    deadline_ms: 0,
                    payload: Vec::new(),
                };
                let _ = self.shared.send(
                    entry.destination,
                    message,
                    TicketPurpose::BestEffort,
                    entry.trace_id,
                );
            }
        }
        while let Ok(request) = self.local_rx.try_recv() {
            request.cancel_token.store(true, Ordering::Release);
        }
        drop(work_gate);
        let deadline = Instant::now() + self.shared.limits.shutdown_timeout;
        while !self.domains.idle() {
            self.progress(self.shared.limits.max_progress_events)?;
            if Instant::now() >= deadline {
                self.shared
                    .state
                    .store(RuntimeState::Failed as u8, Ordering::Release);
                return Err(RuntimeError::ShutdownTimeout);
            }
            std::thread::yield_now();
        }
        while let Some(completion) = self.domains.try_completion() {
            self.handle_completion(completion)?;
        }
        while !self.responses.is_empty() {
            self.progress(self.shared.limits.max_progress_events)?;
            if Instant::now() >= deadline {
                self.shared
                    .state
                    .store(RuntimeState::Failed as u8, Ordering::Release);
                return Err(RuntimeError::ShutdownTimeout);
            }
        }
        self.domains.stop();
        self.dedup.clear();
        let mut events = Vec::new();
        self.driver.shutdown(
            &mut events,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        for event in events {
            if let TransportEvent::LocalSendComplete { ticket }
            | TransportEvent::SendFailed { ticket, .. } = event
            {
                self.shared.tickets.lock().unwrap().remove(&ticket);
            }
        }
        self.shared
            .state
            .store(RuntimeState::Stopped as u8, Ordering::Release);
        let report = ShutdownReport {
            stats: self.stats(),
        };
        if report.stats.pending_calls != 0
            || report.stats.send_tickets != 0
            || report.stats.dedup_entries != 0
            || report.stats.queued_responses != 0
            || report.stats.queued_response_bytes != 0
            || report.stats.transport.retained_bytes() != 0
        {
            return Err(RuntimeError::RetainedResources);
        }
        Ok(report)
    }

    fn dispatch_local(&mut self, request: LocalRequest) -> Result<(), RuntimeError> {
        let request_id = request.message.request;
        let action_id = request.message.action;
        let result = self
            .shared
            .registry
            .get(action_id)
            .cloned()
            .ok_or(RuntimeError::UnknownAction(action_id))
            .and_then(|handler| {
                self.domains.submit(ActionJob {
                    requester: self.shared.local_id,
                    request: request_id,
                    action_id,
                    domain: request.message.domain,
                    trace_id: request.trace_id,
                    input: request.message.payload,
                    handler,
                    cancelled: request.cancel_token,
                    local: true,
                    submitted_at: Instant::now(),
                })
            });
        if let Err(error) = result {
            if let Some(entry) = self.shared.pending.remove(request_id) {
                entry.promise.complete(Err(error));
            }
        }
        Ok(())
    }

    fn handle_transport_event(&mut self, event: TransportEvent) -> Result<(), RuntimeError> {
        match event {
            TransportEvent::LocalSendComplete { ticket } => {
                self.shared.tickets.lock().unwrap().remove(&ticket);
                Ok(())
            }
            TransportEvent::SendFailed { ticket, error } => {
                match self.shared.tickets.lock().unwrap().remove(&ticket) {
                    Some(TicketPurpose::Request(request)) => {
                        if let Some(entry) = self.shared.pending.remove(request) {
                            entry.promise.complete(Err(RuntimeError::Transport(error)));
                        }
                        Ok(())
                    }
                    Some(TicketPurpose::BestEffort) | None => Ok(()),
                    Some(TicketPurpose::RequiredResponse) => Err(RuntimeError::Transport(error)),
                }
            }
            TransportEvent::Incoming { parcel } => {
                self.shared.counters.received[parcel.channel as usize]
                    .fetch_add(1, Ordering::Relaxed);
                let source = parcel.source;
                let trace_id = parcel.trace_id;
                let message = wire::decode(parcel)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
                self.handle_message(source, trace_id, message)
            }
            TransportEvent::PeerFailed { peer, .. } => {
                self.fail(RuntimeError::PeerFailed(peer));
                Err(RuntimeError::PeerFailed(peer))
            }
            TransportEvent::ShutdownComplete => Ok(()),
        }
    }

    fn handle_message(
        &mut self,
        source: LocalityId,
        trace_id: Option<TraceId>,
        message: RuntimeMessage,
    ) -> Result<(), RuntimeError> {
        match message.kind {
            RuntimeMessageKind::Request => self.handle_request(source, trace_id, message),
            RuntimeMessageKind::Cancel => self.dedup.cancel(
                message.request,
                message.action,
                message.domain,
                Instant::now(),
            ),
            RuntimeMessageKind::Success => {
                self.complete_pending(source, message, |message| Ok(message.payload))
            }
            RuntimeMessageKind::Failure => self.complete_pending(source, message, |message| {
                let text = String::from_utf8(message.payload.into_iter().next().unwrap())
                    .map_err(|_| RuntimeError::Protocol("remote error is not UTF-8".into()))?;
                Err(RuntimeError::RemoteAction {
                    request: message.request,
                    message: cap(text),
                })
            }),
            RuntimeMessageKind::Cancelled => self.complete_pending(source, message, |message| {
                Err(RuntimeError::Cancelled(message.request))
            }),
            RuntimeMessageKind::DuplicateResultUnavailable => {
                self.complete_pending(source, message, |message| {
                    Err(RuntimeError::DuplicateResultUnavailable(message.request))
                })
            }
        }
    }

    fn handle_request(
        &mut self,
        source: LocalityId,
        trace_id: Option<TraceId>,
        message: RuntimeMessage,
    ) -> Result<(), RuntimeError> {
        if self.state() != RuntimeState::Running {
            return self.send_failure(source, &message, "runtime is shutting down", trace_id);
        }
        match self.dedup.begin(
            message.request,
            message.action,
            message.domain,
            Instant::now(),
        ) {
            Ok(DedupDisposition::New(cancelled)) => {
                let Some(handler) = self.shared.registry.get(message.action).cloned() else {
                    return self.send_and_cache_failure(
                        source,
                        message,
                        "unknown action",
                        trace_id,
                    );
                };
                if let Err(error) = self.domains.submit(ActionJob {
                    requester: source,
                    request: message.request,
                    action_id: message.action,
                    domain: message.domain,
                    trace_id,
                    input: message.payload.clone(),
                    handler,
                    cancelled,
                    local: false,
                    submitted_at: Instant::now(),
                }) {
                    return self.send_and_cache_failure(
                        source,
                        message,
                        &error.to_string(),
                        trace_id,
                    );
                }
                Ok(())
            }
            Ok(DedupDisposition::Running) => {
                self.shared
                    .counters
                    .duplicate_requests
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Ok(
                DedupDisposition::Replay(response)
                | DedupDisposition::Unavailable(response)
                | DedupDisposition::Cancelled(response),
            ) => {
                self.shared
                    .counters
                    .duplicate_requests
                    .fetch_add(1, Ordering::Relaxed);
                self.queue_response(source, response, trace_id, false)
            }
            Err(error) => self.send_failure(source, &message, &error.to_string(), trace_id),
        }
    }

    fn handle_completion(&mut self, completion: ActionCompletion) -> Result<(), RuntimeError> {
        if completion.local {
            let entry = self.shared.pending.take_checked(
                completion.request,
                self.shared.local_id,
                completion.action_id,
            )?;
            if let Some(entry) = entry {
                let result = if completion.cancelled {
                    Err(RuntimeError::Cancelled(completion.request))
                } else {
                    completion
                        .result
                        .map_err(|error| RuntimeError::RemoteAction {
                            request: completion.request,
                            message: error.message(),
                        })
                };
                entry.promise.complete(result);
                self.shared
                    .counters
                    .completed
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.shared
                    .counters
                    .late_results
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        }
        let (kind, payload) = if completion.cancelled {
            (RuntimeMessageKind::Cancelled, Vec::new())
        } else {
            match completion.result {
                Ok(payload) => (RuntimeMessageKind::Success, payload),
                Err(error) => {
                    self.shared
                        .counters
                        .action_failures
                        .fetch_add(1, Ordering::Relaxed);
                    (
                        RuntimeMessageKind::Failure,
                        vec![error.message().into_bytes()],
                    )
                }
            }
        };
        let message = RuntimeMessage {
            kind,
            request: completion.request,
            action: completion.action_id,
            domain: completion.domain,
            deadline_ms: 0,
            payload,
        };
        self.queue_response(completion.requester, message, completion.trace_id, true)
    }

    fn complete_pending<F>(
        &mut self,
        source: LocalityId,
        message: RuntimeMessage,
        map: F,
    ) -> Result<(), RuntimeError>
    where
        F: FnOnce(RuntimeMessage) -> Result<Segments, RuntimeError>,
    {
        match self
            .shared
            .pending
            .take_checked(message.request, source, message.action)?
        {
            Some(entry) => {
                entry.promise.complete(map(message));
                self.shared
                    .counters
                    .completed
                    .fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.shared
                    .counters
                    .late_results
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn send_failure(
        &mut self,
        destination: LocalityId,
        request: &RuntimeMessage,
        message: &str,
        trace_id: Option<TraceId>,
    ) -> Result<(), RuntimeError> {
        self.queue_response(
            destination,
            RuntimeMessage {
                kind: RuntimeMessageKind::Failure,
                request: request.request,
                action: request.action,
                domain: request.domain,
                deadline_ms: 0,
                payload: vec![cap(message.to_owned()).into_bytes()],
            },
            trace_id,
            false,
        )
    }

    fn send_and_cache_failure(
        &mut self,
        destination: LocalityId,
        request: RuntimeMessage,
        error: &str,
        trace_id: Option<TraceId>,
    ) -> Result<(), RuntimeError> {
        let response = RuntimeMessage {
            kind: RuntimeMessageKind::Failure,
            request: request.request,
            action: request.action,
            domain: request.domain,
            deadline_ms: 0,
            payload: vec![cap(error.to_owned()).into_bytes()],
        };
        self.queue_response(destination, response, trace_id, true)
    }

    fn queue_response(
        &mut self,
        destination: LocalityId,
        message: RuntimeMessage,
        trace_id: Option<TraceId>,
        cache: bool,
    ) -> Result<(), RuntimeError> {
        let bytes = message
            .payload
            .iter()
            .try_fold(0_usize, |total, segment| total.checked_add(segment.len()));
        let bytes = bytes.ok_or(RuntimeError::ResourceExhausted {
            resource: ResourceKind::DedupBytes,
            limit: self.shared.limits.max_dedup_bytes,
        })?;
        let next_bytes =
            self.response_bytes
                .checked_add(bytes)
                .ok_or(RuntimeError::ResourceExhausted {
                    resource: ResourceKind::DedupBytes,
                    limit: self.shared.limits.max_dedup_bytes,
                })?;
        if self.responses.len() >= self.shared.limits.max_dedup_entries {
            return Err(RuntimeError::ResourceExhausted {
                resource: ResourceKind::DedupEntries,
                limit: self.shared.limits.max_dedup_entries,
            });
        }
        if next_bytes > self.shared.limits.max_dedup_bytes {
            return Err(RuntimeError::ResourceExhausted {
                resource: ResourceKind::DedupBytes,
                limit: self.shared.limits.max_dedup_bytes,
            });
        }
        self.response_bytes = next_bytes;
        self.responses.push_back(QueuedResponse {
            destination,
            message,
            trace_id,
            cache,
            bytes,
        });
        Ok(())
    }

    fn drain_responses(&mut self, max_responses: usize) -> Result<usize, RuntimeError> {
        let mut sent = 0;
        while sent < max_responses {
            let Some(response) = self.responses.pop_front() else {
                break;
            };
            self.response_bytes = self.response_bytes.saturating_sub(response.bytes);
            match self.shared.send(
                response.destination,
                response.message.clone(),
                TicketPurpose::RequiredResponse,
                response.trace_id,
            ) {
                Ok(_) => {
                    if response.cache {
                        self.dedup.complete(response.message, Instant::now())?;
                    }
                    sent += 1;
                }
                Err(
                    RuntimeError::Transport(
                        hataori_runtime_foundation::transport::TransportError::QueueFull { .. }
                        | hataori_runtime_foundation::transport::TransportError::ByteLimit { .. },
                    )
                    | RuntimeError::ResourceExhausted {
                        resource: ResourceKind::SendTickets,
                        ..
                    },
                ) => {
                    self.response_bytes = self.response_bytes.saturating_add(response.bytes);
                    self.responses.push_front(response);
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(sent)
    }

    fn expire_pending(&self) {
        for (request, entry) in self.shared.pending.expired(Instant::now()) {
            entry.promise.complete(Err(RuntimeError::DeadlineExceeded {
                request,
                deadline: entry.deadline,
            }));
            self.shared
                .counters
                .timed_out
                .fetch_add(1, Ordering::Relaxed);
            if let Some(token) = entry.cancel_token {
                token.store(true, Ordering::Release);
            }
            if entry.destination != self.shared.local_id {
                let message = RuntimeMessage {
                    kind: RuntimeMessageKind::Cancel,
                    request,
                    action: entry.action,
                    domain: entry.domain,
                    deadline_ms: 0,
                    payload: Vec::new(),
                };
                let _ = self.shared.send(
                    entry.destination,
                    message,
                    TicketPurpose::BestEffort,
                    entry.trace_id,
                );
            }
        }
    }

    fn fail(&self, error: RuntimeError) {
        self.shared
            .state
            .store(RuntimeState::Failed as u8, Ordering::Release);
        for (_, entry) in self.shared.pending.drain() {
            entry.promise.complete(Err(error.clone()));
            if let Some(token) = entry.cancel_token {
                token.store(true, Ordering::Release);
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if !matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            let _work_gate = self.shared.work_gate.lock().unwrap();
            self.shared
                .state
                .store(RuntimeState::Draining as u8, Ordering::Release);
            for (_, entry) in self.shared.pending.drain() {
                entry.promise.complete(Err(RuntimeError::Shutdown));
                if let Some(token) = entry.cancel_token {
                    token.store(true, Ordering::Release);
                }
            }
        }
    }
}

fn decode_state(value: u8) -> RuntimeState {
    match value {
        value if value == RuntimeState::Created as u8 => RuntimeState::Created,
        value if value == RuntimeState::Bootstrapping as u8 => RuntimeState::Bootstrapping,
        value if value == RuntimeState::Running as u8 => RuntimeState::Running,
        value if value == RuntimeState::Draining as u8 => RuntimeState::Draining,
        value if value == RuntimeState::Stopped as u8 => RuntimeState::Stopped,
        _ => RuntimeState::Failed,
    }
}

#[cfg(test)]
mod tests;
