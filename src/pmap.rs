use crate::domain::{Domain, LocalMode};
#[cfg(feature = "rayon")]
use crate::local::{in_rayon_worker_context, run_in_current_pool, run_in_pool};
use crate::map::truncate_message;
use crate::mpi_backend;
use crate::mpi_backend::collective::SystemOperation;
use crate::mpi_backend::traits::*;
use crate::scheduler::{BatchId, Coordinator, Dispatch, ItemResult, SchedulerError};
use crate::wire::{self, ErrorClass, ErrorKey, Header, MessageKind, MessageStatus};
#[cfg(feature = "rayon")]
use rayon::Scope;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cell::Cell;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
#[cfg(feature = "rayon")]
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};

const HEADER_TAG: i32 = 0;
const PAYLOAD_TAG: i32 = 1;

type LocalError = (PmapErrorKind, String);
type RootOutcome<U> = (Vec<U>, i64, Option<LocalError>);

#[cfg(feature = "rayon")]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum RootEvent {
    Local,
    Remote,
    Neither,
}

#[cfg(feature = "rayon")]
fn choose_root_event(
    local_ready: bool,
    remote_ready: bool,
    prefer_local: bool,
) -> (RootEvent, bool) {
    match (local_ready, remote_ready) {
        (true, true) if prefer_local => (RootEvent::Local, false),
        (true, true) => (RootEvent::Remote, true),
        (true, false) => (RootEvent::Local, prefer_local),
        (false, true) => (RootEvent::Remote, prefer_local),
        (false, false) => (RootEvent::Neither, prefer_local),
    }
}

/// Options shared by every rank in one collective [`pmap`] call.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PmapOptions {
    /// Rank that owns the input and receives the ordered output.
    pub root: i32,
    /// Fixed positive number of items assigned in one batch.
    pub batch_size: NonZeroUsize,
    /// Rank-local execution mode; MPI-only calls require [`LocalMode::Sequential`].
    pub local_mode: LocalMode,
}

impl Default for PmapOptions {
    fn default() -> Self {
        Self {
            root: 0,
            batch_size: NonZeroUsize::new(1).unwrap(),
            local_mode: LocalMode::Sequential,
        }
    }
}

/// Stable category of a collective [`pmap`] failure.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PmapErrorKind {
    Reentrant,
    Preflight,
    User,
    Wire,
    Protocol,
}

/// Error converged across every rank in a collective [`pmap`] call.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PmapError {
    kind: PmapErrorKind,
    key: Option<i64>,
    message: String,
}

impl PmapError {
    fn new(kind: PmapErrorKind, message: impl Display) -> Self {
        Self {
            kind,
            key: None,
            message: truncate_message(message.to_string()),
        }
    }

    fn keyed(kind: PmapErrorKind, key: i64, message: impl Display) -> Self {
        Self {
            kind,
            key: Some(key),
            message: truncate_message(message.to_string()),
        }
    }

    pub fn kind(&self) -> PmapErrorKind {
        self.kind
    }

    pub fn key(&self) -> Option<i64> {
        self.key
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for PmapError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for PmapError {}

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
}

pub struct Active;

impl Active {
    fn acquire() -> Result<Self, PmapError> {
        ACTIVE.with(|active| {
            if active.replace(true) {
                Err(PmapError::new(
                    PmapErrorKind::Reentrant,
                    "pmap is already active on this thread",
                ))
            } else {
                Ok(Self)
            }
        })
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.set(false));
    }
}

fn wire_error(error: impl Display) -> PmapError {
    PmapError::new(PmapErrorKind::Wire, error)
}

fn protocol_error(error: impl Display) -> PmapError {
    PmapError::new(PmapErrorKind::Protocol, error)
}

fn scheduler_error(error: SchedulerError) -> PmapError {
    protocol_error(error)
}

fn header(
    kind: MessageKind,
    status: MessageStatus,
    batch_id: u64,
    item_count: u64,
    payload_len: usize,
) -> Result<Header, PmapError> {
    Header::new(kind, status, batch_id, item_count, payload_len as u64).map_err(wire_error)
}

fn send_header<C: Communicator>(comm: &C, rank: i32, value: Header) {
    comm.process_at_rank(rank)
        .send_with_tag(&value.encode(), HEADER_TAG);
}

fn receive_header<C: Communicator>(comm: &C, rank: i32) -> Result<Header, PmapError> {
    let (bytes, _) = comm
        .process_at_rank(rank)
        .receive_vec_with_tag::<u8>(HEADER_TAG);
    Header::decode(&bytes).map_err(wire_error)
}

#[cfg(not(feature = "rayon"))]
fn receive_any_header<C: Communicator>(comm: &C) -> Result<(i32, Header), PmapError> {
    let (bytes, status) = comm.any_process().receive_vec_with_tag::<u8>(HEADER_TAG);
    Header::decode(&bytes)
        .map(|value| (status.source_rank(), value))
        .map_err(wire_error)
}

fn receive_payload<C: Communicator, T: DeserializeOwned>(
    comm: &C,
    rank: i32,
    value: Header,
) -> Result<T, PmapError> {
    let count = wire::checked_mpi_count(value.payload_len()).map_err(wire_error)?;
    let _length = wire::checked_usize_length(value.payload_len()).map_err(wire_error)?;
    let (bytes, _) = comm
        .process_at_rank(rank)
        .receive_vec_with_tag::<u8>(PAYLOAD_TAG);
    if bytes.len() != count as usize {
        return Err(protocol_error("payload length does not match its header"));
    }
    wire::decode_payload(&bytes).map_err(wire_error)
}

fn send_frame<C: Communicator, T: Serialize>(
    comm: &C,
    rank: i32,
    kind: MessageKind,
    status: MessageStatus,
    batch_id: u64,
    item_count: usize,
    payload: &T,
) -> Result<(), PmapError> {
    let bytes = wire::encode_payload(payload).map_err(wire_error)?;
    let value = header(kind, status, batch_id, item_count as u64, bytes.len())?;
    send_header(comm, rank, value);
    comm.process_at_rank(rank)
        .send_with_tag(bytes.as_slice(), PAYLOAD_TAG);
    Ok(())
}

fn send_empty<C: Communicator>(
    comm: &C,
    rank: i32,
    kind: MessageKind,
    batch_id: u64,
) -> Result<(), PmapError> {
    let value = header(kind, MessageStatus::None, batch_id, 0, 0)?;
    send_header(comm, rank, value);
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct CallbackFailure {
    index: usize,
    kind: i32,
    message: String,
}

#[derive(Serialize, Deserialize)]
enum ResultPayload<U> {
    Values(Vec<(usize, U)>),
    Error(CallbackFailure),
}

#[cfg(feature = "rayon")]
enum LocalBatchResult<U> {
    Values(Vec<ItemResult<U>>),
    Error(CallbackFailure),
    Panic,
}

#[cfg(feature = "rayon")]
struct LocalBatchOutcome<U> {
    batch_id: BatchId,
    result: LocalBatchResult<U>,
}

#[cfg(feature = "rayon")]
fn spawn_local_batch<'scope, T, U, E, F>(
    scope: &Scope<'scope>,
    mode: LocalMode,
    batch: crate::scheduler::Batch<T>,
    f: &'scope F,
    sender: SyncSender<LocalBatchOutcome<U>>,
) where
    T: Send + 'scope,
    U: Send + 'scope,
    E: Display + Send,
    F: Fn(T) -> Result<U, E> + Send + Sync,
{
    let batch_id = batch.id();
    let (indices, values): (Vec<_>, Vec<_>) = batch
        .into_items()
        .into_iter()
        .map(|item| (item.original_index(), item.into_value()))
        .unzip();
    scope.spawn(move |_| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_in_current_pool(mode, values, f)
        }));
        let result = match result {
            Ok(Ok(values)) => LocalBatchResult::Values(
                indices
                    .into_iter()
                    .zip(values)
                    .map(|(index, value)| ItemResult::new(index, value))
                    .collect(),
            ),
            Ok(Err(error)) => LocalBatchResult::Error(CallbackFailure {
                index: indices[error.index()],
                kind: PmapErrorKind::User.code(),
                message: error.message().to_owned(),
            }),
            Err(_) => LocalBatchResult::Panic,
        };
        let _ = sender.send(LocalBatchOutcome { batch_id, result });
    });
}

#[cfg(feature = "rayon")]
fn spawn_root_batch<'scope, T, U, E, F>(
    scope: &Scope<'scope>,
    scheduler: &mut Coordinator<T, U>,
    mode: LocalMode,
    f: &'scope F,
) -> Result<Option<Receiver<LocalBatchOutcome<U>>>, PmapError>
where
    T: Send + 'scope,
    U: Send + 'scope,
    E: Display + Send,
    F: Fn(T) -> Result<U, E> + Send + Sync,
{
    let Some(batch) = scheduler.next_root_batch().map_err(scheduler_error)? else {
        return Ok(None);
    };
    let (sender, receiver) = mpsc::sync_channel(1);
    spawn_local_batch(scope, mode, batch, f, sender);
    Ok(Some(receiver))
}

fn error_key(
    task_key: usize,
    class: ErrorClass,
    rank: i32,
    world_size: i32,
) -> Result<i64, PmapError> {
    ErrorKey::new(task_key as u64, class, rank, world_size)
        .map(|key| key.get())
        .map_err(wire_error)
}

fn convergence<C: Communicator>(comm: &C, local_key: i64) -> i64 {
    let mut selected = local_key;
    comm.all_reduce_into(&local_key, &mut selected, SystemOperation::min());
    selected
}

fn broadcast_error<C: Communicator>(
    comm: &C,
    selected: i64,
    local_key: i64,
    local: Option<(PmapErrorKind, String)>,
) -> Result<Option<(PmapErrorKind, String)>, PmapError> {
    if selected == ErrorKey::NO_ERROR_KEY {
        return Ok(None);
    }
    if selected < 0 || comm.size() <= 0 {
        return Err(protocol_error("invalid selected error key"));
    }
    let winner = (selected % i64::from(comm.size())) as i32;
    let is_winner = comm.rank() == winner;
    let winner_local = if is_winner && local_key == selected {
        local
    } else if is_winner {
        return Err(protocol_error("winner does not own selected error"));
    } else {
        None
    };

    let mut kind = winner_local
        .as_ref()
        .map_or(PmapErrorKind::Protocol.code(), |(kind, _)| kind.code());
    let mut key = selected;
    comm.process_at_rank(winner).broadcast_into(&mut kind);
    comm.process_at_rank(winner).broadcast_into(&mut key);
    if key != selected {
        return Err(protocol_error("broadcast error key mismatch"));
    }

    let mut message = winner_local
        .map(|(_, message)| message.into_bytes())
        .unwrap_or_default();
    let mut length = i32::try_from(message.len())
        .map_err(|_| protocol_error("error message length overflow"))?;
    comm.process_at_rank(winner).broadcast_into(&mut length);
    if length < 0 || length as usize > crate::map::MAX_ERROR_MESSAGE_BYTES {
        return Err(protocol_error("invalid broadcast error message length"));
    }
    if !is_winner {
        message.resize(length as usize, 0);
    }
    comm.process_at_rank(winner)
        .broadcast_into(message.as_mut_slice());

    let kind =
        PmapErrorKind::from_code(kind).ok_or_else(|| protocol_error("invalid error kind"))?;
    let message = String::from_utf8(message).map_err(protocol_error)?;
    Ok(Some((kind, message)))
}

fn is_thread_main() -> bool {
    let mut flag = 0;
    unsafe { mpi_backend::ffi::MPI_Is_thread_main(&mut flag) };
    flag != 0
}

fn preflight<C, T>(
    comm: &C,
    options: &PmapOptions,
    root_items: Option<&Vec<T>>,
    execution_valid: bool,
) -> Result<(), PmapError>
where
    C: Communicator,
{
    let rank = comm.rank();
    let size = comm.size();
    let local_batch = options.batch_size.get() as u64;
    let local_mode = match options.local_mode {
        LocalMode::Sequential => 0_i32,
        LocalMode::Outer => 1,
        LocalMode::Inner => 2,
    };

    let mut min_root = options.root;
    let mut max_root = options.root;
    let mut min_batch = local_batch;
    let mut max_batch = local_batch;
    let mut min_mode = local_mode;
    let mut max_mode = local_mode;
    comm.all_reduce_into(&options.root, &mut min_root, SystemOperation::min());
    comm.all_reduce_into(&options.root, &mut max_root, SystemOperation::max());
    comm.all_reduce_into(&local_batch, &mut min_batch, SystemOperation::min());
    comm.all_reduce_into(&local_batch, &mut max_batch, SystemOperation::max());
    comm.all_reduce_into(&local_mode, &mut min_mode, SystemOperation::min());
    comm.all_reduce_into(&local_mode, &mut max_mode, SystemOperation::max());

    let local_valid = is_thread_main()
        && options.root >= 0
        && options.root < size
        && ((rank == options.root) == root_items.is_some())
        && execution_valid
        && min_root == max_root
        && min_batch == max_batch
        && min_mode == max_mode;
    let candidate = wire::preflight_candidate(!local_valid, rank, size).map_err(wire_error)?;
    let mut failing_rank = candidate;
    comm.all_reduce_into(&candidate, &mut failing_rank, SystemOperation::min());
    if failing_rank != size {
        return Err(PmapError::new(
            PmapErrorKind::Preflight,
            format!("pmap preflight failed on rank {failing_rank}"),
        ));
    }
    Ok(())
}

#[cfg(not(feature = "rayon"))]
fn worker_loop<C, T, U, E, F>(
    comm: &C,
    root: i32,
    mut f: F,
) -> Result<(i64, Option<LocalError>), PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
    E: Display,
    F: FnMut(T) -> Result<U, E>,
{
    let rank = comm.rank();
    let size = comm.size();
    let mut local_key = ErrorKey::NO_ERROR_KEY;
    let mut local_error = None;

    loop {
        send_empty(comm, root, MessageKind::Ready, 0)?;
        let value = receive_header(comm, root)?;
        match value.kind() {
            MessageKind::Task => {
                let items: Vec<(usize, T)> = match receive_payload(comm, root, value) {
                    Ok(items) => items,
                    Err(error) => {
                        let index = usize::try_from(value.item_count()).unwrap_or(usize::MAX);
                        let key = error_key(index, ErrorClass::WireProtocol, rank, size)?;
                        local_key = local_key.min(key);
                        local_error = Some((PmapErrorKind::Wire, error.message().to_owned()));
                        let failure = CallbackFailure {
                            index,
                            kind: PmapErrorKind::Wire.code(),
                            message: error.message().to_owned(),
                        };
                        send_frame(
                            comm,
                            root,
                            MessageKind::Result,
                            MessageStatus::Error,
                            value.batch_id(),
                            1,
                            &ResultPayload::<U>::Error(failure),
                        )?;
                        continue;
                    }
                };
                let mut results = Vec::with_capacity(items.len());
                let mut failure = None;
                for (index, item) in items {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(item))) {
                        Ok(Ok(result)) => results.push((index, result)),
                        Ok(Err(error)) => {
                            failure = Some(CallbackFailure {
                                index,
                                kind: PmapErrorKind::User.code(),
                                message: truncate_message(error.to_string()),
                            });
                            break;
                        }
                        Err(_) => comm.abort(71),
                    }
                }
                match failure {
                    Some(failure) => {
                        let key = error_key(failure.index, ErrorClass::Callback, rank, size)?;
                        local_key = local_key.min(key);
                        local_error = Some((PmapErrorKind::User, failure.message.clone()));
                        send_frame(
                            comm,
                            root,
                            MessageKind::Result,
                            MessageStatus::Error,
                            value.batch_id(),
                            value.item_count() as usize,
                            &ResultPayload::<U>::Error(failure),
                        )?;
                    }
                    None => send_frame(
                        comm,
                        root,
                        MessageKind::Result,
                        MessageStatus::Ok,
                        value.batch_id(),
                        results.len(),
                        &ResultPayload::Values(results),
                    )?,
                }
            }
            MessageKind::Stop => {
                send_empty(comm, root, MessageKind::Drain, 0)?;
                return Ok((local_key, local_error));
            }
            _ => {
                return Err(protocol_error("worker received an unexpected message"));
            }
        }
    }
}

#[cfg(feature = "rayon")]
fn hybrid_worker_loop<C, T, U, E, F>(
    comm: &C,
    root: i32,
    mode: LocalMode,
    pool: &rayon::ThreadPool,
    f: F,
) -> Result<(i64, Option<LocalError>), PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned + Send,
    U: Serialize + DeserializeOwned + Send,
    E: Display + Send,
    F: Fn(T) -> Result<U, E> + Send + Sync,
{
    let rank = comm.rank();
    let size = comm.size();
    let mut local_key = ErrorKey::NO_ERROR_KEY;
    let mut local_error = None;

    loop {
        send_empty(comm, root, MessageKind::Ready, 0)?;
        let value = receive_header(comm, root)?;
        match value.kind() {
            MessageKind::Task => {
                let items: Vec<(usize, T)> = match receive_payload(comm, root, value) {
                    Ok(items) => items,
                    Err(error) => {
                        let index = usize::try_from(value.item_count()).unwrap_or(usize::MAX);
                        let key = error_key(index, ErrorClass::WireProtocol, rank, size)?;
                        local_key = local_key.min(key);
                        local_error = Some((PmapErrorKind::Wire, error.message().to_owned()));
                        let failure = CallbackFailure {
                            index,
                            kind: PmapErrorKind::Wire.code(),
                            message: error.message().to_owned(),
                        };
                        send_frame(
                            comm,
                            root,
                            MessageKind::Result,
                            MessageStatus::Error,
                            value.batch_id(),
                            1,
                            &ResultPayload::<U>::Error(failure),
                        )?;
                        continue;
                    }
                };
                let (indices, values): (Vec<_>, Vec<_>) = items.into_iter().unzip();
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_in_pool(pool, mode, values, &f)
                }));
                let result = match outcome {
                    Ok(Ok(values)) => {
                        ResultPayload::Values(indices.into_iter().zip(values).collect())
                    }
                    Ok(Err(error)) => {
                        let index = indices[error.index()];
                        let failure = CallbackFailure {
                            index,
                            kind: PmapErrorKind::User.code(),
                            message: error.message().to_owned(),
                        };
                        let key = error_key(index, ErrorClass::Callback, rank, size)?;
                        local_key = local_key.min(key);
                        local_error = Some((PmapErrorKind::User, failure.message.clone()));
                        ResultPayload::Error(failure)
                    }
                    Err(_) => comm.abort(71),
                };
                match result {
                    ResultPayload::Values(values) => {
                        let item_count = values.len();
                        let payload = ResultPayload::Values(values);
                        send_frame(
                            comm,
                            root,
                            MessageKind::Result,
                            MessageStatus::Ok,
                            value.batch_id(),
                            item_count,
                            &payload,
                        )?;
                    }
                    ResultPayload::Error(failure) => {
                        let payload = ResultPayload::<U>::Error(failure);
                        send_frame(
                            comm,
                            root,
                            MessageKind::Result,
                            MessageStatus::Error,
                            value.batch_id(),
                            value.item_count() as usize,
                            &payload,
                        )?;
                    }
                }
            }
            MessageKind::Stop => {
                send_empty(comm, root, MessageKind::Drain, 0)?;
                return Ok((local_key, local_error));
            }
            _ => return Err(protocol_error("worker received an unexpected message")),
        }
    }
}

#[cfg(not(feature = "rayon"))]
fn execute_root_batch<C, T, U, E, F>(
    comm: &C,
    scheduler: &mut Coordinator<T, U>,
    f: &mut F,
    rank: i32,
    size: i32,
    local_key: &mut i64,
    local_error: &mut Option<LocalError>,
) -> Result<bool, PmapError>
where
    C: Communicator,
    E: Display,
    F: FnMut(T) -> Result<U, E>,
{
    let Some(batch) = scheduler.next_root_batch().map_err(scheduler_error)? else {
        return Ok(false);
    };
    let batch_id = batch.id();
    let mut results = Vec::new();
    let mut failure = None;
    for item in batch.into_items() {
        let index = item.original_index();
        let callback =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(item.into_value())));
        match callback {
            Ok(Ok(result)) => results.push(ItemResult::new(index, result)),
            Ok(Err(error)) => {
                failure = Some((index, truncate_message(error.to_string())));
                break;
            }
            Err(_) => comm.abort(70),
        }
    }
    match failure {
        Some((index, message)) => {
            let key = error_key(index, ErrorClass::Callback, rank, size)?;
            *local_key = (*local_key).min(key);
            *local_error = Some((PmapErrorKind::User, message));
            scheduler.on_root_error(batch_id).map_err(scheduler_error)?;
        }
        None => {
            scheduler
                .on_root_success(batch_id, results)
                .map_err(scheduler_error)?;
        }
    }
    Ok(true)
}

struct RootErrorState<'a> {
    call_task_key: usize,
    rank: i32,
    size: i32,
    local_key: &'a mut i64,
    local_error: &'a mut Option<LocalError>,
}

fn process_remote_header<C, T, U>(
    comm: &C,
    scheduler: &mut Coordinator<T, U>,
    source: i32,
    value: Header,
    errors: &mut RootErrorState<'_>,
) -> Result<(), PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    let call_task_key = errors.call_task_key;
    let rank = errors.rank;
    let size = errors.size;
    let local_key = &mut *errors.local_key;
    let local_error = &mut *errors.local_error;
    match value.kind() {
        MessageKind::Ready => match scheduler.on_remote_ready(source).map_err(scheduler_error)? {
            Dispatch::Task(batch) => {
                let batch_id = batch.id();
                let items: Vec<(usize, T)> = batch
                    .into_items()
                    .into_iter()
                    .map(|item| (item.original_index(), item.into_value()))
                    .collect();
                send_frame(
                    comm,
                    source,
                    MessageKind::Task,
                    MessageStatus::None,
                    batch_id.get(),
                    items.len(),
                    &items,
                )?;
            }
            Dispatch::Stop => send_empty(comm, source, MessageKind::Stop, 0)?,
        },
        MessageKind::Result => {
            let payload: ResultPayload<U> = match receive_payload(comm, source, value) {
                Ok(payload) => payload,
                Err(error) => {
                    let key = error_key(call_task_key, ErrorClass::WireProtocol, rank, size)?;
                    *local_key = (*local_key).min(key);
                    *local_error = Some((error.kind(), error.message().to_owned()));
                    scheduler
                        .on_remote_protocol_error(source)
                        .map_err(scheduler_error)?;
                    return Ok(());
                }
            };
            match (value.status(), payload) {
                (MessageStatus::Ok, ResultPayload::Values(values)) => {
                    let results = values
                        .into_iter()
                        .map(|(index, result)| ItemResult::new(index, result))
                        .collect();
                    scheduler
                        .on_remote_success(source, BatchId::from_raw(value.batch_id()), results)
                        .map_err(scheduler_error)?;
                }
                (MessageStatus::Error, ResultPayload::Error(_failure)) => {
                    scheduler
                        .on_remote_error(source, BatchId::from_raw(value.batch_id()))
                        .map_err(scheduler_error)?;
                }
                _ => {
                    let key = error_key(call_task_key, ErrorClass::WireProtocol, rank, size)?;
                    *local_key = (*local_key).min(key);
                    *local_error =
                        Some((PmapErrorKind::Protocol, "invalid result status".to_owned()));
                    scheduler
                        .on_remote_protocol_error(source)
                        .map_err(scheduler_error)?;
                }
            }
        }
        MessageKind::Drain => scheduler.on_remote_drain(source).map_err(scheduler_error)?,
        _ => {
            let key = error_key(call_task_key, ErrorClass::WireProtocol, rank, size)?;
            *local_key = (*local_key).min(key);
            *local_error = Some((
                PmapErrorKind::Protocol,
                "unexpected message from worker".to_owned(),
            ));
            scheduler
                .on_remote_protocol_error(source)
                .map_err(scheduler_error)?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "rayon"))]
fn root_loop<C, T, U, E, F>(
    comm: &C,
    options: &PmapOptions,
    root_items: Vec<T>,
    mut f: F,
) -> Result<RootOutcome<U>, PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
    E: Display,
    F: FnMut(T) -> Result<U, E>,
{
    let root = options.root;
    let rank = comm.rank();
    let size = comm.size();
    let remotes: Vec<i32> = (0..size).filter(|candidate| *candidate != root).collect();
    let call_task_key = root_items.len();
    let mut scheduler = Coordinator::<T, U>::new(root, remotes, root_items, options.batch_size)
        .map_err(scheduler_error)?;
    let mut local_key = ErrorKey::NO_ERROR_KEY;
    let mut local_error = None;

    loop {
        if size == 1 {
            if !execute_root_batch(
                comm,
                &mut scheduler,
                &mut f,
                rank,
                size,
                &mut local_key,
                &mut local_error,
            )? {
                break;
            }
            continue;
        }

        if scheduler.is_finished() && scheduler.is_quiescent() {
            break;
        }

        let (source, value) = receive_any_header(comm)?;
        process_remote_header(
            comm,
            &mut scheduler,
            source,
            value,
            &mut RootErrorState {
                call_task_key,
                rank,
                size,
                local_key: &mut local_key,
                local_error: &mut local_error,
            },
        )?;

        if !scheduler.is_finished() {
            let _ = execute_root_batch(
                comm,
                &mut scheduler,
                &mut f,
                rank,
                size,
                &mut local_key,
                &mut local_error,
            )?;
        }
    }

    let results = if scheduler.failed() {
        Vec::new()
    } else {
        scheduler.into_results().map_err(scheduler_error)?
    };
    Ok((results, local_key, local_error))
}

#[cfg(feature = "rayon")]
fn hybrid_root_loop<C, T, U, E, F>(
    comm: &C,
    options: &PmapOptions,
    root_items: Vec<T>,
    pool: &rayon::ThreadPool,
    f: &F,
) -> Result<RootOutcome<U>, PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned + Send,
    U: Serialize + DeserializeOwned + Send,
    E: Display + Send,
    F: Fn(T) -> Result<U, E> + Send + Sync,
{
    let root = options.root;
    let rank = comm.rank();
    let size = comm.size();
    let remotes: Vec<i32> = (0..size).filter(|candidate| *candidate != root).collect();
    let call_task_key = root_items.len();
    let mut scheduler = Coordinator::<T, U>::new(root, remotes, root_items, options.batch_size)
        .map_err(scheduler_error)?;
    let mut local_key = ErrorKey::NO_ERROR_KEY;
    let mut local_error = None;

    pool.in_place_scope(|scope| {
        let mut receiver: Option<Receiver<LocalBatchOutcome<U>>> = None;
        let mut pending_local: Option<LocalBatchOutcome<U>> = None;
        let mut prefer_local = true;

        loop {
            if receiver.is_none() {
                receiver =
                    spawn_root_batch::<T, U, E, F>(scope, &mut scheduler, options.local_mode, f)?;
            }
            if receiver.is_none() && scheduler.is_finished() && scheduler.is_quiescent() {
                break;
            }

            let mut local_disconnected = false;
            if pending_local.is_none() {
                if let Some(local) = receiver.as_ref() {
                    match local.try_recv() {
                        Ok(outcome) => pending_local = Some(outcome),
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => local_disconnected = true,
                    }
                }
            }
            let remote_source = if size > 1 {
                comm.any_process()
                    .immediate_probe_with_tag(HEADER_TAG)
                    .map(|status| status.source_rank())
            } else {
                None
            };
            let local_ready = pending_local.is_some() || local_disconnected;
            let (event, next_preference) =
                choose_root_event(local_ready, remote_source.is_some(), prefer_local);
            prefer_local = next_preference;

            match event {
                RootEvent::Local => {
                    if local_disconnected {
                        comm.abort(70);
                    }
                    let outcome = pending_local
                        .take()
                        .ok_or_else(|| protocol_error("missing root-local outcome"))?;
                    receiver = None;
                    match outcome.result {
                        LocalBatchResult::Values(values) => {
                            scheduler
                                .on_root_success(outcome.batch_id, values)
                                .map_err(scheduler_error)?;
                        }
                        LocalBatchResult::Error(failure) => {
                            let key = error_key(failure.index, ErrorClass::Callback, rank, size)?;
                            local_key = local_key.min(key);
                            local_error = Some((PmapErrorKind::User, failure.message.clone()));
                            scheduler
                                .on_root_error(outcome.batch_id)
                                .map_err(scheduler_error)?;
                        }
                        LocalBatchResult::Panic => comm.abort(70),
                    }
                }
                RootEvent::Remote => {
                    let source = remote_source
                        .ok_or_else(|| protocol_error("missing probed remote source"))?;
                    let value = receive_header(comm, source)?;
                    process_remote_header(
                        comm,
                        &mut scheduler,
                        source,
                        value,
                        &mut RootErrorState {
                            call_task_key,
                            rank,
                            size,
                            local_key: &mut local_key,
                            local_error: &mut local_error,
                        },
                    )?;
                }
                RootEvent::Neither => std::thread::yield_now(),
            }
        }

        let results = if scheduler.failed() {
            Vec::new()
        } else {
            scheduler.into_results().map_err(scheduler_error)?
        };
        Ok((results, local_key, local_error))
    })
}

impl PmapErrorKind {
    fn code(self) -> i32 {
        match self {
            Self::Reentrant => 0,
            Self::Preflight => 1,
            Self::User => 2,
            Self::Wire => 3,
            Self::Protocol => 4,
        }
    }

    fn from_code(code: i32) -> Option<Self> {
        Some(match code {
            0 => Self::Reentrant,
            1 => Self::Preflight,
            2 => Self::User,
            3 => Self::Wire,
            4 => Self::Protocol,
            _ => return None,
        })
    }
}

/// Dynamically maps root-owned items across all ranks of `world`.
///
/// Every rank must call this function in the same collective order with the
/// same communicator and options. Only `options.root` supplies `Some(items)`
/// and receives `Ok(Some(results))`; every other rank supplies `None` and
/// receives `Ok(None)`. Results preserve input order. The root rank must lie
/// in `[0, world.size())`.
///
/// This MPI-only entry is synchronous and adds no `Send`, `Sync`, or `'static`
/// bounds to the callback or values.
#[cfg(not(feature = "rayon"))]
pub fn pmap<C, T, U, E, F>(
    world: &C,
    domain: &Domain,
    options: PmapOptions,
    root_items: Option<Vec<T>>,
    f: F,
) -> Result<Option<Vec<U>>, PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
    E: Display,
    F: FnMut(T) -> Result<U, E>,
{
    let _active = Active::acquire()?;
    let admission = domain.try_admit();
    let comm = world.duplicate();
    preflight(
        &comm,
        &options,
        root_items.as_ref(),
        admission.is_ok()
            && domain.id().get() == 0
            && domain.worker_count() == 1
            && options.local_mode == LocalMode::Sequential,
    )?;
    let _admission = admission.expect("successful preflight guarantees domain admission");

    let rank = comm.rank();
    let outcome = if rank == options.root {
        root_loop(&comm, &options, root_items.unwrap(), f)
    } else {
        worker_loop(&comm, options.root, f).map(|(key, error)| (Vec::new(), key, error))
    };
    let (results, local_key, local_error) = match outcome {
        Ok(outcome) => outcome,
        Err(_) => comm.abort(72),
    };

    let selected = convergence(&comm, local_key);
    let error = match broadcast_error(&comm, selected, local_key, local_error) {
        Ok(error) => error,
        Err(_) => comm.abort(73),
    };
    match error {
        Some((kind, message)) => Err(PmapError::keyed(kind, selected, message)),
        None if rank == options.root => Ok(Some(results)),
        None => Ok(None),
    }
}

/// Dynamically maps root-owned items across MPI ranks and each rank's explicit
/// Rayon domain.
///
/// Every rank must call this collective in the same order with identical
/// options. Only `options.root` supplies items and receives ordered results.
/// The call must originate on the MPI initialization/main thread outside every
/// Rayon worker, and MPI must provide `MPI_THREAD_FUNNELED` or stronger.
/// Callback state may borrow non-`'static` data while satisfying the stated
/// Rayon `Send`/`Sync` bounds; values retain the serialization bounds required
/// by remote execution. All MPI calls remain on the calling thread.
#[cfg(feature = "rayon")]
pub fn pmap<C, T, U, E, F>(
    world: &C,
    domain: &Domain,
    options: PmapOptions,
    root_items: Option<Vec<T>>,
    f: F,
) -> Result<Option<Vec<U>>, PmapError>
where
    C: Communicator,
    T: Serialize + DeserializeOwned + Send,
    U: Serialize + DeserializeOwned + Send,
    E: Display + Send,
    F: Fn(T) -> Result<U, E> + Send + Sync,
{
    if in_rayon_worker_context() {
        return Err(PmapError::new(
            PmapErrorKind::Preflight,
            "hybrid pmap cannot enter from a Rayon worker",
        ));
    }
    let _active = Active::acquire()?;
    let admission = domain.try_admit();
    let pool = domain.rayon_pool().cloned();
    let execution_valid = admission.is_ok()
        && pool.is_some()
        && domain.id().get() == 0
        && rayon::current_thread_index().is_none()
        && mpi_backend::environment::threading_support()
            >= mpi_backend::environment::Threading::Funneled;
    let comm = world.duplicate();
    preflight(&comm, &options, root_items.as_ref(), execution_valid)?;
    let _admission = admission.expect("successful preflight guarantees domain admission");
    let pool = pool.expect("successful preflight guarantees a Rayon pool");

    let rank = comm.rank();
    let outcome = if rank == options.root {
        hybrid_root_loop::<_, T, U, E, F>(
            &comm,
            &options,
            root_items.expect("successful preflight guarantees root input"),
            pool.as_ref(),
            &f,
        )
    } else {
        hybrid_worker_loop(&comm, options.root, options.local_mode, pool.as_ref(), f)
            .map(|(key, error)| (Vec::new(), key, error))
    };
    let (results, local_key, local_error) = match outcome {
        Ok(outcome) => outcome,
        Err(_) => comm.abort(72),
    };

    let selected = convergence(&comm, local_key);
    let error = match broadcast_error(&comm, selected, local_key, local_error) {
        Ok(error) => error,
        Err(_) => comm.abort(73),
    };
    match error {
        Some((kind, message)) => Err(PmapError::keyed(kind, selected, message)),
        None if rank == options.root => Ok(Some(results)),
        None => Ok(None),
    }
}

#[cfg(all(test, feature = "rayon"))]
mod tests {
    #[test]
    fn root_event_chooser_alternates_when_both_sources_are_ready() {
        let mut prefer_local = true;
        let mut events = Vec::new();
        for _ in 0..4 {
            let (event, next) = super::choose_root_event(true, true, prefer_local);
            events.push(event);
            prefer_local = next;
        }
        assert_eq!(
            events,
            [
                super::RootEvent::Local,
                super::RootEvent::Remote,
                super::RootEvent::Local,
                super::RootEvent::Remote
            ]
        );
    }

    #[test]
    fn disconnected_local_sender_is_selected_for_abort() {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(1);
        drop(sender);
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Disconnected)
        ));
        let (event, _) = super::choose_root_event(true, false, false);
        assert_eq!(event, super::RootEvent::Local);
    }

    #[test]
    fn root_event_chooser_keeps_one_sided_progress() {
        let (local, turn) = super::choose_root_event(true, false, true);
        assert_eq!(local, super::RootEvent::Local);
        let (remote, _) = super::choose_root_event(false, true, turn);
        assert_eq!(remote, super::RootEvent::Remote);
    }
}
