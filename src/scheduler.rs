use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct BatchId(u64);

impl BatchId {
    const ZERO: Self = Self(0);

    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    pub(crate) const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    fn checked_increment(self) -> Result<Self, SchedulerError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(SchedulerError::BatchIdOverflow)
    }
}

pub(crate) struct ScheduledItem<T> {
    original_index: usize,
    value: T,
}

impl<T> ScheduledItem<T> {
    fn new(original_index: usize, value: T) -> Self {
        Self {
            original_index,
            value,
        }
    }

    pub(crate) fn original_index(&self) -> usize {
        self.original_index
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

pub(crate) struct Batch<T> {
    id: BatchId,
    items: Vec<ScheduledItem<T>>,
}

impl<T> Batch<T> {
    pub(crate) fn id(&self) -> BatchId {
        self.id
    }

    pub(crate) fn into_items(self) -> Vec<ScheduledItem<T>> {
        self.items
    }
}

pub(crate) struct ItemResult<U> {
    original_index: usize,
    value: U,
}

impl<U> ItemResult<U> {
    pub(crate) fn new(original_index: usize, value: U) -> Self {
        Self {
            original_index,
            value,
        }
    }

    pub(crate) fn original_index(&self) -> usize {
        self.original_index
    }

    pub(crate) fn into_value(self) -> U {
        self.value
    }
}

pub(crate) enum Dispatch<T> {
    Task(Batch<T>),
    Stop,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum Lane {
    Root,
    Remote(i32),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct CompletionMeta {
    lane: Lane,
    batch_id: BatchId,
}

impl CompletionMeta {
    #[cfg(test)]
    pub(crate) const fn lane(self) -> Lane {
        self.lane
    }

    #[cfg(test)]
    pub(crate) const fn batch_id(self) -> BatchId {
        self.batch_id
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum LaneStateKind {
    Idle,
    Running,
    Stopped,
    Drained,
}

struct RunningMeta {
    batch_id: BatchId,
    original_indices: Vec<usize>,
}

enum LaneState {
    Idle,
    Running {
        current: RunningMeta,
        prefetched: Option<RunningMeta>,
        stopping: bool,
    },
    Stopped,
    Drained,
}

impl LaneState {
    fn kind(&self) -> LaneStateKind {
        match self {
            Self::Idle => LaneStateKind::Idle,
            Self::Running { .. } => LaneStateKind::Running,
            Self::Stopped => LaneStateKind::Stopped,
            Self::Drained => LaneStateKind::Drained,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SchedulerError {
    InvalidRoot,
    InvalidRemote,
    DuplicateRemote,
    RootInRemotes,
    UnknownRank,
    WrongLaneState,
    BatchIdOverflow,
    BatchMismatch,
    ResultCount,
    IndexSequence,
    IndexOutOfBounds,
    AlreadyFilled,
    DuplicateResultIndex,
    IndexOverflow,
    NotFinished,
    Failed,
    MissingResult,
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidRoot => "invalid root rank",
            Self::InvalidRemote => "invalid remote rank",
            Self::DuplicateRemote => "duplicate remote rank",
            Self::RootInRemotes => "root rank is listed as remote",
            Self::UnknownRank => "unknown scheduler rank",
            Self::WrongLaneState => "scheduler lane is in the wrong state",
            Self::BatchIdOverflow => "scheduler batch ID overflow",
            Self::BatchMismatch => "scheduler batch ID mismatch",
            Self::ResultCount => "scheduler result count mismatch",
            Self::IndexSequence => "scheduler result index sequence mismatch",
            Self::IndexOutOfBounds => "scheduler result index is out of bounds",
            Self::AlreadyFilled => "scheduler result slot is already filled",
            Self::DuplicateResultIndex => "scheduler result index is duplicated",
            Self::IndexOverflow => "scheduler input index overflow",
            Self::NotFinished => "scheduler is not finished",
            Self::Failed => "scheduler failed",
            Self::MissingResult => "scheduler result is missing",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for SchedulerError {}

pub(crate) struct Coordinator<T, U> {
    pending: VecDeque<ScheduledItem<T>>,
    ordered: Vec<Option<U>>,
    batch_size: NonZeroUsize,
    next_batch_id: BatchId,
    root: LaneState,
    remotes: BTreeMap<i32, LaneState>,
    prefetch: bool,
    failed: bool,
}

impl<T, U> Coordinator<T, U> {
    #[cfg(test)]
    pub(crate) fn new(
        root_rank: i32,
        remote_ranks: Vec<i32>,
        items: Vec<T>,
        batch_size: NonZeroUsize,
    ) -> Result<Self, SchedulerError> {
        Self::new_with_prefetch(root_rank, remote_ranks, items, batch_size, false)
    }

    pub(crate) fn new_with_prefetch(
        root_rank: i32,
        remote_ranks: Vec<i32>,
        items: Vec<T>,
        batch_size: NonZeroUsize,
        prefetch: bool,
    ) -> Result<Self, SchedulerError> {
        if root_rank < 0 {
            return Err(SchedulerError::InvalidRoot);
        }

        let mut remotes = BTreeMap::new();
        for rank in remote_ranks {
            if rank < 0 {
                return Err(SchedulerError::InvalidRemote);
            }
            if rank == root_rank {
                return Err(SchedulerError::RootInRemotes);
            }
            if remotes.insert(rank, LaneState::Idle).is_some() {
                return Err(SchedulerError::DuplicateRemote);
            }
        }

        let mut pending = VecDeque::with_capacity(items.len());
        for (index, value) in items.into_iter().enumerate() {
            u64::try_from(index).map_err(|_| SchedulerError::IndexOverflow)?;
            pending.push_back(ScheduledItem::new(index, value));
        }

        let input_len = pending.len();
        Ok(Self {
            pending,
            ordered: (0..input_len).map(|_| None).collect(),
            batch_size,
            next_batch_id: BatchId::ZERO,
            root: LaneState::Idle,
            remotes,
            prefetch,
            failed: false,
        })
    }

    pub(crate) fn on_remote_ready(&mut self, rank: i32) -> Result<Dispatch<T>, SchedulerError> {
        let prefetch_request = match self.remotes.get(&rank) {
            Some(LaneState::Idle) => false,
            Some(LaneState::Running {
                prefetched: None,
                stopping: false,
                ..
            }) if self.prefetch => true,
            Some(_) => return Err(SchedulerError::WrongLaneState),
            None => return Err(SchedulerError::UnknownRank),
        };

        if self.failed || self.pending.is_empty() {
            if prefetch_request {
                let state = self
                    .remotes
                    .get_mut(&rank)
                    .ok_or(SchedulerError::UnknownRank)?;
                match state {
                    LaneState::Running { stopping, .. } => *stopping = true,
                    _ => unreachable!("prefetch request state was validated"),
                }
            } else {
                self.remotes.insert(rank, LaneState::Stopped);
            }
            return Ok(Dispatch::Stop);
        }

        let batch = self.make_batch()?.expect("pending work was checked above");
        let metadata = Self::metadata(&batch);
        if prefetch_request {
            let state = self
                .remotes
                .get_mut(&rank)
                .ok_or(SchedulerError::UnknownRank)?;
            match state {
                LaneState::Running { prefetched, .. } => *prefetched = Some(metadata),
                _ => unreachable!("prefetch request state was validated"),
            }
        } else {
            self.remotes.insert(
                rank,
                LaneState::Running {
                    current: metadata,
                    prefetched: None,
                    stopping: false,
                },
            );
        }
        Ok(Dispatch::Task(batch))
    }

    pub(crate) fn next_root_batch(&mut self) -> Result<Option<Batch<T>>, SchedulerError> {
        if self.root.kind() == LaneStateKind::Running {
            return Err(SchedulerError::WrongLaneState);
        }
        if self.root.kind() == LaneStateKind::Stopped {
            return Ok(None);
        }
        if self.failed || self.pending.is_empty() {
            self.root = LaneState::Stopped;
            return Ok(None);
        }

        let batch = self.make_batch()?.expect("pending work was checked above");
        let metadata = Self::metadata(&batch);
        self.root = LaneState::Running {
            current: metadata,
            prefetched: None,
            stopping: false,
        };
        Ok(Some(batch))
    }

    pub(crate) fn on_root_success(
        &mut self,
        batch_id: BatchId,
        results: Vec<ItemResult<U>>,
    ) -> Result<CompletionMeta, SchedulerError> {
        let metadata = match &self.root {
            LaneState::Running {
                current,
                prefetched: None,
                stopping: false,
            } => current,
            _ => return Err(SchedulerError::WrongLaneState),
        };
        self.validate_completion(metadata, batch_id, &results)?;
        let completion = CompletionMeta {
            lane: Lane::Root,
            batch_id,
        };
        self.root = LaneState::Idle;
        if !self.failed {
            for result in results {
                let original_index = result.original_index();
                self.ordered[original_index] = Some(result.into_value());
            }
        }
        Ok(completion)
    }

    pub(crate) fn on_remote_success(
        &mut self,
        rank: i32,
        batch_id: BatchId,
        results: Vec<ItemResult<U>>,
    ) -> Result<CompletionMeta, SchedulerError> {
        let metadata = self.remote_current(rank)?;
        self.validate_completion(metadata, batch_id, &results)?;
        let completion = CompletionMeta {
            lane: Lane::Remote(rank),
            batch_id,
        };
        self.advance_remote(rank)?;
        if !self.failed {
            for result in results {
                let original_index = result.original_index();
                self.ordered[original_index] = Some(result.into_value());
            }
        }
        Ok(completion)
    }

    pub(crate) fn on_root_error(
        &mut self,
        batch_id: BatchId,
    ) -> Result<CompletionMeta, SchedulerError> {
        let metadata = match &self.root {
            LaneState::Running {
                current,
                prefetched: None,
                stopping: false,
            } => current,
            _ => return Err(SchedulerError::WrongLaneState),
        };
        if metadata.batch_id != batch_id {
            return Err(SchedulerError::BatchMismatch);
        }
        self.root = LaneState::Idle;
        self.failed = true;
        self.pending.clear();
        Ok(CompletionMeta {
            lane: Lane::Root,
            batch_id,
        })
    }

    pub(crate) fn on_remote_error(
        &mut self,
        rank: i32,
        batch_id: BatchId,
    ) -> Result<CompletionMeta, SchedulerError> {
        if self.remote_current(rank)?.batch_id != batch_id {
            return Err(SchedulerError::BatchMismatch);
        }
        self.advance_remote(rank)?;
        self.failed = true;
        self.pending.clear();
        Ok(CompletionMeta {
            lane: Lane::Remote(rank),
            batch_id,
        })
    }

    pub(crate) fn on_remote_protocol_error(
        &mut self,
        rank: i32,
    ) -> Result<CompletionMeta, SchedulerError> {
        let batch_id = self.remote_current(rank)?.batch_id;
        self.advance_remote(rank)?;
        self.failed = true;
        self.pending.clear();
        Ok(CompletionMeta {
            lane: Lane::Remote(rank),
            batch_id,
        })
    }

    fn remote_current(&self, rank: i32) -> Result<&RunningMeta, SchedulerError> {
        match self.remotes.get(&rank) {
            Some(LaneState::Running { current, .. }) => Ok(current),
            Some(_) => Err(SchedulerError::WrongLaneState),
            None => Err(SchedulerError::UnknownRank),
        }
    }

    fn advance_remote(&mut self, rank: i32) -> Result<(), SchedulerError> {
        let state = self
            .remotes
            .get_mut(&rank)
            .ok_or(SchedulerError::UnknownRank)?;
        let valid = matches!(
            state,
            LaneState::Running {
                prefetched: Some(_),
                stopping: false,
                ..
            } | LaneState::Running {
                prefetched: None,
                ..
            }
        );
        if !valid {
            return Err(SchedulerError::WrongLaneState);
        }
        let prior = std::mem::replace(state, LaneState::Idle);
        *state = match prior {
            LaneState::Running {
                prefetched: Some(current),
                stopping: false,
                ..
            } => LaneState::Running {
                current,
                prefetched: None,
                stopping: false,
            },
            LaneState::Running {
                prefetched: None,
                stopping: true,
                ..
            } => LaneState::Stopped,
            LaneState::Running {
                prefetched: None,
                stopping: false,
                ..
            } => LaneState::Idle,
            _ => unreachable!("remote running state was validated"),
        };
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail(&mut self) {
        self.failed = true;
        self.pending.clear();
    }

    pub(crate) fn on_remote_drain(&mut self, rank: i32) -> Result<(), SchedulerError> {
        let state = self.remotes.get(&rank).ok_or(SchedulerError::UnknownRank)?;
        if state.kind() != LaneStateKind::Stopped {
            return Err(SchedulerError::WrongLaneState);
        }
        self.remotes.insert(rank, LaneState::Drained);
        Ok(())
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.pending.is_empty() && self.running_count() == 0
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.root.kind() == LaneStateKind::Stopped
            && self
                .remotes
                .values()
                .all(|state| state.kind() == LaneStateKind::Drained)
            && self.running_count() == 0
    }

    pub(crate) fn into_results(self) -> Result<Vec<U>, SchedulerError> {
        if self.failed {
            return Err(SchedulerError::Failed);
        }
        if !self.is_finished() {
            return Err(SchedulerError::NotFinished);
        }
        self.ordered
            .into_iter()
            .map(|result| result.ok_or(SchedulerError::MissingResult))
            .collect()
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.pending.len()
    }

    fn running_count(&self) -> usize {
        usize::from(self.root.kind() == LaneStateKind::Running)
            + self
                .remotes
                .values()
                .filter(|state| state.kind() == LaneStateKind::Running)
                .count()
    }

    #[cfg(test)]
    fn prefetched_count(&self) -> usize {
        self.remotes
            .values()
            .filter(|state| {
                matches!(
                    state,
                    LaneState::Running {
                        prefetched: Some(_),
                        ..
                    }
                )
            })
            .count()
    }

    #[cfg(test)]
    fn result_count(&self) -> usize {
        self.ordered
            .iter()
            .filter(|result| result.is_some())
            .count()
    }

    pub(crate) fn failed(&self) -> bool {
        self.failed
    }

    fn make_batch(&mut self) -> Result<Option<Batch<T>>, SchedulerError> {
        if self.failed || self.pending.is_empty() {
            return Ok(None);
        }
        let id = self.next_batch_id;
        let next_id = id.checked_increment()?;
        let count = self.batch_size.get().min(self.pending.len());
        let items = self.pending.drain(..count).collect();
        self.next_batch_id = next_id;
        Ok(Some(Batch { id, items }))
    }

    fn metadata(batch: &Batch<T>) -> RunningMeta {
        RunningMeta {
            batch_id: batch.id,
            original_indices: batch
                .items
                .iter()
                .map(ScheduledItem::original_index)
                .collect(),
        }
    }

    fn validate_completion(
        &self,
        metadata: &RunningMeta,
        batch_id: BatchId,
        results: &[ItemResult<U>],
    ) -> Result<(), SchedulerError> {
        if metadata.batch_id != batch_id {
            return Err(SchedulerError::BatchMismatch);
        }
        if metadata.original_indices.len() != results.len() {
            return Err(SchedulerError::ResultCount);
        }

        let mut seen = HashSet::with_capacity(results.len());
        for (position, result) in results.iter().enumerate() {
            let index = result.original_index;
            if index >= self.ordered.len() {
                return Err(SchedulerError::IndexOutOfBounds);
            }
            if !seen.insert(index) {
                return Err(SchedulerError::DuplicateResultIndex);
            }
            if metadata.original_indices[position] != index {
                return Err(SchedulerError::IndexSequence);
            }
            if self.ordered[index].is_some() {
                return Err(SchedulerError::AlreadyFilled);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchId, Coordinator, Dispatch, ItemResult, Lane, SchedulerError};
    use std::num::NonZeroUsize;
    use std::rc::Rc;

    fn one() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }

    fn batch<T>(dispatch: Dispatch<T>) -> super::Batch<T> {
        match dispatch {
            Dispatch::Task(batch) => batch,
            Dispatch::Stop => panic!("expected task"),
        }
    }

    fn indices<T>(batch: super::Batch<T>) -> Vec<usize> {
        batch
            .into_items()
            .into_iter()
            .map(|item| item.original_index())
            .collect()
    }

    fn values<T>(batch: super::Batch<T>) -> Vec<T> {
        batch
            .into_items()
            .into_iter()
            .map(super::ScheduledItem::into_value)
            .collect()
    }

    fn result<T>(index: usize, value: T) -> ItemResult<T> {
        ItemResult::new(index, value)
    }

    #[test]
    fn empty_world_size_one_and_more_ranks_than_items() {
        let mut empty = Coordinator::<(), i32>::new(0, vec![], vec![], one()).unwrap();
        assert!(empty.next_root_batch().unwrap().is_none());
        assert!(empty.is_finished());
        assert_eq!(empty.into_results().unwrap(), Vec::<i32>::new());

        let mut one_rank = Coordinator::new(0, vec![], vec![7, 8], one()).unwrap();
        let first = one_rank.next_root_batch().unwrap().unwrap();
        let first_id = first.id();
        assert_eq!(values(first), vec![7]);
        one_rank
            .on_root_success(first_id, vec![result(0, 70)])
            .unwrap();
        let second = one_rank.next_root_batch().unwrap().unwrap();
        let second_id = second.id();
        assert_eq!(values(second), vec![8]);
        one_rank
            .on_root_success(second_id, vec![result(1, 80)])
            .unwrap();
        assert!(one_rank.next_root_batch().unwrap().is_none());
        assert_eq!(one_rank.into_results().unwrap(), vec![70, 80]);

        let mut many = Coordinator::<i32, i32>::new(0, vec![1, 2, 3], vec![9], one()).unwrap();
        assert!(matches!(
            many.on_remote_ready(1).unwrap(),
            Dispatch::Task(_)
        ));
        assert!(matches!(many.on_remote_ready(2).unwrap(), Dispatch::Stop));
        assert!(matches!(many.on_remote_ready(3).unwrap(), Dispatch::Stop));
    }

    #[test]
    fn fifo_batch_one_and_larger_batches() {
        let mut one_batch = Coordinator::<i32, ()>::new(0, vec![], vec![1, 2, 3], one()).unwrap();
        let first = one_batch.next_root_batch().unwrap().unwrap();
        assert_eq!(first.id().get(), 0);
        assert_eq!(values(first), vec![1]);
        let second = one_batch.next_root_batch();
        assert!(matches!(second, Err(SchedulerError::WrongLaneState)));

        let mut larger = Coordinator::<i32, ()>::new(
            0,
            vec![1],
            vec![10, 20, 30, 40, 50],
            NonZeroUsize::new(2).unwrap(),
        )
        .unwrap();
        assert_eq!(
            indices(batch(larger.on_remote_ready(1).unwrap())),
            vec![0, 1]
        );
        let root = larger.next_root_batch().unwrap().unwrap();
        assert_eq!(indices(root), vec![2, 3]);
        assert_eq!(larger.pending_count(), 1);
    }

    #[test]
    fn reverse_completion_restores_order_and_metadata_is_exact() {
        let mut coordinator = Coordinator::new(0, vec![1], vec![10, 20, 30, 40], one()).unwrap();
        let root = coordinator.next_root_batch().unwrap().unwrap();
        let remote = batch(coordinator.on_remote_ready(1).unwrap());
        let remote_id = remote.id();
        let root_id = root.id();
        assert_eq!(coordinator.running_count(), 2);
        let remote_meta = coordinator
            .on_remote_success(1, remote_id, vec![result(1, 200)])
            .unwrap();
        assert_eq!(remote_meta.lane(), Lane::Remote(1));
        assert_eq!(remote_meta.batch_id(), remote_id);
        coordinator
            .on_root_success(root_id, vec![result(0, 100)])
            .unwrap();
        assert_eq!(coordinator.result_count(), 2);
        let next_remote = batch(coordinator.on_remote_ready(1).unwrap());
        let next_root = coordinator.next_root_batch().unwrap().unwrap();
        coordinator
            .on_remote_success(1, next_remote.id(), vec![result(2, 300)])
            .unwrap();
        coordinator
            .on_root_success(next_root.id(), vec![result(3, 400)])
            .unwrap();
        assert!(coordinator.next_root_batch().unwrap().is_none());
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
        coordinator.on_remote_drain(1).unwrap();
        assert_eq!(
            coordinator.into_results().unwrap(),
            vec![100, 200, 300, 400]
        );
    }

    #[test]
    fn running_slots_are_capacity_one_without_prefetch() {
        let mut coordinator = Coordinator::new(0, vec![1], vec![1, 2, 3], one()).unwrap();
        let root = coordinator.next_root_batch().unwrap().unwrap();
        assert_eq!(coordinator.running_count(), 1);
        let remote = batch(coordinator.on_remote_ready(1).unwrap());
        assert_eq!(coordinator.running_count(), 2);
        assert_eq!(coordinator.pending_count(), 1);
        assert!(matches!(
            coordinator.next_root_batch(),
            Err(SchedulerError::WrongLaneState)
        ));
        assert_eq!(coordinator.pending_count(), 1);
        coordinator
            .on_remote_success(1, remote.id(), vec![result(1, 2)])
            .unwrap();
        assert_eq!(coordinator.running_count(), 1);
        assert_eq!(coordinator.pending_count(), 1);
        coordinator
            .on_root_success(root.id(), vec![result(0, 1)])
            .unwrap();
        assert_eq!(coordinator.running_count(), 0);
        assert_eq!(coordinator.pending_count(), 1);
    }

    #[test]
    fn bounded_prefetch_promotes_in_order_and_never_exceeds_one() {
        let mut coordinator =
            Coordinator::new_with_prefetch(0, vec![1], vec![10, 20, 30], one(), true).unwrap();
        let current = batch(coordinator.on_remote_ready(1).unwrap());
        let prefetched = batch(coordinator.on_remote_ready(1).unwrap());
        assert_eq!(indices(current), vec![0]);
        assert_eq!(indices(prefetched), vec![1]);
        assert_eq!(coordinator.running_count(), 1);
        assert_eq!(coordinator.prefetched_count(), 1);
        assert_eq!(coordinator.pending_count(), 1);
        assert!(matches!(
            coordinator.on_remote_ready(1),
            Err(SchedulerError::WrongLaneState)
        ));

        coordinator
            .on_remote_success(1, BatchId(0), vec![result(0, 100)])
            .unwrap();
        assert_eq!(coordinator.prefetched_count(), 0);
        let third = batch(coordinator.on_remote_ready(1).unwrap());
        assert_eq!(indices(third), vec![2]);
        assert_eq!(coordinator.prefetched_count(), 1);
        coordinator
            .on_remote_success(1, BatchId(1), vec![result(1, 200)])
            .unwrap();
        assert_eq!(coordinator.prefetched_count(), 0);
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
        coordinator
            .on_remote_success(1, BatchId(2), vec![result(2, 300)])
            .unwrap();
        coordinator.on_remote_drain(1).unwrap();
        assert!(coordinator.next_root_batch().unwrap().is_none());
        assert_eq!(coordinator.into_results().unwrap(), vec![100, 200, 300]);
    }

    #[test]
    fn bounded_prefetch_error_and_protocol_error_retain_next_batch_for_drain() {
        for protocol_error in [false, true] {
            let mut coordinator = Coordinator::<i32, i32>::new_with_prefetch(
                0,
                vec![1],
                vec![10, 20, 30],
                one(),
                true,
            )
            .unwrap();
            let current = batch(coordinator.on_remote_ready(1).unwrap());
            let prefetched = batch(coordinator.on_remote_ready(1).unwrap());
            if !protocol_error {
                assert_eq!(
                    coordinator
                        .on_remote_error(1, BatchId::from_raw(999))
                        .unwrap_err(),
                    SchedulerError::BatchMismatch
                );
                assert_eq!(coordinator.running_count(), 1);
                assert_eq!(coordinator.prefetched_count(), 1);
                assert_eq!(coordinator.pending_count(), 1);
            }
            let completion = if protocol_error {
                coordinator.on_remote_protocol_error(1).unwrap()
            } else {
                coordinator.on_remote_error(1, current.id()).unwrap()
            };
            assert_eq!(completion.batch_id(), current.id());
            assert!(coordinator.failed());
            assert_eq!(coordinator.pending_count(), 0);
            assert_eq!(coordinator.running_count(), 1);
            assert_eq!(coordinator.prefetched_count(), 0);
            assert!(matches!(
                coordinator.on_remote_ready(1).unwrap(),
                Dispatch::Stop
            ));
            coordinator
                .on_remote_success(1, prefetched.id(), vec![result(1, 200)])
                .unwrap();
            assert_eq!(coordinator.result_count(), 0);
            coordinator.on_remote_drain(1).unwrap();
            assert!(coordinator.next_root_batch().unwrap().is_none());
            assert_eq!(
                coordinator.into_results().unwrap_err(),
                SchedulerError::Failed
            );
        }
    }

    #[test]
    fn bounded_prefetch_stop_waits_for_current_result_before_drain() {
        let mut coordinator =
            Coordinator::new_with_prefetch(0, vec![1], vec![10], one(), true).unwrap();
        let current = batch(coordinator.on_remote_ready(1).unwrap());
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
        assert_eq!(
            coordinator.on_remote_drain(1).unwrap_err(),
            SchedulerError::WrongLaneState
        );
        coordinator
            .on_remote_success(1, current.id(), vec![result(0, 100)])
            .unwrap();
        coordinator.on_remote_drain(1).unwrap();
        assert!(coordinator.next_root_batch().unwrap().is_none());
        assert_eq!(coordinator.into_results().unwrap(), vec![100]);
    }

    #[test]
    fn ready_stop_and_drain_are_each_once_and_ordered() {
        let mut coordinator = Coordinator::<(), i32>::new(0, vec![1], vec![], one()).unwrap();
        assert!(matches!(
            coordinator.on_remote_drain(1),
            Err(SchedulerError::WrongLaneState)
        ));
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
        assert!(matches!(
            coordinator.on_remote_ready(1),
            Err(SchedulerError::WrongLaneState)
        ));
        coordinator.on_remote_drain(1).unwrap();
        assert!(matches!(
            coordinator.on_remote_drain(1),
            Err(SchedulerError::WrongLaneState)
        ));
        assert!(matches!(
            coordinator.on_remote_ready(99),
            Err(SchedulerError::UnknownRank)
        ));
    }

    #[test]
    fn malformed_completion_is_transactional_and_valid_retry_works() {
        let mut coordinator = Coordinator::new(0, vec![], vec![10], one()).unwrap();
        let batch = coordinator.next_root_batch().unwrap().unwrap();
        let id = batch.id();
        let before = (
            coordinator.pending_count(),
            coordinator.running_count(),
            coordinator.result_count(),
        );
        assert_eq!(
            coordinator.on_root_success(id, vec![]).unwrap_err(),
            SchedulerError::ResultCount
        );
        assert_eq!(
            (
                coordinator.pending_count(),
                coordinator.running_count(),
                coordinator.result_count()
            ),
            before
        );
        assert_eq!(
            coordinator
                .on_root_success(id, vec![result(1, 1)])
                .unwrap_err(),
            SchedulerError::IndexOutOfBounds
        );
        assert_eq!(
            (
                coordinator.pending_count(),
                coordinator.running_count(),
                coordinator.result_count()
            ),
            before
        );
        assert_eq!(
            coordinator
                .on_root_success(BatchId(99), vec![result(0, 1)])
                .unwrap_err(),
            SchedulerError::BatchMismatch
        );
        assert_eq!(
            (
                coordinator.pending_count(),
                coordinator.running_count(),
                coordinator.result_count()
            ),
            before
        );
        coordinator
            .on_root_success(id, vec![result(0, 100)])
            .unwrap();
        assert_eq!(coordinator.result_count(), 1);

        let mut already = Coordinator::new(0, vec![], vec![1], one()).unwrap();
        let batch = already.next_root_batch().unwrap().unwrap();
        already.ordered[0] = Some(10);
        assert_eq!(
            already
                .on_root_success(batch.id(), vec![result(0, 20)])
                .unwrap_err(),
            SchedulerError::AlreadyFilled
        );
        assert_eq!(already.running_count(), 1);
        already.ordered[0] = None;
        already
            .on_root_success(batch.id(), vec![result(0, 20)])
            .unwrap();
        assert_eq!(already.result_count(), 1);
    }

    #[test]
    fn duplicate_and_out_of_order_results_are_transactional() {
        let mut coordinator =
            Coordinator::new(0, vec![], vec![10, 20], NonZeroUsize::new(2).unwrap()).unwrap();
        let batch = coordinator.next_root_batch().unwrap().unwrap();
        let id = batch.id();
        let before = (
            coordinator.pending_count(),
            coordinator.running_count(),
            coordinator.result_count(),
        );

        assert_eq!(
            coordinator
                .on_root_success(id, vec![result(1, 20), result(0, 10)])
                .unwrap_err(),
            SchedulerError::IndexSequence
        );
        assert_eq!(
            (
                coordinator.pending_count(),
                coordinator.running_count(),
                coordinator.result_count()
            ),
            before
        );

        assert_eq!(
            coordinator
                .on_root_success(id, vec![result(0, 10), result(0, 20)])
                .unwrap_err(),
            SchedulerError::DuplicateResultIndex
        );
        assert_eq!(
            (
                coordinator.pending_count(),
                coordinator.running_count(),
                coordinator.result_count()
            ),
            before
        );

        coordinator
            .on_root_success(id, vec![result(0, 10), result(1, 20)])
            .unwrap();
        assert_eq!(coordinator.result_count(), 2);
    }

    #[test]
    fn failure_keeps_running_for_validation_then_discards_valid_success() {
        let mut coordinator = Coordinator::new(0, vec![], vec![1, 2], one()).unwrap();
        let batch = coordinator.next_root_batch().unwrap().unwrap();
        coordinator.fail();
        assert!(coordinator.failed());
        assert_eq!(coordinator.pending_count(), 0);
        assert_eq!(
            coordinator.on_root_success(batch.id(), vec![]).unwrap_err(),
            SchedulerError::ResultCount
        );
        assert_eq!(coordinator.running_count(), 1);
        coordinator
            .on_root_success(batch.id(), vec![result(0, 999)])
            .unwrap();
        assert_eq!(coordinator.running_count(), 0);
        assert_eq!(coordinator.result_count(), 0);
        assert!(coordinator.next_root_batch().unwrap().is_none());
        assert_eq!(
            coordinator.into_results().unwrap_err(),
            SchedulerError::Failed
        );
    }

    #[test]
    fn protocol_error_uses_pinned_batch_and_preserves_invalid_state() {
        let mut coordinator = Coordinator::<i32, ()>::new(0, vec![1], vec![1], one()).unwrap();
        assert_eq!(
            coordinator.on_remote_protocol_error(1).unwrap_err(),
            SchedulerError::WrongLaneState
        );
        let batch = batch(coordinator.on_remote_ready(1).unwrap());
        let completion = coordinator.on_remote_protocol_error(1).unwrap();
        assert_eq!(completion.batch_id(), batch.id());
        assert!(coordinator.failed());
        assert_eq!(coordinator.pending_count(), 0);
        assert_eq!(
            coordinator.on_remote_protocol_error(1).unwrap_err(),
            SchedulerError::WrongLaneState
        );
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
    }

    #[test]
    fn error_completion_clears_only_pending_and_allows_drain() {
        let mut coordinator =
            Coordinator::<i32, ()>::new(0, vec![1], vec![1, 2, 3], one()).unwrap();
        let batch = batch(coordinator.on_remote_ready(1).unwrap());
        assert_eq!(coordinator.pending_count(), 2);
        coordinator.on_remote_error(1, batch.id()).unwrap();
        assert_eq!(coordinator.pending_count(), 0);
        assert_eq!(coordinator.result_count(), 0);
        assert!(coordinator.failed());
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
        coordinator.on_remote_drain(1).unwrap();
    }

    #[test]
    fn into_results_rejects_every_not_ready_or_failed_path() {
        let not_ready = Coordinator::<(), i32>::new(0, vec![], vec![()], one()).unwrap();
        assert_eq!(
            not_ready.into_results().unwrap_err(),
            SchedulerError::NotFinished
        );

        let mut no_result = Coordinator::<(), i32>::new(0, vec![], vec![()], one()).unwrap();
        let batch = no_result.next_root_batch().unwrap().unwrap();
        no_result
            .on_root_success(batch.id(), vec![result(0, 3)])
            .unwrap();
        assert!(no_result.next_root_batch().unwrap().is_none());
        no_result.ordered[0] = None;
        assert_eq!(
            no_result.into_results().unwrap_err(),
            SchedulerError::MissingResult
        );

        let mut failed = Coordinator::<(), i32>::new(0, vec![], vec![], one()).unwrap();
        failed.fail();
        assert_eq!(failed.into_results().unwrap_err(), SchedulerError::Failed);
    }

    #[test]
    fn batch_id_overflow_does_not_consume_work() {
        let mut coordinator = Coordinator::<i32, ()>::new(0, vec![], vec![1], one()).unwrap();
        coordinator.next_batch_id = BatchId(u64::MAX);
        assert!(matches!(
            coordinator.next_root_batch(),
            Err(SchedulerError::BatchIdOverflow)
        ));
        assert_eq!(coordinator.pending_count(), 1);
        assert_eq!(coordinator.running_count(), 0);
    }

    #[test]
    fn dynamic_skew_beats_static_contiguous_assignment() {
        let costs = [100, 1, 1, 1];
        let mut coordinator = Coordinator::new(0, vec![1], costs.to_vec(), one()).unwrap();

        let root = coordinator.next_root_batch().unwrap().unwrap();
        let root_id = root.id();
        let root_cost = values(root)[0];

        let mut remote_cost = 0;
        for index in 1..=3 {
            let remote = batch(coordinator.on_remote_ready(1).unwrap());
            let remote_id = remote.id();
            let cost = values(remote)[0];
            remote_cost += cost;
            coordinator
                .on_remote_success(1, remote_id, vec![result(index, cost)])
                .unwrap();
        }
        assert!(matches!(
            coordinator.on_remote_ready(1).unwrap(),
            Dispatch::Stop
        ));
        coordinator.on_remote_drain(1).unwrap();

        coordinator
            .on_root_success(root_id, vec![result(0, root_cost)])
            .unwrap();
        assert!(coordinator.next_root_batch().unwrap().is_none());

        let dynamic_max = root_cost.max(remote_cost);
        let static_max = (costs[0] + costs[1]).max(costs[2] + costs[3]);
        assert_eq!(dynamic_max, 100);
        assert_eq!(static_max, 101);
        assert!(dynamic_max < static_max);
        assert_eq!(coordinator.into_results().unwrap(), costs);
    }

    #[test]
    fn root_values_need_not_be_serializable() {
        let value = Rc::new(7);
        let mut coordinator =
            Coordinator::<Rc<i32>, ()>::new(0, vec![], vec![Rc::clone(&value)], one()).unwrap();
        let batch = coordinator.next_root_batch().unwrap().unwrap();
        assert!(Rc::ptr_eq(&values(batch)[0], &value));
    }

    #[test]
    fn constructor_validates_ranks() {
        assert!(matches!(
            Coordinator::<(), ()>::new(-1, vec![], vec![], one()),
            Err(SchedulerError::InvalidRoot)
        ));
        assert!(matches!(
            Coordinator::<(), ()>::new(0, vec![-1], vec![], one()),
            Err(SchedulerError::InvalidRemote)
        ));
        assert!(matches!(
            Coordinator::<(), ()>::new(0, vec![1, 1], vec![], one()),
            Err(SchedulerError::DuplicateRemote)
        ));
        assert!(matches!(
            Coordinator::<(), ()>::new(0, vec![0], vec![], one()),
            Err(SchedulerError::RootInRemotes)
        ));
    }
}
