use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

/// The P0 execution-domain identifier.
///
/// P0 supports only [`DomainId::ZERO`]. Other numeric identifiers are rejected
/// by [`TryFrom`].
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DomainId(u32);

impl DomainId {
    /// The only domain identifier supported by P0.
    pub const ZERO: Self = Self(0);

    /// Returns the numeric identifier.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for DomainId {
    type Error = UnsupportedDomainId;

    fn try_from(requested: u32) -> Result<Self, Self::Error> {
        match requested {
            0 => Ok(Self::ZERO),
            requested => Err(UnsupportedDomainId { requested }),
        }
    }
}

/// The requested domain identifier is not supported by P0.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct UnsupportedDomainId {
    requested: u32,
}

impl UnsupportedDomainId {
    /// Returns the unsupported identifier that was requested.
    pub const fn requested(self) -> u32 {
        self.requested
    }
}

impl fmt::Display for UnsupportedDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unsupported domain ID: {}", self.requested)
    }
}

impl std::error::Error for UnsupportedDomainId {}

/// A rank and its execution-domain identifier.
///
/// P0 uses zero-based non-negative ranks and only accepts the supported domain
/// identifiers. [`Place::new`] rejects a negative rank.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Place {
    rank: i32,
    domain_id: DomainId,
}

impl Place {
    /// Creates a place, rejecting a negative rank.
    pub fn new(rank: i32, domain_id: DomainId) -> Result<Self, NegativeRank> {
        if rank < 0 {
            return Err(NegativeRank { requested: rank });
        }
        Ok(Self { rank, domain_id })
    }

    /// Returns the non-negative rank.
    pub const fn rank(self) -> i32 {
        self.rank
    }

    /// Returns the execution-domain identifier.
    pub const fn domain_id(self) -> DomainId {
        self.domain_id
    }
}

/// A negative rank was supplied to [`Place::new`].
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct NegativeRank {
    requested: i32,
}

impl NegativeRank {
    /// Returns the negative rank that was requested.
    pub const fn requested(self) -> i32 {
        self.requested
    }
}

impl fmt::Display for NegativeRank {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "rank must be non-negative: {}", self.requested)
    }
}

impl std::error::Error for NegativeRank {}

/// A P0 execution domain with one sequential worker.
///
/// `Domain::sequential` creates domain zero with one worker. At most one
/// coarse operation may be admitted at a time; [`Domain::try_admit`] returns
/// [`DomainBusy`] immediately instead of waiting when another operation owns
/// the domain.
///
/// # Example
///
/// ```
/// use hataori::{Domain, DomainBusy};
///
/// let domain = Domain::sequential();
/// let admission = domain.try_admit().unwrap();
/// assert_eq!(domain.try_admit().unwrap_err(), DomainBusy);
/// drop(admission);
/// assert!(domain.try_admit().is_ok());
/// ```
#[derive(Debug)]
pub struct Domain {
    id: DomainId,
    worker_count: usize,
    running: AtomicBool,
}

impl Domain {
    /// Creates the P0 sequential domain: ID zero, one worker, and no running
    /// operation.
    pub fn sequential() -> Self {
        Self {
            id: DomainId::ZERO,
            worker_count: 1,
            running: AtomicBool::new(false),
        }
    }

    /// Returns the domain identifier.
    pub const fn id(&self) -> DomainId {
        self.id
    }

    /// Returns the declared worker count.
    pub const fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// Attempts to admit one coarse operation without waiting.
    pub fn try_admit(&self) -> Result<DomainAdmission<'_>, DomainBusy> {
        self.running
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map(|_| DomainAdmission { domain: self })
            .map_err(|_| DomainBusy)
    }
}

/// RAII ownership of a domain's one running-operation slot.
///
/// Dropping the admission releases the slot with release ordering. The guard
/// is scoped and does not provide a waiting or reentrant admission path.
#[must_use]
#[derive(Debug)]
pub struct DomainAdmission<'a> {
    domain: &'a Domain,
}

impl Drop for DomainAdmission<'_> {
    fn drop(&mut self) {
        self.domain.running.store(false, Ordering::Release);
    }
}

/// The domain's admission slot is already occupied.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DomainBusy;

impl fmt::Display for DomainBusy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("domain is busy")
    }
}

impl std::error::Error for DomainBusy {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{mpsc, Barrier};
    use std::thread;

    #[test]
    fn domain_id_zero_conversion_and_nonzero_error() {
        let zero = DomainId::try_from(0).unwrap();
        assert_eq!(zero, DomainId::ZERO);
        assert_eq!(zero.get(), 0);

        let error = DomainId::try_from(1).unwrap_err();
        assert_eq!(error.requested(), 1);
        assert_eq!(error.to_string(), "unsupported domain ID: 1");
    }

    #[test]
    fn place_valid_getters_and_negative_rank_error() {
        let place = Place::new(3, DomainId::ZERO).unwrap();
        assert_eq!(place.rank(), 3);
        assert_eq!(place.domain_id(), DomainId::ZERO);

        let error = Place::new(-1, DomainId::ZERO).unwrap_err();
        assert_eq!(error.requested(), -1);
        assert_eq!(error.to_string(), "rank must be non-negative: -1");
    }

    #[test]
    fn sequential_domain_has_zero_id_and_one_worker() {
        let domain = Domain::sequential();
        assert_eq!(domain.id(), DomainId::ZERO);
        assert_eq!(domain.worker_count(), 1);
    }

    #[test]
    fn nested_admission_is_busy_and_reacquires_after_drop() {
        let domain = Domain::sequential();
        let admission = domain.try_admit().unwrap();
        assert_eq!(domain.try_admit().unwrap_err(), DomainBusy);
        drop(admission);
        assert!(domain.try_admit().is_ok());
    }

    fn returns_error(domain: &Domain) -> Result<(), &'static str> {
        let _admission = domain.try_admit().unwrap();
        Err("ordinary error")
    }

    #[test]
    fn admission_releases_after_helper_returns_error() {
        let domain = Domain::sequential();
        assert_eq!(returns_error(&domain), Err("ordinary error"));
        assert!(domain.try_admit().is_ok());
    }

    #[test]
    fn admission_releases_during_unwind() {
        let domain = Domain::sequential();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _admission = domain.try_admit().unwrap();
            panic!("test panic");
        }));
        assert!(result.is_err());
        assert!(domain.try_admit().is_ok());
    }

    #[test]
    fn scoped_cross_thread_admission_is_nonblocking_and_reacquires() {
        let domain = Domain::sequential();
        let barrier = Barrier::new(2);
        let (release_tx, release_rx) = mpsc::channel();
        let (busy_tx, busy_rx) = mpsc::channel();
        let (released_tx, released_rx) = mpsc::channel();
        let (success_tx, success_rx) = mpsc::channel();
        let domain_ref = &domain;

        thread::scope(|scope| {
            let holder_domain = domain_ref;
            let holder_barrier = &barrier;
            scope.spawn(move || {
                let admission = holder_domain.try_admit().unwrap();
                holder_barrier.wait();
                release_rx.recv().unwrap();
                drop(admission);
                released_tx.send(()).unwrap();
            });

            barrier.wait();
            let contender_domain = domain_ref;
            scope.spawn(move || {
                busy_tx
                    .send(matches!(contender_domain.try_admit(), Err(DomainBusy)))
                    .unwrap();
                release_tx.send(()).unwrap();
                released_rx.recv().unwrap();
                success_tx
                    .send(contender_domain.try_admit().is_ok())
                    .unwrap();
            });

            assert!(busy_rx.recv().unwrap());
            assert!(success_rx.recv().unwrap());
        });
    }
}
