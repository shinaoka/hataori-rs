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

/// Local execution policy for a domain operation.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LocalMode {
    /// Run callbacks one at a time in the selected pool and stop at the first error.
    Sequential,
    /// Run every callback in parallel in the selected pool, preserve input order,
    /// and report the lowest callback-error index after all callbacks finish.
    Outer,
    /// Run callbacks one at a time in the selected pool and allow nested work in
    /// that same pool; stop at the first error.
    Inner,
}

#[cfg(feature = "rayon")]
/// Whether a Rayon pool is owned by Hataori or by its caller.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PoolOwnership {
    /// The domain created and owns the pool.
    Managed,
    /// The pool belongs to the caller.
    External,
}

#[cfg(feature = "rayon")]
/// Whether the domain's CPU placement was verified.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PlacementStatus {
    /// Linux worker startup pinned and verified every worker.
    Verified,
    /// Placement is a declaration supplied by the caller.
    CallerDeclared,
}

#[cfg(feature = "rayon")]
/// A managed or external Rayon domain could not be constructed.
pub enum DomainBuildError {
    EmptyCpuSet,
    DuplicateCpu {
        cpu: usize,
    },
    ZeroWorkers,
    TooManyWorkers {
        worker_count: usize,
        cpu_count: usize,
    },
    CpuOutOfRange {
        cpu: usize,
        cpu_set_size: usize,
    },
    CpuNotAllowed {
        cpu: usize,
    },
    AffinityQuery {
        errno: i32,
    },
    PoolBuild {
        message: String,
    },
    PinFailed {
        worker_index: usize,
        cpu: usize,
        message: String,
    },
    StartTimeout {
        expected: usize,
        received: usize,
    },
    InvalidReport {
        worker_index: usize,
        cpu: usize,
    },
    PoolSizeMismatch {
        expected: usize,
        actual: usize,
    },
}

#[cfg(feature = "rayon")]
impl fmt::Debug for DomainBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCpuSet => formatter.write_str("EmptyCpuSet"),
            Self::DuplicateCpu { cpu } => formatter
                .debug_struct("DuplicateCpu")
                .field("cpu", cpu)
                .finish(),
            Self::ZeroWorkers => formatter.write_str("ZeroWorkers"),
            Self::TooManyWorkers {
                worker_count,
                cpu_count,
            } => formatter
                .debug_struct("TooManyWorkers")
                .field("worker_count", worker_count)
                .field("cpu_count", cpu_count)
                .finish(),
            Self::CpuOutOfRange { cpu, cpu_set_size } => formatter
                .debug_struct("CpuOutOfRange")
                .field("cpu", cpu)
                .field("cpu_set_size", cpu_set_size)
                .finish(),
            Self::CpuNotAllowed { cpu } => formatter
                .debug_struct("CpuNotAllowed")
                .field("cpu", cpu)
                .finish(),
            Self::AffinityQuery { errno } => formatter
                .debug_struct("AffinityQuery")
                .field("errno", errno)
                .finish(),
            Self::PoolBuild { message } => formatter
                .debug_struct("PoolBuild")
                .field("message", message)
                .finish(),
            Self::PinFailed {
                worker_index,
                cpu,
                message,
            } => formatter
                .debug_struct("PinFailed")
                .field("worker_index", worker_index)
                .field("cpu", cpu)
                .field("message", message)
                .finish(),
            Self::StartTimeout { expected, received } => formatter
                .debug_struct("StartTimeout")
                .field("expected", expected)
                .field("received", received)
                .finish(),
            Self::InvalidReport { worker_index, cpu } => formatter
                .debug_struct("InvalidReport")
                .field("worker_index", worker_index)
                .field("cpu", cpu)
                .finish(),
            Self::PoolSizeMismatch { expected, actual } => formatter
                .debug_struct("PoolSizeMismatch")
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
        }
    }
}

#[cfg(feature = "rayon")]
impl fmt::Display for DomainBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCpuSet => formatter.write_str("CPU set must not be empty"),
            Self::DuplicateCpu { cpu } => write!(formatter, "CPU set contains duplicate CPU {cpu}"),
            Self::ZeroWorkers => formatter.write_str("worker count must be greater than zero"),
            Self::TooManyWorkers {
                worker_count,
                cpu_count,
            } => write!(
                formatter,
                "worker count {worker_count} exceeds CPU set size {cpu_count}"
            ),
            Self::CpuOutOfRange { cpu, cpu_set_size } => {
                write!(
                    formatter,
                    "CPU {cpu} is outside CPU set size {cpu_set_size}"
                )
            }
            Self::CpuNotAllowed { cpu } => write!(formatter, "CPU {cpu} is not process-allowed"),
            Self::AffinityQuery { errno } => write!(
                formatter,
                "could not query process affinity (errno {errno})"
            ),
            Self::PoolBuild { message } => {
                write!(formatter, "could not build Rayon pool: {message}")
            }
            Self::PinFailed {
                worker_index,
                cpu,
                message,
            } => write!(
                formatter,
                "worker {worker_index} could not be placed on CPU {cpu}: {message}"
            ),
            Self::StartTimeout { expected, received } => write!(
                formatter,
                "timed out waiting for Rayon worker startup reports ({received}/{expected})"
            ),
            Self::InvalidReport { worker_index, cpu } => write!(
                formatter,
                "invalid Rayon worker startup report for worker {worker_index} on CPU {cpu}"
            ),
            Self::PoolSizeMismatch { expected, actual } => write!(
                formatter,
                "external Rayon pool has {actual} workers, expected {expected}"
            ),
        }
    }
}

#[cfg(feature = "rayon")]
impl std::error::Error for DomainBuildError {}

/// A P0 execution domain.
///
/// `Domain::sequential` creates domain zero with one worker and no Rayon pool.
/// At most one coarse operation may be admitted at a time; [`Domain::try_admit`]
/// returns [`DomainBusy`] immediately instead of waiting when another operation
/// owns the domain.
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
// No Clone: a domain owns admission state and, with Rayon, a pool handle.
pub struct Domain {
    id: DomainId,
    worker_count: usize,
    running: AtomicBool,
    #[cfg(feature = "rayon")]
    pool: Option<std::sync::Arc<rayon::ThreadPool>>,
    #[cfg(feature = "rayon")]
    cpu_set: Option<Vec<usize>>,
    #[cfg(feature = "rayon")]
    ownership: Option<PoolOwnership>,
    #[cfg(feature = "rayon")]
    placement: Option<PlacementStatus>,
}

impl fmt::Debug for Domain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("Domain");
        debug
            .field("id", &self.id)
            .field("worker_count", &self.worker_count)
            .field("running", &self.running.load(Ordering::Relaxed));
        #[cfg(feature = "rayon")]
        debug
            .field("has_pool", &self.pool.is_some())
            .field("cpu_set", &self.cpu_set)
            .field("ownership", &self.ownership)
            .field("placement", &self.placement);
        debug.finish()
    }
}

impl Domain {
    /// Creates the P0 sequential domain: ID zero, one worker, and no running
    /// operation.
    pub fn sequential() -> Self {
        Self {
            id: DomainId::ZERO,
            worker_count: 1,
            running: AtomicBool::new(false),
            #[cfg(feature = "rayon")]
            pool: None,
            #[cfg(feature = "rayon")]
            cpu_set: None,
            #[cfg(feature = "rayon")]
            ownership: None,
            #[cfg(feature = "rayon")]
            placement: None,
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

    #[cfg(feature = "rayon")]
    /// Creates and owns a Rayon pool, verifying worker affinity on Linux.
    pub fn managed(cpu_set: Vec<usize>, worker_count: usize) -> Result<Self, DomainBuildError> {
        validate_cpu_set(&cpu_set, worker_count)?;
        #[cfg(target_os = "linux")]
        {
            validate_linux_cpu_range(&cpu_set)?;
            let allowed = process_allowed_cpus()?;
            for &cpu in &cpu_set {
                if !allowed.contains(&cpu) {
                    return Err(DomainBuildError::CpuNotAllowed { cpu });
                }
            }
        }
        build_managed(cpu_set, worker_count, std::sync::Arc::new(DefaultPinner))
    }

    #[cfg(feature = "rayon")]
    /// Uses a caller-owned Rayon pool without changing its worker affinity or
    /// shutting it down when this domain is dropped.
    pub fn external(
        pool: std::sync::Arc<rayon::ThreadPool>,
        declared_cpu_set: Vec<usize>,
        worker_count: usize,
    ) -> Result<Self, DomainBuildError> {
        validate_cpu_set(&declared_cpu_set, worker_count)?;
        #[cfg(target_os = "linux")]
        validate_linux_cpu_range(&declared_cpu_set)?;
        let actual = pool.current_num_threads();
        if actual != worker_count {
            return Err(DomainBuildError::PoolSizeMismatch {
                expected: worker_count,
                actual,
            });
        }
        Ok(Self {
            id: DomainId::ZERO,
            worker_count,
            running: AtomicBool::new(false),
            pool: Some(pool),
            cpu_set: Some(declared_cpu_set),
            ownership: Some(PoolOwnership::External),
            placement: Some(PlacementStatus::CallerDeclared),
        })
    }

    #[cfg(feature = "rayon")]
    /// Returns the declared or verified CPU set, when this domain has a pool.
    pub fn cpu_set(&self) -> Option<&[usize]> {
        self.cpu_set.as_deref()
    }

    #[cfg(feature = "rayon")]
    /// Returns the pool ownership mode, when this domain has a pool.
    pub fn pool_ownership(&self) -> Option<PoolOwnership> {
        self.ownership
    }

    #[cfg(feature = "rayon")]
    /// Returns the placement status, when this domain has a pool.
    pub fn placement_status(&self) -> Option<PlacementStatus> {
        self.placement
    }

    #[cfg(feature = "rayon")]
    /// Clones the Rayon pool handle associated with this domain, when present.
    ///
    /// This grants no domain admission. Submitting work directly to the returned
    /// pool bypasses Hataori scheduling; integration adapters must enter through
    /// an already admitted [`LocalMode::Inner`] callback.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "rayon")]
    /// # {
    /// use hataori::Domain;
    /// use std::sync::Arc;
    ///
    /// let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap());
    /// let domain = Domain::external(Arc::clone(&pool), vec![0], 1).unwrap();
    /// assert!(Arc::ptr_eq(&pool, &domain.rayon_pool_handle().unwrap()));
    /// # }
    /// ```
    pub fn rayon_pool_handle(&self) -> Option<std::sync::Arc<rayon::ThreadPool>> {
        self.pool.clone()
    }

    #[cfg(feature = "rayon")]
    pub(crate) fn rayon_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>> {
        self.pool.as_ref()
    }
}

#[cfg(feature = "rayon")]
fn validate_cpu_set(cpu_set: &[usize], worker_count: usize) -> Result<(), DomainBuildError> {
    if cpu_set.is_empty() {
        return Err(DomainBuildError::EmptyCpuSet);
    }
    let mut seen = std::collections::HashSet::with_capacity(cpu_set.len());
    for &cpu in cpu_set {
        if !seen.insert(cpu) {
            return Err(DomainBuildError::DuplicateCpu { cpu });
        }
    }
    if worker_count == 0 {
        return Err(DomainBuildError::ZeroWorkers);
    }
    if worker_count > cpu_set.len() {
        return Err(DomainBuildError::TooManyWorkers {
            worker_count,
            cpu_count: cpu_set.len(),
        });
    }
    Ok(())
}

#[cfg(all(feature = "rayon", target_os = "linux"))]
fn validate_linux_cpu_range(cpu_set: &[usize]) -> Result<(), DomainBuildError> {
    let cpu_set_size = libc::CPU_SETSIZE as usize;
    for &cpu in cpu_set {
        if cpu >= cpu_set_size {
            return Err(DomainBuildError::CpuOutOfRange { cpu, cpu_set_size });
        }
    }
    Ok(())
}

#[cfg(all(feature = "rayon", target_os = "linux"))]
fn process_allowed_cpus() -> Result<std::collections::HashSet<usize>, DomainBuildError> {
    let mut allowed = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    let result =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut allowed) };
    if result != 0 {
        return Err(DomainBuildError::AffinityQuery {
            errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(-1),
        });
    }
    let mut cpus = std::collections::HashSet::new();
    for cpu in 0..libc::CPU_SETSIZE as usize {
        if unsafe { libc::CPU_ISSET(cpu, &allowed) } {
            cpus.insert(cpu);
        }
    }
    Ok(cpus)
}

#[cfg(feature = "rayon")]
trait Pinner: Send + Sync {
    fn pin_and_verify(&self, cpu: usize) -> Result<(), String>;
}

#[cfg(feature = "rayon")]
struct DefaultPinner;

#[cfg(all(feature = "rayon", target_os = "linux"))]
impl Pinner for DefaultPinner {
    fn pin_and_verify(&self, cpu: usize) -> Result<(), String> {
        let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        unsafe { libc::CPU_ZERO(&mut set) };
        unsafe { libc::CPU_SET(cpu, &mut set) };
        let result = unsafe {
            libc::pthread_setaffinity_np(
                libc::pthread_self(),
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            )
        };
        if result != 0 {
            return Err(format!("pthread_setaffinity_np failed (errno {result})"));
        }
        let actual = unsafe { libc::sched_getcpu() };
        if actual != cpu as i32 {
            return Err(format!("worker ran on CPU {actual}"));
        }
        Ok(())
    }
}

#[cfg(all(feature = "rayon", not(target_os = "linux")))]
impl Pinner for DefaultPinner {
    fn pin_and_verify(&self, _cpu: usize) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(feature = "rayon")]
struct WorkerReport {
    index: usize,
    cpu: usize,
    result: Result<(), String>,
}

#[cfg(feature = "rayon")]
fn build_managed(
    cpu_set: Vec<usize>,
    worker_count: usize,
    pinner: std::sync::Arc<dyn Pinner>,
) -> Result<Domain, DomainBuildError> {
    let expected_cpus = cpu_set.clone();
    let start_cpus = expected_cpus.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    let builder = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .thread_name(|index| format!("hataori-worker-{index}"))
        .start_handler(move |index| {
            let cpu = start_cpus.get(index).copied().unwrap_or(usize::MAX);
            let result = if cpu == usize::MAX {
                Err(String::from("worker index is outside the declared CPU set"))
            } else {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pinner.pin_and_verify(cpu)
                }))
                .unwrap_or_else(|_| Err(String::from("worker pinner panicked")))
            };
            let _ = sender.send(WorkerReport { index, cpu, result });
        });
    let pool = builder
        .build()
        .map_err(|error| DomainBuildError::PoolBuild {
            message: error.to_string(),
        })?;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut seen = vec![false; worker_count];
    for received in 0..worker_count {
        let report = match receiver
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        {
            Ok(report) => report,
            Err(_) => {
                drop(pool);
                return Err(DomainBuildError::StartTimeout {
                    expected: worker_count,
                    received,
                });
            }
        };
        if report.index >= worker_count
            || seen[report.index]
            || expected_cpus.get(report.index).copied() != Some(report.cpu)
        {
            drop(pool);
            return Err(DomainBuildError::InvalidReport {
                worker_index: report.index,
                cpu: report.cpu,
            });
        }
        seen[report.index] = true;
        if let Err(message) = report.result {
            drop(pool);
            return Err(DomainBuildError::PinFailed {
                worker_index: report.index,
                cpu: report.cpu,
                message,
            });
        }
    }

    Ok(Domain {
        id: DomainId::ZERO,
        worker_count,
        running: AtomicBool::new(false),
        pool: Some(std::sync::Arc::new(pool)),
        cpu_set: Some(cpu_set),
        ownership: Some(PoolOwnership::Managed),
        placement: Some(if cfg!(target_os = "linux") {
            PlacementStatus::Verified
        } else {
            PlacementStatus::CallerDeclared
        }),
    })
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
        #[cfg(feature = "rayon")]
        {
            assert_eq!(domain.cpu_set(), None);
            assert_eq!(domain.pool_ownership(), None);
            assert_eq!(domain.placement_status(), None);
            assert!(domain.pool.is_none());
        }
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

    #[cfg(feature = "rayon")]
    struct FailingPinner;

    #[cfg(feature = "rayon")]
    impl Pinner for FailingPinner {
        fn pin_and_verify(&self, _cpu: usize) -> Result<(), String> {
            Err(String::from("injected failure"))
        }
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn managed_rejects_invalid_worker_configuration() {
        assert!(matches!(
            Domain::managed(Vec::new(), 1),
            Err(DomainBuildError::EmptyCpuSet)
        ));
        assert!(matches!(
            Domain::managed(vec![0, 0], 1),
            Err(DomainBuildError::DuplicateCpu { cpu: 0 })
        ));
        assert!(matches!(
            Domain::managed(vec![0], 0),
            Err(DomainBuildError::ZeroWorkers)
        ));
        assert!(matches!(
            Domain::managed(vec![0], 2),
            Err(DomainBuildError::TooManyWorkers {
                worker_count: 2,
                cpu_count: 1
            })
        ));
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn external_rejects_pool_size_mismatch() {
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        assert!(matches!(
            Domain::external(pool, vec![0], 1),
            Err(DomainBuildError::PoolSizeMismatch {
                expected: 1,
                actual: 2
            })
        ));
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn external_does_not_own_or_pin_caller_pool() {
        let starts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let starts_in_handler = std::sync::Arc::clone(&starts);
        let caller_pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .start_handler(move |_| {
                    starts_in_handler.fetch_add(1, Ordering::Relaxed);
                })
                .build()
                .unwrap(),
        );
        let domain = Domain::external(std::sync::Arc::clone(&caller_pool), vec![0], 1).unwrap();
        assert_eq!(domain.pool_ownership(), Some(PoolOwnership::External));
        assert_eq!(
            domain.placement_status(),
            Some(PlacementStatus::CallerDeclared)
        );
        drop(domain);

        caller_pool.install(|| assert_eq!(rayon::current_num_threads(), 1));
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    }

    #[cfg(all(feature = "rayon", target_os = "linux"))]
    #[test]
    fn managed_rejects_linux_cpu_bounds_and_process_disallowed_cpu() {
        let out_of_range = libc::CPU_SETSIZE as usize;
        assert!(matches!(
            Domain::managed(vec![out_of_range], 1),
            Err(DomainBuildError::CpuOutOfRange {
                cpu,
                cpu_set_size
            }) if cpu == out_of_range && cpu_set_size == out_of_range
        ));

        let allowed = process_allowed_cpus().unwrap();
        let disallowed = (0..libc::CPU_SETSIZE as usize).find(|cpu| !allowed.contains(cpu));
        let Some(disallowed) = disallowed else {
            return;
        };
        assert!(matches!(
            Domain::managed(vec![disallowed], 1),
            Err(DomainBuildError::CpuNotAllowed { cpu }) if cpu == disallowed
        ));
    }

    #[cfg(feature = "rayon")]
    #[test]
    fn managed_pin_failure_is_typed_and_does_not_panic() {
        let result = catch_unwind(AssertUnwindSafe(|| {
            build_managed(vec![0], 1, std::sync::Arc::new(FailingPinner))
        }))
        .unwrap();
        assert!(matches!(result, Err(DomainBuildError::PinFailed { .. })));
    }

    #[cfg(all(feature = "rayon", target_os = "linux"))]
    #[test]
    fn managed_workers_are_on_distinct_allowed_cpus() {
        let allowed = process_allowed_cpus().unwrap();
        assert!(!allowed.is_empty());
        let cpus: Vec<_> = allowed.iter().copied().take(2).collect();
        let worker_count = cpus.len().max(1);
        let domain = Domain::managed(cpus.clone(), worker_count).unwrap();
        assert_eq!(domain.placement_status(), Some(PlacementStatus::Verified));
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let copy = std::sync::Arc::clone(&observed);
        domain.pool.as_ref().unwrap().broadcast(|_| {
            let cpu = unsafe { libc::sched_getcpu() } as usize;
            copy.lock().unwrap().push(cpu);
        });
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), worker_count);
        assert!(observed.iter().all(|cpu| cpus.contains(cpu)));
        let distinct: std::collections::HashSet<_> = observed.iter().copied().collect();
        assert_eq!(distinct.len(), worker_count);
    }

    #[cfg(all(feature = "rayon", not(target_os = "linux")))]
    #[test]
    fn managed_non_linux_placement_is_caller_declared() {
        let domain = Domain::managed(vec![0], 1).unwrap();
        assert_eq!(
            domain.placement_status(),
            Some(PlacementStatus::CallerDeclared)
        );
    }
}
