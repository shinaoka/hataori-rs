//! Bind a Hataori Rayon domain to tenferro's caller-managed CPU backend.
//!
//! Use the adapter only from an admitted [`hataori::LocalMode::Inner`] callback;
//! Hataori owns coarse admission and tenferro owns provider-internal fan-out.
//!
//! # Examples
//!
//! ```
//! use hataori::{map_in, Domain, LocalMode};
//! use hataori_tenferro::TenferroDomain;
//! use std::sync::Arc;
//!
//! let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build()?);
//! let domain = Domain::external(pool, vec![0, 1], 2)?;
//! let adapter = TenferroDomain::new(domain)?;
//! let budgets = map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
//!     adapter
//!         .with_backend(|backend| backend.execution_info().thread_budget())
//!         .map_err(|error| error.to_string())
//! })?;
//! assert_eq!(budgets, vec![2]);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use hataori::Domain;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tenferro_cpu::{
    CpuBackend, CpuBackendError, CpuDomainExecutor, ExternalCpuDomain, ExternalCpuDomainError,
    RayonCpuDomainExecutor,
};
use tenferro_tensor::CpuDomainId;

/// Failure to construct or enter a [`TenferroDomain`].
///
/// # Examples
///
/// ```
/// use hataori::Domain;
/// use hataori_tenferro::{TenferroAdapterError, TenferroDomain};
///
/// let error = TenferroDomain::new(Domain::sequential()).unwrap_err();
/// assert!(matches!(error, TenferroAdapterError::MissingPool));
/// ```
#[derive(Debug)]
pub enum TenferroAdapterError {
    /// The Hataori domain has no Rayon pool.
    MissingPool,
    /// tenferro rejected the caller-managed domain descriptor.
    ExternalDomain(ExternalCpuDomainError),
    /// tenferro could not construct the caller-managed CPU backend.
    Backend(CpuBackendError),
    /// The call did not originate on the bound Hataori domain's Rayon pool.
    OutsideDomainPool,
    /// Another adapter entry is active for this domain binding.
    ConcurrentEntry,
}

impl fmt::Display for TenferroAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPool => formatter.write_str("Hataori domain has no Rayon pool"),
            Self::ExternalDomain(error) => write!(formatter, "invalid tenferro domain: {error}"),
            Self::Backend(error) => {
                write!(formatter, "could not construct tenferro backend: {error}")
            }
            Self::OutsideDomainPool => {
                formatter.write_str("tenferro entry must run on the bound Hataori domain pool")
            }
            Self::ConcurrentEntry => formatter
                .write_str("tenferro adapter cannot be entered recursively or concurrently"),
        }
    }
}

impl std::error::Error for TenferroAdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ExternalDomain(error) => Some(error),
            Self::Backend(error) => Some(error),
            Self::MissingPool | Self::OutsideDomainPool | Self::ConcurrentEntry => None,
        }
    }
}

/// A tenferro CPU backend bound to one Hataori Rayon domain.
///
/// The binding retains the exact pool selected by `domain`. Call
/// [`TenferroDomain::with_backend`] only from an admitted
/// [`hataori::LocalMode::Inner`] callback. The adapter does not acquire a second
/// Hataori admission slot and never creates or shuts down a Rayon pool.
///
/// # Examples
///
/// ```
/// use hataori::Domain;
/// use hataori_tenferro::TenferroDomain;
/// use std::sync::Arc;
///
/// let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(1).build()?);
/// let domain = Domain::external(pool, vec![0], 1)?;
/// let adapter = TenferroDomain::new(domain)?;
/// assert_eq!(format!("{adapter:?}"), "TenferroDomain { domain_id: 0, worker_count: 1 }");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct TenferroDomain {
    domain: Domain,
    pool: Arc<rayon::ThreadPool>,
    backend: CpuBackend,
    active: AtomicBool,
}

struct EntryGuard<'active>(&'active AtomicBool);

impl Drop for EntryGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl fmt::Debug for TenferroDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenferroDomain")
            .field("domain_id", &self.domain.id().get())
            .field("worker_count", &self.domain.worker_count())
            .finish()
    }
}

impl TenferroDomain {
    /// Binds the exact Rayon pool and full worker budget of `domain` to tenferro.
    ///
    /// This constructs no pool and acquires no Hataori admission. tenferro uses
    /// caller-managed admission and validates its Faer provider contract before
    /// this function returns.
    ///
    /// # Examples
    ///
    /// ```
    /// use hataori::Domain;
    /// use hataori_tenferro::TenferroDomain;
    /// use std::sync::Arc;
    ///
    /// let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(1).build()?);
    /// let domain = Domain::external(pool, vec![0], 1)?;
    /// assert!(TenferroDomain::new(domain).is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TenferroAdapterError::MissingPool`] for a sequential domain,
    /// or preserves tenferro's typed domain/backend construction error.
    pub fn new(domain: Domain) -> Result<Self, TenferroAdapterError> {
        let pool = domain
            .rayon_pool_handle()
            .ok_or(TenferroAdapterError::MissingPool)?;
        // INVARIANT: every Rayon-backed Domain validates a positive worker count.
        let thread_budget = NonZeroUsize::new(domain.worker_count()).unwrap_or(NonZeroUsize::MIN);
        let executor: Arc<dyn CpuDomainExecutor> =
            Arc::new(RayonCpuDomainExecutor::new(Arc::clone(&pool)));
        let id = CpuDomainId::new(u64::from(domain.id().get()));
        let external = ExternalCpuDomain::new_caller_managed(id, executor, thread_budget)
            .map_err(TenferroAdapterError::ExternalDomain)?;
        let backend = CpuBackend::from_external_managed_domains(id, [external])
            .map_err(TenferroAdapterError::Backend)?;
        Ok(Self {
            domain,
            pool,
            backend,
            active: AtomicBool::new(false),
        })
    }

    /// Returns the Hataori domain owned by this adapter.
    ///
    /// Pass this reference to [`hataori::map_in`] or a feature-enabled collective
    /// `pmap` call with [`hataori::LocalMode::Inner`].
    ///
    /// # Examples
    ///
    /// ```
    /// use hataori::Domain;
    /// use hataori_tenferro::TenferroDomain;
    /// use std::sync::Arc;
    ///
    /// let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(1).build()?);
    /// let adapter = TenferroDomain::new(Domain::external(pool, vec![0], 1)?)?;
    /// assert_eq!(adapter.domain().worker_count(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn domain(&self) -> &Domain {
        &self.domain
    }

    /// Runs a borrowed operation through a cheap clone of the bound backend.
    ///
    /// This method must be called from the bound pool inside an admitted
    /// [`hataori::LocalMode::Inner`] callback. Pool membership is checked before
    /// `operation` runs. tenferro's shared caller-managed guard rejects
    /// simultaneous or recursive adapter entry with a typed error before the
    /// closure runs. tenferro retains its own final public-backend guard.
    ///
    /// # Examples
    ///
    /// ```
    /// use hataori::{map_in, Domain, LocalMode};
    /// use hataori_tenferro::TenferroDomain;
    /// use std::sync::Arc;
    ///
    /// let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(1).build()?);
    /// let domain = Domain::external(pool, vec![0], 1)?;
    /// let adapter = TenferroDomain::new(domain)?;
    /// let modes = map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
    ///     adapter
    ///         .with_backend(|backend| backend.execution_info().admission_mode())
    ///         .map_err(|error| error.to_string())
    /// })?;
    /// assert_eq!(modes, vec![tenferro_cpu::CpuAdmissionMode::CallerManaged]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TenferroAdapterError::OutsideDomainPool`] when called from any
    /// thread outside the bound pool, or [`TenferroAdapterError::ConcurrentEntry`]
    /// for simultaneous or recursive adapter entry. Neither case invokes
    /// `operation`.
    pub fn with_backend<R>(
        &self,
        operation: impl FnOnce(&mut CpuBackend) -> R,
    ) -> Result<R, TenferroAdapterError> {
        if self.pool.current_thread_index().is_none() {
            return Err(TenferroAdapterError::OutsideDomainPool);
        }
        if self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err(TenferroAdapterError::ConcurrentEntry);
        }
        let _entry = EntryGuard(&self.active);
        let mut backend = self.backend.clone();
        Ok(operation(&mut backend))
    }
}
