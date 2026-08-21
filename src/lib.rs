//! Hataori: synchronous serial, Rayon, and MPI data-parallel execution.

mod domain;
#[cfg(feature = "rayon")]
mod local;
mod map;
#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), not(feature = "rayon")))]
mod pmap;
#[cfg(any(feature = "mpi", feature = "rsmpi-rt"))]
mod scheduler;
#[cfg(any(feature = "mpi", feature = "rsmpi-rt"))]
mod wire;

pub use domain::{
    Domain, DomainAdmission, DomainBusy, DomainId, LocalMode, NegativeRank, Place,
    UnsupportedDomainId,
};
#[cfg(feature = "rayon")]
pub use domain::{DomainBuildError, PlacementStatus, PoolOwnership};
#[cfg(feature = "rayon")]
pub use local::{map_in, MapInError};
pub use map::{map, MapError};
#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), not(feature = "rayon")))]
pub use pmap::{pmap, PmapError, PmapErrorKind, PmapOptions};

#[cfg(all(feature = "mpi", feature = "rsmpi-rt"))]
compile_error!("hataori: features `mpi` and `rsmpi-rt` are mutually exclusive");

#[cfg(all(feature = "mpi", not(feature = "rsmpi-rt")))]
#[allow(unused_imports)]
use mpi_upstream as mpi_backend;

#[cfg(all(feature = "rsmpi-rt", not(feature = "mpi")))]
#[allow(unused_imports)]
use mpi_runtime as mpi_backend;
