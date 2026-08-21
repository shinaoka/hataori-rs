//! Hataori: synchronous serial, Rayon, and MPI data-parallel execution.

mod domain;
mod map;

pub use domain::{
    Domain, DomainAdmission, DomainBusy, DomainId, NegativeRank, Place, UnsupportedDomainId,
};
pub use map::{map, MapError};

#[cfg(all(feature = "mpi", feature = "rsmpi-rt"))]
compile_error!("hataori: features `mpi` and `rsmpi-rt` are mutually exclusive");

#[cfg(all(feature = "mpi", not(feature = "rsmpi-rt")))]
#[allow(unused_imports)]
use mpi_upstream as mpi_backend;

#[cfg(all(feature = "rsmpi-rt", not(feature = "mpi")))]
#[allow(unused_imports)]
use mpi_runtime as mpi_backend;
