//! Backend alias shared by the MPI tutorial binaries.
//!
//! The `mpi` feature links the upstream rsmpi crate; the `rsmpi-rt` feature
//! loads an MPIABI-compatible runtime at process start. Both expose the same
//! rsmpi API, so tutorial code only needs one alias.

#[cfg(all(feature = "mpi", not(feature = "rsmpi-rt")))]
pub use mpi_upstream as mpi_api;

#[cfg(all(feature = "rsmpi-rt", not(feature = "mpi")))]
pub use mpi_runtime as mpi_api;

/// Prints the runtime library used by the `rsmpi-rt` backend, if any.
pub fn describe_backend() -> &'static str {
    if cfg!(feature = "rsmpi-rt") {
        "rsmpi-rt (runtime-loaded MPI)"
    } else {
        "mpi (link-time MPI)"
    }
}
