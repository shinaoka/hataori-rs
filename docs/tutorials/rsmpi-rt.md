# 6. Runtime-loaded MPI (rsmpi-rt)

**Model:** the same as tutorials 3–5. **Features:** `rsmpi-rt` instead of
`mpi`.

The `rsmpi-rt` feature swaps the upstream rsmpi crate for
[tensor4all/rsmpi-rt](https://github.com/tensor4all/rsmpi-rt), which loads
an MPIABI-compatible shared library at process start instead of linking MPI
at build time. Nothing in the Hataori API changes; the tutorial binaries only
differ in the crate alias they import, which the tutorial code selects with a
`cfg`:

<!-- snippet-source: docs/tutorial-code/src/support/mpi_backend.rs -->
```rust
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
```
<!-- end-snippet-source -->

## Why use it

- The build machine needs no MPI headers, `mpicc`, or libclang.
- The same binary can run against the MPI runtime already used by MPI.jl or
  mpi4py in a mixed-language job.
- `mpi` and `rsmpi-rt` are mutually exclusive, so a crate never links two MPI
  bindings by accident.

## Build the MPIABI library

Any MPIABI implementation works; the project CI builds
[MPIwrapper](https://github.com/eschnett/MPIwrapper) over the system Open MPI:

```bash
git clone https://github.com/eschnett/MPIwrapper.git
cmake -S MPIwrapper -B MPIwrapper/build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PWD/mpiwrapper-install"
cmake --build MPIwrapper/build --parallel
cmake --install MPIwrapper/build
# the library is $PWD/mpiwrapper-install/lib/libmpiwrapper.so (.dylib on macOS)
```

## Build and run the tutorials

`MPI_RT_LIB` must be an absolute path and must be set before MPI is
initialized:

```bash
cargo build -p hataori-tutorial-code --features rsmpi-rt --bin mpi_pmap
MPI_RT_LIB=/abs/path/mpiwrapper-install/lib/libmpiwrapper.so \
  mpiexec -n 4 target/debug/mpi_pmap

cargo build -p hataori-tutorial-code --features rsmpi-rt,rayon --bin hybrid_pmap
MPI_RT_LIB=/abs/path/mpiwrapper-install/lib/libmpiwrapper.so \
  mpiexec -n 2 target/debug/hybrid_pmap
```

The whole matrix, including these lanes, runs with:

```bash
docs/tutorial-code/scripts/check.sh /abs/path/mpiwrapper-install/lib/libmpiwrapper.so
```
