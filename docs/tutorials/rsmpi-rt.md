# 6. Runtime-loaded MPI (rsmpi-rt)

**Model:** the same as tutorials 3–5. **Features:** `rsmpi-rt` instead of
`mpi`. **Examples:** `rsmpi_rt_mandelbrot_pmap`,
`rsmpi_rt_mandelbrot_placement`, `rsmpi_rt_mandelbrot_hybrid`.

The `rsmpi-rt` feature swaps the upstream rsmpi crate for
[tensor4all/rsmpi-rt](https://github.com/tensor4all/rsmpi-rt), which loads
an MPIABI-compatible shared library at process start instead of linking MPI
at build time. Nothing in the Hataori API changes; the Mandelbrot examples
only differ in the crate alias they import, which the shared support module
selects with a `cfg`:

<!-- snippet-source: examples/support/mandelbrot_common.rs#backend -->
```rust
// The MPI crate alias shared by the MPI examples.
//
// The `mpi` feature links the upstream rsmpi crate at build time; the
// `rsmpi-rt` feature loads an MPIABI-compatible runtime at process start.
// Both expose the same rsmpi API, so the examples only need one alias and
// the same source builds against either backend.
#[cfg(all(feature = "rsmpi-rt", not(feature = "mpi")))]
#[allow(unused_imports)]
pub use mpi_runtime as mpi_api;
#[cfg(all(feature = "mpi", not(feature = "rsmpi-rt")))]
#[allow(unused_imports)]
pub use mpi_upstream as mpi_api;

/// Names the MPI backend the example was compiled with.
pub fn describe_backend() -> &'static str {
    if cfg!(feature = "rsmpi-rt") {
        "rsmpi-rt (runtime-loaded MPI)"
    } else {
        "mpi (link-time MPI)"
    }
}
```
<!-- end-snippet-source -->

Cargo's `required-features` cannot express "`mpi` *or* `rsmpi-rt`", so each
MPI example is registered twice: `mpi_mandelbrot_pmap` requires `mpi`, and
`rsmpi_rt_mandelbrot_pmap` — a four-line wrapper that includes the same
source file with `#[path]` — requires `rsmpi-rt`.

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
# the library is $PWD/mpiwrapper-install/lib/libmpiwrapper.so (also on macOS)
```

::: {.callout-note}
## macOS with Homebrew Open MPI

`mpifort --showme:link` on Homebrew Open MPI emits `-Wl,-flat_namespace`,
which CMake's `FindMPI` copies into `MPI_Fortran_LINK_FLAGS`. MPIwrapper's
own post-link check then fails with "does not use a two-level namespace"
(a flat-namespace plugin would resolve `MPI_*` to itself and recurse).
Override the flag when configuring:

```bash
brew install open-mpi cmake gcc   # gcc provides gfortran
cmake -S MPIwrapper -B MPIwrapper/build -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PWD/mpiwrapper-install" \
  -DMPI_Fortran_LINK_FLAGS="-Wl,-twolevel_namespace"
```

Passing an empty value does not work — `FindMPI` re-detects the flag — so
the override must be non-empty. With this, every `rsmpi-rt` lane of
`scripts/check-tutorial-examples.sh` passes on Apple Silicon.
:::

## Build and run the tutorials

`MPI_RT_LIB` is passed to the dynamic loader verbatim and must be set before
MPI is initialized; use an absolute path so that it does not depend on the
working directory `mpiexec` starts each rank in:

```bash
cargo build --release --no-default-features --features rsmpi-rt,tenferro \
  --example rsmpi_rt_mandelbrot_pmap
MPI_RT_LIB=/abs/path/mpiwrapper-install/lib/libmpiwrapper.so \
  mpiexec -n 4 target/release/examples/rsmpi_rt_mandelbrot_pmap --width 1024 --height 1024

cargo build --release --no-default-features --features rsmpi-rt,rayon,tenferro \
  --example rsmpi_rt_mandelbrot_hybrid
MPI_RT_LIB=/abs/path/mpiwrapper-install/lib/libmpiwrapper.so \
  mpiexec -n 2 target/release/examples/rsmpi_rt_mandelbrot_hybrid --workers 2 --width 1024 --height 1024
```

Each example prints `Backend: rsmpi-rt (runtime-loaded MPI)` on the root.
The whole matrix, including these lanes, runs with:

```bash
scripts/check-tutorial-examples.sh /abs/path/mpiwrapper-install/lib/libmpiwrapper.so
```
