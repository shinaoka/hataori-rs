# Getting Started

## Add the dependency

Hataori has no default features. Enable only the backends you use:

```toml
[dependencies]
# serial only
hataori = { git = "https://github.com/shinaoka/hataori-rs" }

# rank-local threads
hataori = { git = "https://github.com/shinaoka/hataori-rs", features = ["rayon"] }

# MPI, linked at build time against the system MPI
hataori = { git = "https://github.com/shinaoka/hataori-rs", features = ["mpi"] }

# MPI + Rayon hybrid
hataori = { git = "https://github.com/shinaoka/hataori-rs", features = ["mpi", "rayon"] }

# MPI loaded at runtime through MPIABI (no MPI headers or C compiler needed)
hataori = { git = "https://github.com/shinaoka/hataori-rs", features = ["rsmpi-rt"] }
```

`mpi` and `rsmpi-rt` expose the same API and are mutually exclusive; enabling
both is a compile error. The minimum supported Rust version is 1.85.

## Prerequisites per feature

| Feature | Needs |
| --- | --- |
| none | a Rust toolchain |
| `rayon` | nothing else; on Linux, CPU pinning is verified at pool start |
| `mpi` | an MPI implementation with `mpicc` and `mpiexec` on `PATH` (for example `brew install open-mpi` on macOS, `apt-get install libopenmpi-dev openmpi-bin` on Debian/Ubuntu) |
| `rsmpi-rt` | an MPIABI-compatible shared library such as [MPIwrapper](https://github.com/eschnett/MPIwrapper), passed through `MPI_RT_LIB` at run time |

Hybrid runs additionally need the MPI implementation to provide
`MPI_THREAD_FUNNELED` or stronger; Open MPI and MPICH do by default.

## Pick an execution model

Read [Parallel Execution Models](../guides/parallel-models.md) for the
decision rules. The short version:

- one process, one thread → [`map`](../tutorials/serial-map.md)
- one process, many threads → [`map_in`](../tutorials/rayon-map-in.md)
- many processes, one thread each → [`pmap` without `rayon`](../tutorials/mpi-pmap.md)
- many processes, many threads each → [`pmap` with `rayon`](../tutorials/hybrid-pmap.md)

## Run the tutorial code

The tutorial pages quote `docs/tutorial-code`, a non-published workspace
member. From the repository root:

```bash
# serial and Rayon tutorials
cargo test -p hataori-tutorial-code --features rayon

# every tutorial, launching the MPI binaries with `mpiexec -n 2`
docs/tutorial-code/scripts/check.sh
```

Each MPI tutorial can also be built and launched by hand; the tutorial pages
show the exact commands.
