# Hataori

Hataori is a Rust engine for simple serial, Rayon, MPI, and hybrid data-parallel execution. It is inspired by Distributed.jl's dynamically scheduled `pmap` while retaining Rust's scoped execution and MPI's SPMD model.

## Name

**Hataori** comes from the Japanese word **機織り** (*hataori*), meaning weaving on a loom. The name reflects the engine's job: weave independent strands of work across MPI ranks and Rayon threads into one ordered result.

## Planned features

Hataori has no default dependencies. Optional execution backends are selected explicitly:

| Feature | Backend |
|---|---|
| `rayon` | rank-local thread parallelism |
| `mpi` | upstream rsmpi with a build/link-time MPI implementation |
| `rsmpi-rt` | [rsmpi-rt](https://github.com/tensor4all/rsmpi-rt) with MPIABI runtime loading |

`mpi` and `rsmpi-rt` expose the same Hataori API and are mutually exclusive. The `rsmpi-rt` feature supports build environments without MPI headers, a C compiler, or libclang and can share the MPI runtime used by MPI.jl or mpi4py.

## Status

Hataori is currently in the design phase.

- [P0 design](docs/design.md)
- [Implementation tracker](https://github.com/shinaoka/hataori-rs/issues/1)
