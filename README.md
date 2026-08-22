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

The non-publishable `hataori-tenferro` workspace adapter binds an admitted
whole-domain `Inner` callback to tenferro's caller-managed Faer backend without
adding tenferro to ordinary Hataori builds. See its
[design and usage contract](docs/design/tenferro-adapter.md).

## Status

Hataori's P0 core and tenferro-only adapter foundation are implemented. The
tensor4all explicit-context and tensor-reconstruction layer remains blocked on
[tensor4all-rs#663](https://github.com/tensor4all/tensor4all-rs/issues/663).

- [P0 design](docs/design.md)
- [Implementation readiness and validation matrix](docs/implementation-readiness.md)
- [Review gate log](docs/review-log.md)
- [Implementation tracker](https://github.com/shinaoka/hataori-rs/issues/1)
