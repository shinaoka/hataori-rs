# Hataori

Hataori is a Rust engine for simple serial, Rayon, MPI, and hybrid data-parallel execution. It is inspired by Distributed.jl's dynamically scheduled `pmap` while retaining Rust's scoped execution and MPI's SPMD model.

## Name

**Hataori** comes from the Japanese word **機織り** (*hataori*), meaning weaving on a loom. The name reflects the engine's job: weave independent strands of work across MPI ranks and Rayon threads into one ordered result.

## Status

Hataori is currently in the design phase.

- [P0 design](docs/design.md)
- [Implementation tracker](https://github.com/shinaoka/hataori-rs/issues/1)
