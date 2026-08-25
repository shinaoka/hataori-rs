# API Reference

The rustdoc for the `hataori` crate is published alongside this site:

- [`hataori` crate documentation](hataori/index.html)

It is built with `--features mpi,rayon`, so the MPI, hybrid, and Rayon items
are all visible; feature-gated items are annotated with the feature that
enables them. The `rsmpi-rt` backend exposes the same items as `mpi`.

## Public surface by model

| Model | Items |
| --- | --- |
| Serial | `map`, `MapError` |
| Rayon | `map_in`, `MapInError`, `Domain::managed`, `Domain::external`, `LocalMode`, `PoolOwnership`, `PlacementStatus`, `DomainBuildError` |
| MPI and hybrid | `pmap`, `PmapOptions`, `PmapError`, `PmapErrorKind`, `Domain::sequential` |
| Placement | `broadcast`, `scatter`, `gather`, `PlacementError`, `PlacementErrorKind` |
| Common | `Domain`, `DomainId`, `Place`, `DomainAdmission`, `DomainBusy` |

Build it locally with:

```bash
cargo doc --no-deps --no-default-features --features mpi,rayon --open
```
