# Tutorials

Each tutorial shows one execution model end to end. Read them in order: every
later model keeps the calling convention of the previous one and adds only
the coordination that the new model needs.

| Tutorial | Model | Features | Run with |
| --- | --- | --- | --- |
| [1. Serial map](serial-map.md) | one thread | none | `cargo run` |
| [2. Rayon map_in](rayon-map-in.md) | rank-local thread pool, three `LocalMode`s | `rayon` | `cargo run` |
| [3. MPI pmap](mpi-pmap.md) | dynamic scheduling across ranks | `mpi` | `mpiexec` |
| [4. MPI placement collectives](mpi-placement.md) | `broadcast`, `scatter`, `gather` | `mpi` | `mpiexec` |
| [5. Hybrid MPI + Rayon pmap](hybrid-pmap.md) | a Rayon pool on every rank, optional prefetch | `mpi,rayon` | `mpiexec` |
| [6. Runtime-loaded MPI](rsmpi-rt.md) | tutorials 3–5 without linking MPI | `rsmpi-rt` | `MPI_RT_LIB=… mpiexec` |

The larger benchmark-style programs under
[`examples/`](https://github.com/shinaoka/hataori-rs/tree/main/examples) —
`rayon_mandelbrot`, `mpi_pi_pmap`, `mpi_mandelbrot_pmap`,
`mpi_mandelbrot_hybrid`, and their raw-MPI counterparts — apply the same
patterns to real workloads and compare them against hand-written MPI.

## Running the tutorial code

The code on these pages is quoted verbatim from
[`docs/tutorial-code`](https://github.com/shinaoka/hataori-rs/tree/main/docs/tutorial-code),
a non-published workspace member, and is re-synchronized by
`scripts/check-doc-snippets.py`. Every binary asserts its own results, so a
successful exit is the test.

```bash
# single-process tutorials
cargo test -p hataori-tutorial-code --features rayon

# all tutorials, with the MPI binaries launched through `mpiexec -n 2`
docs/tutorial-code/scripts/check.sh

# additionally exercise the rsmpi-rt backend
docs/tutorial-code/scripts/check.sh /absolute/path/to/libmpiwrapper.so
```

Set `HATAORI_MPIEXEC` to use a different launcher (for example `srun`) and
`HATAORI_MPI_RANKS` to change the rank count used by the tests.
