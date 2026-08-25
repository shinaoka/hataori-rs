# Hataori Tutorial Code

Runnable source for the online tutorials under `docs/tutorials/`. The
tutorial pages quote regions of these binaries verbatim
(`// snippet-start:NAME` … `// snippet-end:NAME`), and
`scripts/check-doc-snippets.py --check` fails when a page drifts from its
source.

| Binary | Model | Features |
| --- | --- | --- |
| `serial_map` | `hataori::map` | none |
| `rayon_map_in` | `hataori::map_in`, all `LocalMode`s, managed and external domains | `rayon` |
| `mpi_pmap` | MPI-only `hataori::pmap` | `mpi` or `rsmpi-rt` (without `rayon`) |
| `mpi_placement` | `broadcast`, `scatter`, `gather` | `mpi` or `rsmpi-rt` |
| `hybrid_pmap` | hybrid `hataori::pmap` with and without prefetch | `mpi`/`rsmpi-rt` + `rayon` |

Binaries compiled without the features they need print
`HATAORI_TUTORIAL_SKIP: …` and exit successfully.

Run everything from the repository root:

```bash
docs/tutorial-code/scripts/check.sh [/abs/path/to/libmpiwrapper.so]
```

`tests/tutorial_binaries.rs` launches the MPI binaries through
`$HATAORI_MPIEXEC` (default `mpiexec`) with `$HATAORI_MPI_RANKS` ranks
(default 2).
