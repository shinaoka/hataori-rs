# 5. Hybrid MPI + Rayon pmap

**Model:** MPI ranks, each with its own Rayon pool. **Features:** `mpi` (or
`rsmpi-rt`) **and** `rayon`.

With both feature groups enabled, `hataori::pmap` is the hybrid entry point.
The collective protocol is the same as in [tutorial 3](mpi-pmap.md): the root
owns the items and dynamically hands out batches. What changes is that each
rank evaluates its batch inside a pooled `Domain` with the requested
`LocalMode`, so in a one-rank-per-node deployment all cores of every node
participate.

## Initialize MPI for funneled threads

All MPI calls stay on the thread that calls `pmap`, while Rayon workers
evaluate callbacks concurrently. That is exactly `MPI_THREAD_FUNNELED`, so
ask for it explicitly and check what the implementation provided.

<!-- snippet-source: docs/tutorial-code/src/bin/hybrid_pmap.rs#hybrid-init -->
```rust
/// Hybrid `pmap` keeps every MPI call on the thread that initialized MPI
/// while Rayon workers evaluate callbacks, so MPI must be initialized with
/// at least `MPI_THREAD_FUNNELED`.
fn initialize_funneled() -> mpi_api::environment::Universe {
    let (universe, provided) = mpi_api::initialize_with_threading(Threading::Funneled)
        .expect("MPI must not already be initialized");
    assert!(
        provided >= Threading::Funneled,
        "hybrid pmap requires MPI_THREAD_FUNNELED or stronger"
    );
    universe
}
```
<!-- end-snippet-source -->

## The collective call with a pooled domain

<!-- snippet-source: docs/tutorial-code/src/bin/hybrid_pmap.rs#hybrid-call -->
```rust
fn heavy(item: u64) -> Result<u64, String> {
    // A callback that is worth spreading across threads and ranks.
    let mut acc = 0_u64;
    for k in 0..(1_000 + item * 100) {
        acc = acc.wrapping_add(k * k % 7);
    }
    Ok(acc)
}

/// Every rank builds a Rayon domain and passes it to the collective. The
/// root hands out batches to itself and to remote ranks; each rank runs
/// its batch through its own pool with the requested `LocalMode`.
fn run_hybrid<C: Communicator>(world: &C, workers: usize, prefetch: bool) -> Option<Vec<u64>> {
    let rank = world.rank();
    let size = world.size();
    let cpu_set: Vec<usize> = (0..workers).collect();
    let domain = Domain::managed(cpu_set, workers).expect("rayon domain");

    let items = (rank == 0).then(|| (0..64_u64).collect::<Vec<_>>());
    // Batches should hold at least `workers` items so that `Outer` can
    // spread one batch over the whole pool.
    let batch_size = NonZeroUsize::new((64 / (size as usize * 4)).max(workers)).unwrap();
    let options = PmapOptions {
        root: 0,
        batch_size,
        local_mode: LocalMode::Outer,
        // When true, each remote rank may hold one extra batch so that the
        // next transfer overlaps the current computation.
        prefetch,
    };

    pmap(world, &domain, options, items, heavy).expect("hybrid pmap must succeed")
}
```
<!-- end-snippet-source -->

- Every rank builds a `Domain::managed` (or `Domain::external`) pool; the
  worker count can differ per rank, but the `PmapOptions` must be identical.
- `LocalMode::Outer` spreads one batch over the pool, so `batch_size`
  should be at least the worker count. `Inner` is the choice when each
  callback is itself a parallel kernel.
- The callback and the values need the Rayon `Send`/`Sync` bounds in
  addition to serde, but still not `'static`: `heavy` could borrow local
  data.
- The call must originate outside any Rayon worker; calling `pmap` from
  inside a pool is rejected in preflight.

## Prefetch

The binary runs the same call with `prefetch: false` and `prefetch: true`
and checks that both give identical, ordered results. With prefetch on, each
remote rank may hold one additional batch, so receiving the next batch and
sending the previous results overlap the current computation. The root's own
domain always stays at capacity one. The design and its bounds are described
in [P1 bounded prefetch](../design/bounded-prefetch.md).

Source: [`docs/tutorial-code/src/bin/hybrid_pmap.rs`](https://github.com/shinaoka/hataori-rs/blob/main/docs/tutorial-code/src/bin/hybrid_pmap.rs)

## Build and run

```bash
cargo build -p hataori-tutorial-code --features mpi,rayon --bin hybrid_pmap
mpiexec -n 2 target/debug/hybrid_pmap
```

On a laptop keep `ranks × workers` at or below the core count; the binary
uses two workers per rank.

Real workload: [`examples/mpi_mandelbrot_hybrid.rs`](https://github.com/shinaoka/hataori-rs/blob/main/examples/mpi_mandelbrot_hybrid.rs)
takes `--workers N` and `--batch-factor N` and renders the same image as the
Rayon-only and MPI-only examples.

Next: [6. Runtime-loaded MPI](rsmpi-rt.md).
