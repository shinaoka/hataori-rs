# 3. MPI pmap

**Model:** dynamic scheduling across MPI ranks, one sequential worker per
rank. **Features:** `mpi` (or `rsmpi-rt`), without `rayon`.

`hataori::pmap` is a collective. Every rank calls it in the same order with
the same communicator and `PmapOptions`; the root rank supplies the items
and receives the ordered results, and every other rank supplies `None` and
receives `None`. Internally the root hands out batches to idle ranks —
including itself — until the input is exhausted, so uneven work balances
automatically.

## Serializable task types

Items and results travel between processes, so they need serde. The
callback itself does not: it runs on whichever rank received the batch and
may borrow local state.

<!-- snippet-source: docs/tutorial-code/src/bin/mpi_pmap.rs#mpi-pmap-types -->
```rust
/// Task inputs and outputs cross rank boundaries, so they must be serde
/// serializable. Plain numbers work too; a struct keeps the example clear.
#[derive(Debug, Serialize, Deserialize)]
struct Task {
    id: u32,
    cost: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Outcome {
    id: u32,
    value: u64,
}

fn evaluate(task: Task) -> Result<Outcome, String> {
    // Simulate uneven work so that dynamic scheduling matters.
    std::thread::sleep(std::time::Duration::from_millis(task.cost));
    Ok(Outcome {
        id: task.id,
        value: task.cost * 2,
    })
}
```
<!-- end-snippet-source -->

## The collective call

<!-- snippet-source: docs/tutorial-code/src/bin/mpi_pmap.rs#mpi-pmap-call -->
```rust
/// One collective `pmap` call. Every rank enters this function; only the
/// root supplies the items and only the root receives the results.
fn run_pmap<C: Communicator>(world: &C, root: i32) -> Option<Vec<Outcome>> {
    let rank = world.rank();
    let items = (rank == root).then(|| {
        (0..24_u32)
            .map(|id| Task {
                id,
                cost: u64::from((id * 7) % 5),
            })
            .collect::<Vec<_>>()
    });

    // The MPI-only entry point always executes callbacks sequentially in
    // a one-worker domain; `batch_size` controls how many items travel in
    // one message.
    let options = PmapOptions {
        root,
        batch_size: NonZeroUsize::new(4).unwrap(),
        local_mode: LocalMode::Sequential,
        prefetch: false,
    };

    pmap(world, &Domain::sequential(), options, items, evaluate).expect("pmap must succeed")
}
```
<!-- end-snippet-source -->

- `Domain::sequential()` is the only domain the MPI-only entry accepts: one
  worker, no pool.
- `batch_size` is the number of items per message. Larger batches amortize
  the per-batch handshake; smaller batches balance better. The `mpi_pi_pmap`
  example documents a measured choice.
- `root` may be any rank in `[0, world.size())`; the binary also runs the
  same call with the last rank as root.
- `local_mode` must be `Sequential` and `prefetch` must be `false` here;
  other values fail collective preflight before any callback runs.

## Errors converge on every rank

<!-- snippet-source: docs/tutorial-code/src/bin/mpi_pmap.rs#mpi-pmap-errors -->
```rust
/// A callback error on any rank converges to one `PmapError` on every
/// rank, so all ranks can agree on the failure and keep using MPI.
fn run_failing_pmap<C: Communicator>(world: &C) {
    let rank = world.rank();
    let items = (rank == 0).then(|| (0..8_u32).collect::<Vec<_>>());
    let error = pmap(
        world,
        &Domain::sequential(),
        PmapOptions::default(),
        items,
        |item| {
            if item == 5 {
                Err(format!("item {item} is not allowed"))
            } else {
                Ok(item + 1)
            }
        },
    )
    .expect_err("item 5 fails on whichever rank evaluates it");
    assert_eq!(error.kind(), PmapErrorKind::User);
    assert!(error.message().contains("item 5 is not allowed"));
}
```
<!-- end-snippet-source -->

A user error on one rank becomes the same `PmapError` on all ranks —
`kind()` is `PmapErrorKind::User`, `message()` carries the callback's
`Display` output — so the program can agree on what happened and keep using
MPI afterwards. Panics inside a callback abort the whole MPI job instead.

Source: [`docs/tutorial-code/src/bin/mpi_pmap.rs`](https://github.com/shinaoka/hataori-rs/blob/main/docs/tutorial-code/src/bin/mpi_pmap.rs)

## Build and run

```bash
cargo build -p hataori-tutorial-code --features mpi --bin mpi_pmap
mpiexec -n 4 target/debug/mpi_pmap
```

::: {.callout-note}
Build this tutorial **without** the `rayon` feature. When `rayon` is enabled
`pmap` becomes the [hybrid entry point](hybrid-pmap.md) and requires a
domain with a pool; the binary prints a skip message in that configuration.
:::

Real workloads: [`examples/mpi_pi_pmap.rs`](https://github.com/shinaoka/hataori-rs/blob/main/examples/mpi_pi_pmap.rs)
and [`examples/mpi_mandelbrot_pmap.rs`](https://github.com/shinaoka/hataori-rs/blob/main/examples/mpi_mandelbrot_pmap.rs),
each paired with a hand-written `*_raw.rs` MPI version for comparison.

Next: [4. MPI placement collectives](mpi-placement.md).
