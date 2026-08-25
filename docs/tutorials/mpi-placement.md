# 4. MPI placement collectives

**Model:** moving owned values between ranks. **Features:** `mpi` (or
`rsmpi-rt`).

Besides `pmap`, the MPI backends provide three collectives for serde values.
They complement `pmap`: use `broadcast` for parameters every rank needs,
`scatter` to give each rank its own shard, and `gather` to collect one value
per rank on the root.

| Collective | Root passes | Every rank receives |
| --- | --- | --- |
| `broadcast(world, root, Option<T>)` | `Some(value)` | an owned copy of `value` |
| `scatter(world, root, Option<Vec<T>>)` | `Some(shards)` with exactly one shard per rank | its rank-indexed shard |
| `gather(world, root, T)` | (every rank passes one value) | root: `Some(Vec<T>)` in rank order; others: `None` |

## Code

<!-- snippet-source: docs/tutorial-code/src/bin/mpi_placement.rs#placement -->
```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Config {
    tolerance: f64,
    label: String,
}

fn placement_round_trip<C: Communicator>(world: &C) {
    let rank = world.rank();
    let size = world.size();

    // broadcast: the root supplies `Some`, every rank receives an owned copy.
    let config = broadcast(
        world,
        0,
        (rank == 0).then(|| Config {
            tolerance: 1e-9,
            label: "shared".to_owned(),
        }),
    )
    .expect("broadcast must succeed");
    assert_eq!(config.label, "shared");

    // scatter: the root supplies exactly one shard per rank; each rank
    // receives the shard at its own index.
    let shard: Vec<u32> = scatter(
        world,
        0,
        (rank == 0).then(|| {
            (0..size)
                .map(|target| (0..4).map(|i| target as u32 * 10 + i).collect())
                .collect()
        }),
    )
    .expect("scatter must succeed");
    assert_eq!(
        shard,
        (0..4).map(|i| rank as u32 * 10 + i).collect::<Vec<_>>()
    );

    // gather: every rank supplies one value; the root receives them in rank order.
    let local_sum: u32 = shard.iter().sum();
    let sums = gather(world, 0, local_sum).expect("gather must succeed");
    if rank == 0 {
        let sums = sums.expect("root receives the gathered values");
        assert_eq!(sums.len(), size as usize);
        for (source, sum) in sums.iter().enumerate() {
            assert_eq!(*sum, (0..4).map(|i| source as u32 * 10 + i).sum::<u32>());
        }
        println!(
            "mpi_placement: backend {}, gathered {sums:?}",
            describe_backend()
        );
    } else {
        assert!(sums.is_none());
    }
}
```
<!-- end-snippet-source -->

Errors are `PlacementError` values with a `PlacementErrorKind`; like `pmap`,
a failure (for example a serialization error on one rank) converges to the
same error on every rank.

Source: [`docs/tutorial-code/src/bin/mpi_placement.rs`](https://github.com/shinaoka/hataori-rs/blob/main/docs/tutorial-code/src/bin/mpi_placement.rs)

## Build and run

```bash
cargo build -p hataori-tutorial-code --features mpi --bin mpi_placement
mpiexec -n 4 target/debug/mpi_placement
```

Next: [5. Hybrid MPI + Rayon pmap](hybrid-pmap.md).
