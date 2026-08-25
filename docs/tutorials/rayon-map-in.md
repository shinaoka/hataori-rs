# 2. Rayon map_in

**Model:** rank-local thread parallelism inside one process.
**Features:** `rayon`.

`hataori::map_in` maps over items inside an explicit Rayon `Domain`. Compared
with calling `par_iter` directly you get: an explicit, verifiable CPU
placement; a single admission slot per domain; and the three
[`LocalMode`](../guides/parallel-models.md#local-modes)s with their
well-defined error rules.

## Build a domain

A managed domain creates and owns its pool. The CPU set must be non-empty
and at least as large as the worker count; on Linux the workers are pinned
and verified at startup.

<!-- snippet-source: docs/tutorial-code/src/bin/rayon_map_in.rs#managed-domain -->
```rust
/// Builds a Hataori-owned Rayon pool with `workers` threads.
///
/// The CPU set must be non-empty and must not be smaller than the worker
/// count. On Linux every worker is pinned to its CPU and the placement is
/// verified at startup; on other platforms the set is only a declaration.
fn managed_domain(workers: usize) -> Domain {
    let cpu_set: Vec<usize> = (0..workers).collect();
    let domain = Domain::managed(cpu_set, workers).expect("managed Rayon domain");
    assert_eq!(domain.worker_count(), workers);
    assert_eq!(domain.pool_ownership(), Some(PoolOwnership::Managed));
    domain
}
```
<!-- end-snippet-source -->

If the application already has a `rayon::ThreadPool`, wrap it instead. Hataori
records the CPU set as caller-declared and never re-pins or shuts down the
pool.

<!-- snippet-source: docs/tutorial-code/src/bin/rayon_map_in.rs#external-domain -->
```rust
/// Wraps a pool the application already owns.
///
/// Hataori neither re-pins the workers nor shuts the pool down when the
/// domain is dropped; the declared CPU set is recorded as `CallerDeclared`.
fn external_domain(pool: Arc<rayon::ThreadPool>) -> Domain {
    let workers = pool.current_num_threads();
    let cpu_set: Vec<usize> = (0..workers).collect();
    let domain = Domain::external(pool, cpu_set, workers).expect("external Rayon domain");
    assert_eq!(domain.pool_ownership(), Some(PoolOwnership::External));
    assert_eq!(
        domain.placement_status(),
        Some(PlacementStatus::CallerDeclared)
    );
    domain
}
```
<!-- end-snippet-source -->

## The three local modes

<!-- snippet-source: docs/tutorial-code/src/bin/rayon_map_in.rs#local-modes -->
```rust
fn slow_square(item: u64) -> Result<u64, String> {
    // Reverse the cost so that out-of-order completion is likely under `Outer`.
    std::thread::sleep(std::time::Duration::from_millis(8 - item));
    Ok(item * item)
}

fn demonstrate_local_modes(domain: &Domain) {
    let items: Vec<u64> = (0..8).collect();
    let expected: Vec<u64> = items.iter().map(|item| item * item).collect();

    // Sequential: one callback at a time inside the pool, stop at first error.
    let sequential = map_in(domain, LocalMode::Sequential, items.clone(), slow_square).unwrap();
    assert_eq!(sequential, expected);

    // Outer: every item runs in parallel across the pool's workers; the
    // result vector still follows the input order.
    let outer = map_in(domain, LocalMode::Outer, items.clone(), slow_square).unwrap();
    assert_eq!(outer, expected);

    // Inner: callbacks run one at a time, but each callback may use nested
    // Rayon parallelism in the same pool (for example a `par_iter`).
    let inner = map_in(domain, LocalMode::Inner, items, |item| {
        let partial: u64 = (0..item).into_par_iter().map(|_| item).sum();
        Ok::<_, String>(partial)
    })
    .unwrap();
    assert_eq!(inner, expected);
}
```
<!-- end-snippet-source -->

- `Sequential` runs one callback at a time on a pool thread.
- `Outer` runs every callback in parallel and still returns results in input
  order.
- `Inner` runs one callback at a time but leaves the pool free for nested
  Rayon work inside the callback — here a `par_iter` sum.

## Error rules differ by mode

<!-- snippet-source: docs/tutorial-code/src/bin/rayon_map_in.rs#outer-errors -->
```rust
fn demonstrate_outer_errors(domain: &Domain) {
    let failing = |item: u64| {
        if item % 3 == 2 {
            Err(format!("item {item} rejected"))
        } else {
            Ok(item)
        }
    };

    // Sequential stops at the first failing input.
    let error = map_in(domain, LocalMode::Sequential, (0..9).collect(), failing).unwrap_err();
    match error {
        MapInError::Callback(inner) => assert_eq!(inner.index(), 2),
        other => panic!("unexpected error: {other:?}"),
    }

    // Outer evaluates every input exactly once, then reports the lowest
    // failing index even if a later failure finished first.
    let error = map_in(domain, LocalMode::Outer, (0..9).collect(), failing).unwrap_err();
    match error {
        MapInError::Callback(inner) => {
            assert_eq!(inner.index(), 2);
            assert_eq!(inner.message(), "item 2 rejected");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
```
<!-- end-snippet-source -->

`Sequential` and `Inner` stop at the first failing input. `Outer` evaluates
every input exactly once and then reports the **lowest** failing index, so
the result is deterministic regardless of which callback finished first.

## Preconditions

`map_in` checks, before calling the callback, that the domain has a pool
(`MapInError::MissingPool`), that the call does not originate on another
Rayon pool (`MapInError::ForeignPool`), and that no other operation is
admitted on the domain (`MapInError::DomainBusy`). The full binary
demonstrates the `ForeignPool` case by calling `map_in` from inside
`pool.install`.

Source: [`docs/tutorial-code/src/bin/rayon_map_in.rs`](https://github.com/shinaoka/hataori-rs/blob/main/docs/tutorial-code/src/bin/rayon_map_in.rs)

## Run

```bash
cargo run -p hataori-tutorial-code --features rayon --bin rayon_map_in
```

For a real workload see
[`examples/rayon_mandelbrot.rs`](https://github.com/shinaoka/hataori-rs/blob/main/examples/rayon_mandelbrot.rs),
which renders a 4096-column Mandelbrot set with `LocalMode::Outer`.

Next: [3. MPI pmap](mpi-pmap.md) distributes the same kind of loop across
processes.
