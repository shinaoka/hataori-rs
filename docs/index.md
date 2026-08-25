# Hataori

Hataori (機織り, "weaving on a loom") is a Rust engine for simple serial,
Rayon, MPI, and hybrid data-parallel execution. It is inspired by
Distributed.jl's dynamically scheduled `pmap` while keeping Rust's scoped
execution and MPI's SPMD model: every entry point is a synchronous function
call, results always come back in input order, and the first callback error is
reported with its input index.

The crate has no default dependencies. You choose an execution model by
enabling Cargo features and calling the matching function:

| Model | Feature(s) | Entry point | What runs where |
| --- | --- | --- | --- |
| Serial | none | `hataori::map` | one thread, one process |
| Rayon | `rayon` | `hataori::map_in` | a rank-local thread pool (`Domain`) |
| MPI | `mpi` or `rsmpi-rt` | `hataori::pmap` | one sequential worker per MPI rank |
| Hybrid | `mpi`/`rsmpi-rt` + `rayon` | `hataori::pmap` | a Rayon pool on every MPI rank |

The MPI backends also provide `broadcast`, `scatter`, and `gather` placement
collectives for owned values.

## Where to start

| Workflow | Start with |
| --- | --- |
| Install the crate and pick features | [Getting Started](getting-started/index.md) |
| Understand the four execution models and `LocalMode` | [Parallel Execution Models](guides/parallel-models.md) |
| See every model as runnable code | [Tutorials](tutorials/index.md) |
| Link-time vs runtime-loaded MPI | [Choosing Features and Backends](guides/features-and-backends.md) |
| Function-level documentation | [API Reference](api/index.md) |
| Why the engine is shaped this way | [P0 design](design.md), [P1 bounded prefetch](design/bounded-prefetch.md) |

## Thirty-second example

Serial execution needs no features and no trait bounds beyond `Display` on the
error type:

<!-- snippet-source: docs/tutorial-code/src/bin/serial_map.rs#serial-map -->
```rust
use hataori::{map, MapError};

/// A callback error type: anything that implements `Display` works.
#[derive(Debug)]
struct NegativeInput(i64);

impl std::fmt::Display for NegativeInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "negative input: {}", self.0)
    }
}

fn checked_square(item: i64) -> Result<i64, NegativeInput> {
    if item < 0 {
        return Err(NegativeInput(item));
    }
    Ok(item * item)
}

fn main() {
    // `map` applies the callback one item at a time, in order, on the calling
    // thread. Nothing here needs `Send`, `Sync`, `'static`, or serde.
    let squares = map(vec![1_i64, 2, 3, 4], checked_square).expect("all inputs are non-negative");
    assert_eq!(squares, vec![1, 4, 9, 16]);

    // The callback may borrow local state; a counter shows the exactly-once
    // evaluation and the stop-at-first-error rule.
    let mut calls = 0_usize;
    let error: MapError = map(vec![5_i64, -1, 7], |item| {
        calls += 1;
        checked_square(item)
    })
    .expect_err("the second item fails");
    assert_eq!(error.index(), 1);
    assert_eq!(error.message(), "negative input: -1");
    // Evaluation stopped at the failing item: `7` was never visited.
    assert_eq!(calls, 2);

    println!(
        "serial_map: {squares:?}; first error at index {}",
        error.index()
    );
}
```
<!-- end-snippet-source -->

Every other model keeps the same shape — a `Vec<T>` in, a
`Result<Vec<U>, _>` out, in input order — and adds only the coordination that
the model needs: a `Domain` for Rayon, an MPI communicator plus `PmapOptions`
for MPI and hybrid runs.

All tutorial snippets are compiled and executed by
`cargo test -p hataori-tutorial-code`; see
[Tutorials](tutorials/index.md#running-the-tutorial-code).
