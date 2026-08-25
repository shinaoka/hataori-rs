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
error type. Every tutorial renders the same Mandelbrot image, one column per
item; this is the serial version:

<!-- snippet-source: examples/serial_mandelbrot.rs#serial-map -->
```rust
/// Render every column in input order on the calling thread.
///
/// `map` takes a `Vec<T>` and a fallible callback and returns a `Vec<U>` in
/// the same order, or the first error together with the failing index. The
/// callback borrows `param`, `x`, and `y` from the caller's stack.
fn render(param: &Param) -> Result<Vec<Vec<i64>>, MapError> {
    let (x, y) = param.make_axes();
    let columns: Vec<usize> = (0..param.width).collect();
    map(columns, |col_idx| {
        Ok::<_, String>(mandelbrot_common::compute_column(param, x[col_idx], &y))
    })
}
```
<!-- end-snippet-source -->

Every other model keeps the same shape — a `Vec<T>` in, a
`Result<Vec<U>, _>` out, in input order — and adds only the coordination that
the model needs: a `Domain` for Rayon, an MPI communicator plus `PmapOptions`
for MPI and hybrid runs.

All tutorial snippets are quoted from `examples/` and are compiled and run by
`scripts/check-tutorial-examples.sh`; see
[Tutorials](tutorials/index.md#running-the-tutorial-code).
