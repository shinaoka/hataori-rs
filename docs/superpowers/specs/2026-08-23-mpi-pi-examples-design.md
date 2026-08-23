# MPI π Examples Design

## Goal

Add Rust examples under `./examples` that are equivalent to the provided Julia MPI
program. The Julia program estimates π by counting coprime integer pairs with a
round-robin MPI distribution, then benchmarks the computation for several values
of `N`.

## Scope

Two new example binaries plus one shared helper module:

- `examples/mpi_pi_raw.rs` — uses the upstream `mpi` crate directly.
- `examples/mpi_pi_pmap.rs` — uses `hataori::pmap` for MPI distribution.
- `examples/support/pi_common.rs` — shared computation helpers.

Both examples target the existing `mpi` feature (`mpi-upstream` backend). A
matching `rsmpi-rt` variant is out of scope for this change.

## Files

### `examples/support/pi_common.rs`

Pure computation helpers with no MPI dependency.

- `gcd(a: u64, b: u64) -> u64` — Euclidean algorithm.
- `count_coprime_pairs(start: u64, step: u64, n: u64) -> u64` — counts coprime
  pairs `(a, b)` where `a` follows the round-robin sequence
  `start, start+step, start+2*step, ...` and `b ∈ 1..=n`.
- `count_for_a(a: u64, n: u64) -> u64` — counts coprime pairs for a single `a`.
- `estimate_pi(total_coprime: u64, n: u64) -> f64` — computes `sqrt(6 / prob)`
  where `prob = total_coprime / n / n`.
- `run_benchmark` / print helpers — rank 0 prints labels, elapsed time, and the
  estimated value, matching the Julia `@info`/`@time`/`@show` behavior.

### `examples/mpi_pi_raw.rs`

1. Initialize MPI with `mpi_upstream::initialize()`.
2. Obtain `rank` and `size` from `world`.
3. Rank 0 prints a start message and the MPI size.
4. Warm-up run with `N = 100`:
   - Every rank computes its local coprime count via
     `count_coprime_pairs(rank + 1, size, N)`.
   - Reduce with `world.all_reduce_into(..., SystemOperation::sum())`.
   - Rank 0 estimates π and prints elapsed time.
5. Benchmark loop over `N ∈ [100, 1000, 10_000, 50_000, 100_000]` with the same
   reduce-and-print pattern.
6. `universe` is dropped at process exit, which finalizes MPI.

### `examples/mpi_pi_pmap.rs`

1. Initialize MPI and obtain `rank`/`size`.
2. For each `N`, rank 0 builds the input vector `(1..=N).collect::<Vec<u64>>()`.
3. Use `hataori::pmap` with `Domain::sequential()` and default `PmapOptions` to
   map each `a` to `count_for_a(a, N)`. The `pmap` call returns the full result
   vector on rank 0 only.
4. Sum the returned vector to get `total_coprime`.
5. Rank 0 estimates π and prints elapsed time.
6. Non-root ranks participate in the `pmap` computation but do not receive the
   output vector.

## `Cargo.toml` Changes

Add two example entries:

```toml
[[example]]
name = "mpi_pi_raw"
required-features = ["mpi"]

[[example]]
name = "mpi_pi_pmap"
required-features = ["mpi"]
```

## Error Handling

- Computation helpers return plain values; they do not perform I/O or MPI calls.
- MPI errors in the raw example are surfaced with `.expect()`.
- The `pmap` callback returns `Result<u64, String>` so user errors propagate
  through `hataori`'s error reporting.

## Testing

Build:

```bash
cargo build --example mpi_pi_raw --features mpi
cargo build --example mpi_pi_pmap --features mpi
```

Run with four MPI ranks:

```bash
mpiexec -n 4 cargo run --example mpi_pi_raw --features mpi
mpiexec -n 4 cargo run --example mpi_pi_pmap --features mpi
```

Success criteria:

- Both examples compile without warnings.
- Rank 0 prints π estimates that converge toward `3.1415...` as `N` grows.
- Output format matches the Julia original (start message, warm-up, benchmark
  table).

## Dependencies

No new crate dependencies are required. The examples reuse the existing optional
`mpi-upstream` feature and `hataori` public API.
