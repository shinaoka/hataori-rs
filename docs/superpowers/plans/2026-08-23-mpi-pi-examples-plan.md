# MPI π Examples Implementation Plan

> **Status (2026-08-25):** Implemented in commits `f35c4fc`, `f398410`, `e53ec1c`,
> `e0dccb6`, with follow-ups `f60fee5`, `5003360`, `fb1965e`, `808ed2c`. The current
> code differs from this document in: the helpers use `i64`, the timer is
> `timed(rank, label, f)`, `mpi_pi_pmap` uses `batch_size = 10`, and all ranks time
> the full computation while only rank 0 prints. There is no automated test for
> these examples.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two MPI example binaries (`mpi_pi_raw.rs` and `mpi_pi_pmap.rs`) and a shared helper module (`examples/support/pi_common.rs`) that mirror the provided Julia π benchmark.

**Architecture:** The pure math helpers live in `examples/support/pi_common.rs`. `mpi_pi_raw.rs` uses the upstream `mpi` crate directly for rank/size, round-robin counting, and `all_reduce_into`. `mpi_pi_pmap.rs` uses `hataori::pmap` to distribute the same workload across MPI ranks.

**Tech Stack:** Rust, `mpi` crate (via `mpi-upstream` feature), `hataori` crate.

---

### Task 1: Create shared helper module

**Files:**
- Create: `examples/support/pi_common.rs`

- [ ] **Step 1: Write `examples/support/pi_common.rs`**

```rust
use std::time::{Instant};

/// Euclidean algorithm for the greatest common divisor.
pub fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let tmp = a % b;
        a = b;
        b = tmp;
    }
    a
}

/// Count coprime pairs `(a, b)` where `a` follows the round-robin sequence
/// `start, start + step, ...` and `b ∈ 1..=n`.
pub fn count_coprime_pairs(start: i64, step: i64, n: i64) -> i64 {
    let mut local_cnt = 0_i64;
    for a in (start..=n).step_by(step as usize) {
        let a = a as u64;
        for b in 1..=n {
            if gcd(a, b as u64) == 1 {
                local_cnt += 1;
            }
        }
    }
    local_cnt
}

/// Count coprime pairs for a single value of `a`.
pub fn count_for_a(a: u64, n: i64) -> i64 {
    let mut cnt = 0_i64;
    for b in 1..=n {
        if gcd(a, b as u64) == 1 {
            cnt += 1;
        }
    }
    cnt
}

/// Estimate π from the coprime probability.
pub fn estimate_pi(total_coprime: i64, n: i64) -> f64 {
    let prob = total_coprime as f64 / n as f64 / n as f64;
    (6.0 / prob).sqrt()
}

/// Print an informational line, matching Julia's `@info` style.
pub fn print_info<N: std::fmt::Display>(label: &str, value: N) {
    println!("[ Info: {label} ] {value}");
}

/// Time a computation and print the elapsed time and result.
pub fn timed<F: FnOnce() -> f64>(label: &str, f: F) -> f64 {
    let start = Instant::now();
    let output = f();
    let elapsed = start.elapsed();
    println!("[ Info: {label} ] elapsed = {elapsed:?}, output = {output}");
    output
}
```

- [ ] **Step 2: Commit the helper module**

```bash
git add examples/support/pi_common.rs
git commit -m "feat: add shared pi computation helpers for MPI examples"
```

---

### Task 2: Register the new examples in `Cargo.toml`

**Files:**
- Modify: `Cargo.toml:29-43`

- [ ] **Step 1: Add two `[[example]]` entries after the existing `mpi_placement_smoke` entry**

```toml
[[example]]
name = "mpi_pi_raw"
required-features = ["mpi"]

[[example]]
name = "mpi_pi_pmap"
required-features = ["mpi"]
```

- [ ] **Step 2: Commit the Cargo.toml change**

```bash
git add Cargo.toml
git commit -m "build: register mpi_pi_raw and mpi_pi_pmap examples"
```

---

### Task 3: Create the raw MPI example

**Files:**
- Create: `examples/mpi_pi_raw.rs`

- [ ] **Step 1: Write `examples/mpi_pi_raw.rs`**

```rust
use mpi_upstream as mpi_api;
use mpi_upstream::traits::*;

#[path = "support/pi_common.rs"]
mod pi_common;

fn main() {
    let universe =
        mpi_api::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    if rank == 0 {
        pi_common::print_info("Start processing...", "") as ();
        pi_common::print_info("MPI size", size);
    }

    // Warm up
    let n_warm = 100_i64;
    if rank == 0 {
        pi_common::print_info("Warm up", "");
        pi_common::print_info("N", n_warm);
    }
    let local_warm = pi_common::count_coprime_pairs(rank as i64 + 1, size as i64, n_warm);
    let mut total_warm = 0_i64;
    world.all_reduce_into(
        &local_warm,
        &mut total_warm,
        mpi_api::collective::SystemOperation::sum(),
    );
    if rank == 0 {
        let _ = pi_common::timed("warmup", || pi_common::estimate_pi(total_warm, n_warm));
    }

    // Benchmarks
    for n in [100_i64, 1000, 10_000, 50_000, 100_000] {
        if rank == 0 {
            pi_common::print_info("N", n);
        }
        let local = pi_common::count_coprime_pairs(rank as i64 + 1, size as i64, n);
        let mut total = 0_i64;
        world.all_reduce_into(
            &local,
            &mut total,
            mpi_api::collective::SystemOperation::sum(),
        );
        if rank == 0 {
            let _ = pi_common::timed("benchmark", || pi_common::estimate_pi(total, n));
        }
    }
}
```

- [ ] **Step 2: Commit the raw example**

```bash
git add examples/mpi_pi_raw.rs
git commit -m "feat: add raw MPI pi benchmark example"
```

---

### Task 4: Create the `hataori::pmap` example

**Files:**
- Create: `examples/mpi_pi_pmap.rs`

- [ ] **Step 1: Write `examples/mpi_pi_pmap.rs`**

```rust
use hataori::{pmap, Domain, PmapOptions};
use mpi_upstream as mpi_api;
use mpi_upstream::traits::*;

#[path = "support/pi_common.rs"]
mod pi_common;

fn run_one(world: &mpi_api::topology::SystemCommunicator, n: i64) -> i64 {
    let rank = world.rank();
    let input = (rank == 0).then(|| (1..=n).collect::<Vec<u64>>());
    let result = pmap(
        world,
        &Domain::sequential(),
        PmapOptions::default(),
        input,
        |a| Ok::<_, String>(pi_common::count_for_a(a, n)),
    )
    .expect("pmap must succeed");

    result.map_or(0_i64, |values| values.iter().sum::<i64>())
}

fn main() {
    let universe =
        mpi_api::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    let rank = world.rank();

    if rank == 0 {
        pi_common::print_info("Start processing...", "");
        pi_common::print_info("MPI size", world.size());
    }

    // Warm up
    let n_warm = 100_i64;
    if rank == 0 {
        pi_common::print_info("Warm up", "");
        pi_common::print_info("N", n_warm);
    }
    let total_warm = run_one(&world, n_warm);
    if rank == 0 {
        let _ = pi_common::timed("warmup", || pi_common::estimate_pi(total_warm, n_warm));
    }

    // Benchmarks
    for n in [100_i64, 1000, 10_000, 50_000, 100_000] {
        if rank == 0 {
            pi_common::print_info("N", n);
        }
        let total = run_one(&world, n);
        if rank == 0 {
            let _ = pi_common::timed("benchmark", || pi_common::estimate_pi(total, n));
        }
    }
}
```

- [ ] **Step 2: Commit the pmap example**

```bash
git add examples/mpi_pi_pmap.rs
git commit -m "feat: add hataori pmap pi benchmark example"
```

---

### Task 5: Build the examples

**Files:**
- Test command only.

- [ ] **Step 1: Build `mpi_pi_raw`**

```bash
cargo build --example mpi_pi_raw --no-default-features --features mpi
```

Expected: `Finished dev` profile with no warnings or errors.

- [ ] **Step 2: Build `mpi_pi_pmap`**

```bash
cargo build --example mpi_pi_pmap --no-default-features --features mpi
```

Expected: `Finished dev` profile with no warnings or errors.

---

### Task 6: Run MPI smoke tests

**Files:**
- Test command only.

- [ ] **Step 1: Run the raw example with four ranks**

```bash
mpiexec -n 4 target/debug/examples/mpi_pi_raw
```

Expected output (rank 0 only):

```text
[ Info: Start processing... ]
[ Info: MPI size ] 4
[ Info: Warm up ]
[ Info: N ] 100
[ Info: warmup ] elapsed = ..., output = 3.13682644...
...
```

- [ ] **Step 2: Run the pmap example with four ranks**

```bash
mpiexec -n 4 target/debug/examples/mpi_pi_pmap
```

Expected output similar to the raw example; values should converge toward π as `N` grows.

- [ ] **Step 3: Single-rank sanity check**

```bash
mpiexec -n 1 target/debug/examples/mpi_pi_raw
mpiexec -n 1 target/debug/examples/mpi_pi_pmap
```

Expected: no MPI errors and rank 0 prints values converging toward π.

---

### Task 7: Final integration commit (if changes were needed during testing)

**Files:**
- Any modified files.

- [ ] **Step 1: Commit any fixes**

```bash
git add -A
git commit -m "fix: address review/test issues in MPI pi examples"
```

---

## Self-Review Checklist

- [ ] `pi_common.rs` provides all math helpers used by both examples.
- [ ] `Cargo.toml` lists both examples with `required-features = ["mpi"]`.
- [ ] `mpi_pi_raw.rs` uses `count_coprime_pairs(rank + 1, size, N)` for round-robin work.
- [ ] `mpi_pi_pmap.rs` builds `(1..=N)` input on rank 0 and uses `pmap` with `Domain::sequential()`.
- [ ] Both examples warm up with `N = 100` and benchmark `[100, 1000, 10_000, 50_000, 100_000]`.
- [ ] Build commands and `mpiexec` smoke tests are defined with expected output.
