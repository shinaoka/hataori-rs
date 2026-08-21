# Hataori P0 Implementation Readiness

**Status:** Core design implementation-ready with recorded `Correct-to-merge`; optional tensor integration remains upstream-blocked.

This document records why the core design is implementable, what evidence is still required, and what must not be built early. The canonical behavior remains in [`design.md`](design.md).

## 1. Scope split

| Scope | Readiness |
|---|---|
| Hataori core | Implementation-ready; independent review `Correct-to-merge` |
| Optional tensor adapter | Blocked on tenferro-rs #1716 and tensor4all-rs #663 |
| P1 overlap, expanding queues, multiple domains | Deferred behind the evidence gates in `design.md` |

Core implementation and release do not wait for the tensor adapter. Integrated tensor compatibility must not be claimed until both upstream contracts and the joint gate pass.

## 2. Findings and resolutions (Blackboard Round 3 + review rounds)

| Finding | Resolution in `design.md` |
|---|---|
| Root compute vs. FUNNELED MPI | Root uses `ThreadPool::in_place_scope`; the scope body and MPI event loop stay on the initialization thread while borrowed callback work runs in the explicit pool. |
| Root event multiplexing fairness | Each loop checks local completion and at most one `MPI_Iprobe` message, alternates priority, and yields only when neither is ready. |
| MPI-only non-`Send` callback | Root executes one local batch synchronously; communication resumes after the finite callback. |
| Undefined `Inner { budget }` | P0 uses whole-domain `Inner` with no partial budget. The adapter captures and validates its explicit same-pool context. |
| Divergent local validation | A collective preflight converges validation before scheduler traffic. Collective order/communicator identity remain caller preconditions. |
| Message collision and framing | Every call duplicates the communicator and uses fixed header/payload tags with exact-source payload receive and a checked `i32::MAX` wire limit. |
| Drain/error state ambiguity | Normative remote-domain transitions and one-RESULT/one-DRAIN rules are specified. |
| Unsigned `MPI_MIN` portability bug | Error keys use checked nonnegative `i64` and `MPI_INT64_T`/`MPI_MIN`; preflight uses signed rank reduction. |
| Non-Linux strict affinity impossible | Linux placement is verified; non-Linux managed placement is explicitly degraded and `CallerDeclared`. |
| Scheduler testability vs. transport abstraction | Coordinator transitions are private pure state, tested directly. No generic transport trait is added. |
| `rsmpi-rt` API/pin uncertainty | Pin `065cc7622c6ad72b6bcbc808296aaf597ff56b70`; compile evidence is recorded below. |
| Root memory and task granularity | Root-memory residency and coarse-task assumptions are explicit. |
| Preflight no-error and error-key overflow | Passing ranks contribute `world_size`; largest-key overflow is a typed preflight failure. |
| MPI public trait ambiguity | `Communicator` is explicitly the selected rsmpi dependency trait; core adds no transport trait and lists the used surface. |
| Upstream rsmpi feature drift | Upstream `mpi` is pinned to `=0.8.1` with default/optional features disabled. |

## 3. Upstream evidence

### Upstream rsmpi

The `mpi` backend pins crates.io `mpi = "=0.8.1"` with `default-features = false`. In that release `mpi-sys` is mandatory, while the default feature only adds `user-operations`; disabling defaults therefore keeps the normal build/link-time backend and removes `libffi`. `derive` and `complex` remain disabled.

Hataori declares Rust 1.85 as its initial MSRV because fixed bincode 2.0.1 requires 1.85; the complete Task #9 feature matrix was checked with Rust 1.85.0. Rayon resolves to 1.12.0 and remains within that bound.

### rsmpi-rt

Pinned revision:

```text
065cc7622c6ad72b6bcbc808296aaf597ff56b70
```

Verified against that revision:

- feature `mpi-rt-sys-backend` exists and selects `mpi-rt-sys`;
- `Communicator::duplicate` calls `MPI_Comm_dup`;
- `Source::immediate_probe[_with_tag]` calls `MPI_Iprobe`;
- blocking probe/receive and tagged exact-source receive APIs exist;
- Rust `i64` maps to `RSMPI_INT64_T_fn`, and built-in `SystemOperation::min()` maps to `RSMPI_MIN_fn` without the optional `user-operations` feature;
- the runtime backend builds with:

```bash
cargo check --no-default-features --features mpi-rt-sys-backend
```

The check passed on 2026-08-21. The no-headers/no-C-compiler environment and `MPI_RT_LIB` multi-rank smoke remain implementation CI gates, not design assumptions.

### Rayon

The selected API is `ThreadPool::in_place_scope`, whose scope closure runs on the calling thread and whose `Scope::spawn` jobs enter the selected pool. This is the required shape for a FUNNELED root coordinator with borrowed callback state.

A temporary proof built and ran with `rayon 1.12.0` / `rayon-core 1.13.0` on 2026-08-21. It verified that:

- non-`Send` `Cell` state remains in the caller-side scope body;
- the scope body stays on the calling thread;
- a spawned callback borrows stack-owned input and runs on a target-pool worker;
- nested parallel iteration observes only the target pool's two worker indices.

The equivalent proof becomes a checked-in regression test before the hybrid scheduler merges.

### MPI error reduction

Unsigned `MPI_MIN` is avoided because released implementations produced wrong answers:

- MPICH 4.0.2: <https://github.com/pmodels/mpich/issues/6083>
- Open MPI 4.1.4: <https://github.com/open-mpi/ompi/issues/10648>

### Tensor prerequisites

- tenferro caller-managed admission and same-pool execution: <https://github.com/tensor4all/tenferro-rs/issues/1716>
- tensor4all explicit plain/graph/eager-AD contexts: <https://github.com/tensor4all/tensor4all-rs/issues/663>

Both remain external blockers for the adapter only.

## 4. Minimal implementation order

1. **Crate and features** — single crate, dependency-free default, `rayon`, `mpi`, and pinned `rsmpi-rt`; reject both MPI features together.
2. **Serial `map`** — ordered results, bounded callback errors, no serde/Send/Sync/`'static` leakage.
3. **Domain and local Rayon** — managed/external ownership, RAII admission, `Sequential`/`Outer`/whole-domain `Inner`, Linux affinity and non-Linux declared placement.
4. **Pure protocol state and wire** — coordinator transitions, stable IDs, framing, checked codec, signed deterministic error keys; no transport trait.
5. **MPI-only `pmap`** — private communicator, collective preflight, dynamic scheduler, synchronous root participation, drain and reuse.
6. **Hybrid `pmap`** — `in_place_scope`, local completion channel, fair `MPI_Iprobe` loop, rendezvous progress.
7. **Collective placement helpers** — root-coordinated `broadcast`, `scatter`, and `gather` on the same private wire rules.
8. **Tensor adapter** — only after both upstream issues land and known-good revisions are pinned.

Each numbered item is an independently reviewable implementation task. Do not combine scheduler, hybrid execution, affinity, and tensor integration into one initial change.

## 5. Acceptance and feature matrix

Commands become runnable when Step 1 creates `Cargo.toml`; they are merge gates for the corresponding step.

| Area | Required evidence |
|---|---|
| Default | `cargo test --no-default-features`; `cargo tree` contains no MPI, serde, bincode, tenferro, or tensor4all |
| Rayon | `cargo test --no-default-features --features rayon`; borrowed non-`'static` compile/run test; global-pool sentinel remains untouched |
| Upstream MPI | `cargo test --no-default-features --features mpi`; multi-rank watchdog suite under a build/link-time MPI |
| Runtime MPI | `cargo test --no-default-features --features rsmpi-rt`; build in an image without MPI headers/C compiler/libclang; `MPI_RT_LIB` multi-rank smoke |
| Hybrid upstream | `cargo test --no-default-features --features mpi,rayon`; rendezvous progress and root-local fairness test |
| Hybrid runtime | `cargo test --no-default-features --features rsmpi-rt,rayon`; same hybrid and thread-identity suite |
| Feature exclusion | `cargo check --no-default-features --features mpi,rsmpi-rt` must fail with the intended compile error |
| Docs/lints | `cargo fmt --check`; `cargo clippy --all-targets` for each valid feature set; `cargo doc --no-deps` for each public API feature set |

### Protocol tests

- empty input, world size one, ranks greater than items, batch one, and fixed batches greater than one;
- reverse completion preserves input order and every successful index executes once;
- skewed coarse workload beats a static contiguous partition without a benchmark framework;
- trace assertions prove `running <= 1`, `prefetched = 0`, one complete result per batch, and one STOP/DRAIN per remote rank;
- preflight mismatch emits no scheduler traffic, and a largest possible error key at or above `i64::MAX` returns the typed preflight failure;
- version, kind, source/tag, length/count, overflow, truncation, and trailing-byte failures are typed;
- simultaneous user/wire failures select one signed deterministic key on every rank;
- recoverable failure leaves the caller communicator reusable and no private frame unmatched;
- root-local codec instrumentation remains zero for `pmap`, `broadcast`, `scatter`, and `gather`;
- serial `map`, Rayon `Sequential`/`Inner`, and Rayon `Outer` each satisfy the documented stop/finish/report-lowest-index error semantics;
- callback panic takes the abort path rather than attempting recoverable drain.

### Domain tests

- Linux managed workers occupy distinct declared allowed CPUs and report `Verified`;
- non-Linux managed mode does not claim pinning and reports `CallerDeclared`;
- external pools are not repinned or shut down;
- admission is released after success, error, and unwind;
- foreign-pool entry and conflicting outer/inner ownership fail before callback execution.

### Compile-time boundary tests

Use compile-pass/compile-fail fixtures to prove:

- serial local code needs no serde, `Send`, `Sync`, or `'static`;
- MPI-only callback/input/output need no Rayon bounds;
- Rayon and hybrid callbacks can borrow non-`'static` state but satisfy the required `Send`/`Sync` bounds;
- communicator/backend types receive no unsafe `Send`/`Sync` wrapper.

## 6. Explicitly deferred work

Do not add placeholders, traits, or partial implementations for:

- `Isend`/`Irecv`, request pipelines, prefetch, progress threads, or async runtimes;
- expanding/controller-generated queues or parked-READY machinery;
- partial inner worker budgets;
- multiple domains, dedicated coordinator CPUs, NUMA/topology discovery, or multithreaded providers;
- public transport/executor/place/domain trait hierarchies;
- retries, cancellation, heartbeats, rank-failure recovery, remote objects, futures, registries, or distributed GC;
- tensor/provider policy in core.

## 7. Review gate

The blackboard Round 3 is design input, not the required independent reviewer verdict. Before any delegated implementation begins:

1. review the amended `docs/design.md` and this readiness record with the user-selected independent reviewer;
2. record reviewer identity, round, findings, fixes, and final verdict;
3. begin implementation only after `Correct-to-merge`, or after findings are fixed and re-reviewed.

This gate was completed by `reviewer-flash-opencode-go`; the final exact-state verdict is recorded in [`review-log.md`](review-log.md). Each later implementation task still requires its own post-diff different-family review.
