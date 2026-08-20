# Hataori P0 Design

**Status:** Final design consensus

**Scope:** A generic synchronous collective data-parallel engine for MPI ranks and rank-local Rayon execution domains

**Tracking:** [hataori-rs#1](https://github.com/shinaoka/hataori-rs/issues/1), [tenferro-rs#1716](https://github.com/tensor4all/tenferro-rs/issues/1716), [tensor4all-rs#663](https://github.com/tensor4all/tensor4all-rs/issues/663)

## 1. Positioning

### Name

**Hataori** comes from the Japanese word **機織り** (*hataori*), meaning weaving on a loom. The name reflects the engine's job: weave independent strands of work across MPI ranks and Rayon threads into one ordered result.

### Scope

Hataori makes common local, process-parallel, and hybrid data-parallel execution easy:

| Cargo features | Execution mode |
|---|---|
| none | single process, single thread |
| `rayon` | single process, multiple threads |
| `mpi` | multiple processes, one compute lane per rank |
| `mpi,rayon` | multiple processes, one multithreaded execution domain per rank |

Hataori is inspired by Distributed.jl's dynamically scheduled `pmap`, but P0 is not a Distributed.jl-equivalent distributed object runtime. It does not provide arbitrary remote calls, futures, remote object handles, distributed garbage collection, or dynamic process creation.

The default cross-rank path uses serialization. Applications needing zero-copy communication, MPI derived datatypes, GPU-aware MPI, custom collectives, or specialized communication should use MPI directly.

## 2. Architectural boundary

### 2.1 Core

Hataori core owns only generic execution concerns:

- local `map` and collective `pmap`;
- coordinator-driven dynamic scheduling;
- `Place = (rank, domain_id)`;
- rank-local execution-domain ownership and admission;
- blocking MPI transport and generic serialization envelopes;
- managed or caller-owned affinity;
- lifecycle, ordering, and error convergence.

Core treats input `T`, output `U`, and error `E` as generic values. It has no tensor, backend, cache, or provider semantics.

### 2.2 Optional integration adapter

Tensor integration belongs in an optional adapter that depends on Hataori core, tenferro, and tensor4all:

```text
hataori-core
    ^
    |
hataori tensor adapter ---> tenferro
    |
    +----------------------> tensor4all

tensor4all ---> tenferro
tenferro   -X-> hataori
tensor4all -X-> hataori
```

The adapter owns:

- tenferro external-domain handles and caller-managed admission mapping;
- tensor4all explicit contexts and domain-owned runtimes/caches;
- tensor logical wire representation and reconstruction in the target context;
- backend/provider checks such as BLAS or OpenMP thread policy;
- integration-specific errors and capability gates.

Neither tenferro nor tensor4all may depend on Hataori types. Hataori core must build without either dependency.

## 3. Public execution model

The conceptual entry points are:

```rust
map(items, f) -> Result<Vec<U>, MapError>

pmap(world, root, root_items, f)
    -> Result<Option<Vec<U>>, PmapError>
```

- `map` is local and has no MPI or serialization requirements.
- `pmap` is collective: all ranks call it in the same order on the same communicator.
- Only `root` supplies `Some(Vec<T>)`; other ranks supply `None`.
- Only `root` receives `Some(Vec<U>)` on success.
- Each input is evaluated exactly once in successful executions.
- Results are returned in input order, independent of completion order.
- `map` is not implemented as world-size-one `pmap`.

Hataori does not transmit closures. All ranks run the same binary and construct `f` at the same SPMD call site. Root-only captured data must be included in input or distributed explicitly.

## 4. Place and execution domain

A place is:

```text
(rank, domain_id)
```

A domain is a multithreaded execution resource, not a Rayon worker thread. P0 supports exactly one domain per rank, so only `domain_id = 0` is valid. The value model leaves room for multiple domains later without implementing a registry or selection policy now.

A core domain contains only Hataori-owned execution facts:

```text
Domain
├─ domain_id
├─ Rayon pool handle, when enabled
├─ declared or verified CPU set
├─ worker budget
├─ pool ownership mode
└─ nonblocking running slot
```

It never contains tenferro or tensor4all types.

At most one coarse operation is admitted per domain. Admission is nonblocking and RAII-managed. Contention returns a typed `DomainBusy` error; a worker never blocks waiting for admission.

## 5. Affinity and lifecycle

### 5.1 Hataori-managed mode

Managed mode is the standard P0 mode.

- The builder receives an explicit CPU set and worker count.
- The CPU set must be a subset of the process's actual allowed CPUs.
- Hataori creates and owns one Rayon pool.
- A worker start hook pins each worker to a distinct declared CPU.
- Pin failure, duplicate assignment, an out-of-range CPU, or too many workers is a construction error.
- Hataori owns pool shutdown.

Hataori does not infer a second resource policy from global Rayon state, `RAYON_NUM_THREADS`, `from_env`, or automatic topology discovery.

### 5.2 External-managed mode

External mode accepts a caller-owned pool plus declared CPU set and worker budget.

- Hataori never re-pins workers.
- Hataori never shuts down the pool.
- It validates only observable facts and reports whether placement is verified or caller-declared.
- A pool injected first and pinned afterward by Hataori is not supported.

The core constructor does not accept a tenferro handle. The optional adapter associates backend handles and contexts with the core `DomainId`.

## 6. Intra-domain execution modes

When Rayon is enabled, an operation selects one generic fan-out policy:

```rust
enum LocalMode {
    Sequential,
    Outer,
    Inner { budget: usize },
}
```

- `Sequential`: no local fan-out.
- `Outer`: Hataori maps independent items in the selected domain; each item must not perform another competing fan-out.
- `Inner { budget }`: Hataori presents items sequentially and gives the item operation exclusive use of up to `budget` workers in the same domain.

`Inner` is a generic resource contract, not a tenferro-specific mode. Its availability depends only on the core Rayon domain. A tenferro adapter maps it to caller-managed same-pool execution and rejects the integrated operation if that backend capability is unavailable.

`Outer` and `Inner` are mutually exclusive for one operation. `1 <= budget <= worker_count`. Synchronous entry from a foreign pool is rejected.

## 7. Dynamic P0 scheduler

P0 uses coordinator-driven dynamic scheduling rather than static rank chunks.

```text
READY(rank, domain=0)
    -> TASK(batch_id, indexed_items, payload)
    -> RESULT(batch_id, indices, status, payload)

READY(rank, domain=0)
    -> STOP
    -> DRAIN
```

- The root owns the queue of indexed items.
- Batch size is a fixed positive call parameter; the default is 1.
- Each domain has `running <= 1` and `prefetched = 0`.
- Every `READY` receives exactly one `TASK` or `STOP`.
- `READY` supplies backpressure; P0 has no credit, acknowledgement, heartbeat, retry, cancellation, cost model, or adaptive batch policy.
- The root restores order with an O(N) indexed result table.

The MPI initialization thread drives all communication. CPU work runs in the rank's execution domain. The coordinator has no dedicated CPU in P0 and makes no communication/computation-overlap guarantee.

## 8. Error and drain protocol

After the first recoverable error:

1. the root stops assigning new work;
2. every current or later `READY` receives `STOP`;
3. already assigned batches finish or fail;
4. every announced length and payload is received or drained;
5. each rank sends `DRAIN` once stopped and idle;
6. after all ranks drain, one final collective selects the deterministic winning error;
7. the winning bounded UTF-8 error message is broadcast.

The final winner is selected with a checked packed `u64` and `MPI_UINT64_T`/`MPI_MIN`; an arbitrary Rust struct is never reduced directly. There is no collective after every batch.

Recoverable errors must not return early and strand a blocking sender. Process death, OOM, `panic=abort`, MPI transport failure, or rank loss may require `MPI_Abort`.

Only one high-level `pmap` may be active on an MPI initialization thread. A small RAII reentrancy guard is sufficient; P0 has no communicator registry.

## 9. MPI thread model

P0 targets `MPI_THREAD_FUNNELED`.

- Only the thread that initialized MPI may call MPI.
- Communicators never enter Rayon closures.
- A mutex does not permit MPI calls from another thread under FUNNELED.
- Hataori adds no unsafe `Send` or `Sync` implementation for communicators.
- P0 uses blocking MPI and no background progress thread.

## 10. Generic wire and locality

Cross-rank values use one fixed bincode 2 serde configuration:

```rust
bincode::config::standard()
    .with_fixed_int_encoding()
    .with_little_endian()
```

Each envelope includes a protocol version and checked `u64` byte length. Decoding validates the MPI count, integer conversions, protocol version, expected structure, and full byte consumption. Truncation, overflow, and trailing bytes are protocol errors.

Same-rank movement is an owned move and bypasses serialization and MPI. Distributed `pmap` retains serde bounds because placement is decided at runtime; callers needing non-serializable local work use `map`.

Core transports only opaque serializable `T` and `U` values or bytes. Tensor shape, dtype, layout, allocation, and target-context reconstruction belong to the optional tensor adapter. Closures, communicators, pools, contexts, admission state, cache identity, and addresses never appear on the wire.

## 11. Data placement helpers

P0 provides three synchronous coordinator-only helpers that reuse the same wire and error rules:

- `broadcast`: copy one root-owned value to every rank;
- `scatter`: distribute rank-local owned shards;
- `gather`: collect one rank-local owned value from every rank.

Returned values are caller-owned. P0 has no remote registry, cross-call object cache, remote references, or distributed garbage collector.

## 12. Trait bounds

P0 uses scoped synchronous execution and does not add `'static` for hypothetical future overlap.

| Entry point | Bounds |
|---|---|
| serial `map` | no serde, `Send`, `Sync`, or `'static` requirement; `E: Display` |
| Rayon `map` | `T/U/E: Send`; `F: Fn(T) -> Result<U, E> + Send + Sync`; `E: Display`; no `'static` |
| MPI-only `pmap` | `T/U: Serialize + DeserializeOwned`; `E: Display`; no `Send`, `Sync`, or `'static` |
| hybrid `pmap` | MPI bounds plus `T/U/E: Send`; `F: Fn(T) -> Result<U, E> + Send + Sync`; no `'static` |

`E` is moved from a worker and does not require `Sync`. Shared-data APIs add `Sync` only where actual sharing requires it.

## 13. Tensor integration contract

The optional adapter establishes this ownership chain:

```text
Hataori domain
  -> tensor4all explicit context and domain-owned runtimes/caches
    -> tenferro caller-managed external domain
      -> the same Rayon pool
```

Required tenferro capabilities:

- caller-managed external CPU admission that bypasses the process-global CPU arbiter;
- synchronous borrowed same-executor entry from a caller-owned pool;
- no second pool, blocking admission, or public-backend recursive entry;
- typed errors for unsupported reentry and budget violations.

Required tensor4all capabilities:

- explicit context entry points for plain, graph, and eager AD paths;
- no fallback to `DEFAULT_CPU_CONTEXT`, `DEFAULT_BACKEND`, `DEFAULT_GRAPH_RUNTIME`, `DEFAULT_EAGER_RUNTIME`, `from_env`, or a global backend mutex;
- backend/runtime/cache ownership scoped to the domain context;
- typed rejection of cross-context compiled-object or workspace reuse.

Tensor transport contains only logical host data and metadata. The receiving rank reconstructs the tensor in its target domain context. Executor, admission, runtime, and cache identities are never serialized.

## 14. Provider threads

Core does not encode tensor provider policy. The tensor adapter must account for all provider threads within the selected CPU budget.

The default hybrid integration requires single-threaded BLAS/OpenMP providers. Thread counts are configured before execution, not changed while operations are running. An integrated mode is rejected when provider behavior cannot be verified or would introduce an uncontrolled thread source.

## 15. P0 hard failures

Core rejects before work begins:

- more than one domain per rank;
- ambient/global Rayon or hidden pool construction;
- synchronous foreign-pool entry;
- invalid or unverifiable managed placement;
- invalid external ownership declarations;
- worker or background-thread MPI calls;
- domain admission contention or budget overflow;
- requests for P0 overlap or dedicated-coordinator behavior.

The optional adapter separately rejects:

- duplicate Hataori and tenferro CPU admission;
- unavailable caller-managed same-pool capability;
- tensor4all global-default fallback;
- context/cache mismatch;
- uncontrolled provider threads;
- conflicting Outer/Inner backend fan-out.

Backend-specific errors do not enter the core error enum.

## 16. Acceptance criteria

### Core scheduling and transport

- Empty input, more ranks than inputs, batch size 1, and fixed batch size greater than 1 work.
- Every successful input executes once and root results preserve input order under reverse completion order.
- A skewed workload demonstrates meaningful improvement over static contiguous partitioning.
- Instrumentation proves `running <= 1`, `prefetched = 0`, and bounded resident batches.
- Large rendezvous-sized results make progress while the root domain also computes.
- Simultaneous user/decode errors converge on one deterministic winner without deadlock.
- A communicator is reusable after a recoverable failure.
- Every MPI call passes `MPI_Is_thread_main`.
- Same-rank serialization count is zero.
- Version, length, count, overflow, truncation, and trailing-byte errors are typed.

### Domain and affinity

- Managed workers remain on distinct declared CPUs within the launcher/cgroup allowed set.
- External pools are neither re-pinned nor shut down.
- Global Rayon is never entered.
- Admission is reacquired after success, error, or unwind.
- Foreign-pool entry and budget overflow fail before execution.

### Compile-time boundaries

- Scoped local and hybrid execution can borrow non-`'static` data.
- MPI-only use is not forced to satisfy Rayon `Send`/`Sync` bounds.
- Local non-serialized use is not forced to implement serde.
- No communicator or backend receives an unsafe thread-safety wrapper.
- Core's feature matrix builds without tenferro or tensor4all in its dependency tree.

### Three-repository integration

- Hataori's running slot is the only coarse CPU admission.
- tenferro's CPU arbiter is not acquired in caller-managed mode.
- Same-pool inner fork/join creates no pool or OS thread and does not panic or deadlock.
- tensor4all plain, graph, and eager AD paths all use the explicit target context.
- Global/default tensor runtime sentinels remain untouched.
- Remote tensor reconstruction occurs in the target context and serializes no execution identity.
- Outer and Inner each have exactly one fan-out owner.
- Managed and external modes respect the declared total thread budget.

## 17. Explicit P0 non-goals

- Arbitrary `remotecall`, spawn, futures, task graphs, or task registries
- Remote object handles, channels, distributed reference counting, or distributed GC
- Dynamic process creation; ranks are owned by the MPI launcher or scheduler
- Transmitting Rust closures or executable code
- Nonblocking communication/computation overlap or background MPI progress
- `MPI_THREAD_MULTIPLE`
- Multiple concurrently scheduled domains per rank
- A dedicated coordinator CPU
- Automatic topology or NUMA discovery and memory-placement guarantees
- Adaptive batches, cost models, retries, cancellation, heartbeats, or fault recovery
- A generic `Executor`, `Place`, or `Domain` trait hierarchy
- A process-global Hataori CPU broker or tensor cache
- Zero-copy, MPI derived datatypes, GPU-aware MPI, or custom transport/codec abstractions

## 18. P1 gates

P1 work is demand-driven, not predesigned into P0.

### Nonblocking overlap

Prototype init-thread `Isend`/`Irecv`/`Test*` with at most one running and one prefetched batch. Productize it only if:

- exposed communication/serialization wait is at least 15% of wall time in a real workload;
- across at least seven runs, median end-to-end time improves by at least 10%;
- representative compute-heavy and small-payload regressions are at most 5%;
- memory remains bounded by two resident batches per domain;
- FUNNELED identity, ordering, draining, and reuse remain correct.

Only a detached/overlapped entry that truly requires ownership may add `Send + 'static`; those bounds must not leak into P0 scoped APIs.

### Other P1 candidates

- multiple disjoint domains per rank, after caller-managed backend admission is proven;
- a dedicated coordinator policy, only if progress latency is measured as a bottleneck;
- NUMA-aware placement or multithreaded providers, each behind separate correctness and performance evidence.

Future remote execution is not structurally blocked: `(rank, domain_id)` provides addressing and the versioned generic envelope provides transport. If real workloads require it, add one explicit registered-task or scoped remote-call primitive then; do not serialize arbitrary Rust closures or build a distributed object runtime preemptively.

## 19. Completion definition

P0 is complete only when:

1. Hataori core scheduling, transport, affinity, bounds, and error tests pass;
2. tenferro provides and tests caller-managed admission and same-pool borrowed entry;
3. tensor4all provides and tests explicit plain/graph/eager-AD contexts with no default fallback;
4. the optional adapter passes the joint ownership, admission, context, wire, and thread-budget gate;
5. Hataori pins known-good tenferro and tensor4all revisions.

Core may ship earlier, but integrated tensor compatibility must not be claimed before the joint gate passes.
