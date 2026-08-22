# P1 bounded prefetch

## Status

Approved for implementation. The independent pre-implementation review gate is recorded in `docs/review-log.md`.

## Scope

P1 adds one opt-in hybrid-MPI scheduling mode:

```rust
PmapOptions {
    prefetch: bool, // default: false
    ..
}
```

`prefetch = false` preserves the P0 protocol exactly. `prefetch = true` is accepted only by `mpi,rayon` and `rsmpi-rt,rayon` builds and permits each remote domain to hold at most one running batch and one prefetched batch. MPI-only execution rejects it during collective preflight because its callback runs on the MPI initialization thread and cannot overlap task transfer.

The root domain is not prefetched. Same-rank dispatch is an owned move with no wire latency, so reserving a second root batch would only reduce dynamic-scheduling fairness.

This phase does not add arbitrary prefetch depth, adaptive policy, a progress thread, an async runtime, `MPI_THREAD_MULTIPLE`, or new `'static` bounds.

## Minimal mechanism

P1 does not require nonblocking MPI requests. On a remote hybrid rank,
`ThreadPool::in_place_scope` keeps the scope body and every MPI call on the MPI
initialization thread. Borrowed callback jobs are spawned into the explicit
pool and report through a rank-local capacity-one outcome channel; the scope
joins them before return or unwind. This replaces the P0 worker's synchronous
`pool.install` call and does not add `'static` bounds.

The scope body:

1. receives the current batch;
2. spawns its callback into the explicit Rayon pool;
3. sends `READY` while that callback runs;
4. receives either one prefetched `TASK` or `STOP`;
5. after current completion, starts the prefetched callback before sending the current `RESULT`;
6. repeats, or sends `DRAIN` after the final result when `STOP` was received.

Blocking transfer of batch N+1 overlaps callback N, and blocking transfer of
result N overlaps callback N+1.

`STOP` received while a current batch is running means "no later assignment." It does not cancel the current batch. The worker sends the current `RESULT` before `DRAIN`.

## Remote lane states

With prefetch disabled, the P0 states are unchanged. With prefetch enabled,
the initial `READY` is accepted only while idle and assigns the current batch.
A later prefetch `READY` is accepted only while the lane has a current batch and
no prefetched batch. Each accepted `READY` receives exactly one `TASK` or
`STOP`.

One remote lane uses:

```text
Idle --READY/TASK--> Running(current)
Running(current) --READY/TASK--> Running(current, prefetched)
Running(current, prefetched) --RESULT(current)--> Running(prefetched)
Running(current) --READY/STOP--> Stopping(current)
Stopping(current) --RESULT(current)--> Stopped --DRAIN--> Drained

Running(prefetched) --READY/TASK--> Running(current, prefetched)
Running(prefetched) --READY/STOP--> Stopping(prefetched)
Stopping(prefetched) --RESULT(prefetched)--> Stopped --DRAIN--> Drained
```

A worker sends `READY` for another prefetch only after it has sent the preceding
`RESULT`, so headers from one source remain in lane order. No new wire message
kind is added. Collective preflight includes `prefetch` in its min/max option
agreement on both MPI backends; MPI-only local validation additionally requires
`prefetch == false`. The coordinator records both batch identities and exact
index sequences. Global output ordering remains the original indexed result
table. Thus `running <= 1`, `prefetched <= 1`, and resident work is bounded by
two batches per remote domain.

## Failure and drain

Assigned work is not cancelled. After the first recoverable error, root assigns no new batch, replies `STOP` to every later `READY`, validates every already-running or prefetched result, discards successful values after validation, then waits for one `DRAIN` per remote rank.

Every current-batch completion while the coordinator is
`Running(current, prefetched)`—success after another lane failed, callback
error, decode error, or recoverable protocol error—consumes the pinned current
metadata, retains the prefetched `RunningMeta`, and transitions to
`Running(prefetched)`. An error also sets the call-wide failed state and clears
only unassigned input. The following `READY` therefore receives `STOP`; the
prefetched result is still validated while `Stopping(prefetched)`, after which
the lane becomes `Stopped` and accepts `DRAIN`. A corrupt received batch ID uses
the coordinator-pinned current ID and cannot release or overwrite the
prefetched metadata.

A callback error in batch N does not cancel already-prefetched batch N+1; N+1 executes and reports exactly one result. This preserves the existing no-cancellation drain rule and avoids a second cancellation protocol.

If a prefetched payload cannot be decoded, the worker retains a typed failure outcome for that batch, sends the current result first, then sends the prefetched failure result without invoking the callback.

While a remote worker callback is live, failures in its prefetch `READY` send,
the following prefetch header receive or kind validation, or the preceding
`RESULT` encode and send abort directly with code 75 from the MPI
initialization-thread scope body. Payload decode/validation failures that were
fully received remain the typed prefetched-failure outcome described above. Callback-outcome channel disconnect and callback panic retain their
existing direct init-thread abort ownership. These paths must not return an
error and unwind the scope first: scope join could defer abort behind unbounded
user work while root is blocked on an unmatched frame. Failures detected while
no remote worker callback is live may return to the existing outer abort
boundary. The P0 root-local scope behavior is unchanged.

## Acceptance

Both MPI backends must prove:

- `prefetch = false` retains the P0 traces and results;
- MPI-only collective preflight rejects `prefetch = true` before scheduler traffic;
- hybrid traces prove `running <= 1`, `prefetched <= 1`, and at most two resident batches per remote domain;
- a deterministic blocked-transfer fixture proves task N+1 transfer overlaps callback N;
- a rendezvous-sized result fixture proves result N transfer overlaps callback N+1;
- reverse completion still preserves input order and each assigned input executes once;
- empty input, fewer than two batches, more ranks than items, and world size one complete;
- current and prefetched user/decode failures drain, converge deterministically, and permit communicator reuse;
- a subprocess fault fixture makes result N serialization fail after callback N+1 starts and proves prompt init-thread `MPI_Abort(75)` without waiting for scoped join or attempting drain;
- every MPI call passes `MPI_Is_thread_main`;
- scoped hybrid callbacks can still borrow non-`'static` data;
- seven-run performance measurements are recorded before considering default-on behavior.

The option remains default-off unless representative workloads show a median end-to-end improvement of at least 10%, regressions stay at most 5%, and the correctness and memory gates above pass.
