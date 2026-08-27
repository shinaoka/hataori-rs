# Phase B long-lived runtime work log

**Tracking:** [hataori-rs#14](https://github.com/shinaoka/hataori-rs/issues/14)

## Summary

Added the unpublished `hataori-runtime` workspace crate above the Phase A
foundation. It owns long-lived owner-thread progress, bounded worker domains,
typed action registration, must-use remote futures, structured scopes,
deadlines, cooperative cancellation, bounded deduplication, retryable response
backpressure, statistics, and explicit shutdown. Existing P0/P1 source and
measured paths are unchanged.

## Decisions

- Used an object-safe Phase A transport handle/driver pair. Runtime/action types
  store only logical IDs; MPI communicators and TCP endpoints stay below the
  boundary.
- Used fixed standard-library worker domains and synchronous bounded queues.
  `Runtime::block_on` only polls a future while pumping the single transport
  owner; it is not another executor or dependency.
- Required codecs to preserve segmented values and action/schema IDs to be
  explicit. Registry fingerprints sort IDs, so registration order does not
  affect the Phase A handshake.
- Removed a local waiter immediately on future drop. Cancellation uses the
  control channel and a pre-execution token; already-running Rust code is never
  claimed to be forcibly stopped.
- Kept required action responses in a bounded runtime outbox. Transport
  backpressure retries without losing a completed action; dedup state changes
  from running to completed only after the response enters transport.
- Kept completion markers when the dedup byte budget cannot retain a result.
  Duplicates then receive `DuplicateResultUnavailable` and never execute user
  code twice.
- Made explicit shutdown the only clean lifecycle. An atomic work gate closes
  submission before pending/local queues are drained, and a single domain
  in-flight counter keeps shutdown from observing the receive-to-running or
  completion-to-owner handoff as falsely idle. `Drop` never enters an MPI
  barrier or waits on blocked user code.

## Verification

`scripts/check-phase-b-runtime.sh` covers:

- action/schema registration and stable fingerprints;
- checked runtime message headers and malformed input;
- pending capacity, source/action identity, deadlines, late results, and future
  drop;
- bounded domain queues, shutdown/submission races, completion handoff,
  handler failure/panic conversion, scopes, and local execution;
- in-memory pass, duplicate, delay/cancellation, loss/deadline, saturation,
  response retry, disconnect, clean shutdown, and process reuse;
- TCP runtime request/result and two complete runtime lifecycles;
- MPI runtime request/result and reuse at world sizes 1, 2, and 4;
- default/MPI clippy `-D warnings`, rustdoc, Rust 1.85, dependency/backend
  scanners, and process-group watchdogs.

## Boundary after Phase B

No object directory, leases, `Remote<T>`, migration, placement policy, rebuilt
`pmap`, collectives, or facade replacement is included. Those remain Phase C-E.
