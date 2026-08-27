# Repository Rules

These are Hataori-specific rules. Apply them on top of the shared tensor4all
rules loaded through `AGENTS.md`.

## Architecture sources of truth

- `docs/design.md` describes the implemented synchronous P0 architecture.
- `docs/design/bounded-prefetch.md` describes the implemented P1 extension.
- `docs/design/distributed-runtime.md` describes the accepted long-term runtime
  and records which phases are implemented.
- `docs/design/phase-a-transport-foundation.md` describes the implemented
  unpublished Phase A protocol and transport boundary.
- `docs/design/phase-b-runtime.md` describes the implemented unpublished Phase B
  lifecycle, typed-action, future, scope, deduplication, and shutdown boundary.
- `docs/design/phase-c-objects.md` describes the implemented pinned-object,
  directory/resolver, lease/root, typed object-action, and placement boundary.
- `docs/design/phase-d-migration.md` describes implemented manual migration,
  snapshot/restore, stable authority, epoch commit, redirect, rollback,
  forwarding, retirement, and reconstructible-resource behavior; it does not
  include automatic policy or rebuilt algorithms.
- Do not present a future-runtime capability as implemented until source,
  runnable examples, and tests provide it.
- Backward compatibility with the current P0 API is not a design requirement
  for the future-runtime work. Prefer one clean canonical contract over
  compatibility shims.

## Layering and dependency direction

- Keep protocol, transport, runtime services, actions/futures, remote objects,
  algorithms, and tensor adapters as distinct ownership layers.
- Keep the `hataori` public facade and current-style sugar above the canonical
  algorithm/runtime APIs. Sugar lowers directly to those APIs; it must not own
  another scheduler, protocol, queue, retry policy, or resource lifecycle.
- Higher layers may depend on lower layers; lower layers must not depend on
  algorithms, tenferro, tensor4all, or application object types.
- `pmap`, broadcast, scatter, and gather are algorithms built on runtime
  services, not transport primitives or runtime lifecycle owners.
- Keep tenferro and tensor4all integration in adapter crates. Core runtime and
  transport crates must build without those dependency trees.

## Current-style facade and performance continuity

- Preserve the useful vocabulary and call shape of `map`, `map_in`, `pmap`,
  `Domain`, `LocalMode`, and `PmapOptions` in a supported sugar facade, without
  treating source compatibility with P0/P1 as a requirement.
- Local `map`/`map_in` may continue to accept borrowed closures. Distributed
  sugar accepts a registered typed action token; it must not pretend to
  serialize a Rust closure or executable code.
- The initiating locality owns `pmap` input and ordered output. Other
  localities service actions through the running runtime; they do not need to
  enter a matching collective facade call or pass `None` placeholders.
- Lower sugar directly to the batch-oriented algorithm path. Do not introduce
  an extra payload copy, serialization pass, transport round trip, queue hop,
  per-item heap allocation, remote-object lookup, or lease operation.
- Same-locality batches remain owned moves with no codec or transport pass.
  Pending-call and future accounting is per in-flight batch, not per item,
  unless the requested batch size is one.
- Object-aware `pmap` placement resolves and accounts per batch, not per item.
- Before replacing the implemented P0/P1 path, compare the new facade and its
  canonical lowering against the exact legacy baseline named in
  `docs/design/distributed-runtime.md`. Use a predeclared paired release-mode
  experiment; candidate results must not change cases, metrics, noise limits,
  or thresholds.
- A required performance case classified `FAIL` or `INCONCLUSIVE` blocks
  acceptance. Measurement tolerance represents the resolution of a valid
  host, not an allowed performance budget, and must be established from
  baseline-only noise data with the cap specified in the design.
- TCP has no P0/P1 transport baseline. It must pass the same semantic suite and
  the sugar-versus-canonical transparency gate; TCP-specific absolute or
  scaling targets must be declared before its candidate is measured.

## Transport independence

- Production transport backends are MPI and TCP. An in-memory transport may be
  kept private for deterministic tests and fault injection.
- Runtime, action, future, object, directory, lease, and migration layers use
  transport-independent `LocalityId` and parcel types. They must not expose or
  store MPI ranks, communicators, tags, TCP sockets, or socket addresses.
- MPI communicators and requests remain owned by the MPI transport driver. Do
  not add unsafe `Send` or `Sync` implementations to make MPI handles fit a
  higher-layer abstraction.
- TCP and MPI must pass the same transport contract suite. A new transport is
  not accepted by compilation alone.
- Keep bootstrap, membership, transport progress, and runtime protocol as
  separate concerns even when one backend implements several of them.
- Native collectives, zero-copy, RDMA, and backend-specific bulk operations are
  optional optimizations. Do not make them requirements of the base transport
  contract.

## Protocol and boundedness

- Every connection performs a bounded handshake covering run identity,
  protocol version, registry compatibility, and negotiated capabilities.
- Every received frame validates version, kind, channel, lengths, counts, and
  checked integer conversions before allocation or dispatch.
- Control, action, and bulk-data traffic have distinct logical channels.
  Control progress must not be starved by bulk payloads.
- Every queue, mailbox, pending-request table, deduplication table, forwarding
  entry, tombstone set, cache, and in-flight byte counter has an explicit owner
  and a bounded capacity, deadline, or documented durable root.
- Payload representation must permit segmentation and streaming. Do not make a
  single contiguous `Vec<u8>` a permanent public protocol requirement.

## Structured execution and failures

- Ordinary spawned work belongs to a scope or a must-use join/future handle.
  Detached work is explicit, quota-controlled, and observable.
- Local scoped work may borrow non-`'static` state. Work that can outlive its
  call frame or cross a process boundary owns `Send + 'static` serializable
  state.
- Distinguish local send completion, remote receipt, and action completion.
- Deadlines remove local waiters mechanically. Cancellation is best effort and
  does not silently claim that already-running user code was stopped.
- Protocol corruption, peer loss, remote action failure, cancellation, resource
  exhaustion, and runtime shutdown are distinct typed failure classes.

## Remote object identity and migration

- A public `ObjectId` is stable and location-independent. Never encode a rank,
  socket address, local slot, pointer, or current home locality into it.
- Current location is a separate versioned record containing at least a
  locality and epoch. Every object call validates the observed epoch.
- Remote handles route through the location resolver even when a same-locality
  fast path is available.
- Object migration preserves object identity and lifetime ownership. It changes
  only location/version state.
- Migration uses an explicit freeze, transfer, prepare, directory commit,
  activation, and forwarding/retirement state machine. Directory commit is the
  authority boundary; rollback is permitted only before that commit.
- Classify object state as pinned, reconstructible, or migratable. Do not
  serialize executor pools, communicators, provider handles, device contexts,
  caches, raw addresses, or admission guards as migratable state.
- Exact placement uses a logical `(LocalityId, DomainId)` and never a transport
  endpoint. `Remote::call` follows the authoritative object location.
- Distinguish hard colocation from soft affinity. Soft affinity has an explicit
  bounded fallback and does not bypass admission or raise queue priority.
- Hard colocation acquires a bounded placement ticket/pin so migration and
  admission cannot race across a directory epoch.

## Local object concurrency

- An object belongs to one domain. Rayon workers in that domain may share it;
  another locality accesses it only through typed actions.
- Default `Exclusive` object actions are serialized and receive `&mut T`, so
  `T: Send` is sufficient and user locking is not required.
- Opt-in `ReadWrite` objects admit concurrent read actions with `&T` and
  exclusive write actions with `&mut T`; enforce the corresponding `Send +
  Sync` bounds.
- Acquire reader/writer admission before submitting work to Rayon. Do not park a
  Rayon worker on an object lock or hold an object guard across `.await`, a
  remote call, detached work, or migration snapshot.
- User object state may contain locks or atomics, but lock state is not
  migratable. User locks do not replace runtime admission, leases, or pins.

## Resource lifecycle and leak prevention

- Remote strong references are RAII values backed by locality-scoped leases.
  Local clones share one locality lease rather than creating network traffic
  per clone.
- Strong remote references do not implement unrestricted serialization. Moving
  one between localities goes through a runtime lease-transfer protocol.
- Prefer weak remote references inside persistent object state to prevent
  distributed strong-reference cycles. Any supported strong persistent edge
  needs an explicit owner and cycle policy.
- An object is collectible only when durable roots, active leases, in-flight
  calls, migration pins, transfer pins, and retained mailbox work are all zero.
- Normal `Drop`/release is the fast cleanup path; lease expiry is the bounded
  fallback for lost releases, forgotten values, or failed localities.
- Shutdown exposes and tests counts for live objects, leases, pending calls,
  queued parcels, detached tasks, migration pins, and retained bytes.
- Long-lived caches and buffer pools are runtime-owned, bounded by default,
  configurable, clearable, and observable in entries and retained bytes.

## Validation

- Keep deterministic in-memory protocol tests for delay, duplication, loss,
  reordering, saturation, disconnect, stale locations, migration races, and
  lease expiry even though memory is not a production backend.
- Test pure lifecycle and migration state machines independently from MPI and
  TCP I/O.
- Add integration coverage for both production backends, clean shutdown, large
  segmented payloads, backpressure, and reuse after recoverable errors.
- For lifecycle concurrency, combine state-machine/property tests with focused
  concurrency checking where practical. Record untested crash or partition
  behavior explicitly.
- Test exact placement, soft fallback, migration/admission races, consecutive
  exclusive calls on different Rayon workers, parallel readers, exclusive
  writers, cancellation, and lock/admission cleanup.

## Deployment assumptions

- The initial fixed-membership runtime trusts its configured participants. Do
  not add TLS, authentication, authorization, ACLs, sandboxing, capability
  security, or hostile-peer frameworks without a new concrete requirement.
- Keep protocol validation, run/version/registry checks, checked lengths, and
  resource limits for correctness and memory safety; do not remove them in the
  name of avoiding security overengineering.

## Provenance

- Hataori's future architecture is informed by HPX concepts, including parcels,
  actions, futures, locality scheduling, and AGAS. Credit HPX in durable design
  documentation and distinguish conceptual influence from copied or translated
  implementation.
- Do not translate HPX source code closely without recording the affected files,
  copyright, license obligations, and derivation notice at implementation time.
