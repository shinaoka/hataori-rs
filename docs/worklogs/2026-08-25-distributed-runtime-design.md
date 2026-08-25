# Distributed runtime design work log

**Date:** 2026-08-25

**Scope:** Establish a long-term Hataori architecture for MPI/TCP transport,
typed asynchronous execution, migration-ready remote objects, and mechanical
resource cleanup. Add repository routing to the shared tensor4all agent rules.

**Tracking:** [hataori-rs#14](https://github.com/shinaoka/hataori-rs/issues/14)

## Session summary

Documented a clean-break evolution from the implemented synchronous P0/P1
engine to a long-lived distributed runtime. The resulting design fixes the
layer boundaries, common MPI/TCP contract, identity and location model, remote
object lifetime rules, migration state machine, bounded-resource policy, and
phased acceptance gates without claiming that those facilities are implemented.
The repository now follows the same shared-rule routing pattern as tenferro-rs.
A follow-up added a supported current-style facade and made performance
continuity with the exact current implementation a blocking Phase E gate.
Another follow-up specified exact remote-object placement, object-colocated task
affinity, and synchronized access from the owning domain's Rayon workers while
keeping security mechanisms outside the trusted initial runtime.

## Context reviewed

- current Hataori P0/P1 design and implementation, especially `Domain`,
  `Coordinator`, `pmap`, placement helpers, wire framing, and bounded prefetch;
- current facade exports and examples for `map`, `map_in`, `pmap`,
  `PmapOptions`, `LocalMode`, `broadcast`, `scatter`, and `gather`;
- current scheduling fast paths: batch-granular remote work, root-local owned
  moves, ordered result storage, and same-rank serialization bypass;
- current π and Mandelbrot MPI/hybrid examples used as initial performance
  workload seeds;
- current logical `Place = (rank, domain_id)` boundary and same-rank owned-move
  path used to shape future transport-independent placement;
- current README status and Cargo feature/dependency boundaries;
- `tenferro-rs/AGENTS.md` and its shared-rule routing pattern;
- shared tensor4all common repository, performance, documentation/testing,
  provenance, and Rust rules;
- HPX documentation for runtime resources, actions/futures, AGAS/components,
  and parcel transports.

## Decisions

- Treat the future runtime as a clean architecture rather than a compatibility
  extension of synchronous `pmap`.
- Keep MPI and TCP as the first production transports; retain memory transport
  only as a private deterministic test facility.
- Separate bootstrap, membership, transport handle/driver, protocol, runtime,
  actions/futures, remote objects, and algorithms.
- Make remote object identity location-independent from its first
  implementation, even while placement is initially fixed.
- Use locality leases, RAII guards, deadlines, quotas, weak persistent edges,
  and observable shutdown to prevent resource leaks mechanically.
- Preserve future migration through versioned locations, epochs, explicit
  freeze/prepare/commit/activate/forward transitions, and mobility capability
  classification.
- Keep tensor/runtime/provider execution resources out of migratable state and
  out of the transport-independent core.
- Route repository agents through `tensor4all-agent-rules` without vendoring
  shared policy.
- Add a supported facade that preserves current Hataori vocabulary and concise
  usage while using registered typed actions for distributed execution.
- Lower sugar directly to the canonical batch controller, excluding additional
  copies, serialization, hops, queues, per-item futures, and object leases.
- Fix legacy performance comparison at commit
  `34cb1b1371c8b2f8ef750e2d49d10f9ef8f0782e`; require paired legacy-continuity
  and sugar-transparency gates before Phase E can replace P0/P1.
- Treat a failed or inconclusive required performance case as blocking, with a
  baseline-calibrated measurement tolerance capped at 2% rather than an
  allowable slowdown budget.
- Permit exact object creation at `(LocalityId, DomainId)`, runtime-owned roots,
  hard colocation, and bounded soft affinity with explicit fallback.
- Keep default object access exclusive; add opt-in read/write admission for
  parallel Rayon readers and exclusive writers without blocking workers on
  lock acquisition.
- Treat the initial fixed-membership deployment as trusted and omit TLS,
  authentication, authorization, ACL, sandbox, and hostile-peer frameworks
  while retaining protocol and allocation validation for memory safety.

## Alternatives rejected or deferred

- Extending the existing call-scoped root coordinator into the global runtime
  scheduler was rejected because it would make all runtime work collective and
  root-centric.
- Preserving the collective `Option<Vec<T>>`/`Option<Vec<U>>` facade exactly was
  rejected: the long-lived runtime lets one locality initiate work while peers
  service registered actions. The facade preserves the mental model instead.
- Sending an arbitrary closure through distributed sugar was rejected because
  Rust closures are not a stable portable wire/executable format; local sugar
  may still borrow closures.
- Measuring only the new facade against its own primitive was rejected because
  that could hide a runtime-wide regression relative to current Hataori.
- Exposing raw shared-memory pointers or locks across localities was rejected;
  cross-node access remains a typed object action.
- A global multi-object placement optimizer and security framework were
  rejected as initial overengineering.
- Encoding the home rank/locality in `ObjectId` was rejected because it makes
  migration leak physical placement into public identity.
- Distributed reference counting per local handle was rejected in favor of one
  locality lease shared by local clones, with expiry as an abnormal-cleanup
  backstop.
- LCI, libfabric, RDMA, dynamic membership, process-failure recovery,
  distributed cycle collection, and automatic migration remain deferred until
  a concrete workload and failure contract justify them.

## Provenance

HPX was used as a conceptual reference through its public documentation. No HPX
source code was copied or translated in this documentation change.

## Verification

- The relevant online shared-rule files were compared with the sibling
  checkout; the two differing files were read from the online source.
- All local Markdown links in the changed and added documents resolve.
- Code fences are balanced, and the final diff was checked for whitespace
  errors.
- No Rust source or executable behavior changed, so Rust tests were not needed.

## Remaining risks

- Exact async executor, MPI thread-level ownership, TCP event loop, directory
  implementation, lease timing, and snapshot compatibility remain phase-local
  decisions.
- The complete performance case manifest, verification hardware lanes, and TCP
  absolute targets must be frozen before implementation changes the measured
  P0/P1 path; candidate data cannot be used to choose them.
- The private reader/writer admission primitive and fairness policy remain an
  implementation decision, but must not park Rayon workers while waiting.
- Lease expiry under long progress stalls requires conservative defaults and
  fault-injection evidence before implementation claims safe collection.
- Crash-tolerant directory authority and post-commit migration recovery are not
  part of the initial fixed-membership runtime.
