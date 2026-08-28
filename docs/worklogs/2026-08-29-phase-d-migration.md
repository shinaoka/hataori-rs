# Phase D explicit migration worklog

## Scope

Implemented Issue #14 Phase D in the unpublished `hataori-runtime` crate. The
change is limited to explicit object migration and its Phase C seams. Phase E
algorithms, automatic placement policy, replication, crash recovery, direct
transport streaming, and concrete GPU/tensor adapters remain deferred.

## Sources read

- Issue #14 and `docs/design/distributed-runtime.md`
- Phase C object and placement contracts and acceptance ledger
- current object registry/store, admission, lease/root, resolver, pending,
  deduplication, runtime progress/shutdown, TCP smoke, and MPI smoke code
- latest online tensor4all common repository, docs/tests, common performance,
  Rust index, and Rust performance rules

HPX remains conceptual provenance only; no third-party implementation source was
read, copied, or translated for this change.

## Decisions

- Keep one stable creator-locality directory authority. It is internal routing
  metadata and is not encoded in `ObjectId`.
- Reuse Phase B typed actions/futures and Phase A segmented parcels for the six
  freeze/prepare/commit/activate/retire/rollback steps. The initial snapshot is
  relayed by the initiating locality; no second bulk subsystem was added.
- Use `MobileObject` with one logical snapshot and destination `RestoreContext`.
  `Migratable` and `Reconstructible` differ in user restore intent, not in a
  duplicated runtime state machine.
- Freeze is a write admitted after readers/writers and placement tickets. No
  Rayon worker waits on object admission or a migration lock.
- Directory commit is the rollback boundary. Pre-commit failures drive explicit
  rollback. Post-commit activation/retirement failure marks the runtime failed.
- `Moved` is a structured response. Redirect releases the stale locality's
  running dedup entry, then retries with the same request ID; this also handles
  an object moving back to a locality that previously returned `Moved`.
- A migration future retains the shared locality lease. Authority metadata
  queues bounded retirement of the remote resident after the final root/lease
  disappears.
- Prepared slots, frozen admission, forwarders, snapshots, redirects, active
  migrations, and retirement requests are bounded and observable.

Rejected as unnecessary: consensus, replication, direct source-to-destination
DMA, a separate migration executor, unrestricted strong-handle serialization,
automatic rebalance policy, and backend-specific migration APIs.

## Correctness fixes found during preflight

- Prevented the destination prepared entry from being collected in the small
  activation window by serializing lifecycle activation and collection.
- Routed same-locality internal requests through the same object admission path
  as remote requests, and wrapped hard/preferred colocated actions with an
  epoch-checked object envelope so destination-owned placement tickets close
  the resolve/submit migration race without an extra control round trip.
- Prevented migration from racing hard-colocation tickets.
- Added TTL recovery for a dropped/cancelled freeze and prepared state.
- Fixed same-locality cross-domain migration so source retirement cannot delete
  the newly activated state.
- Preserved remote collection through an authority-owned bounded retirement
  queue and idempotent destination retirement.
- Made object fast paths consult the resolver before trusting a handle hint.
- Released dedup state for redirects so same-request retry can execute after an
  object returns to the same locality.
- Made snapshot byte/segment limits fit negotiated protocol limits including
  the migration header.

## Verification

Canonical command: `scripts/check-phase-d-migration.sh`.

The lane covers default/MPI unit tests, clippy `-D warnings`, rustdoc, Rust
1.85, source boundary scans, TCP migration/reuse, and MPI migration/reuse at
world sizes 1, 2, and 4. Focused tests cover repeated migration, same-locality
domain movement, roots/leases/collection, reconstructible resources,
restore-failure rollback, dropped futures, placement-ticket drain, structured
redirect retry with one request ID, malformed packets, snapshot limits, and a
segmented snapshot above 1 MiB. Phase A-C, P0/P1, manifest, and rendered-doc
regressions are run before commit.

The selected `reviewer-flash` timed out twice before completing the design and
issued no verdict. No implementation was delegated; the absence of an external
verdict is recorded explicitly in `docs/review-log.md`.

## Remaining risks and boundary

The coordinator relay adds one extra network leg when the initiator is neither
source nor destination. This is bounded and correct but not claimed as a
zero-copy optimization. Loss of directory authority or an ambiguous failure
after commit remains a run-level failure. Concrete provider/device adapters and
automatic migration require separate Phase E-or-later designs and hardware
evidence.
