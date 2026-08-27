# Phase C fixed-placement objects worklog

## Summary

Implemented Issue #14 Phase C in the unpublished `hataori-runtime` layer while
keeping Phase A transport and Phase B request/future semantics canonical.
Objects are pinned; identity/location and epoch routing are ready for Phase D,
but no migration behavior is claimed.

## Sources read

- Issue #14 and `docs/design/distributed-runtime.md` sections 10-14;
- `docs/design/object-placement-api.md` and the Phase B runtime contract;
- current Phase B action, pending, domain, wire, progress, and shutdown paths;
- current shared tensor4all repository, docs/test, Rust, and performance rules.

No third-party object-runtime source was copied or translated. HPX remains the
conceptual reference recorded in the parent design.

## Decisions

- Kept the object service in `hataori-runtime` as a distinct module rather than
  adding another crate before a publication boundary requires one.
- Added logical object IDs to the foundation protocol; routing always uses a
  separate checked `ObjectLocation` and observed epoch.
- Reused the Phase B request, pending-future, cancellation, deduplication,
  response-backpressure, and transport paths. Object factories/actions install
  erased handlers only after the object-registry handshake is sealed.
- Used a direct typed same-locality path so state and action codecs and transport
  are mechanically skipped. Remote creation encodes state once; calls never
  encode resident state.
- Backed runtime domains with runtime-owned Rayon pools. Remote object admission
  happens before Rayon submission. Per-object FIFO admission permits front
  readers concurrently, prevents readers bypassing a queued writer, and never
  parks a Rayon worker waiting for admission.
- Used one locality lease shared by local `Remote` clones. Last drop queues a
  release; owner progress renews live handles; TTL plus suspect grace handles
  lost release/peer failure. `WeakRemote` acquisition and destination-specific
  non-serializable transfer tokens are explicit.
- A transfer establishes the destination lease before returning its token, so
  collection cannot race token delivery. Import validates the destination and
  acknowledges the bounded transfer state; abandoned tokens release through
  normal control traffic or lease expiry.
- Pinned hard colocation retains the lease and one bounded placement ticket for
  the task future. Soft affinity performs one preferred attempt and at most one
  declared fallback; no affinity scheduler or retry queue was added.
- Explicit shutdown invalidates local leases, drains admitted/mailbox work,
  clears roots/directory/cache, and retains the Phase B transport leak gate.

## Verification

The Phase C lane covers default/MPI unit tests, clippy `-D warnings`, rustdoc,
Rust 1.85, TCP reuse, and MPI world sizes 1/2/4. Tests include identity,
registry sealing, exact placement, local no-codec/no-transport operation,
remote typed calls, 1 MiB+ state, resolver/stale epoch, admission fairness,
parallel readers, saturated writers, roots, clone-shared leases, weak upgrade,
transfer/import, collection, deadline/cancellation/error paths, peer failure,
and clean shutdown.

Canonical command: `scripts/check-phase-c-objects.sh`.

The selected `reviewer-flash` pre-implementation consultation exceeded its
60-second watchdog before reading the complete design and issued no verdict;
it is not recorded as gate credit. Implementation remained parent-owned. The
parent preflight found and fixed object-mailbox wakeup, shutdown/local-call,
cross-registry ID collision, and repeated resolver-lock issues before the final
lane.

## Deferred boundary

Phase D owns migration snapshots, destination preparation, directory commit,
forwarding, rollback, and reconstructible resources. Phase E owns object-aware
algorithms and performance promotion against the frozen legacy manifest.
