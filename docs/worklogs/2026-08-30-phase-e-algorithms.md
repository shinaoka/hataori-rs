# Phase E algorithms and facade work log

**Tracking:** [hataori-rs#14](https://github.com/shinaoka/hataori-rs/issues/14)

## Contract and review

`docs/design/phase-e-algorithms.md` fixes the Phase E boundary: one canonical
bounded batch controller, action/future collectives, direct async/blocking
facades, per-batch placement, and no automatic migration. The user-selected
`reviewer-flash` reviewer returned **Correct-to-merge** after one incomplete
budget-limited attempt and one focused continuation. The exact record is in
`docs/review-log.md`.

## Implementation decisions

- Added a separate unpublished `hataori-algorithms` layer rather than putting
  algorithm policy into transport or runtime lifecycle code.
- Extended registered actions with a checked type-erased local move path. Local
  typed work retains the same domain admission, pending promise, cancellation,
  and completion path while avoiding `WireValue` and transport calls.
- Kept encoded local requests for object/control internals that already own
  protocol segments; the fast path is selected only for ordinary typed actions.
- Kept P0/P1 source available. Replacement is forbidden until the immutable
  performance manifest passes on a valid measurement host.
- Deferred barriers, reductions, per-item affinity, native collectives, and
  automatic migration because Issue #14 does not require them for the frozen
  Phase E case set and no measured workload justifies their policy cost.

## Performance-host status

`scripts/check-phase-e-host.py` records the frozen manifest digest and required
host observations and fails closed. The current host is **INCONCLUSIVE** before
candidate timing: its CPU governor is `schedutil`, and required network/thread/
provider declarations are unset. This is not relabeled as a pass and no
performance claim is made from local correctness runs.

## Verification

- `scripts/check-phase-e-algorithms.sh`: PASS, including runtime and algorithm
  tests, clippy, rustdoc, Rust 1.85, top-level facade check, TCP smoke, and MPI
  smoke at 1/2/4 localities.
- The algorithm integration suite proves local zero-codec dispatch, remote
  segmented codec use, ordered multi-batch results, batch-granular counters,
  and action/future collectives.
- `scripts/build_docs_site.sh`: PASS.
- `scripts/check-phase-e-host.py`: expected fail-closed `INCONCLUSIVE` on this
  host; no candidate timing or performance acceptance claim was made.
