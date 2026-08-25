# Immutable legacy performance runner work log

**Tracking:** [hataori-rs#14](https://github.com/shinaoka/hataori-rs/issues/14), Phase A-1

## Summary

Added a standalone release-mode runner for the immutable P0/P1 reference commit
`34cb1b1371c8b2f8ef750e2d49d10f9ef8f0782e`. It executes raw `map`, `map_in`,
`pmap`, broadcast, scatter, and gather observations without modifying the
reference source or claiming a performance result.

## Decisions

- Kept the runner outside the main workspace and pinned both its Hataori git
  dependency and lockfile to the full accepted commit.
- Used one hand-written CLI and TSV output instead of adding a benchmark or CLI
  framework.
- Generated fresh owned inputs outside each timed region and validated ordered
  output, byte counts, and a value-dependent checksum before reporting.
- Used managed Rayon domains for `map_in`. Hybrid MPI uses an explicit external
  Rayon pool: pinning every local test rank to CPU 0 stalled the two-rank local
  smoke, while the external pool preserves the requested worker count without
  imposing a false shared-host affinity policy.
- Added process-group timeouts to every smoke execution. A manual hybrid probe
  without that watchdog hung and was terminated; no unbounded MPI launch is
  retained in the checked script.

## Verification

- `scripts/check-legacy-runner.sh` passed: exact manifest/lock pin, invalid CLI,
  serial map, all three Rayon modes, MPI `pmap` and placement helpers at world
  sizes one and two, and two-rank prefetched hybrid `pmap`.
- Clippy with `-D warnings` passed for default, `rayon`, `mpi`, and
  `mpi,rayon` feature sets.
- The fixed dependency resolved to the full accepted git source revision.

## Review gate

The design in `docs/design/legacy-performance-runner.md` received a
**Correct-to-merge** pre-implementation verdict from the user-selected
read-only `reviewer-flash`; `docs/review-log.md` records the gate.

## Remaining scope

This change does not freeze the complete experiment manifest, host validity
rules, paired interleaving, statistics, or thresholds. Candidate measurements
and runtime/protocol implementation remain blocked until that next Phase A
slice lands.
