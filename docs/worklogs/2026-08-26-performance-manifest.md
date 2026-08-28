# Phase A performance manifest work log

**Tracking:** [hataori-rs#14](https://github.com/shinaoka/hataori-rs/issues/14)

## Summary

Froze the performance experiment contract before any future-runtime candidate
measurement. The machine-readable manifest fixes the immutable legacy commit,
case families, host validity observations, repetitions, paired statistics,
noise tolerance, acceptance thresholds, instrumentation gates, and TCP targets.

## Decisions

- Used TOML plus a standard-library Python validator; no benchmark framework or
  statistics dependency was added.
- Declared case matrices rather than expanding thousands of individual rows.
  The dimensions are immutable experiment inputs, not candidate-selected cases.
- Kept world size eight conditional only on a declared host with eight valid
  slots; missing required host observations classify the whole experiment as
  `INCONCLUSIVE`.
- Fixed 30 calibration pairs and 30 paired repetitions, natural-log paired
  ratios, a one-sided 95% upper bound, and a baseline-noise tolerance capped at
  2%. Selective retries and outlier removal are forbidden.
- Included future-runtime, TCP, same-locality, placement, concurrency, and
  cleanup cases without claiming those paths are implemented.
- Declared conservative loopback and 1-GbE-or-better TCP latency, throughput,
  and four-locality scaling targets before candidate results exist.

## Verification

`scripts/check-performance-manifest.py` validates the exact baseline, hard
statistics and zero-overhead gates, required host metadata, TCP targets, unique
case set, MPI/Rayon dimensions, 1 MiB payload coverage, object placement and
concurrency paths, and cleanup outcomes. It prints the manifest SHA-256 for use
in future reports and is invoked by `scripts/check-legacy-runner.sh` in CI.

## Remaining risks

The future experiment orchestrator must expand the matrices without omission,
record the selected host profile and manifest digest, and fail closed if its
candidate runner cannot execute every selected comparison lane. No candidate
measurement is permitted before that capability exists.
