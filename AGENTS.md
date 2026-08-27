# AGENTS.md

Before acting, read the latest shared tensor4all agent rules from the
[`tensor4all-agent-rules`](https://github.com/tensor4all/tensor4all-agent-rules)
repository online. Start from:

- `https://github.com/tensor4all/tensor4all-agent-rules/blob/main/rules/index.md`

If internet access is unavailable or the remote cannot be resolved, use the
sibling checkout:

- `../tensor4all-agent-rules/rules/index.md`

Load only the common, Rust, performance, numerical, docs, benchmark, or agent
consumer rule files relevant to the task. If neither source is available,
continue from this repository's local rules and state that the shared rules
were unavailable when preparing external-facing work.

Then read the Hataori-specific rules:

- `REPOSITORY_RULES.md`

For the user-facing documentation, start from `docs/llms.txt`: it states the
calling conventions shared by every execution model and links each guide,
tutorial, API page, and design document with a one-line description. Keep it
in sync when adding, renaming, or removing pages under `docs/`;
`scripts/check-llms-index.py` enforces that every entry resolves.

The shared rules are not vendored here. This repository is expected to move to
the tensor4all GitHub organization in the future, so cross-repository policy
belongs in `tensor4all-agent-rules`; only Hataori-specific durable constraints
belong in this repository.

## Current status

Hataori's synchronous P0 core and P1 bounded-prefetch extension are implemented.
The unpublished `hataori-runtime-foundation` crate implements Issue #14 Phase A
protocol and transport infrastructure. The unpublished `hataori-runtime` crate
implements Phase B lifecycle, domains, typed actions/futures, scopes,
deadlines, cancellation, bounded deduplication, observability, and shutdown.
Phase C-E object, migration, and algorithm architecture remains forward-looking.
Read:

- `docs/design.md` for the implemented P0 architecture;
- `docs/design/phase-a-transport-foundation.md` for Phase A;
- `docs/design/phase-b-runtime.md` for Phase B;
- `docs/design/distributed-runtime.md` for the complete long-term architecture.

For changes that establish or revise durable architecture, update the design
document and add a concise reviewer-facing work log under `docs/worklogs/`.

## Local command watchdogs

- Every agent-run build, test, smoke, or backend process uses a short
  process-group watchdog: normally at most 30 seconds, and at most 60 seconds
  for MPI or a bounded integration stage. Split a longer matrix into stages;
  do not remove or merely inflate the watchdog. Use `setsid timeout
  --signal=TERM --kill-after=2s` for commands that may leave descendants, and
  verify that no child process remains after a timeout.
- A guard-backed submission permit holds a non-reentrant mutex until it is
  committed or dropped. Tests and production code must not call another gated
  reservation while retaining such a permit: that is a same-thread
  self-deadlock. Commit/drop the first permit before the next reservation, and
  run lock/concurrency tests under the short watchdog above.
