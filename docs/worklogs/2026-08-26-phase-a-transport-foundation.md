# Phase A protocol and transport foundation work log

**Tracking:** [hataori-rs#14](https://github.com/shinaoka/hataori-rs/issues/14)

## Summary

Implemented the Phase A protocol and production transport foundation in the
unpublished `hataori-runtime-foundation` workspace crate without changing the
existing P0/P1 API or measured path. The crate owns transport-independent
identities, handshake/framing, segmented parcels, a shared bounded transport
contract, deterministic memory fault injection, TCP rendezvous/full-mesh
progress, and MPI chunked progress.

## Decisions

- Kept the first increment in one unpublished crate with strict module
  ownership. Splitting modules into publishable crates is mechanical and does
  not justify extra dependency or release work now.
- Used checked fixed-width codecs and standard-library queues/sockets; no async,
  serialization, CLI, or benchmark framework was added.
- Kept payloads segmented in the common model. TCP uses one bounded stream per
  logical channel, and every backend reserves negotiated per-peer bytes for
  control that action/bulk traffic cannot consume. MPI copies a
  bounded encoded parcel into 4 KiB backend-private chunks so control progress
  can occur between bulk chunks.
- Did not require transport drivers to be `Send`. The MPI driver owns its
  duplicated communicator and checks `MPI_Is_thread_main`; no unsafe
  thread-safety implementation exists.
- Added a submission gate so explicit shutdown atomically rejects new handle
  submissions before flush.
- Treated TCP EOF/I/O and malformed frames as visible typed run failures and
  released queued/write/read state before shutdown. MPI process loss remains a
  run-level failure, consistent with the accepted design.
- Used one shared conformance assertion module for memory, TCP, and MPI evidence
  rather than duplicating success semantics in each backend.

## Verification

`scripts/check-runtime-foundation.sh` passed:

- pure protocol/handshake/malformed-frame and 1 MiB segmented tests;
- deterministic memory pass, delay, duplicate, loss, reorder, saturation, and
  disconnect traces plus zero-retention shutdown;
- TCP wrong-run rendezvous/handshake, fixed membership at four localities,
  control/action/bulk delivery, count/byte backpressure, oversized-frame
  rejection, peer failure cleanup, pending flush, 1 MiB segmented transfer,
  clean shutdown, and process reuse;
- the same completion/incoming conformance assertions for all three backends;
- MPI pure chunk validation and `mpiexec` world sizes 1, 2, and 4, including
  1 MiB multi-chunk transfer, control progress, flush-retained events,
  communicator reuse, thread-main ownership, and zero retained resources;
- default/MPI clippy with `-D warnings`, rustdoc, Rust 1.85 checks, backend leak
  scanners, and absence of unsafe `Send`/`Sync` implementations.

Every backend launch in the acceptance script has a process-group watchdog.

## Deferred beyond Phase A

Runtime lifecycle, scopes, actions/futures, pending promises, deduplication,
remote objects, leases, placement, migration, rebuilt algorithms, and the sugar
facade remain Phase B or later. The Phase A parcel kind is intentionally
transport-level; action completion is not inferred from transport completion.
