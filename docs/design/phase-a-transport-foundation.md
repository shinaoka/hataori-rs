# Phase A protocol and transport foundation

## Status

Implemented contract for Issue #14 Phase A. This document covers protocol,
transport, and backend lifecycle only; runtime scheduling, actions/futures,
remote objects, migration, and rebuilt algorithms remain later phases.

## Incremental crate boundary

Phase A introduces the unpublished workspace crate
`hataori-runtime-foundation`. It contains separate `protocol`, `transport`,
`memory`, `tcp`, and feature-gated `mpi` modules. This is an incremental crate
layout permitted by the distributed-runtime design: module dependency direction
is fixed now, while splitting backend modules into publishable crates remains a
mechanical later change.

The existing `hataori` P0/P1 crate does not depend on this crate. Its public API,
feature trees, wire protocol, and measured paths remain unchanged.

## Protocol

The protocol owns transport-independent `RunId`, `LocalityId`, `MessageId`,
`Channel`, parcel kind, handshake, limits, and segmented payload representation.
IDs use checked constructors where zero is reserved. A parcel names logical
source and destination only.

Every peer connection exchanges a fixed-size hello containing:

- magic and protocol version;
- run identity and runtime version;
- action and object registry hashes;
- capability bits;
- advertised frame, payload, segment, queue, and in-flight-byte limits.

A mismatch is typed. Effective limits are the checked component-wise minimum;
zero or internally inconsistent limits are rejected.

Parcel framing validates magic, version, channel, kind, source, destination,
message ID, segment count, per-segment length, total payload length, checked
integer conversions, and configured limits before allocation or dispatch.
Payloads remain `Vec<Vec<u8>>`; codecs may copy into bounded backend frames but
no public contract requires one unbounded contiguous payload.

## Common transport contract

A cloneable `TransportHandle: Send + Sync` performs bounded nonblocking
submission and returns a `SendTicket`. A locality-owned driver performs
progress, emits bounded events, flushes, reports statistics, and shuts down.
The driver is deliberately not required to be `Send`, so MPI resources never
need unsafe thread-safety wrappers.

Events distinguish:

- local send completion, after backend-owned bytes may be released;
- incoming parcel acceptance;
- visible peer failure;
- shutdown completion.

No event implies action completion. Control, action, and bulk submissions have
independent bounded queues, progress services control before action before
bulk, and every negotiated per-peer byte budget reserves explicit capacity for
control traffic that action/bulk submissions cannot consume. Every queue and
retained read/write/reassembly buffer has count and byte limits exposed in
stats.

## Deterministic memory backend

The private memory backend builds fixed virtual membership with bounded shared
queues. A deterministic per-send fault script supports pass, delay,
duplication, loss, reordering, saturation, and disconnect. Tests cover exact
event traces, stale/wrong-run frames, queue/byte exhaustion, and cleanup to zero
retained resources.

## TCP backend

TCP bootstrap first uses a bounded coordinator endpoint. It validates the
`RunId`, expected locality count, unique declared endpoints, message lengths,
and a deadline; assigns logical IDs; and returns one identical fixed-membership
table to every participant. Every locality then binds its declared transport
endpoint; higher locality IDs connect to lower IDs, and all peers exchange and
validate hello records. Sockets and endpoints stay inside the backend.

Each stream uses bounded length-prefixed protocol frames, partial read/write
state, nonblocking progress, per-channel outbound queues, checked inbound
allocation, visible EOF/error, explicit flush, and shutdown. Bootstrap and
progress have deadlines. Tests use loopback endpoints and process-group-like
wall-clock watchdogs, including reconnecting a fresh run after clean shutdown.

## MPI backend

The MPI backend maps ranks to logical localities only during bootstrap and owns
a duplicated communicator. The common protocol/runtime layers never store MPI
ranks, communicators, requests, or tags. The driver is called only from the MPI
initialization thread and adds no unsafe `Send`/`Sync` implementation.

Large encoded parcels are split into bounded transport chunks and reassembled
under configured count/byte limits. Logical channels map to backend-private
tags. Tests run the same contract at world sizes one, two, and four, transfer at
least 1 MiB over multiple chunks, verify thread-main ownership, flush/shutdown,
and communicator reuse.

## Acceptance

Phase A transport work is accepted only when:

- pure protocol and handshake tests reject every checked malformed field;
- memory, TCP, and MPI pass the same backend-independent contract assertions;
- deterministic memory faults cover delay, duplication, loss, reordering,
  saturation, and disconnect without unbounded retention;
- TCP and MPI transfer a checksum-verified payload of at least 1 MiB through
  multiple segments/chunks;
- control traffic progresses under bounded bulk saturation;
- success, protocol error, peer failure, flush, and shutdown release all
  queues, partial frames, chunks, and peer resources;
- a new backend instance can reuse the process/communicator after clean
  shutdown;
- format, clippy, MSRV, docs, feature-boundary, and existing P0/P1 acceptance
  checks pass without changing the legacy fast path.
