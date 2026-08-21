# Phase 20a tenferro adapter acceptance ledger

Run the complete Phase 20a gate from the repository root:

```bash
scripts/check-tenferro.sh
```

| Contract | Evidence |
|---|---|
| Core default and Rayon builds contain no tenferro/tensor4all | root `cargo tree` assertions |
| Adapter pins merged tenferro PR #1717 and contains no tensor4all | adapter lock/tree assertions |
| Core retains Rust 1.85; adapter follows tenferro Rust 1.96 | separate toolchain checks |
| Exact Hataori Rayon pool and full budget are retained | adapter diagnostics and pool start counter |
| Caller-managed Faer, no placement, caller-owned shutdown | integration diagnostics |
| Known tensor operation executes on the supplied named team | known-value add plus worker-name census |
| Global Rayon and extra pools stay unused | global sentinel and fixed start count |
| Foreign entry runs no callback | typed `OutsideDomainPool` test |
| Hataori busy path is nonblocking | held-admission integration test |
| Callback error and unwind release both guards | failure then successful reuse tests |
| Outer cannot reach tenferro's collision panic | deterministic overlap, typed `ConcurrentEntry`, reuse |
| Borrowed values require no `'static` bound | borrowed input/output integration test |
| Public docs remain runnable | adapter and core doctests |

The exact upstream caller-managed implementation separately verifies that
`ResourceArbiter` is bypassed, all native parallel work uses the retained Rayon
team, provider incompatibility is rejected before execution, backend clones
share the entry guard, and unwind releases it. Hataori does not duplicate those
internal tests.

This ledger claims no tensor4all context, tensor wire, MPI tensor reconstruction,
or three-repository integration behavior; those belong to Phases 20b and 20c.
