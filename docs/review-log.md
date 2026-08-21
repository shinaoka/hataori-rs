# Hataori Review Gate Log

## Design input — Blackboard Round 3

- Artifact: `/home/shinaoka/tensor4all/.pi-meetings/2026-08-21-hataori_p0_实现就绪审查/transcript.md`
- SHA-256: `6cc95faf9a7f919297285109c36441c27e923a82263534b4179859c12109f436`
- Participants: GPT, DeepSeek, Researcher
- Result: design input only; not a formal pre-implementation reviewer verdict
- Incorporated findings:
  - root `in_place_scope` mechanism and event-loop fairness;
  - whole-domain `Inner` without partial budgets;
  - collective preflight, private communicator, framing, and state transitions;
  - signed deterministic error reduction;
  - non-Linux affinity behavior;
  - exact `rsmpi-rt` pin and validation matrix;
  - explicit deferred work to prevent premature abstraction.

## Pre-implementation design review

### Round 1

- Artifacts: `docs/design.md`, `docs/implementation-readiness.md`, `docs/review-log.md`, `README.md`
- Reviewer selected by user: `reviewer-flash-opencode-go` (DeepSeek family, read-only)
- Verdict: **Correct-to-merge**
- Important non-blocking findings:
  1. define the preflight no-error sentinel;
  2. clarify that `Communicator` is the selected rsmpi trait and enumerate its used surface;
  3. pin upstream rsmpi and disable its optional/default features.
- Minor findings:
  1. define error-key overflow behavior;
  2. define local-map error stopping behavior;
  3. extend same-rank codec bypass to placement helpers;
  4. correct meeting transcript provenance.
- Fixes: all findings incorporated into the reviewed artifacts immediately after Round 1.

### Round 2

- Reviewer: `reviewer-flash-opencode-go` (same retained review role, read-only)
- Scope: exact document state after all Round 1 fixes
- Verdict: **Correct-to-merge**
- Blocking findings: none
- Non-blocking findings fixed after Round 2:
  1. retitle the combined findings table;
  2. require valid error keys to be strictly less than the reserved `i64::MAX` sentinel;
  3. add explicit error-key-overflow, local-map error-semantics, and all-helper codec-bypass test gates;
  4. record the external transcript SHA-256 and ensure new review artifacts remain visible in the working-tree status.

### Round 3 — final exact-state follow-up

- Reviewer: `reviewer-flash-opencode-go` (read-only)
- Scope: exact document state after every Round 2 non-blocking fix
- Verified: combined findings title, strict `< i64::MAX` sentinel, typed overflow/collision preflight failure, explicit acceptance gates, transcript provenance, and absence of introduced contradiction or overengineering
- Blocking findings: none
- Verdict: **Correct-to-merge**
- Gate status: **COMPLETE — core implementation may start**

## Implementation task reviews

Each independently mergeable implementation step in `docs/implementation-readiness.md` receives its own post-diff review by the selected different-family reviewer.

### Task #9 — crate and feature matrix

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable)
- Scope: `.gitignore`, `Cargo.toml`, minimal `src/lib.rs`, and related status/evidence documentation
- Pre-review refinements applied: defer `libc` to the affinity task; ignore library `Cargo.lock`; keep distinct MPI dependency keys; assert the exact exclusivity diagnostic; enumerate valid feature sets instead of `--all-features`.
- Integration corrections: set honest MSRV 1.85 after dependency evidence; disable bincode's unused derive default; make backend aliases mutually exclusive so the both-feature build reports only Hataori's intended error; ignore local `.codegraph/` state.
- Verification: default, Rayon, upstream MPI, runtime MPI, and both hybrid checks/tests/clippy/docs passed; Rust 1.85.0 check passed for all six valid feature sets; default and Rayon dependency boundaries passed; no tenferro/tensor4all dependency; exact mutual-exclusion diagnostic passed. Upstream MPI on this host required `BINDGEN_EXTRA_CLANG_ARGS=-I$(gcc -print-file-name=include)` because the installed libclang lacks builtin `stddef.h` discovery.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after initial tool-budget stop)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Non-blocking deferral: choose and add project license metadata/files before any crates.io publication; Task #9 does not publish.

### Task #10 — serial `map`

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable)
- Scope: unconditional serial `map`, non-generic `MapError`, bounded UTF-8 message handling, rustdoc, and focused tests
- Pre-review refinements applied: derive `Debug`; fix accessor signatures; document zero-based/stop/drop/bounds behavior; avoid post-1.85 UTF-8 APIs; test exactly-once, boundary straddle, and non-`Send` borrowed types/state.
- Integration verification: default tests and doctest pass on current and Rust 1.85; all feature-set gates are run before push.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Non-blocking note: the crate-visible 4096-byte cap is retained for later `PmapError` reuse.

### Task #11 — `Domain` and RAII admission

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after timeout)
- Pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable)
- Scope: dependency-free `DomainId`, `Place`, sequential `Domain`, typed validation errors, and nonblocking RAII `DomainAdmission`
- Pre-review refinements applied: name `NegativeRank`; expose only `DomainId::get`; derive `Debug`; omit speculative guard accessors/auto-trait controls; defer the MPI pmap reentrancy guard to the first pmap task.
- Integration verification: ID/Place validation, sequential facts, nested contention, success/error/unwind release, deterministic scoped cross-thread contention, and runnable rustdoc are covered; all feature-set gates run before push.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none

### Task #12 — Rayon local execution and affinity

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Pre-implementation Round 1 verdict: **Changes requested**
- Required design fixes: return typed `MapInError`; make `Outer` fully evaluate for deterministic lowest-index errors; wait for every non-panicking managed worker start hook via a bounded channel; do not expose the raw `Arc<ThreadPool>`.
- Fixes applied: `docs/design.md` §§3/5/6/15 and readiness Steps/tests now define the error split and precedence, deterministic Outer contract, bounded worker-start reporting, and no raw-pool API.
- Follow-up pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Follow-up pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable; continued after bounded timeouts)
- Scope: `LocalMode`, typed `MapInError`, explicit-pool `map_in`, managed/external domains, Linux pinning, non-Linux declared placement, constructor and execution tests
- Integration corrections: split start-handler CPU ownership from report validation; use one total startup deadline; import only required Rayon iterator traits; preserve raw pool access as crate-private only.
- Integration verification: 28 Rayon unit tests and 3 doctests pass; all six feature-set checks/tests and Rust 1.85 Rayon checks pass; full lint/doc matrix is run before push.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Non-blocking notes: defensive unreachable worker-index sentinel and the deterministic Outer second pass are retained as simple, safe implementation choices.

### Task #13 — wire codec and signed error keys

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after timeout)
- Pre-implementation verdict: **Correct-to-merge**
- Scope: MPI-feature-gated fixed header/codec/conversion primitives and deterministic signed error-key arithmetic; no MPI calls, scheduler, transport trait, or same-rank helper
- Pre-review refinements applied: use a 28-byte header without speculative reserved bytes; keep wire/error types crate-private until a public `PmapError` consumer exists; document status as a checked tag with payload schemas owned by later consumers; defer collective-helper message kinds.
- Implementer: Luna (high, write-capable; parent completed integration after bounded timeout)
- Scope: 28-byte checked header, bincode2 fixed/LE/full-consumption codec, conversion bounds, signed error keys and preflight candidates
- Integration corrections: fixed the exact-overflow boundary assertion; added a narrowly reasoned module-level dead-code allowance because this independently pushed primitive slice precedes its Task #14/#15 consumers. The allowance must be removed when those consumers land.
- Integration verification: 9 wire tests plus all existing tests pass under the runtime MPI feature; full feature/lint/doc matrix runs before push.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Non-blocking coverage notes (world-size decode guard and oversized-header decode branch) are deferred to the final Task #19 coverage audit; shared validation paths are already covered.
