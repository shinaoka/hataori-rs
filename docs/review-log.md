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

### Task #14 — pure scheduler state machine

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Pre-implementation Round 1 verdict: **Changes requested**
- Required design fixes: reject root dispatch while running without mutation; validate success completions even after failure; remove speculative separate `TaskId`; pin internal accessor/error-completion/extraction contracts and deterministic skew metric.
- Fixes applied: P0 uses checked original indices as task keys and batch IDs from zero; root-running dispatch is a state-preserving error; all completion metadata is two-pass validated after failure and successful values are then discarded; readiness tests define exact skew and drain assertions.
- Follow-up pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after timeout)
- Follow-up pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable; parent completed integration/test repair after bounded timeouts)
- Scope: private FIFO coordinator, root/remote capacity-one lanes, checked batches, transactional completions, failure/drain transitions, ordered extraction, deterministic skew tests
- Integration corrections: fixed generic ownership moves, aligned tests to rank-aware completion APIs, replaced an invalid skew trace with real READY/completion scheduling, and removed lint-only test issues.
- Integration verification: 13 scheduler tests pass under the runtime MPI feature; full feature/lint/doc matrix runs before push.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Reviewer-requested coverage added: duplicate and out-of-order completion metadata both return typed errors with state/results unchanged, then a valid retry succeeds.
- `is_quiescent` is retained for Task #15 transport termination and must become a real consumer there.

### Task #15 — upstream MPI-only `pmap`

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after timeout)
- Pre-implementation Round 1 verdict: **Changes requested**
- Required design fixes: reject an agreed root outside the communicator range; release a running lane after corrupt completion metadata using coordinator-pinned batch state rather than the untrusted received batch ID.
- Fixes applied: preflight now validates `0 <= root < world_size`; scheduler/transport contract adds dedicated root/remote protocol-error completion transitions that derive the batch ID from `RunningMeta`, mark failure, clear pending, and return the lane to idle for READY→STOP.
- Follow-up pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; two bounded retries after tool timeouts)
- Follow-up pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable; parent completed production integration after repeated bounded timeouts)
- Scope: public MPI-only `pmap`, private communicator/preflight, blocking worker/root loops, scheduler protocol-error release, signed winner convergence, and MPI smoke example
- Integration corrections: fixed root fairness and failed-result extraction; kept remote candidates owned by reporting workers; winner rank now broadcasts class/key/message; added domain admission/thread-main/root-range preflight; added callback abort path, protocol-error reporting, and scheduler consumer visibility.
- Integration verification: upstream MPI library tests/clippy and mpiexec smoke at n=1/2/4 pass; full matrix runs before push.
- Post-implementation Round 1 verdict: **Changes requested**
- Blocking fix: every post-preflight `root_loop`/`worker_loop` error now calls private-communicator `MPI_Abort(72)` and every convergence-broadcast defensive failure calls `MPI_Abort(73)`, so no rank returns while peers remain in blocking transport/collectives.
- Follow-up post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after tool-budget stop)
- Follow-up post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Non-blocking future hardening: Task #19 must inject malformed frames and verify recoverable in-band paths where promised; unexpected non-folded paths are deliberately abort-only.

### Task #16 — `rsmpi-rt` backend parity

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; retry after timeout)
- Pre-implementation Round 1 verdict: **Changes requested**
- Clarification/fixes: Task #16 is MPI-only runtime parity; Task #17 owns `rsmpi-rt,rayon` hybrid pmap. A checked script must poison build-time MPI/C tool discovery, prove dependency-tree exclusion, build the runtime example, use an absolute per-rank `MPI_RT_LIB`, run n=1/2/4 with whole-process-group timeout, and prove invalid/unset library fails without fallback. MPIwrapper fixture commit is recorded before execution.
- Follow-up pre-implementation verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable; parent integrated the runtime fixture script and upstream pin)
- Scope: runtime-only smoke example, poisoned no-system-MPI build/dependency-tree gate, negative loader checks, and shared-implementation parity at n=1/2/4
- Upstream blocker found by the acceptance gate: pinned `rsmpi-rt` loaded MPIABI data objects incorrectly, resolved native `MPI_*` dependencies instead of `MPIABI_*`/`MPIXABI_*` adapters, and used the wrong LP64 `MPI_Status` size/alignment. The separately gated upstream fix merged as `tensor4all/rsmpi-rt#6`, commit `6db6a2d6f96115b17c9a925e53ce719797c15dbb`, after all ten CI checks passed.
- Runtime fixture: MPIwrapper commit `966f4231c96153a08295fc7d0bcbd65e916a73fd`.
- Integration verification: poisoned fresh-target check/build and dependency exclusion pass; unset/invalid `MPI_RT_LIB` fail without fallback; runtime pmap smoke passes under `mpiexec` n=1/2/4 with empty input, batch size two, ordering, converged user error, and reuse.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; bounded retry after tool timeout)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none

### Task #17 — hybrid `pmap` and root compute

- Pre-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; bounded retry after initial tool timeout)
- Pre-implementation Round 1 verdict: **Changes requested**
- Required design fixes: make root panic/channel-disconnect directions explicit; pin scope join-before-return/unwind for borrowed jobs; reject entry from every Rayon worker including the selected pool; state provided-thread-level and no-worker-MPI rules; make trace evidence, not payload-size timing, prove rendezvous progress.
- Fixes applied: capacity-one outcome protocol and abort ownership are normative; Rayon joins scoped jobs before return/unwind; hybrid entry requires the MPI main/initialization thread outside all pools; provided `FUNNELED` or stronger is accepted while `SINGLE` is rejected; the ≥1 MiB fixture uses a private serviced-header-before-local-completion trace plus payload integrity and a whole-group watchdog.
- Follow-up pre-implementation verdict: **Correct-to-merge**
- During implementation, FUNNELED analysis exposed that a Rayon-worker rank cannot safely join an MPI preflight merely to report its invalid context. Implementation stopped and the design gate was reopened.
- Corrected pre-implementation rounds: **Changes requested** until the portable boundary was exact. The final contract locally rejects exactly `rayon::current_thread_index() == Some` before any state/MPI call, documents rank disagreement as collective caller misuse, and empirically pins plain/custom/global worker, `install`, `scope`, and `in_place_scope` contexts in a Rayon-only guard test.
- Corrected pre-implementation final verdict: **Correct-to-merge**
- Implementer: Luna (high, write-capable; two bounded continuation timeouts), with parent completion of the event loop, fairness buffering fix, integration examples, and watchdog gates.
- Scope: shared admitted local-mode kernel; hybrid worker execution; root `in_place_scope` coordinator with capacity-one outcome channel; alternating local/`MPI_Iprobe` event selection; both MPI backends; world-size-one/error/reuse/rendezvous/panic watchdog coverage.
- Integration correction: a completed local outcome is retained when fairness selects a simultaneously ready remote event; it is never consumed and dropped before its turn.
- Post-implementation reviewer: `reviewer-flash-opencode-go` (read-only, high; evidence-preserving bounded retry after repository-tool timeouts)
- Post-implementation verdict: **Correct-to-merge**
- Blocking findings: none
- Reviewer-requested hardening: added a focused disconnected-local-sender test proving the chooser selects the abort path immediately.
