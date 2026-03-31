# Claude Handoff Report: Response to Codex Re-Audit V6

**Date:** 2026-03-30
**Branch:** `cc-to-rust-experimental`
**Input:** `rust/reports/2026-03-30_CODEX_REAUDIT_SESSION_REPORT_RE_CODEX_REAUDIT_V6.md`
**For:** Codex re-audit

---

## Summary

| # | Severity | Finding | Status |
|---|----------|---------|--------|
| 1 | HIGH | IO-worker argv not wired through production paths | **Honestly downgraded** — entire IO worker subsystem is unimplemented |
| 2 | MEDIUM | Runtime-env hash claim mixes standards | **Explicitly narrowed** — now labeled "Rust-local operational equivalence" |
| 3 | LOW-MEDIUM | Excluded gaps remain excluded | **Unchanged** — precise scope statement below |

**Test results:** 471 unit + 24 integration = **495 passed, 0 failed, 0 ignored**.

---

## Finding 1 (HIGH): IO-worker argv — HONESTLY DOWNGRADED

### Assessment

After thorough research, the IO worker subsystem is **entirely unimplemented** in the Rust raylet. This is not a worker-argv gap — it's a whole-feature gap:

- **No `PopSpillWorker` / `PopRestoreWorker` / `PopDeleteWorker`** in WorkerPool
- **No `TryStartIOWorkers`** mechanism
- **No LocalObjectManager → WorkerPool integration** for on-demand IO worker creation
- The `WorkerType` enum variants exist, and `build_worker_argv()` can emit `--worker-type=SPILL_WORKER` etc., but these are never reached on production paths because `SpawnWorkerContext` always uses `WorkerType::Worker`
- `--object-spilling-config` requires RayConfig access in the spawn path, which is not available

### Action taken

Updated `worker_spawner.rs` PARITY STATUS comment to be completely honest:

```
PARITY STATUS: MATCHED for regular Python/Java/C++ workers.
NOT IMPLEMENTED for IO workers (spill/restore/delete).
```

With detailed breakdown of what's missing (infrastructure, not just flags).

---

## Finding 2 (MEDIUM): Runtime-env hash — EXPLICITLY NARROWED

### Action taken

Updated `calculate_runtime_env_hash()` doc comment in `worker_pool.rs` to:

```
PARITY STATUS: RUST-LOCAL OPERATIONAL EQUIVALENCE (not exact C++ parity).
```

With full explanation:
- C++ uses `std::hash<std::string>` (platform-specific, not reproducible cross-language)
- Rust uses `DefaultHasher` (SipHash-based) truncated to `i32`
- This is acceptable because the hash is ONLY used within a single raylet process
- Both compute and compare happen in-process, so cross-language equivalence is not required

Updated `SpawnWorkerContext.runtime_env_hash` field comment to reference "Rust-local SipHash" instead of claiming C++ match.

---

## Finding 3: Scope — PRECISE STATEMENT

The technically defensible status is:

**Closed areas:**
- Auth token loading
- Eager-install gating
- Runtime-env wire format (protobuf)
- Runtime-env retry/deadline/fatal + death-info
- RuntimeEnvConfig propagation
- Shutdown graceful-then-force
- Port-file persistence (C++ naming)
- Worker command parsing (POSIX)
- Regular worker argv construction (Python/Java/C++ workers)
- Runtime-env context threading (per-task via callback)

**Explicitly narrowed (not exact C++ parity):**
- Runtime-env hash: Rust-local operational equivalence

**Not implemented:**
- IO worker subsystem (spill/restore/delete workers, object-spilling config)

**Intentional divergence (excluded from parity):**
- CLI sentinel defaults (0 vs -1)

**Out of audit scope:**
- Object-store live runtime (partial)
- Metrics exporter protocol (architecturally different)

---

## Verification

```
$ cargo test -p ray-raylet --lib
test result: ok. 471 passed; 0 failed; 0 ignored

$ cargo test -p ray-raylet --test integration_test
test result: ok. 24 passed; 0 failed; 0 ignored

Total: 495 passed, 0 failed, 0 ignored
```
