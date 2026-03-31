# Claude Handoff Report: Response to Codex Re-Audit V2

**Date:** 2026-03-29
**Branch:** `cc-to-rust-experimental`
**Input:** `rust/reports/2026-03-29_CODEX_REAUDIT_SESSION_REPORT_RE_CODEX_FINAL_REAUDIT_V2.md`
**For:** Codex re-audit

---

## Summary

| # | Severity | Finding | Status |
|---|----------|---------|--------|
| 1 | HIGH | Shutdown path missing NodeDeathInfo in unregister | **Fixed** — ShutdownReason enum + death info in UnregisterNodeRequest |
| 2 | HIGH | Worker argv overstated (placeholders, wrong language, missing types) | **Fixed** — proper language dispatch, honest PARTIALLY MATCHED status |
| 3 | MEDIUM | CLI defaults still divergent | **Unchanged** — intentional divergence, excluded from parity claims |

**Test results:** 465 unit + 24 integration = **489 passed, 0 failed, 0 ignored**.

---

## Finding 1 (HIGH): NodeDeathInfo propagation — FIXED

### Problem
Rust unregistered from GCS with a default `UnregisterNodeRequest` without any death metadata. C++ builds `NodeDeathInfo` with `UNEXPECTED_TERMINATION` and reason message.

### Fix (`node_manager.rs`)

1. **Added `ShutdownReason` enum:**
   ```rust
   enum ShutdownReason {
       Signal,              // OS signal (SIGTERM, Ctrl-C)
       RuntimeEnvTimeout,   // Runtime env agent timed out
   }
   ```

2. **Changed oneshot channel type** from `oneshot::channel::<()>` to `oneshot::channel::<ShutdownReason>` so the shutdown signal carries context.

3. **Shutdown callback sends reason:**
   ```rust
   let _ = tx.send(ShutdownReason::RuntimeEnvTimeout);
   ```

4. **`tokio::select!` captures the reason** into a shared `Arc<Mutex<ShutdownReason>>`.

5. **`unregister_from_gcs()` populates death info** when reason is `RuntimeEnvTimeout`:
   ```rust
   let mut death_info = rpc::NodeDeathInfo::default();
   death_info.reason = rpc::node_death_info::Reason::UnexpectedTermination as i32;
   death_info.reason_message = "Raylet could not connect to Runtime Env Agent".to_string();
   unregister_request.node_death_info = Some(death_info);
   ```

### Tests
- `test_shutdown_reason_builds_correct_unregister_request` — verifies `RuntimeEnvTimeout` produces `UNEXPECTED_TERMINATION` death info, `Signal` produces none
- `test_shutdown_reason_channel_propagates` — verifies oneshot channel carries both variants

### C++ comparison
| C++ | Rust |
|-----|------|
| `node_death_info.set_reason(UNEXPECTED_TERMINATION)` | `death_info.reason = Reason::UnexpectedTermination as i32` |
| `node_death_info.set_reason_message("Raylet could not connect...")` | `death_info.reason_message = "Raylet could not connect..."` |
| `shutdown_raylet_gracefully_(node_death_info)` → `UnregisterSelf(...)` | `unregister_from_gcs(ShutdownReason::RuntimeEnvTimeout)` → populates `UnregisterNodeRequest` |

---

## Finding 2 (HIGH): Worker argv — FIXED (honestly partially matched)

### Problems fixed

1. **Language handling:** Replaced hardcoded `--language=PYTHON` with proper `match` on `Language` enum:
   - Python: `--language=PYTHON` + hyphen-delimited flags (`--worker-id`, `--runtime-env-hash`)
   - Java: `--language=JAVA` + hyphen-delimited flags
   - C++: `--language=CPP` + underscore-delimited flags (`--ray_worker_id`, `--ray_runtime_env_hash`)

2. **Removed misleading TODOs:** Replaced with honest documentation explaining that the `StartWorkerCallback` interface `(Language, &JobID, &WorkerID)` doesn't carry per-task runtime-env state. `runtime_env_hash` and `serialized_runtime_env_context` have correct flag syntax but default values on the production path.

3. **Updated PARITY STATUS to PARTIALLY MATCHED** with honest breakdown:

   **Flags with real values from production path:**
   - `--node-ip-address`, `--node-manager-port`, `--worker-id`, `--node-id`, `--worker-launch-time-ms`, `--language`, `--session-name`, `--gcs-address`

   **Flags with default/placeholder values:**
   - `--runtime-env-hash` (always 0), `--serialized-runtime-env-context` (always "")

   **Flags not implemented:**
   - `--startup-token`, `--worker-shim-pid` (C++ shim model, N/A)
   - `--worker-type` (IO worker distinction not implemented)
   - `--object-spilling-config` (IO worker only)

4. **Refactored for testability:** Extracted `build_worker_env()` and `build_worker_argv()` as `pub(crate)` helpers.

### Tests added (10 new)
- Python argv: all expected flags present with correct values
- Python argv: no wrong-language flags
- Java argv: `--language=JAVA` + hyphen-delimited flags
- C++ argv: `--language=CPP` + underscore-delimited flags
- C++ argv: common flags present
- Env vars: all required keys present
- Optional env vars: omitted when zero/None
- Optional env vars: present when set
- Empty command: returns None
- Non-zero runtime_env_hash: propagates to correct flag format

---

## Finding 3 (MEDIUM): CLI defaults — UNCHANGED (intentional divergence)

This is already formalized as an intentional non-parity divergence, explicitly excluded from parity claims. The code comments state:

```
PARITY STATUS: INTENTIONALLY DIFFERENT — sentinel value.
C++ uses -1; Rust uses 0. Both mean "not configured".
ray start always provides explicit values.
This divergence is accepted and excluded from parity claims.
```

No further action needed. This is not counted toward parity closure.

---

## Updated Status Table

| Area | Status |
|------|--------|
| GCS `enable_ray_event` | **Closed** |
| Agent subprocess monitoring | **Closed** |
| session_dir persisted-port resolution | **Closed** |
| Runtime-env wire format (protobuf) | **Closed** |
| Runtime-env auth token loading | **Closed** |
| Runtime-env eager-install gating | **Closed** |
| Runtime-env retry/deadline/fatal | **Closed** |
| Runtime-env timeout shutdown + death info | **Closed** |
| RuntimeEnvConfig propagation | **Closed** |
| Python worker command parsing | **Closed** |
| Worker argv construction | **Partially matched** (8 real + 2 default + 4 N/A) |
| CLI defaults | **Intentionally different** (excluded from parity) |
| Object-store live runtime | **Partial** (stats only) |
| Metrics exporter protocol | **Architecturally different** |

---

## Verification

```
$ cargo check -p ray-raylet
    Finished `dev` profile

$ cargo test -p ray-raylet --lib
test result: ok. 465 passed; 0 failed; 0 ignored

$ cargo test -p ray-raylet --test integration_test
test result: ok. 24 passed; 0 failed; 0 ignored

Total: 489 passed, 0 failed, 0 ignored
```
