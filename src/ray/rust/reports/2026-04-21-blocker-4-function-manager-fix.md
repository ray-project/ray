# 2026-04-21 — Blocker 4 fix: `GCSFunctionManager` parity

## Problem

C++ `GcsServer::InitFunctionManager` (`gcs_server.cc:624-627`)
constructs a shared `GCSFunctionManager` and threads it into the job
and actor managers. That manager is a small refcounter:
`AddJobReference` / `RemoveJobReference` per job id, with a
cleanup side effect when a job's refcount hits zero — three
prefix-deletes in KV namespace `fun`:

- `RemoteFunction:<job_id_hex>:`
- `ActorClass:<job_id_hex>:`
- `FunctionsToRun:<job_id_hex>:` (the
  `kWorkerSetupHookKeyName` constant from `common/constants.h:25`).

C++ call sites:

- Job manager: `gcs_job_manager.cc:38` (recovery),
  `:136` (register), `:175` (mark-finished).
- Actor manager: `gcs_actor_manager.cc:731` (register),
  `:1114` (destroy-not-restartable), `:1730` (recovery).

Rust had nothing corresponding — no type, no wiring, no cleanup. A
long-running cluster would accumulate per-job function / actor-class /
worker-setup-hook entries in KV indefinitely, and the job-completion
criterion C++ uses ("driver exited AND all detached actors from this
job are dead") had no Rust representation.

Severity: medium — not on the hot RPC path, but a real architectural
mismatch in job/actor-associated cleanup semantics.

---

## Fix

### 1. `GcsFunctionManager` module

File: `crates/gcs-managers/src/function_manager.rs` (new).

Full port of the C++ class in one file:

- `add_job_reference(job_id)` — increments the counter (maps C++ line 44).
- `remove_job_reference(job_id)` — decrements; on the
  zero-transition, runs `remove_exported_functions`. A call against a
  job not in the map is a silent no-op — parity with the C++
  "network retry duplicate" early-return at line 48-50.
- `remove_exported_functions(job_id)` — prefix-deletes the three
  per-job KV entries. Byte-for-byte parity with C++ lines 61-70
  (namespace `fun`, prefixes `RemoteFunction:`, `ActorClass:`,
  `FunctionsToRun:`, `del_by_prefix = true`, job id hex-encoded).

Three unit tests in the same file:

- `add_and_remove_balance_plus_idempotent_remove` — balances + the
  idempotent-remove case (parity with C++ lines 48-50).
- `removes_fun_namespace_entries_when_refcount_hits_zero` — seeds
  four KV keys (three for job A, one for job B), runs a single
  refcount cycle on A, confirms A's keys are gone and B's is intact.
- `partial_remove_keeps_kv_entries` — transition-based cleanup:
  refcount 2 → 1 must not fire cleanup.

### 2. Job manager wiring

File: `crates/gcs-managers/src/job_manager.rs`.

- New `function_manager: Mutex<Option<Arc<GcsFunctionManager>>>` field
  + `set_function_manager` setter. Setter used post-construction to
  avoid a circular init order with the actor manager.
- `initialize` calls `add_job_reference(job_id)` per recovered job.
  Parity with C++ `gcs_job_manager.cc:38`.
- `add_job` (the RPC handler) calls `add_job_reference` after the
  storage put succeeds — same success-callback placement as C++
  `gcs_job_manager.cc:136`.
- `mark_job_finished` calls `remove_job_reference` after storage +
  publish, before listeners fire. Parity with C++
  `gcs_job_manager.cc:175`.

### 3. Actor manager wiring

File: `crates/gcs-managers/src/actor_stub.rs`.

- Same `Mutex<Option<Arc<GcsFunctionManager>>>` + setter pattern.
- New helper `job_id_from_actor_id(&[u8])` — extracts the 4-byte
  `JobID` suffix from an `ActorID`. Mirrors C++ `ActorID::JobId`
  (`common/id.cc:150-154`) which reads bytes
  `[kUniqueBytesLength..kUniqueBytesLength + JobID::kLength]` =
  `[12..16]`. Returns `None` on short input so the manager logs
  rather than panics.
- `initialize` (recovery) calls `add_job_reference` for every
  recovered actor's job id. Parity with C++
  `gcs_actor_manager.cc:1730`.
- `register_actor` (RPC handler) calls `add_job_reference` after the
  actor is committed to `actors`. Parity with
  `gcs_actor_manager.cc:731`.
- Both paths in `destroy_actor` (the RPC path at line 1114 and the
  node-dead / no-restarts branch inside `on_node_dead` at line
  ~1513) call `remove_job_reference` inside the
  `!is_actor_restartable` branch. C++ also only decrements for
  not-restartable destroys — a restartable actor stays tracked, so
  its reference is preserved until the final retirement.
- Public test helper `destroy_actor_for_test` wraps the private
  `destroy_actor` so cross-crate integration tests can drive the
  not-restartable path without reaching into internals.

### 4. Server wiring

File: `crates/gcs-server/src/lib.rs`.

- `GcsServer` gains one field: `function_manager: Arc<GcsFunctionManager>`.
- `new_with_store` constructs it from the same `kv_store` every
  other KV consumer uses, then calls
  `job_manager.set_function_manager(function_manager.clone())` and
  `actor_manager.set_function_manager(function_manager.clone())`.
  One shared `Arc`, exactly like C++ — so the two managers'
  refcounts move the same counter.
- Public accessor `function_manager()` for tests and for downstream
  code that needs to observe per-job refcount state during teardown
  or replay.

---

## Tests

### Unit — `function_manager::tests` (3 new)

Documented above — balance / idempotent-remove / KV cleanup / partial-remove.

### Integration — `gcs_server::tests` (1 new)

**`function_manager_cleans_kv_on_last_reference_retirement`** — the
end-to-end parity guard.

1. Registers a job via `add_job` (bumps refcount to 1).
2. Seeds three per-job KV entries under namespace `fun`
   (RemoteFunction, ActorClass, FunctionsToRun) plus one entry under
   a different job id as a negative control.
3. Inserts two alive actors directly (via the new
   `insert_registered_actor` helper) and bumps the function-manager
   refcount twice (total = 3). Assert both the refcount and the KV
   keys' presence.
4. Destroys both actors via `destroy_actor_for_test` with the
   not-restartable `RayKill` reason — refcount drops to 1. Asserts
   KV is still intact (cleanup must wait for the final transition).
5. Calls `mark_job_finished` — refcount hits 0. Asserts the three
   per-job KV entries are gone and the control key under a different
   job id is untouched.
6. Checks `Arc::strong_count` on the shared function manager to
   confirm both managers and the server hold it — a refactor that
   silently drops one of the setters would fail this assertion.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    247 passed   (+3 vs previous)
gcs-server:       41 passed   (+1 vs previous)
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions.

---

## Before → after behaviour

| Scenario                                                          | Before                                                  | After                                                           |
| ----------------------------------------------------------------- | ------------------------------------------------------- | --------------------------------------------------------------- |
| Long-running cluster churning through 10k jobs                     | `fun`-namespace entries accumulate indefinitely         | Per-job entries prefix-deleted on last retirement (parity w/ C++) |
| Job retires with live detached actors                             | No hook                                                 | KV cleanup waits for the *last* (job or actor) retirement       |
| Actor recovered from storage and then retired                      | No hook                                                 | Recovery bumps refcount, destroy decrements — balanced           |
| Restartable actor transitions DEAD → RESTARTING → ALIVE            | No hook                                                 | No decrement (restartable path doesn't fire cleanup)             |
| Actor destroy on non-restartable path                              | No hook                                                 | One refcount dropped; cleanup on the transition to zero          |
| Network-retry duplicate `mark_job_finished` after KV cleanup ran   | No hook; no cleanup ran the first time either            | First call triggers cleanup; second call is a silent no-op       |

---

## Scope and caveats

What's included:

- Full behavioral parity for the six C++ call sites the blocker
  named.
- Shared `Arc<GcsFunctionManager>` across the two managers — one
  refcounter, exactly like C++.
- Byte-for-byte KV key parity (namespace, three prefixes, hex-encoded
  job id, `del_by_prefix = true`).
- Integration test that drives the full flow through the real
  handlers.

What's deliberately out of scope:

- **Actor id bit layout other than 12+4.** Ray's `ActorID` is 16
  bytes (12 unique + 4 job-id). A short actor id (malformed client)
  silently skips the refcount update rather than panicking — same
  graceful-degrade posture the other managers use for malformed
  inputs.
- **AWS testing.** Not needed. The integration test drives the
  whole flow in-process against the in-memory `InternalKVInterface`:
  refcount transitions are pure state, and the KV prefix-delete is
  the same code path production uses. AWS would test tonic
  transport, not the parity we're closing.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/function_manager.rs` —
  new module: `GcsFunctionManager`, 3 unit tests.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/lib.rs` — register
  `pub mod function_manager`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs` —
  field + setter + call sites in `initialize` / `add_job` /
  `mark_job_finished`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs` —
  field + setter + `job_id_from_actor_id` + call sites in
  `initialize` / `register_actor` / both not-restartable destroy
  branches; new public `destroy_actor_for_test` wrapper.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs` — `GcsServer`
  holds the shared `Arc<GcsFunctionManager>`, sets it on both
  managers inside `new_with_store`, exposes `function_manager()`
  accessor, adds the end-to-end integration test.
