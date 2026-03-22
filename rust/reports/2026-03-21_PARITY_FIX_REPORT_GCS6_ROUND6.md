# Claude Round 6 — GCS-6 Callback Ordering Fix Report for Codex

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 6 re-audit (`2026-03-21_BACKEND_PORT_REAUDIT_GCS6_ROUND6.md`) identified one remaining gap:

> `on_actor_creation_success()` still resolves creation-success callbacks before the async actor-table persistence finishes. In C++, those callbacks run inside the `ActorTable().Put()` completion path, after persistence and publication.

Round 5 fixed publication ordering but left callback ordering broken. Round 6 closes this.

---

## Step A: C++ Contract Re-Derived

### Files read

| File | Lines | What it defines |
|---|---|---|
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1677–1692 | `OnActorCreationSuccess` — Put + callback ordering |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1984–1995 | `RunAndClearActorCreationCallbacks` |

### C++ `OnActorCreationSuccess` exact ordering (lines 1677–1692)

```cpp
// 1. Persist to storage
gcs_table_storage_->ActorTable().Put(
    actor_id,
    *mutable_actor_table_data,
    {[this, actor_id, actor_data_only_with_states, actor, reply](Status status) mutable {
         // 2. Publish (inside Put callback)
         gcs_publisher_->PublishActor(actor_id, std::move(actor_data_only_with_states));
         // 3. Resolve callbacks (inside Put callback, after publish)
         RunAndClearActorCreationCallbacks(actor, reply, Status::OK());
     },
     io_context_});
```

### C++ `RunAndClearActorCreationCallbacks` (lines 1984–1995)

```cpp
void GcsActorManager::RunAndClearActorCreationCallbacks(
    const std::shared_ptr<GcsActor> &actor,
    const rpc::PushTaskReply &creation_task_reply,
    const Status &creation_task_status) {
  auto iter = actor_to_create_callbacks_.find(actor->GetActorID());
  if (iter != actor_to_create_callbacks_.end()) {
    for (auto &callback : iter->second) {
      callback(actor, creation_task_reply, creation_task_status);
    }
    actor_to_create_callbacks_.erase(iter);
  }
}
```

### Key observations

1. All three events happen inside the `Put()` callback — they are strictly ordered after persistence
2. Publication happens before callback resolution
3. **A successful creation callback implies:**
   - the actor-table record is already durably written
   - the actor state has already been published via pubsub
4. Callbacks never fire if persistence fails (the Put callback only runs on success)

---

## Step B: What Was Still Wrong in Rust Before This Round

### `on_actor_creation_success()` (lines 577–612 before fix)

```rust
// Round 5 code — persist + publish are ordered, but callbacks are NOT
if let Some((msg, actor_data)) = pub_msg {
    let storage = self.table_storage.clone();
    let pubsub = self.pubsub_handler.get().cloned();
    let aid = *actor_id;
    tokio::spawn(async move {
        // 1. Persist (correct)
        storage.actor_table().put(&key, &actor_data).await?;
        // 2. Publish (correct)
        handler.publish_pubmessage(msg);
        // --- callbacks NOT here ---
    });
}

// 3. Callbacks fire HERE — synchronously, BEFORE the spawn even starts
let callbacks = self.create_callbacks.remove(actor_id);
if let Some((_, senders)) = callbacks {
    for tx in senders {
        let _ = tx.send(Ok(reply.clone()));  // fires immediately
    }
}
```

**The bug:** Callback senders were resolved synchronously on the calling thread, racing far ahead of the spawned persist-then-publish task. A caller awaiting the `oneshot::Receiver` would get `Ok(reply)` before the actor-table state was written to storage.

---

## Step C: Tests Added (3 new callback ordering tests)

All 3 tests use a `setup_callback_ordering_test` helper that:
- Creates a `GatedStoreClient` (blocks Actor table puts until signalled)
- Sets up pubsub
- Registers an ALIVE actor, preempts it, kills the node (RESTARTING)
- Inserts a `create_callback` oneshot sender (simulating `handle_create_actor`)

### 1. `test_creation_success_callbacks_fire_only_after_actor_table_persist`

**Action:** Call `on_actor_creation_success()`. Wait for persist to reach the gate (blocked).

**Assertions while persist is blocked:**
- `rx_cb.try_recv().is_err()` — callback has NOT fired

**After releasing the gate:**
- `rx_cb.try_recv()` succeeds — callback fires
- Storage contains the ALIVE actor record

**Proves:** A successful creation callback implies persistence has completed.

### 2. `test_creation_success_callbacks_do_not_run_while_persist_blocked`

**Action:** Call `on_actor_creation_success()`. Wait for persist to reach the gate.

**Assertions while persist is blocked:**
- Poll `rx_cb.try_recv()` 5 times with `yield_now()` between each — all return `Err`
- Sleep 20ms, poll again — still `Err`

**After releasing the gate:**
- Callback fires

**Proves:** Callbacks genuinely remain pending the entire time persistence is blocked, not just at the first check.

### 3. `test_creation_success_callbacks_are_ordered_after_publication`

**Action:** Set up an `AtomicBool` flag. Spawn a task that waits for the pubsub ALIVE message and sets the flag. Call `on_actor_creation_success()`, unblock persist.

**Assertion:** By the time `rx_cb.await` resolves, the `published` flag is `true`.

**Proves:** Publication occurs before (or by the time of) callback resolution — matching the C++ `publish → callbacks` ordering.

---

## Step D: Code Change

### `rust/ray-gcs/src/actor_manager.rs` — `on_actor_creation_success()`

**Before (Round 5):**
```rust
if let Some((msg, actor_data)) = pub_msg {
    tokio::spawn(async move {
        persist(...).await;
        publish(...);
    });
}
// Callbacks resolved HERE — synchronously, before persist/publish
let callbacks = self.create_callbacks.remove(actor_id);
for tx in senders { tx.send(Ok(reply)); }
```

**After (Round 6):**
```rust
// Remove callbacks from DashMap synchronously (prevents double-resolution)
// but move the oneshot senders into the async block
let callback_senders = self.create_callbacks.remove(actor_id).map(|(_, s)| s);

if let Some((msg, actor_data)) = pub_msg {
    tokio::spawn(async move {
        // 1. Persist
        storage.actor_table().put(&key, &actor_data).await?;
        // 2. Publish
        handler.publish_pubmessage(msg);
        // 3. Resolve callbacks
        if let Some(senders) = callback_senders {
            for tx in senders { tx.send(Ok(reply)); }
        }
    });
} else {
    // No pub_msg = actor removed/dead — resolve callbacks immediately
    // (matches C++ early-return paths)
    if let Some(senders) = callback_senders {
        for tx in senders { tx.send(Ok(reply)); }
    }
}
```

### Design notes

- `create_callbacks` are removed from the DashMap synchronously to prevent concurrent `on_actor_creation_success` calls from double-resolving the same callbacks
- The `oneshot::Sender` values are `Send`, so they move safely into the async block
- If persistence fails, neither publication nor callbacks fire — matching C++ behavior where the Put callback only runs on success
- The `else` branch handles the edge case where the actor was already removed or dead before the spawn — matching C++ `RunAndClearActorCreationCallbacks` on early-return paths

---

## Step E: Test Results

### New callback ordering tests

```
CARGO_TARGET_DIR=target_cb_ordering cargo test -p ray-gcs --lib -- \
    test_creation_success_callbacks_fire_only_after_actor_table_persist \
    test_creation_success_callbacks_do_not_run_while_persist_blocked \
    test_creation_success_callbacks_are_ordered_after_publication

test result: ok. 3 passed; 0 failed
```

### Full ray-gcs package

```
CARGO_TARGET_DIR=target_cb_ordering cargo test -p ray-gcs --lib

test result: ok. 471 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

471 = 468 existing + 3 new. Zero regressions.

### Existing create-actor lifecycle tests

```
CARGO_TARGET_DIR=target_cb_ordering cargo test -p ray-gcs --lib -- test_create_actor_basic test_named_actor_discoverable_after_alive

test result: ok. 2 passed; 0 failed
```

---

## Complete File Change List

| File | What Changed |
|---|---|
| `ray-gcs/src/actor_manager.rs` | Moved callback resolution into post-persist async block in `on_actor_creation_success()`; added `setup_callback_ordering_test` helper; added 3 callback ordering tests |

---

## Full Actor-Preemption Lifecycle Parity Checklist

| C++ Behavior | Rust Status | Proof |
|---|---|---|
| `SetPreemptedAndPublish` marks `preempted=true` in memory | **Matches** | `test_autoscaler_drain_marks_node_actors_preempted` (R2) |
| `SetPreemptedAndPublish` publishes via pubsub | **Matches** | `test_autoscaler_drain_publishes_actor_preemption_state` (R2) |
| `SetPreemptedAndPublish` persists to `ActorTable().Put()` | **Matches** | `test_preempted_actor_state_persisted_to_actor_table` (R3) |
| `SetPreemptedAndPublish` publishes only after persistence | **Matches** | `test_preempted_publish_occurs_only_after_actor_table_persist` (R5) |
| Preempted state survives GCS restart | **Matches** | `test_preempted_actor_state_survives_gcs_restart_until_consumed` (R3) |
| `RestartActor` increments `num_restarts_due_to_node_preemption` when preempted | **Matches** | `test_node_preemption_increments_num_restarts_due_to_node_preemption` (R3) |
| `RestartActor` excludes preemption restarts from budget | **Matches** | `test_node_preemption_restart_does_not_consume_regular_restart_budget` (R3) |
| `RestartActor` grants extra restart for preempted actor at budget limit | **Matches** | `test_node_preemption_restart_does_not_consume_regular_restart_budget` (R3) |
| `OnActorCreationSuccess` resets `preempted=false` in memory | **Matches** | `test_on_actor_creation_success_persists_preempted_reset` (R4) |
| `OnActorCreationSuccess` persists full ALIVE record to `ActorTable().Put()` | **Matches** | `test_on_actor_creation_success_persists_preempted_reset` (R4) |
| `OnActorCreationSuccess` publishes only after persistence | **Matches** | `test_creation_success_publish_occurs_only_after_actor_table_persist` (R5) |
| `OnActorCreationSuccess` persisted state survives GCS restart | **Matches** | `test_preempted_reset_survives_gcs_restart_after_creation_success` (R4) |
| Full preempt->kill->restart->creation-success flow persists correctly | **Matches** | `test_real_creation_success_path_clears_preempted_in_storage_not_just_memory` (R4) |
| Pubsub observers only see already-persisted actor state | **Matches** | `test_pubsub_observer_cannot_see_unpersisted_actor_state` (R5) |
| `OnActorCreationSuccess` callbacks fire only after persistence | **Matches** | `test_creation_success_callbacks_fire_only_after_actor_table_persist` (R6) |
| `OnActorCreationSuccess` callbacks remain pending while persist blocked | **Matches** | `test_creation_success_callbacks_do_not_run_while_persist_blocked` (R6) |
| `OnActorCreationSuccess` callbacks are ordered after publication | **Matches** | `test_creation_success_callbacks_are_ordered_after_publication` (R6) |

17/17 behaviors matched. 0 gaps.

---

## Reports Updated

- `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` — added 3 R6 callback ordering test entries
- `2026-03-21_BACKEND_PORT_REAUDIT_GCS6_ROUND6.md` — updated "Still open" to closed, added Round 6 fix section
- `2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md` — added 3 callback ordering rows to checklist, added Round 6 section

---

## GCS-6 Final Status: **fixed**

The full C++ `OnActorCreationSuccess` callback-ordering contract is now matched in Rust:

1. `set_preempted_and_publish` -> persist -> **then** publish
2. `on_actor_creation_success` -> persist -> **then** publish -> **then** resolve callbacks
3. If persistence fails, neither publication nor callbacks fire
4. A pubsub subscriber cannot observe actor state that storage does not yet contain
5. **A successful creation callback implies the actor-table update is already durably written and published**

This is not "close enough" or "publication ordering only". The full persist → publish → callback ordering now matches C++.

---

## Round 7 — Actor Lifecycle/Export Event Analysis (2026-03-21)

The Round 7 re-audit asked whether C++ actor lifecycle/export events (`WriteActorExportEvent`) are a parity requirement.

### Status: **not a parity requirement**

### C++ event-emission contract extracted

`WriteActorExportEvent` (in `gcs_actor.cc` lines 105–160) has two independent paths:

1. **Ray Events (aggregator)** — guarded by `enable_ray_event` (default: `false`)
   - Emits `RayActorDefinitionEvent` / `RayActorLifecycleEvent` to `ray_event_recorder_`
   - Consumed by dashboard `AggregatorAgent` via gRPC `AddEvents`

2. **Export API (file-based audit log)** — guarded by `enable_export_api_write` (default: `false`)
   - Writes `ExportActorData` proto to `<log_dir>/export_events/event_EXPORT_ACTOR.log`
   - No Ray component reads this file at runtime

Called at 6 transitions in `gcs_actor_manager.cc`:
- Registration success (line 761)
- PENDING_CREATION transition (line 867)
- Actor death/cancellation (line 1145)
- Restart transition (line 1523)
- Death in RestartActor (line 1572)
- Creation success (line 1686)

### Source-backed reasoning for de-scoping

1. **Both paths disabled by default** — `enable_ray_event=false` (line 562), `enable_export_api_write=false` (line 1004), `enable_export_api_write_config={}` (line 1012) in `ray_config_def.h`

2. **No live Ray component reads export event files** — verified by searching all Python code:
   - `ray list actors` / `ray.util.state` → gRPC `GetAllActorInfo`
   - Dashboard → gRPC to GCS
   - No Python consumer of `export_events/event_EXPORT_ACTOR.log`
   - Only C++ unit test (`gcs_actor_manager_export_event_test.cc`) reads the file

3. **Write-only audit sink for external tools** — designed for enterprise log shippers that scrape files out-of-band; no runtime dependency

4. **Rust externally visible contract is complete** — actor-table persistence, pubsub publication, callback resolution all match C++; all user-visible APIs use gRPC

5. **Would require building entirely new infrastructure** — Rust `EventManager` + `LogEventReporter` + file sinks; cross-cutting observability work, not a GCS-6 fix

### Test validation

```
CARGO_TARGET_DIR=target_r7 cargo test -p ray-gcs --lib
test result: ok. 471 passed; 0 failed
```

---

## Complete Parity Status — All 12 Codex Re-Audit Findings

| Finding | Area | Status | Closed In |
|---|---|---|---|
| GCS-1 | Internal KV persistence | fixed | Round 2 |
| GCS-4 | Job info enrichment/filters | fixed | Round 6 |
| GCS-6 | Drain node side effects | **fixed (lifecycle + ordering + export de-scoped)** | **GCS-6 Round 7** |
| GCS-8 | Pubsub unsubscribe scoping | fixed | Round 2 |
| GCS-12 | PG create lifecycle | fixed | Round 3 |
| GCS-16 | Autoscaler version handling | fixed | Round 2 |
| GCS-17 | Cluster status payload | fixed | Round 4 |
| RAYLET-3 | Worker backlog reporting | fixed | Round 5 |
| RAYLET-4 | Pin eviction subscription | fixed | Round 11 |
| RAYLET-5 | Resource resize validation | fixed | Round 5 |
| RAYLET-6 | GetNodeStats RPC parity | fixed | Round 8 |
| CORE-10 | Object location subscriptions | fixed | Round 11 |

**12/12 fixed. 0 partial. 0 open.**
**1 additional item (export events) explicitly de-scoped with source-backed evidence.**
