# Claude Round 5 — GCS-6 Publication Ordering Fix Report for Codex

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 4 re-audit (`2026-03-21_BACKEND_PORT_REAUDIT_GCS6_ROUND4.md`) identified the final remaining ordering gap:

> C++ persists actor-table updates first and publishes from the `ActorTable().Put()` callback.
> The live Rust code still publishes first and then spawns async persistence.

This means a pubsub subscriber could observe actor state that storage does not yet contain — a real behavioral difference in crash/recovery semantics.

Round 5 closes this gap.

---

## Step A: C++ Contract Re-Derived

### Files read

| File | Lines | What it defines |
|---|---|---|
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1417–1444 | `SetPreemptedAndPublish` |
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1628–1693 | `OnActorCreationSuccess` |

### C++ `SetPreemptedAndPublish` exact sequence (lines 1417–1444)

```cpp
void GcsActorManager::SetPreemptedAndPublish(const NodeID &node_id) {
    // ...
    for (const auto &id_iter : created_actors_.find(node_id)->second) {
        // 1. Update in-memory state
        actor_iter->second->GetMutableActorTableData()->set_preempted(true);

        // 2. Persist to storage
        gcs_table_storage_->ActorTable().Put(
            actor_id,
            actor_table_data,
            {[this, actor_id, actor_table_data](Status status) {
                // 3. Publish INSIDE the Put callback
                gcs_publisher_->PublishActor(actor_id,
                    GenActorDataOnlyWithStates(actor_table_data));
            }, io_context_});
    }
}
```

### C++ `OnActorCreationSuccess` exact sequence (lines 1655–1693)

```cpp
// 1. Update in-memory fields
mutable_actor_table_data->set_timestamp(time);
actor->UpdateState(ALIVE);
mutable_actor_table_data->set_node_id(node_id.Binary());
mutable_actor_table_data->set_preempted(false);

// 2. Update in-memory indexes
created_actors_[node_id].emplace(worker_id, actor_id);

// 3. Persist to storage
gcs_table_storage_->ActorTable().Put(
    actor_id,
    *mutable_actor_table_data,
    {[...](Status status) mutable {
        // 4. Publish INSIDE the Put callback
        gcs_publisher_->PublishActor(actor_id, ...);
        // 5. Resolve creation callbacks
        RunAndClearActorCreationCallbacks(actor, reply, Status::OK());
    }, io_context_});
```

### Key observations

1. Both paths call `ActorTable().Put()` first
2. Both paths publish **inside** the Put callback — publication is strictly ordered after persistence
3. If the Put fails, no publication occurs
4. This guarantees: when a subscriber receives an actor state update, that state has already been durably written

---

## Step B: What Was Still Wrong in Rust Before This Round

### `set_preempted_and_publish()` (line 968)

```rust
// OLD — publishes BEFORE persistence
actor.preempted = true;
self.publish_actor_state(&updated);          // ← publish immediately
actors_to_persist.push((actor_id, ...));
// ...
tokio::spawn(async move {
    storage.actor_table().put(...).await;    // ← persist later, fire-and-forget
});
```

### `on_actor_creation_success()` (line 518)

```rust
// OLD — publishes BEFORE persistence
actor.state = ActorState::Alive as i32;
actor.preempted = false;
// ...
self.publish_deferred_messages(vec![msg]);   // ← publish immediately
tokio::spawn(async move {
    storage.actor_table().put(...).await;    // ← persist later, fire-and-forget
});
```

Both paths: **publish first, persist later**. The exact opposite of C++.

---

## Step C: Tests Added (3 new ordering tests)

All 3 tests use a `GatedStoreClient` — a wrapper around `InMemoryStoreClient` that blocks `put` on the Actor table until a `tokio::sync::Notify` is signalled. This lets tests control exactly when persistence completes, and verify that publication does not race ahead.

### 1. `test_preempted_publish_occurs_only_after_actor_table_persist`

**Setup:** Register an ALIVE actor on a node. Set up pubsub subscriber on the actor channel.

**Action:** Call `set_preempted_and_publish()`. Wait until the persist task reaches the gate (blocked).

**Assertions while persist is blocked:**
- `rx.try_recv().is_err()` — no pubsub message has been emitted

**After releasing the gate:**
- `rx.try_recv()` succeeds — publication occurs after persistence

**Proves:** Preemption publication is strictly ordered after persistence.

### 2. `test_creation_success_publish_occurs_only_after_actor_table_persist`

**Setup:** Actor goes through preempt → kill → restart cycle, ending in RESTARTING state. Pubsub subscriber set up.

**Action:** Call `on_actor_creation_success()`. Wait until persist is blocked at the gate.

**Assertions while persist is blocked:**
- `rx.try_recv().is_err()` — no ALIVE pubsub message has been emitted

**After releasing the gate:**
- `rx.try_recv()` succeeds — ALIVE publication occurs after persistence

**Proves:** Creation-success publication is strictly ordered after persistence.

### 3. `test_pubsub_observer_cannot_see_unpersisted_actor_state`

**Setup:** Register actor, set up pubsub subscriber.

**Action:** Call `set_preempted_and_publish()`. Wait until persist is blocked.

**Assertions while persist is blocked:**
- Storage does NOT contain `preempted=true` (either no record or `preempted=false`)
- Pubsub has NOT delivered a message

**After releasing the gate:**
- Pubsub delivers the message
- Storage now contains `preempted=true`

**Proves:** When a pubsub observer receives a state update, the corresponding state is already in storage. There is no window where a subscriber can observe state that storage does not yet contain.

---

## Step D: Code Changes

### `rust/ray-gcs/src/actor_manager.rs` — `set_preempted_and_publish()`

**Before:**
```rust
self.publish_actor_state(&updated);              // publish immediately
// ...
tokio::spawn(async move {
    storage.actor_table().put(&key, &data).await; // persist later
});
```

**After:**
```rust
let msg = Self::build_actor_pub_message(&updated);  // build message now
// ...
tokio::spawn(async move {
    // Persist FIRST
    if let Err(e) = storage.actor_table().put(&key, &data).await {
        tracing::error!(?actor_id, ?e, "Failed to persist preempted actor state");
        continue; // Do not publish if persist failed
    }
    // Publish ONLY after successful persistence
    if let Some(ref handler) = pubsub {
        handler.publish_pubmessage(pub_msg);
    }
});
```

### `rust/ray-gcs/src/actor_manager.rs` — `on_actor_creation_success()`

**Before:**
```rust
self.publish_deferred_messages(vec![msg]);        // publish immediately
// ...
tokio::spawn(async move {
    storage.actor_table().put(&key, &data).await; // persist later
});
```

**After:**
```rust
tokio::spawn(async move {
    // Persist FIRST
    if let Err(e) = storage.actor_table().put(&key, &data).await {
        tracing::error!(?aid, ?e, "Failed to persist actor creation success");
        return; // Do not publish if persist failed
    }
    // Publish ONLY after successful persistence
    if let Some(handler) = pubsub {
        handler.publish_pubmessage(msg);
    }
});
```

### Design notes

- The pubsub handler `Arc` is cloned into the spawned task — safe and cheap
- If persistence fails, publication is skipped — no false positive observer notifications
- In-memory state is still updated synchronously (same as C++ — indexes need to be current for concurrent operations)
- The spawned task pattern matches C++'s `ActorTable().Put(callback)` — both are async from the caller's perspective, with publication ordered inside the completion handler

---

## Step E: Test Results

### New ordering tests

```
CARGO_TARGET_DIR=target_ordering cargo test -p ray-gcs --lib -- \
    test_preempted_publish_occurs_only_after_actor_table_persist \
    test_creation_success_publish_occurs_only_after_actor_table_persist \
    test_pubsub_observer_cannot_see_unpersisted_actor_state

test result: ok. 3 passed; 0 failed
```

### Full ray-gcs package

```
CARGO_TARGET_DIR=target_ordering cargo test -p ray-gcs --lib

test result: ok. 468 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

468 = 465 existing + 3 new. Zero regressions.

---

## Complete File Change List

| File | What Changed |
|---|---|
| `ray-gcs/src/actor_manager.rs` | Fixed ordering in `set_preempted_and_publish()` and `on_actor_creation_success()`; added `GatedStoreClient` test helper; added 3 ordering tests |

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

## Round 6 — Callback Ordering Fix (2026-03-21)

Round 5 closed publication ordering but left callback ordering open: `create_callbacks` were resolved synchronously before the async persist-then-publish task ran.

### What was wrong

```rust
// Round 5 — callbacks fire BEFORE persist + publish
tokio::spawn(async move {
    persist(...).await;
    publish(...);
});
// Callbacks resolved here, synchronously, before the spawn even starts:
let callbacks = self.create_callbacks.remove(actor_id);
for tx in senders { tx.send(Ok(reply)); }
```

### C++ contract (re-derived)

`OnActorCreationSuccess` (lines 1677–1692 of `gcs_actor_manager.cc`):
1. `ActorTable().Put(actor_id, data, callback)` — **persist**
2. Inside the Put callback:
   - `PublishActor(...)` — **publish** (line 1685)
   - `RunAndClearActorCreationCallbacks(...)` — **resolve callbacks** (line 1690)

A successful callback implies: persistence completed, publication completed.

### Fix

Moved callback resolution into the spawned async block, after persist and publish:

```rust
let callback_senders = self.create_callbacks.remove(actor_id)...;
tokio::spawn(async move {
    // 1. Persist
    storage.actor_table().put(&key, &actor_data).await?;
    // 2. Publish
    handler.publish_pubmessage(msg);
    // 3. Resolve callbacks
    for tx in senders { tx.send(Ok(reply)); }
});
```

The oneshot senders are removed from `create_callbacks` synchronously (to prevent double-resolution by concurrent paths), but moved into the async block for deferred resolution.

### Tests added (3 new)

1. `test_creation_success_callbacks_fire_only_after_actor_table_persist` — callback remains pending while persist is gated; fires after unblock; storage contains ALIVE state
2. `test_creation_success_callbacks_do_not_run_while_persist_blocked` — polls callback receiver 5 times with yields + 20ms sleep while blocked; all attempts confirm pending
3. `test_creation_success_callbacks_are_ordered_after_publication` — atomic flag set by pubsub subscriber; by callback resolution time, publication has occurred

### Test results

```
CARGO_TARGET_DIR=target_cb_ordering cargo test -p ray-gcs --lib
test result: ok. 471 passed; 0 failed (+3 from 468)
```

---

## Reports Updated

- `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` — added 3 R5 publication ordering tests + 3 R6 callback ordering tests
- `2026-03-21_BACKEND_PORT_REAUDIT_GCS6_ROUND6.md` — updated "Still open" to closed, added Round 6 fix section
- `2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md` — added 3 callback ordering rows to checklist, added Round 6 section

---

## GCS-6 Final Status: **fixed**

The full C++ `OnActorCreationSuccess` callback-ordering contract is now matched:

1. `set_preempted_and_publish` -> persist `preempted=true` -> **then** publish
2. `on_actor_creation_success` -> persist full ALIVE record -> **then** publish -> **then** resolve callbacks
3. If persistence fails, neither publication nor callbacks fire
4. A pubsub subscriber cannot observe actor state that storage does not yet contain
5. A successful creation callback implies the actor-table update is already durably written and published

## Complete Parity Status — All 12 Codex Re-Audit Findings

| Finding | Area | Status | Closed In |
|---|---|---|---|
| GCS-1 | Internal KV persistence | fixed | Round 2 |
| GCS-4 | Job info enrichment/filters | fixed | Round 6 |
| GCS-6 | Drain node side effects | **fixed (full lifecycle + callback ordering)** | **GCS-6 Round 6** |
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
