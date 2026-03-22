# Claude Round 4 — GCS-6 Creation-Success Persistence Fix Report for Codex

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 3 re-audit (`2026-03-21_BACKEND_PORT_REAUDIT_GCS6_ROUND3.md`) identified one remaining actor-preemption lifecycle gap that Round 3 did not close:

1. **Rust `on_actor_creation_success()` reset `preempted=false` only in memory and pubsub.** C++ persists the full updated ALIVE actor record — including `preempted=false` — to `ActorTable().Put()`. Without that persistence, a GCS restart after creation success would resurrect stale `preempted=true` from storage, causing the later restart-accounting path to diverge from C++.

The Round 3 re-audit also noted:

2. **The Round 3 tests that claimed to prove the `preempted=false` reset went through manual in-memory mutation, not the real `on_actor_creation_success()` path.** Specifically, `test_node_preemption_restart_does_not_consume_regular_restart_budget` manually set `a.preempted = false` in the test body rather than calling `on_actor_creation_success()`.

---

## Step A: C++ Contract Re-Derived

### Files read

| File | Lines | What it defines |
|---|---|---|
| `src/ray/gcs/actor/gcs_actor_manager.cc` | 1655–1693 | `OnActorCreationSuccess` — full creation-success path |

### C++ `OnActorCreationSuccess` exact sequence (lines 1655–1692)

```cpp
// 1. Update in-memory fields
mutable_actor_table_data->set_timestamp(time);
if (actor->GetState() != RESTARTING) {
    mutable_actor_table_data->set_start_time(time);
}
actor->UpdateState(ALIVE);
mutable_actor_table_data->set_node_id(node_id.Binary());
mutable_actor_table_data->set_repr_name(reply.actor_repr_name());
mutable_actor_table_data->set_preempted(false);              // ← line 1669

// 2. Update in-memory indexes
created_actors_[node_id].emplace(worker_id, actor_id);

// 3. Persist to storage
gcs_table_storage_->ActorTable().Put(                        // ← line 1677
    actor_id,
    *mutable_actor_table_data,                               // ← full record including preempted=false
    {[...](Status status) {
        // 4. Publish in the Put callback
        gcs_publisher_->PublishActor(actor_id, ...);         // ← line 1685
        // 5. Resolve creation callbacks
        RunAndClearActorCreationCallbacks(actor, reply, Status::OK());
    }, io_context_});
```

### Key observations

1. `preempted=false` is set **before** the `Put()` call, so the persisted record has `preempted=false`
2. The `Put()` persists the **full** `ActorTableData`, not just a delta — this includes `state=ALIVE`, `preempted=false`, `num_restarts`, `num_restarts_due_to_node_preemption`, `node_id`, `address`, etc.
3. Publish happens **inside** the `Put()` callback, so publication is ordered after persistence
4. Creation callbacks are resolved after both persist and publish

---

## Step B: What Was Still Missing After Round 3

| # | C++ Behavior | Rust After Round 3 | Gap |
|---|---|---|---|
| 1 | `OnActorCreationSuccess` persists full ALIVE record to `ActorTable().Put()` | Only updates in-memory + publishes; no `actor_table().put()` call | **Yes** |
| 2 | Persisted record has `preempted=false` | `preempted=false` only in memory | **Yes** |
| 3 | GCS restart after creation success loads `preempted=false` from storage | GCS restart would load stale `preempted=true` | **Yes** |

The Round 3 test `test_node_preemption_restart_does_not_consume_regular_restart_budget` tested the budget semantics correctly, but its `preempted=false` reset was done by manually mutating the actor in the test body, not by calling `on_actor_creation_success()`. That meant the test passed without proving the real code path persists.

---

## Step C: Tests Added (3 new, all failing before fix)

All tests were confirmed to fail before the code change was made.

All 3 tests use the **real `on_actor_creation_success()` method**, not manual in-memory mutation.

### 1. `test_on_actor_creation_success_persists_preempted_reset`

Setup:
- Register actor as ALIVE on node 1
- Persist initial actor to storage
- `set_preempted_and_publish` → storage has `preempted=true`
- `on_node_dead` → actor transitions to RESTARTING

Action:
- Call **real** `on_actor_creation_success(actor_id, address, pid, resources, node_2)`

Assertions:
- In-memory actor has `preempted=false`
- `storage.actor_table().get(key)` returns record with `preempted=false`

**Proves:** the real creation-success path persists the `preempted=false` reset to storage, not just memory.

### 2. `test_preempted_reset_survives_gcs_restart_after_creation_success`

Setup:
- Same as test 1: preempt → kill → real creation-success

Action:
- Create a fresh `GcsActorManager`, call `initialize()` (loads from storage)

Assertions:
- Loaded actor has `preempted=false`
- Loaded actor has `state=ALIVE`

**Proves:** after a GCS restart, the creation-success state is correctly loaded from storage. No stale `preempted=true` resurrection.

### 3. `test_real_creation_success_path_clears_preempted_in_storage_not_just_memory`

Setup:
- Register actor with `max_restarts=5` as ALIVE on node 1
- Persist initial actor
- `set_preempted_and_publish` → verify storage has `preempted=true`
- `on_node_dead` → verify RESTARTING with `num_restarts_due_to_node_preemption=1`

Action:
- Call **real** `on_actor_creation_success(actor_id, address, pid, resources, node_2)`

Assertions:
- `storage.actor_table().get(key)` returns:
  - `preempted=false`
  - `state=ALIVE`
  - `num_restarts_due_to_node_preemption=1` (preserved, not lost)

**Proves:** the full preempt → kill → restart → creation-success flow produces correct persisted state, including both the reset field and the preserved counters.

---

## Step D: Code Change

### `rust/ray-gcs/src/actor_manager.rs` — `on_actor_creation_success()`

**Before (Round 3):**

```rust
let msg = Self::build_actor_pub_message(&actor);
self.registered_actors.insert(*actor_id, Arc::new(actor));
Some(msg)
// ...
self.publish_deferred_messages(pub_msg.into_iter().collect());
// NO persistence to actor table storage
```

**After (Round 4):**

```rust
let msg = Self::build_actor_pub_message(&actor);
let actor_data_for_persist = actor.clone();
self.registered_actors.insert(*actor_id, Arc::new(actor));
Some((msg, actor_data_for_persist))
// ...
if let Some((msg, actor_data)) = pub_msg {
    self.publish_deferred_messages(vec![msg]);

    // C++ parity: ActorTable().Put() with full ALIVE record
    let storage = self.table_storage.clone();
    let aid = *actor_id;
    tokio::spawn(async move {
        let key = hex::encode(aid.binary());
        if let Err(e) = storage.actor_table().put(&key, &actor_data).await {
            tracing::error!(?aid, ?e, "Failed to persist actor creation success");
        }
    });
}
```

The `tokio::spawn` pattern matches:
- C++ `ActorTable().Put()` with async callback — both are fire-and-forget from the caller's perspective
- The full actor record is persisted, including `preempted=false`, `state=ALIVE`, `num_restarts`, `num_restarts_due_to_node_preemption`, etc.

---

## Step E: Test Results

### New targeted tests (before fix → after fix)

```
Before: 0 passed, 3 failed
After:  3 passed, 0 failed
```

### Full package tests

```
$ CARGO_TARGET_DIR=target_preempt2 cargo test -p ray-gcs --lib
test result: ok. 465 passed; 0 failed  (+3 from 462)

$ CARGO_TARGET_DIR=target_preempt2 cargo test -p ray-raylet --lib
test result: ok. 394 passed; 0 failed

$ CARGO_TARGET_DIR=target_preempt2 cargo test -p ray-core-worker --lib
test result: ok. 483 passed; 0 failed

$ CARGO_TARGET_DIR=target_preempt2 cargo test -p ray-pubsub --lib
test result: ok. 63 passed; 0 failed

Total: 1,405 passed, 0 failed
```

---

## Complete File Change List

| File | Lines Changed | What |
|---|---|---|
| `ray-gcs/src/actor_manager.rs` | +150 ~8 | Persist in `on_actor_creation_success`, 3 new tests |

---

## Full Actor-Preemption Lifecycle Parity Checklist

| C++ Behavior | Rust Status | Proof |
|---|---|---|
| `SetPreemptedAndPublish` marks `preempted=true` in memory | **Matches** | `test_autoscaler_drain_marks_node_actors_preempted` (R2) |
| `SetPreemptedAndPublish` publishes via pubsub | **Matches** | `test_autoscaler_drain_publishes_actor_preemption_state` (R2) |
| `SetPreemptedAndPublish` persists to `ActorTable().Put()` | **Matches** | `test_preempted_actor_state_persisted_to_actor_table` (R3) |
| Preempted state survives GCS restart | **Matches** | `test_preempted_actor_state_survives_gcs_restart_until_consumed` (R3) |
| `RestartActor` increments `num_restarts_due_to_node_preemption` when preempted | **Matches** | `test_node_preemption_increments_num_restarts_due_to_node_preemption` (R3) |
| `RestartActor` excludes preemption restarts from budget | **Matches** | `test_node_preemption_restart_does_not_consume_regular_restart_budget` (R3) |
| `RestartActor` grants extra restart for preempted actor at budget limit | **Matches** | `test_node_preemption_restart_does_not_consume_regular_restart_budget` (R3) |
| `OnActorCreationSuccess` resets `preempted=false` in memory | **Matches** | `test_on_actor_creation_success_persists_preempted_reset` (R4) |
| `OnActorCreationSuccess` persists full ALIVE record to `ActorTable().Put()` | **Matches** | `test_on_actor_creation_success_persists_preempted_reset` (R4) |
| `OnActorCreationSuccess` persisted state survives GCS restart | **Matches** | `test_preempted_reset_survives_gcs_restart_after_creation_success` (R4) |
| Full preempt→kill→restart→creation-success flow persists correctly | **Matches** | `test_real_creation_success_path_clears_preempted_in_storage_not_just_memory` (R4) |
| `SetPreemptedAndPublish` publishes only after persistence (ordering) | **Matches** | `test_preempted_publish_occurs_only_after_actor_table_persist` (R5) |
| `OnActorCreationSuccess` publishes only after persistence (ordering) | **Matches** | `test_creation_success_publish_occurs_only_after_actor_table_persist` (R5) |
| Pubsub observers only see already-persisted actor state | **Matches** | `test_pubsub_observer_cannot_see_unpersisted_actor_state` (R5) |

---

## Reports Updated

- `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` — added 3 new test entries under GCS-6 (R4), then 3 more for R5 ordering
- `2026-03-21_BACKEND_PORT_REAUDIT_GCS6_ROUND4.md` — updated with Round 5 fix details closing the ordering gap
- `2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND4.md` — updated parity checklist to include ordering tests

---

## Round 5 — Publication Ordering Fix (2026-03-21)

Round 4 closed the persistence gap but left an ordering gap: Rust published actor state before persistence completed. Round 5 closes this.

### C++ ordering contract (re-derived)

Both `SetPreemptedAndPublish` (lines 1435–1442) and `OnActorCreationSuccess` (lines 1677–1692) in `gcs_actor_manager.cc`:
1. Update in-memory state
2. Call `ActorTable().Put(actor_id, actor_table_data, callback)`
3. **Publish from inside the Put callback** — publication is strictly ordered after persistence

### What was wrong in Rust (before Round 5)

Both `set_preempted_and_publish()` and `on_actor_creation_success()`:
1. Updated in-memory state
2. Published immediately (synchronous)
3. Spawned async fire-and-forget persistence

This meant a subscriber could observe actor state that storage did not yet contain.

### Round 5 fix

Both methods now:
1. Update in-memory state
2. Spawn async task that: persists first → publishes only after successful persist
3. If persistence fails, publication is skipped (no false positive observer notifications)

### Tests added (3 new, all ordering-specific)

1. `test_preempted_publish_occurs_only_after_actor_table_persist` — `GatedStoreClient` blocks persist; asserts no pubsub message while blocked
2. `test_creation_success_publish_occurs_only_after_actor_table_persist` — same gated pattern for ALIVE publication
3. `test_pubsub_observer_cannot_see_unpersisted_actor_state` — asserts storage contains state when pubsub delivers

### Test results

```
CARGO_TARGET_DIR=target_ordering cargo test -p ray-gcs --lib
test result: ok. 468 passed; 0 failed (+3 from 465)
```

---

## GCS-6 Final Status: **fixed**

The full C++ actor-preemption lifecycle is now implemented in Rust with matching durability/publication ordering:

1. `SetPreemptedAndPublish` → mark `preempted=true`, persist, **then** publish ✓
2. `on_node_dead` → preemption-aware restart accounting, `num_restarts_due_to_node_preemption` ✓
3. `on_actor_creation_success` → reset `preempted=false`, persist full ALIVE record, **then** publish ✓
4. Both directions of the `preempted` flag survive GCS restart ✓
5. Publication is strictly ordered after persistence — observers only see already-persisted state ✓

## Complete Parity Status — All 12 Codex Re-Audit Findings

| Finding | Area | Status | Closed In |
|---|---|---|---|
| GCS-1 | Internal KV persistence | fixed | Round 2 |
| GCS-4 | Job info enrichment/filters | fixed | Round 6 |
| GCS-6 | Drain node side effects | **fixed (full lifecycle + ordering)** | **GCS-6 Round 5** |
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
