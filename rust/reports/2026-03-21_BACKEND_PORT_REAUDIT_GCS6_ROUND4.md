# Backend Port Re-Audit After GCS-6 Round 4

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND4.md`

## Bottom Line

Round 4 fixes the specific persistence hole from the previous re-audit:

- `on_actor_creation_success()` now persists the updated ALIVE actor record
- the new tests exercise the real creation-success path instead of manually mutating in-memory state

That is a real improvement.

But I still would not call full parity complete yet.

The strongest remaining mismatch I verified is ordering:

- C++ persists actor-table updates first and publishes from the `ActorTable().Put()` callback
- the live Rust code still publishes first and then spawns async persistence

I verified this in the live Rust creation-success path and the preempt-and-publish path.

That means the Rust backend is still not fully equivalent to C++ for the durability/publication contract of actor state changes.

## Step Trace

### 1. Reviewed the new Round 4 report

I read:

- [`2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND4.md`](/Users/istoica/src/ray/rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND4.md)

The report correctly identifies the previous gap:

- reset of `preempted=false` needed to be persisted through the real `on_actor_creation_success()` path

The live code now does add persistence there.

### 2. Re-checked the C++ contract

File checked:

- [`gcs_actor_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc)

Relevant C++ behavior:

1. `SetPreemptedAndPublish(...)`
   - set `preempted=true`
   - `ActorTable().Put(...)`
   - publish from the `Put` callback

2. `OnActorCreationSuccess(...)`
   - update ALIVE state and reset `preempted=false`
   - `ActorTable().Put(...)`
   - publish from the `Put` callback

The ordering is part of the contract:

- the published state corresponds to a state that has already been durably written to actor table storage

### 3. Re-checked the live Rust implementation

File checked:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)

What is now fixed:

1. `on_actor_creation_success()` now clones the updated actor record and schedules `actor_table().put(...)`.
2. the new tests now go through the real creation-success path and read storage afterward.

What still differs from C++:

#### A. Creation-success ordering still differs

In [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs), `on_actor_creation_success()` currently:

1. updates in-memory state
2. publishes the ALIVE actor message immediately
3. then spawns async storage persistence

C++ does:

1. update in-memory state
2. persist to actor table
3. publish in the `Put` callback

So the Rust publish can still race ahead of persistence.

#### B. Preempt-and-publish ordering still differs too

The same issue also remains in `set_preempted_and_publish(...)`:

1. Rust publishes first
2. then spawns async `actor_table().put(...)`

C++ again persists first, then publishes from the callback.

This means the Round 4 report still overstates parity. The persistence hole for creation-success was fixed, but the stronger C++ durability-before-publication contract is still not matched.

### 4. Why the current tests are still insufficient

The new tests prove:

- persistence eventually happens
- restart after persistence observes the updated stored state

They do not prove:

- that publication only happens after persistence
- that a subscriber cannot observe actor state that is not yet durably reflected in storage

That is the remaining gap.

## Current Status

### Fixed enough inside `GCS-6`

- preempted actor state persistence at preemption time
- preemption-aware restart accounting
- creation-success persistence of `preempted=false`
- real creation-success path tests

### Previously open inside `GCS-6` (now closed in Round 5)

1. ~~`set_preempted_and_publish()` still publishes before persistence~~ — **fixed in Round 5**: persist first, publish from the persist callback
2. ~~`on_actor_creation_success()` still publishes before persistence~~ — **fixed in Round 5**: persist first, publish from the persist callback

## Round 5 Fix (2026-03-21)

The remaining ordering gap identified in this re-audit has been closed:

### Code changes (`rust/ray-gcs/src/actor_manager.rs`)

1. **`set_preempted_and_publish()`**: Moved `publish_actor_state()` call into the spawned async block, after `storage.actor_table().put()` succeeds. Publication is now strictly ordered after persistence, matching the C++ `ActorTable().Put()` callback pattern.

2. **`on_actor_creation_success()`**: Moved `publish_deferred_messages()` call into the spawned async block, after `storage.actor_table().put()` succeeds. Same persist-then-publish ordering as C++.

### Tests added (3 new)

1. `test_preempted_publish_occurs_only_after_actor_table_persist` — uses a `GatedStoreClient` that blocks persistence; proves no pubsub message is emitted while persist is blocked
2. `test_creation_success_publish_occurs_only_after_actor_table_persist` — same gated-store pattern for the creation-success path
3. `test_pubsub_observer_cannot_see_unpersisted_actor_state` — proves that when a pubsub message is delivered, storage already contains the corresponding state

### Test results

```
cargo test -p ray-gcs --lib
test result: ok. 468 passed; 0 failed (+3 from 465)
```

## Final Assessment (updated after Round 5)

**GCS-6 is now fully closed.** Both actor-state publication paths (preemption and creation-success) now publish only after successful persistence, matching the C++ `ActorTable().Put()` callback contract. The 3 new ordering tests prove this is not merely eventual persistence but strict persist-before-publish ordering.
