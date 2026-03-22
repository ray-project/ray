# Backend Port Re-Audit After GCS-6 Round 5 Claim

Date: 2026-03-21
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md`

## Bottom Line

The latest round fixes publication ordering, but I still do not think full parity is complete.

The remaining gap is narrow and clear:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs): `on_actor_creation_success()` still resolves creation-success callbacks before the async actor-table persistence finishes.

In C++, those callbacks run inside the `ActorTable().Put()` completion path, after persistence and publication.

So Rust can still acknowledge actor creation before the corresponding actor-table state is durably written. That is still a real observable difference from C++.

## Step Trace

### 1. Reviewed the latest Round 5 report

I read:

- [`2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md`](/Users/istoica/src/ray/rust/reports/2026-03-21_PARITY_FIX_REPORT_GCS6_ROUND5.md)

The report correctly claims one real improvement:

- pubsub publication now occurs after actor-table persistence in the two targeted paths

That is reflected in the live Rust code.

### 2. Re-checked the C++ creation-success path

File checked:

- [`gcs_actor_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc)

The relevant ordering in `OnActorCreationSuccess(...)` is:

1. update in-memory state
2. `ActorTable().Put(...)`
3. inside the `Put` callback:
   - publish actor state
   - run actor-creation callbacks

So C++ gives a stronger guarantee than publish-after-persist alone:

- a successful create callback also implies persistence has already completed

### 3. Re-checked the live Rust creation-success path

File checked:

- [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)

What is now fixed:

1. `on_actor_creation_success()` persists first
2. it publishes only after persistence succeeds

What still differs from C++:

1. after spawning the persist-and-publish task, Rust immediately removes and resolves `create_callbacks`
2. those callbacks are therefore not gated on persistence completion
3. they are also not ordered after publication

This is the remaining parity gap.

### 4. Why the current tests are still insufficient

The new tests prove:

- publication does not happen before persistence

They do not prove:

- create callbacks do not happen before persistence
- create callbacks do not happen before publication

So the latest report still overstates closure.

## Current Status

### Fixed enough inside `GCS-6`

- drain-node semantics
- preemption state persistence
- preemption-aware restart accounting
- creation-success reset persistence
- publication-after-persistence ordering

### Previously open inside `GCS-6` (now closed in Round 6 fix)

1. ~~creation-success callbacks still fire before persistence completes~~ — **fixed**: callbacks now resolve inside the spawned async task, after persist and publish

## Round 6 Fix (2026-03-21)

The remaining callback ordering gap identified in this re-audit has been closed.

### Code change (`rust/ray-gcs/src/actor_manager.rs` — `on_actor_creation_success()`)

**Before:** `create_callbacks` were removed from the DashMap and resolved synchronously, immediately after spawning the persist-then-publish task. Callbacks fired before persistence or publication.

**After:** `create_callbacks` are removed from the DashMap synchronously (to prevent double-resolution), but the oneshot senders are moved into the spawned async block. Inside that block, the ordering is:
1. Persist (`storage.actor_table().put(...)`)
2. Publish (`handler.publish_pubmessage(msg)`)
3. Resolve callbacks (`tx.send(Ok(reply))`)

If persistence fails, neither publication nor callbacks fire.

### Tests added (3 new)

1. `test_creation_success_callbacks_fire_only_after_actor_table_persist` — uses `GatedStoreClient`; proves callback receiver stays pending while persist is blocked; after unblock, callback fires and storage contains ALIVE state
2. `test_creation_success_callbacks_do_not_run_while_persist_blocked` — polls callback receiver multiple times (with yields and sleep) while persist is blocked; proves it remains pending the entire time
3. `test_creation_success_callbacks_are_ordered_after_publication` — uses an atomic flag set by a pubsub subscriber; proves publication has already occurred by the time the callback resolves

### Test results

```
CARGO_TARGET_DIR=target_cb_ordering cargo test -p ray-gcs --lib
test result: ok. 471 passed; 0 failed (+3 from 468)
```

## Final Assessment (updated after Round 6 fix)

**GCS-6 is now fully closed.** The full C++ `OnActorCreationSuccess` callback-ordering contract is matched:
- persist → publish → callbacks
- a successful callback implies the actor-table update is already durably written and published
- callbacks do not fire while persistence is blocked
