# Claude Remaining Plan After GCS-6 Round 3

Date: 2026-03-21
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim full backend parity yet.

The remaining gap is narrow but real:

- `on_actor_creation_success()` resets `preempted=false` in memory, but the live Rust code does not yet prove that this reset is also persisted to actor table storage like C++.

## Non-Negotiable Rule

Do not call `GCS-6` fixed unless the reset side of the actor-preemption lifecycle is also persisted and tested through the real creation-success path.

## Required Work Order

### 1. Re-derive the exact C++ creation-success contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`

Focus on:

- `OnActorCreationSuccess`

Write down explicitly:

- where `preempted=false` is set
- where the updated actor table record is persisted
- where the updated actor state is published

### 2. Add failing tests first

Add targeted tests for the remaining gap:

- `test_on_actor_creation_success_persists_preempted_reset`
- `test_preempted_reset_survives_gcs_restart_after_creation_success`
- `test_real_creation_success_path_clears_preempted_in_storage_not_just_memory`

Run them and confirm they fail before changing code.

### 3. Fix the real creation-success path

Read and modify:

- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

Required behavior:

1. when `on_actor_creation_success()` resets `preempted=false`, persist the updated actor record to actor table storage
2. make sure the persisted ALIVE actor state matches what C++ writes
3. keep pubsub behavior consistent with the persisted state

Acceptance:

- after successful creation, both in-memory state and persisted storage show `preempted=false`
- a fresh `GcsActorManager::initialize()` must load the cleared state after restart

### 4. Re-run validation

- rerun the new targeted actor tests
- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status for `GCS-6`: `fixed` or `still open`

Do not rely on tests that mutate the actor state manually in-memory and then call that parity.
The proof must go through the real creation-success path.
