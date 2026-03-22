# Claude Remaining Plan After GCS-6 Round 4

Date: 2026-03-21
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim full backend parity yet.

The remaining gap is ordering:

- Rust now persists the right actor state, but it still publishes before persistence in both major actor-preemption lifecycle paths
- C++ persists first and publishes from the `ActorTable().Put()` callback

## Non-Negotiable Rule

Do not call `GCS-6` fixed unless actor publication ordering matches C++ closely enough that a published state implies the corresponding actor-table state has already been persisted.

## Required Work Order

### 1. Re-derive the exact C++ ordering contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`

Focus on:

- `SetPreemptedAndPublish`
- `OnActorCreationSuccess`

Write down explicitly:

- where `ActorTable().Put()` happens
- where publication happens
- whether publication is inside the `Put` callback

### 2. Add failing tests first

Add targeted tests for ordering, not just eventual persistence:

- `test_preempted_publish_occurs_only_after_actor_table_persist`
- `test_creation_success_publish_occurs_only_after_actor_table_persist`
- `test_pubsub_observer_cannot_see_unpersisted_actor_state`

The tests should fail under the current publish-first Rust behavior.

### 3. Fix the ordering in Rust

Read and modify:

- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

Required behavior:

1. in `set_preempted_and_publish(...)`, persist first, then publish
2. in `on_actor_creation_success(...)`, persist first, then publish
3. if async behavior is needed, preserve callback ordering semantics rather than fire-and-forget publish-first semantics

Acceptance:

- actor pubsub messages correspond to already-persisted actor table state

### 4. Re-run validation

- rerun the new targeted ordering tests
- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ ordering contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status for `GCS-6`: `fixed` or `still open`

Do not use “eventual persistence” as a substitute for matching the C++ publish-after-persist contract.
