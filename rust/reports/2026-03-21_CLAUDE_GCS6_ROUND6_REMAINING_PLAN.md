# Claude Remaining Plan After GCS-6 Round 5 Claim

Date: 2026-03-21
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim full backend parity yet.

The remaining gap is callback ordering in `on_actor_creation_success()`:

- Rust still resolves create callbacks before the async actor-table persistence completes
- C++ resolves those callbacks only in the `ActorTable().Put()` completion path, after persistence and publication

## Non-Negotiable Rule

Do not call `GCS-6` fixed unless a successful actor-creation callback implies the actor-table update has already been persisted.

## Required Work Order

### 1. Re-derive the exact C++ ordering contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`

Focus on:

- `OnActorCreationSuccess`
- `RunAndClearActorCreationCallbacks`

Write down explicitly:

- where `ActorTable().Put()` happens
- where publication happens
- where callbacks happen
- the exact ordering among those three events

### 2. Add failing tests first

Add targeted tests:

- `test_creation_success_callbacks_fire_only_after_actor_table_persist`
- `test_creation_success_callbacks_do_not_run_while_persist_blocked`
- `test_creation_success_callbacks_are_ordered_after_publication`

The tests should fail under the current Rust behavior.

### 3. Fix Rust callback ordering

Read and modify:

- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

Required behavior:

1. move success callback resolution for `on_actor_creation_success()` into the async post-persist path
2. preserve C++-style ordering:
   - persist
   - publish
   - resolve callbacks

Acceptance:

- no success callback is delivered while persistence is blocked
- success callbacks imply already-persisted actor state

### 4. Re-run validation

- rerun the new targeted ordering tests
- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ callback ordering contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status for `GCS-6`: `fixed` or `still open`

Do not use “publication ordering fixed” as a substitute for full callback-ordering parity.
