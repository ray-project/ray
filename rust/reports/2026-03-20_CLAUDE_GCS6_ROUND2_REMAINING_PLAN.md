# Claude Remaining Plan After GCS-6 Round 2

Date: 2026-03-20
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim full backend parity yet.

`GCS-6` is closer, but there is still an open actor-preemption lifecycle gap:

1. preempted actor state is not clearly persisted to actor table storage at preemption time
2. preempted actor state is not later used to drive `num_restarts_due_to_node_preemption` accounting

## Non-Negotiable Rule

Do not call `GCS-6` fixed unless the full actor-preemption lifecycle matches C++, not just the initial mark-and-publish step.

## Required Work Order

### 1. Re-derive the full C++ actor-preemption lifecycle

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.h`

Write down explicitly:

- where `preempted` is persisted
- where `preempted` is later consumed
- how `num_restarts_due_to_node_preemption` changes restart accounting
- whether/when `preempted` is reset

### 2. Add failing Rust tests first

Add targeted tests for the remaining semantics:

- `test_preempted_actor_state_persisted_to_actor_table`
- `test_node_preemption_increments_num_restarts_due_to_node_preemption`
- `test_node_preemption_restart_does_not_consume_regular_restart_budget`
- `test_preempted_actor_state_survives_gcs_restart_until_consumed`

Run them and confirm they fail before changing code.

### 3. Fix Rust actor-preemption lifecycle

Read and modify at least:

- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

Required behavior:

1. when `set_preempted_and_publish(...)` runs, persist the updated actor record to table storage, not just in-memory + pubsub
2. when the later actor failure/restart path runs, consume `preempted` the same way C++ does
3. update `num_restarts_due_to_node_preemption` when appropriate
4. make restart budgeting treat node-preemption restarts like C++ does

Acceptance:

- the full lifecycle matches C++ behavior, not just the initial mark

### 4. Re-run validation

- rerun the new targeted actor-preemption tests
- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status for `GCS-6`: `fixed` or `still open`

Do not rename “initial mark-and-publish parity” to full parity.
The issue is fixed only when the later restart-accounting semantics also match.
