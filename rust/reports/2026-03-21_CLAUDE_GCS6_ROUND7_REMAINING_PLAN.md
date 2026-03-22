# Claude Remaining Plan After GCS-6 Round 6

Date: 2026-03-21
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim full backend parity yet.

The last specifically tracked callback-ordering gap now appears fixed, but a broader actor-lifecycle gap remains:

- C++ emits actor lifecycle/export events across actor state transitions
- Rust `ray-gcs` still does not appear to implement an equivalent event-emission path

## Non-Negotiable Rule

Do not call full parity complete unless you either:

1. implement the missing actor lifecycle/export events, or
2. prove with code references that those events are intentionally outside the Rust backend’s externally visible contract

## Required Work Order

### 1. Re-derive the exact C++ event-emission contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.h`

Focus on:

- `WriteActorExportEvent(...)`
- the transitions where it is called

Write down explicitly:

- which actor lifecycle transitions emit events
- what event payload fields are derived from actor table data
- whether these events are observable outside the process

### 2. Read the live Rust actor manager

Read:

- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

State clearly:

- whether an equivalent event-emission path exists
- if not, which transitions still lack it

### 3. Add failing tests first if you implement it

Add targeted tests for event emission on the relevant transitions, such as:

- `test_actor_creation_success_emits_lifecycle_event`
- `test_actor_restart_emits_lifecycle_event`
- `test_actor_death_emits_lifecycle_event`

If the Rust architecture intentionally has no equivalent event sink, do not invent fake tests. Instead write the source-backed argument explaining why this is not part of the Rust parity contract.

### 4. Fix or explicitly de-scope

Option A:
- implement the missing lifecycle/export event emission in Rust and test it

Option B:
- produce a rigorous, source-backed explanation for why this is not an externally required parity item for the Rust backend

Do not hand-wave this as “architectural” without proof.

### 5. Re-run validation

- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ event-emission contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status: `fixed`, `not a parity requirement`, or `still open`

Do not call this closed unless one of those three outcomes is justified clearly.
