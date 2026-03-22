# Claude Remaining Plan After GCS-6 Round 7

Date: 2026-03-22
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim FULL parity yet.

The remaining gap is optional-feature parity:

- C++ supports configurable actor lifecycle/export event emission
- Rust `ray-gcs` still does not implement that feature

## Non-Negotiable Rule

Do not de-scope this as “not a parity requirement” if the user is explicitly asking for FULL parity.

Disabled-by-default is not the same as absent from the contract.

## Required Work Order

### 1. Re-derive the exact C++ configurable event contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor.cc`
- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor.h`
- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/Users/istoica/src/ray/src/ray/common/ray_config_def.h`
- `/Users/istoica/src/ray/src/ray/util/event.cc`

Write down explicitly:

- which flags enable actor lifecycle/export events
- which lifecycle transitions emit them
- what output channels exist when enabled

### 2. Read the existing Rust observability infrastructure

Read:

- Rust observability crates already identified in the Round 7 report
- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`

State clearly:

- what reusable event/export infrastructure already exists
- what wiring into `ray-gcs` is missing

### 3. Add failing tests first

If you implement the feature, add tests for enabled behavior, for example:

- `test_actor_export_event_emitted_on_creation_success_when_enabled`
- `test_actor_export_event_emitted_on_restart_when_enabled`
- `test_actor_export_event_emitted_on_death_when_enabled`

Also add tests for disabled behavior if relevant:

- `test_actor_export_event_not_emitted_when_disabled`

The tests must prove the feature exists behind configuration, like in C++.

### 4. Implement the missing feature

Required behavior:

1. actor lifecycle/export events must be emitted on the relevant actor transitions
2. emission must be controlled by configuration, consistent with the C++ model
3. emitted content should match the C++ event payload closely enough for parity

Do not add a fake stub and call it parity.

### 5. Re-run validation

- rerun the new targeted event tests
- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ configurable event contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status: `fixed` or `still open`

Do not reclassify this as “not a parity requirement” if the user still wants FULL parity.
