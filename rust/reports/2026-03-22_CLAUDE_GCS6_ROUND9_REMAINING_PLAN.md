# Claude Remaining Plan After GCS-6 Round 8

Date: 2026-03-22
Branch: `cc-to-rust-experimental`

## Bottom Line

Do not claim FULL parity yet.

The remaining gap is no longer whether Rust emits any actor events at all.
The remaining gap is whether Rust matches the full C++ configurable actor export/event feature.

## Non-Negotiable Rule

Do not call this fixed just because `buffer_len() > 0` in tests.

FULL parity here means:

1. config-model parity
2. output-channel parity
3. payload-shape parity

## Required Work Order

### 1. Re-derive the exact C++ feature contract

Read first:

- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor.cc`
- `/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/Users/istoica/src/ray/src/ray/common/ray_config_def.h`
- `/Users/istoica/src/ray/src/ray/util/event.cc`

Write down explicitly:

- how `enable_export_api_write` behaves
- how `enable_export_api_write_config` behaves
- how `enable_ray_event` behaves
- what outputs are produced when each is enabled

### 2. Read the Rust implementation and identify exact gaps

Read:

- `/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs`
- `/Users/istoica/src/ray/rust/ray-observability/src/export.rs`
- `/Users/istoica/src/ray/rust/ray-observability/src/events.rs`

State clearly:

- what parts of the C++ config model are missing
- what sinks/outputs are missing
- what payload fields are missing

### 3. Add failing tests first

Add targeted tests for the missing pieces, for example:

- `test_actor_export_event_emitted_to_file_when_export_api_enabled`
- `test_actor_export_event_honors_export_api_write_config_actor_only`
- `test_actor_export_event_payload_matches_expected_fields`
- `test_actor_ray_event_path_emits_when_enable_ray_event_is_enabled`

If exact names differ, keep the semantics identical.

The tests should fail under the current Rust implementation.

### 4. Implement the missing feature pieces

You need to close all of the following before calling FULL parity:

1. Config parity
- support the C++-style enablement semantics, not just a single boolean

2. Output parity
- implement the actual output channel(s) needed for equivalence
- at minimum, do not claim parity until the export-file path is real

3. Payload parity
- include the major missing `ExportActorData` fields or justify each omission rigorously

Do not add test-only adapters and call that parity.

### 5. Re-run validation

- rerun the new targeted event tests
- rerun targeted `ray-gcs` tests
- `cargo test -p ray-gcs --lib`

## Reporting Requirements

When you report back, include:

- exact C++ feature contract extracted
- exact Rust files changed
- exact tests added
- exact test commands run
- exact final status: `fixed` or `still open`

Do not claim FULL parity unless the configurable actor export/event feature is actually equivalent enough to the C++ backend.
