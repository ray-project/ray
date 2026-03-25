# Claude Remaining Plan — GCS-6 Round 17

Date: 2026-03-24
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

Round 16 added the missing gRPC path, but the live fallback behavior still differs from C++.

## Remaining Work

### Remove or align the file fallback in the `enable_ray_event` path

Current Rust behavior:

- gRPC path when `metrics_agent_port` is configured and connect succeeds
- file fallback when:
  - gRPC connect fails
  - or `metrics_agent_port` is absent and `log_dir` exists

C++ behavior:

- ray-event export starts only when the exporter/metrics agent is ready
- on failure, events are not exported
- no file fallback for the `enable_ray_event` path

## Required workflow

1. Re-read:
   - `src/ray/gcs/gcs_server.cc`
   - `src/ray/observability/ray_event_recorder.cc`
   - `rust/ray-gcs/src/server.rs`

2. Add failing tests first, such as:
   - `test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file`
   - `test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path`

3. Fix the live Rust path in `rust/ray-gcs/src/server.rs` so that:
   - `enable_ray_event` uses gRPC when available
   - if unavailable, it behaves like C++ (no export, clear warning), not file fallback

4. Re-run:
   - targeted `ray-gcs` tests
   - targeted `ray-observability` tests
   - `cargo test -p ray-gcs --lib`
   - `cargo test -p ray-observability --lib`

5. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-24_BACKEND_PORT_REAUDIT_GCS6_ROUND17.md`
   - `rust/reports/2026-03-24_PARITY_FIX_REPORT_GCS6_ROUND16.md`

## Acceptance Criteria

Only call FULL parity achieved if:

1. the live Rust `enable_ray_event` path uses the gRPC aggregator-service path
2. when that path is unavailable, Rust behavior matches C++ failure semantics closely enough
3. tests prove that

If not, status remains:

- `still open`
