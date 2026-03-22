# Claude Round 11 Remaining Plan

Date: 2026-03-20
Reference: `rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND11.md`

## Non-Negotiable Rule

Do not claim the backend is fully parity-complete while the report itself still lists `GCS-6` as only improved.

## Remaining Work

1. Re-open and finish `GCS-6`

## GCS-6

### Read first

- the C++ GCS node drain flow
- the C++ autoscaler/node-manager coordination around drain

### Rust files to inspect first

- `/Users/istoica/src/ray/rust/ray-gcs/src/node_manager.rs`
- `/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs`
- `/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs`

### What is still wrong

- the Round 11 report itself says the full cross-manager drain protocol remains narrower than C++

### Required workflow

1. derive the exact remaining C++ contract
2. identify the still-missing Rust side effects
3. add failing tests first
4. fix Rust
5. rerun targeted tests
6. update the reports

### Tests to add

- `test_gcs_drain_node_cross_manager_side_effects_match_cpp`
- `test_gcs_drain_node_matches_autoscaler_and_node_manager_state_transitions`
- `test_gcs_drain_node_runtime_observable_effects_match_cpp`

### Acceptance

- only mark full parity complete once `GCS-6` is no longer described as narrower than C++

## Update

After fixing `GCS-6`, update:

- `/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_BACKEND_PORT_REAUDIT_ROUND11.md`
- `/Users/istoica/src/ray/rust/reports/2026-03-20_PARITY_FIX_REPORT_ROUND11.md`
