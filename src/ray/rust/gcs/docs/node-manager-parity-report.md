# Node Manager Behavior Parity Report

## Blocker Statement

> Node manager behavior is materially weaker than C++.

**Cited evidence:**
1. "`get_all_node_info` ignores filters and always reports `num_filtered = 0`" — `node_manager.rs:259-279`
2. "`check_alive` returns empty `ray_version`" — `node_manager.rs:282-299`
3. "`drain_node` returns a placeholder empty node ID" — `node_manager.rs:302-316`
4. "`get_all_node_address_and_liveness` only returns alive nodes and always sets `death_info = None`" — `node_manager.rs:318-340`

## Resolution: Blocker is already fully resolved

Every cited issue was fixed in earlier work. The line numbers reference OLD code that no longer exists.

---

## Evidence: Claim-by-claim refutation

### Claim 1: "`get_all_node_info` ignores filters and `num_filtered = 0`"

**Current code** (lines 420-519): Full filtering support matching C++ `HandleGetAllNodeInfo` (gcs_node_manager.cc:237-378):
- `state_filter`: filters by ALIVE or DEAD
- `node_selectors`: OR-logic filtering by node_id, node_name, node_ip_address, is_head_node
- `limit`: caps results
- `num_filtered = total - infos.len()`: accurate count

Tests: `test_state_filter_alive`, `test_state_filter_dead`, `test_node_id_selector`, `test_node_name_selector`, `test_ip_address_selector`, `test_is_head_node_selector`, `test_multiple_selectors_or_logic`, `test_limit`, `test_limit_with_state_filter`, `test_state_filter_with_selectors`, `test_state_filter_alive_with_selector_for_dead_node`

### Claim 2: "`check_alive` returns empty `ray_version`"

**Current code** (lines 522-540):
```rust
ray_version: RAY_VERSION.to_string(),  // "3.0.0.dev0"
```
`RAY_VERSION` constant is defined at line 20 as `"3.0.0.dev0"`.
Test: `test_grpc_check_alive` asserts `reply.ray_version == RAY_VERSION`.

### Claim 3: "`drain_node` returns placeholder empty node ID"

**Current code** (lines 542-583): Iterates `drain_node_data`, echoes each `node_id` in `DrainNodeStatus`:
```rust
statuses.push(DrainNodeStatus { node_id: node_id.clone() });
```
Tests: `test_grpc_drain_node_echoes_node_ids` (verifies both node IDs echoed), `test_grpc_drain_node_unknown_node_still_echoed` (verifies unknown nodes also echoed).

### Claim 4: "`get_all_node_address_and_liveness` only returns alive and `death_info = None`"

**Current code** (lines 586-671): Full implementation matching C++ `HandleGetAllNodeAddressAndLiveness` + `GetAllNodeAddressAndLiveness` (gcs_node_manager.cc:395-485):
- Returns BOTH alive and dead nodes
- `state_filter` support (ALIVE, DEAD, or both)
- `node_ids` filter (optimized lookup path)
- `limit` support
- `state` copied from `GcsNodeInfo.state` (not hardcoded)
- `death_info` copied from `GcsNodeInfo.death_info` (not None)

Tests: `test_liveness_alive_node`, `test_liveness_includes_dead_nodes`, `test_liveness_dead_node_has_death_info`, `test_liveness_state_filter_alive`, `test_liveness_state_filter_dead`, `test_liveness_node_ids_filter`, `test_liveness_node_ids_filter_alive_and_dead`, `test_liveness_limit`, `test_liveness_unregister_with_death_info`

---

## Test coverage: 42 tests pass

All node manager tests verify the behaviors described above. No code changes needed.
