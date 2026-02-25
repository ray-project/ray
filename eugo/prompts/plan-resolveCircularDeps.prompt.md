# Plan: Resolve Circular Dependency Cycles in Meson Build

**TL;DR:** Of the 8 circular dependency workarounds ("NOT AVAILABLE" comments), 5 can be fully resolved through header-only target inlining (4) + `subdir()` reordering (1), 1 is already resolved (stale comment), and only 2 truly require link-time resolution (static libraries with deep dependency chains). One additional header-only case is structurally unfixable but harmless. Net effect: reduce 13 "NOT AVAILABLE" comments down to ~4, with the remaining ones clearly documented as safe.

---

## Phase 1: Fix stale comments — `metric_interface_cpp_lib_dep` (dep #2)

This dep IS already available (defined at src/ray/meson.build L7-21, before `scheduling/` at L124) but is incorrectly marked "NOT AVAILABLE" in 3 locations:

1. In src/ray/raylet/scheduling/meson.build L105: Uncomment `metric_interface_cpp_lib_dep` in `local_resource_manager_cpp_lib_dependencies`
2. In src/ray/raylet/scheduling/meson.build L414: Uncomment `metric_interface_cpp_lib_dep` in `cluster_resource_scheduler_cpp_lib_dependencies`
3. Update the header comment at lines 4-5 to remove `metric_interface` from the "NOT available" list

---

## Phase 2: Inline 4 header-only targets into `src/ray/meson.build`

All 4 are **header-only** (no `.cc` files), verified against BUILD.bazel. Each is moved from its original `meson.build` to an inline definition in src/ray/meson.build, following the existing precedent of `metric_interface_cpp_lib_dep` (L7), `open_telemetry_metric_recorder_cpp_lib` (L38), and `stats_cpp_lib` (L74).

### Step 2a: Inline `observability_metrics_cpp_lib_dep` (resolves prerequisite for dep #1)

- **Source:** src/ray/observability/meson.build L235-246
- **Insert after:** `stats_cpp_lib_dep` definition (~L96 in src/ray/meson.build)
- **Dependencies:** Only `stats_cpp_lib_dep` — already available (inline at L74-96)
- **Cleanup:** Remove the target from `observability/meson.build`. Update the umbrella `observability_cpp_lib_dep` to reference the now-external variable (it already will — Meson variables are globally scoped within the project after definition)
- **Add comment:** Following the existing pattern, note it's inlined from `observability/` due to circular dependency with `scheduling/`

### Step 2b: Inline `raylet_metrics_cpp_lib_dep` (resolves dep #1 — 3 consumers in scheduling/)

- **Source:** src/ray/raylet/meson.build L12-25
- **Insert after:** `observability_metrics_cpp_lib_dep` from step 2a
- **Dependencies:** `observability_metrics_cpp_lib_dep` + `stats_cpp_lib_dep` — both now available inline
- **Cleanup:** Remove from `raylet/meson.build`. Uncomment in scheduling/meson.build at lines 137, 160, and 446. Update the header comment at lines 3-5

### Step 2c: Inline `worker_interface_cpp_lib_dep` (resolves dep #4 — 1 consumer in scheduling/)

- **Source:** src/ray/raylet/meson.build L132-147
- **Insert after:** `raylet_metrics_cpp_lib_dep` from step 2b (or anywhere after `common/` at L28)
- **Dependencies:** `id_cpp_lib_dep` (from common/, L28), `process_interface_cpp_lib_dep` (from util/, L5), `absl_time` (external) — all available
- **Cleanup:** Remove from `raylet/meson.build`. Uncomment in scheduling/meson.build L447

### Step 2d: Inline `rpc_client_cpp_lib_dep` (resolves dep #6 — 1 consumer in pubsub/)

- **Source:** src/ray/gcs_rpc_client/meson.build L20-34
- **Insert after:** `subdir('object_manager_rpc_client/')` (~L108) and before `subdir('pubsub/')` (~L110)
- **Dependencies:** `ray_config_cpp_lib_dep` (common/), `autoscaler_proto_cpp_grpc_lib_dep` + `gcs_service_proto_cpp_grpc_lib_dep` (protobuf/), `retryable_grpc_client_cpp_lib_dep` + `rpc_callback_types_cpp_lib_dep` (rpc/), `network_util_cpp_lib_dep` (util/) — all available before `pubsub/`
- **Cleanup:** Remove from `gcs_rpc_client/meson.build` (other targets there still reference it; they should use the now-globally-visible variable). Uncomment in pubsub/meson.build L140. Update header/comments in both files

---

## Phase 3: Reorder `subdir()` calls to resolve `gcs_client_cpp_lib_dep` (dep #7)

Research confirms the reorder is safe — `gcs/` and `gcs_rpc_client/` have **zero dependencies** on `raylet_rpc_client/`.

**Change in src/ray/meson.build (~L110-121):**

Current order:
```
subdir('pubsub/')
subdir('raylet_ipc_client/')
subdir('raylet_rpc_client/')     ← step 16, needs gcs_client
subdir('gcs/')                   ← step 17
subdir('gcs_rpc_client/')        ← step 18, defines gcs_client
```

New order:
```
subdir('pubsub/')
subdir('raylet_ipc_client/')
subdir('gcs/')                   ← moved up (no deps on raylet_rpc_client/)
subdir('gcs_rpc_client/')        ← moved up (no deps on raylet_rpc_client/)
subdir('raylet_rpc_client/')     ← moved down, now gcs_client_cpp_lib_dep is in scope
```

**Cleanup:** Uncomment `gcs_client_cpp_lib_dep` in raylet_rpc_client/meson.build L60. Update header comments at L7. Note: `object_manager/` is at L106 (`subdir('object_manager/')`), which is BEFORE the gcs block, so `object_manager/` still can't see `gcs_client_cpp_lib_dep` even after the reorder. That comment stays.

---

## Phase 4: Accept and document 3 remaining link-time-resolved deps

These cannot be resolved through inlining or reordering due to structural constraints:

| # | Missing Dep | Why it can't be resolved | Risk |
|---|---|---|---|
| 3 | `lease_dependency_manager_cpp_lib_dep` | Static library. Depends on `object_manager_cpp_lib_dep` (inline step 24). Consumer `scheduling/` is step 19. 5-step gap, no shortcut. | **Low** — symbols included transitively in both `_raylet.so` (via `core_worker` → scheduling chain) and `raylet` executable (explicit dep) |
| 5 | `worker_pool_cpp_lib_dep` | Static library. Depends on `gcs_client` (step 18), `core_worker_client_interface` (step 21), `runtime_env_agent_client` (step 25). Massive dep chain. | **Low** — same transitive inclusion reasoning |
| 8 | `object_manager_grpc_client_manager_cpp_lib_dep` | **Header-only** but consumer `object_manager_server_cpp_lib` is inside `rpc/meson.build` (step 8), while the target needs `grpc_client_cpp_lib_dep` also from `rpc/` (step 8). Structural chicken-and-egg. | **None** — header-only means no linker risk. Header found via global `-I` paths. Only impact is imperfect rebuild tracking. |

For each remaining "NOT AVAILABLE" comment:
- Update to clarify whether it's header-only (cosmetic) vs static-lib (link-time resolved)
- For the static libs, add a note confirming transitive reachability through the final link targets

---

## Phase 5: Cleanup removed targets from origin files

After Phases 2-3, several targets need housekeeping in their original `meson.build` files:

1. src/ray/observability/meson.build: Remove `observability_metrics_cpp_lib` block (L235-246). Add `# @skip` or note that it's inlined in `src/ray/meson.build`
2. src/ray/raylet/meson.build: Remove `raylet_metrics_cpp_lib` (L12-25) and `worker_interface_cpp_lib` (L132-147). Adjust any intra-file deps that referenced these
3. src/ray/gcs_rpc_client/meson.build: Remove `rpc_client_cpp_lib` (L20-34). Other targets in this file that depend on it (`gcs_client_target_cpp_lib_dep`, etc.) will still see the variable from the parent scope

---

## Phase 6: Update `IMPORTANT` header comments

Every modified `meson.build` needs its header comment updated:

- src/ray/meson.build: Update the inline target inventory to include the 4 new inlines
- src/ray/raylet/scheduling/meson.build: Update header to remove resolved deps from the "NOT available" list (only `lease_dependency_manager`, `worker_pool` remain)
- src/ray/raylet/meson.build: Note that `raylet_metrics` and `worker_interface` are inlined in parent
- src/ray/pubsub/meson.build: Remove any NOT AVAILABLE header comments
- src/ray/raylet_rpc_client/meson.build: Remove NOT AVAILABLE header for gcs_client

---

## Verification

1. `python3 eugo_sync.py` — must still report "No raptors found!" (no source files moved, only definitions relocated)
2. `./eugo_meson_setup.sh` — Meson configure must succeed (validates variable scoping: all referenced `*_cpp_lib_dep` variables are defined before use)
3. `./eugo_meson_compile.sh` — Full compile. This is the real test for the remaining 2 static-lib link-time deps
4. `grep -rn "NOT AVAILABLE" src/ray/ --include="*.build" | wc -l` — should drop from 15 to ~4 (the 2 static-lib deps in scheduling/ + 1 object_manager_grpc_client_manager in rpc/ + possibly `gcs_client` in object_manager/)
5. `./eugo_pip3_wheel.sh` — Wheel build validates `_raylet.so` linking

---

## Decisions

- **Inline over restructure:** Chose to inline header-only targets into `src/ray/meson.build` rather than creating separate interface `.build` files — follows existing precedent (`metric_interface`, `stats_cpp_lib`, `object_manager_cpp_lib`) and avoids adding new files
- **Reorder over workaround for gcs_client:** The `subdir()` swap is cleaner than inlining and verified safe — zero reverse dependencies
- **Accept remaining 3:** `lease_dependency_manager` and `worker_pool` have dependency chains too deep to untangle within the static library model. `object_manager_grpc_client_manager` is structurally unfixable but harmless (header-only). All 3 are properly resolved at final link time. These would vanish under Step 2 (shared_library consolidation) if pursued later
