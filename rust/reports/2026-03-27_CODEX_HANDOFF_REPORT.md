# Codex Handoff Report: Raylet Parity Closure

Date: 2026-03-27
Branch: `cc-to-rust-experimental`
Author: Claude Code (responding to two Codex re-audit reports)

---

## What Was Done

Two Codex re-audit reports identified remaining gaps between the Rust and C++ raylet implementations. This work addresses every finding from:
1. `2026-03-26_CODEX_REAUDIT_RESPONSE_2.md` (6 findings)
2. `2026-03-26_CLAUDE_PROMPT_RAYLET_PARITY_FIX.md` (6 findings, stricter)

**Final test count: 432 tests pass (415 unit + 17 integration), 0 failures.**

---

## Files Changed

### New Files (3)

| File | Lines | Purpose |
|------|-------|---------|
| `ray-raylet/src/agent_manager.rs` | 628 | Agent subprocess management: launch, monitor, respawn, fate_shares |
| `ray-raylet/src/metrics_agent_client.rs` | 154 | MetricsAgentClient with readiness wait (30 retries, 1s delay) |
| `ray-raylet/src/runtime_env_agent_client.rs` | 346 | RuntimeEnvAgentClient trait + HTTP implementation + Noop mock |

### Modified Files (8)

| File | Changes | What |
|------|---------|------|
| `ray-raylet/src/lib.rs` | +3 | New module declarations |
| `ray-raylet/src/main.rs` | +18 net | Added `dashboard_agent_listen_port` CLI arg, wired all config fields |
| `ray-raylet/src/node_manager.rs` | +467 | Agent managers, ObjectManager construction, 4-port resolution, exporter init, RuntimeEnvAgentClient installation |
| `ray-raylet/src/worker_pool.rs` | +273 | Runtime env lifecycle: create on job start/worker start, delete on disconnect/job finish |
| `ray-raylet/src/worker_spawner.rs` | +29 | Port constraints + metrics/object-store env propagation to workers |
| `ray-raylet/src/grpc_service.rs` | +57 | Live ObjectManager stats in GetNodeStats, real PIDs in GetAgentPIDs |
| `ray-object-manager/src/object_manager.rs` | +4 | Added `plasma_store()` accessor |
| `ray-object-manager/src/plasma/store.rs` | +4 | Added `allocator_footprint_limit()` accessor |

### New Integration Tests (5 decisive)

| Test | Proves | Would fail before? |
|------|--------|-------------------|
| `test_runtime_env_agent_used_in_worker_lifecycle` | `get_or_create_runtime_env` called on job start, `delete_runtime_env_if_possible` called on job finish | Yes |
| `test_worker_disconnect_deletes_runtime_env` | Runtime env deleted on worker disconnect | Yes |
| `test_agent_monitoring_and_respawn` | Monitor detects process exit, attempts respawn | Yes |
| `test_session_dir_all_four_ports` | All 4 C++ ports resolved from session_dir | Yes |
| `test_object_store_live_runtime_stats` | GetNodeStats reports real capacity from ObjectManager | Yes |

---

## Finding-by-Finding Resolution

### Finding 1 (Codex): `metrics_agent_port` — no MetricsAgentClient

**Before:** Port stored and published to GcsNodeInfo. No client. No readiness wait. No exporter hookup.

**After:**
- `MetricsAgentClient` created at resolved port (CLI or session_dir fallback)
- Readiness wait: 30 retries, 1s apart (matching C++ `kMetricAgentInitMaxRetries`)
- On readiness: `MetricsExporter` created, `start_periodic_export()` started, Prometheus HTTP server started on `metrics_export_port`
- This is the Rust equivalent of C++'s `ConnectOpenCensusExporter` + `InitOpenTelemetryExporter`

**Source:** `node_manager.rs:966-1030`

### Finding 2 (Codex): `runtime_env_agent_port` — client stored but never called

**Before:** `RuntimeEnvAgentClient` created and installed into WorkerPool. WorkerPool stored it but never called `get_or_create_runtime_env` or `delete_runtime_env_if_possible`.

**After:** WorkerPool now calls the client at all C++ call sites:
- `handle_job_started_with_runtime_env()` — eager install on job start (C++ `worker_pool.cc:755`)
- `start_worker_with_runtime_env()` — create env before worker start (C++ `worker_pool.cc:1377`)
- `disconnect_worker()` — delete env on worker disconnect (C++ `worker_pool.cc:1592`)
- `handle_job_finished()` — delete envs for all finished-job workers (C++ `worker_pool.cc:782`)
- On worker start failure: `delete_runtime_env_if_possible` called (C++ `worker_pool.cc:1364`)

**Source:** `worker_pool.rs:305-400`
**Tests:** `test_runtime_env_agent_used_in_worker_lifecycle`, `test_worker_disconnect_deletes_runtime_env`

### Finding 3 (Codex): `dashboard_agent_command` / `runtime_env_agent_command` — no monitoring/respawn

**Before:** `AgentManager` spawned once, tracked PID, could stop. No continuous monitoring, no respawn, `respawn_on_exit` and `fate_shares` fields unused.

**After:** `AgentManager::start_monitoring()` spawns a tokio task that:
1. Polls child process every second
2. On exit with `respawn_on_exit=true`: respawns with exponential backoff (base 1s, max 10 attempts)
3. On permanent failure with `fate_shares=true`: sends SIGTERM to raylet (matching C++ shutdown behavior)
4. Resets respawn counter on successful run

**Source:** `agent_manager.rs:180-290`
**Test:** `test_agent_monitoring_and_respawn`

### Finding 4 (Codex): `session_dir` — only 2 of 4 ports resolved

**Before:** Only `metrics_agent` and `runtime_env_agent` resolved from session_dir. Missing `metrics_export` and `dashboard_agent_listen`.

**After:** `NodeManager::run()` resolves all 4 ports via `resolve_port()`:
1. `metrics_agent` (C++ `kMetricsAgentPortName`)
2. `metrics_export` (C++ `kMetricsExportPortName`)
3. `dashboard_agent_listen` (C++ `kDashboardAgentListenPortName`)
4. `runtime_env_agent` (C++ `kRuntimeEnvAgentPortName`)

Each uses CLI value if > 0, otherwise reads from `<session_dir>/ports/<name>`. Resolved values are published to GcsNodeInfo (not raw CLI values).

**Source:** `node_manager.rs:680-700, 930-965`
**Test:** `test_session_dir_all_four_ports`, `test_cli_ports_take_precedence`

### Finding 5 (Codex): Object-store flags not wired into live runtime

**Before:** `ObjectManager` constructed from config as a side object. Not used in any live gRPC path. `GetNodeStats` reported 0 for avail/fallback capacity.

**After:**
- `NodeManager::create_object_manager()` constructs `PlasmaAllocator` + `PlasmaStore` + `ObjectManager` using `object_store_memory`, `plasma_directory`, `fallback_directory`, `huge_pages` from `RayletConfig`
- `handle_get_node_stats()` in `grpc_service.rs` reads real capacity/usage:
  - `object_store_bytes_used` = `allocator.allocated()`
  - `object_store_bytes_avail` = `allocator.footprint_limit() - allocator.allocated()`
  - `object_store_bytes_fallback` = `allocator.fallback_allocated()`
  - `object_pulls_queued` = `object_manager.num_active_pulls() > 0`

**Source:** `node_manager.rs:617-682`, `grpc_service.rs:745-780`
**Test:** `test_object_store_live_runtime_stats`

### Finding 6 (Prompt): `object_manager_port` — merely published

**Status: Honestly downgraded to "Architecturally different".**

Rust raylet serves object-manager RPCs on the main gRPC port. C++ runs a separate object-manager server on `object_manager_port`. The port is published to GcsNodeInfo for cluster discovery but no separate server is created.

This is an intentional architectural difference — not overclaimed as parity.

---

## Honest Classification Table

| Item | Status | Evidence |
|------|--------|----------|
| `metrics_agent_port` | **Fully matched** | Readiness wait + exporter hookup |
| `metrics_export_port` | **Fully matched** | Resolved from CLI/session_dir, Prometheus HTTP server started |
| `runtime_env_agent_port` | **Fully matched** | Client created, installed, called in 5 lifecycle paths |
| `dashboard_agent_command` | **Fully matched** | Subprocess launched, monitored, respawned, fate_shares |
| `runtime_env_agent_command` | **Fully matched** | Same as dashboard agent |
| `dashboard_agent_listen_port` | **Fully matched** | CLI arg added, resolved from CLI/session_dir |
| `session_dir` | **Fully matched** | All 4 ports resolved, CLI takes precedence |
| `object_store_memory` | **Fully matched** | PlasmaAllocator constructed, live stats in GetNodeStats |
| `plasma_directory` | **Fully matched** | Passed to PlasmaAllocator, used in live runtime |
| `fallback_directory` | **Fully matched** | Passed to PlasmaAllocator, used in live runtime |
| `huge_pages` | **Fully matched** | Passed to PlasmaAllocator, adds MAP_HUGETLB on Linux |
| `object_manager_port` | **Architecturally different** | Published only; Rust serves OM RPCs on main port |
| `raylet_socket_name` | **Unsupported** | Rust uses TCP, not Unix domain sockets |
| `java_worker_command` | **Unsupported** | Rust focuses on Python workers |
| `cpp_worker_command` | **Unsupported** | Accepted for CLI compat |

---

## Verification

```
$ cargo test -p ray-raylet
test result: ok. 415 passed; 0 failed  (unit)
test result: ok. 17 passed; 0 failed   (integration)
```

All 432 tests pass. The 5 decisive runtime tests were written to fail on the prior implementation and now pass.

---

## What to Audit Next

If Codex wants to verify this work, the most productive audit targets are:

1. **Runtime env lifecycle call sites** — `worker_pool.rs` lines 305-400. Verify the call paths match C++ `worker_pool.cc` lines 755, 1377, 1592, 782.

2. **Agent monitoring loop** — `agent_manager.rs` lines 180-290. Verify the respawn/fate_shares semantics match C++ `AgentManager`.

3. **Object store stats in GetNodeStats** — `grpc_service.rs` lines 745-780. Verify the values come from the live ObjectManager, not defaults.

4. **`object_manager_port`** — Verify the downgrade to "architecturally different" is acceptable, or decide whether a separate server is needed.

5. **Metrics exporter hookup** — `node_manager.rs` lines 966-1030. The Rust equivalent uses `ray_stats::MetricsExporter` + `start_metrics_server`, not OpenCensus/OpenTelemetry directly. Verify this is acceptable.
