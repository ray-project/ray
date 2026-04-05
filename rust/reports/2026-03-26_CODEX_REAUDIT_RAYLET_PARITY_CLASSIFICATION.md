# Codex Re-Audit of Raylet Parity Classification

Date: 2026-03-26
Branch: `cc-to-rust-experimental`
Subject: Re-audit of `2026-03-26_RAYLET_PARITY_CLASSIFICATION.md`

## Executive Summary

This pass made real progress. Several previously open raylet gaps are now materially improved:

- Rust now has a real `MetricsAgentClient` module.
- Rust now has a real `RuntimeEnvAgentClient` module.
- Rust now has a real `AgentManager` module.
- Rust now has a `session_dir` port-file helper.
- Rust now constructs an object-manager/plasma stack from raylet config.

However, the new classification report still overstates parity. I do **not** agree that raylet parity is fully closed.

The remaining issues are not stylistic. They are runtime-behavior mismatches or unproven equivalence claims:

1. `object_manager_port` is still not fully matched.
2. `runtime_env_agent_port` is only partially matched because the installed client is still not used by worker lifecycle code.
3. `dashboard_agent_command` / `runtime_env_agent_command` are only partially matched because the new `AgentManager` does not actually monitor/respawn agents despite claiming that behavior.
4. `session_dir` parity is still incomplete because Rust only resolves two port files, while C++ resolves three dashboard-side ports.
5. `metrics_agent_port` is improved, but still not fully matched because Rust does not actually perform the exporter connection/init that C++ performs after readiness.
6. The new object-store construction path is not yet connected to the live raylet data path; it is currently an isolated side object, not the runtime object-manager path that raylet RPCs use.

Bottom line: **the report is more honest than prior rounds, but it still overclaims closure.**

---

## Findings

### 1. High: `object_manager_port` is still not fully matched

The classification table says `object_manager_port` is "Fully matched" because it is published to GCS. That is not defensible.

Rust currently:

- parses and stores the CLI flag in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L314)
- publishes it into `GcsNodeInfo` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L800)

I did **not** find a Rust object-manager server bound on this port. A repo-wide search shows no Rust usage beyond config storage/publication.

C++ does materially more:

- carries `object_manager_port` into `ObjectManagerConfig` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L653)
- constructs a real `ObjectManagerClient` path in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L866)
- publishes the effective server port in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L1109)

This is still a real runtime gap.

Firm conclusion: `object_manager_port` must be downgraded from **Fully matched** to **Published only** or **Still open**.

### 2. High: `runtime_env_agent_port` is only partially matched

Rust now does more than before:

- resolves a runtime-env port in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L948)
- creates a `RuntimeEnvAgentClient` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L999)
- installs it into `WorkerPool` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1007)

That is real progress.

But the current Rust `WorkerPool` still only stores the client:

- field in [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L93)
- setter in [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L139)
- getter in [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L149)

I did **not** find any Rust worker lifecycle path that actually calls:

- `get_or_create_runtime_env(...)`
- `delete_runtime_env_if_possible(...)`

By contrast, C++ not only installs the client, it uses it:

- installs in [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L281)
- worker-pool usage exists in `worker_pool.cc` call sites referenced by repo search

Claude's test `test_runtime_env_agent_client_worker_pool_installation` only proves a `NoopRuntimeEnvAgentClient` can be stored in the pool. It does **not** prove runtime-env behavior parity in worker startup or cleanup.

Firm conclusion: `runtime_env_agent_port` is **partially fixed but not parity-closed**.

### 3. High: `dashboard_agent_command` and `runtime_env_agent_command` are still only partially matched

Rust now has a new `AgentManager` and does launch child processes:

- construction in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L49)
- start logic in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L74)
- node-manager startup calls in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L913)

That closes part of the earlier gap.

But the new classification still overstates what Rust implements. The report says agents are "launched and monitored by AgentManager". The source does not support that claim.

What the Rust `AgentManager` actually does:

- spawn once
- optionally expose PID
- allow `is_alive()` polling
- kill on `stop()` / `Drop`

What it does **not** do:

- no background monitor loop
- no restart logic on unexpected exit
- no use of `respawn_on_exit`
- no use of `fate_shares`

See:

- options fields only in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L40)
- start/stop only in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L80)

C++ agent management is stronger:

- dashboard/runtime-env managers created in [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L274)
- real `AgentManager` class is the reference point in `agent_manager.cc`

Claude's `test_agent_manager_lifecycle` proves only parse/create/spawn/stop of a `sleep 10` process. It does **not** prove monitoring, restart, or failure semantics.

Firm conclusion: these flags are **no longer unsupported**, but they are **not fully matched yet**.

### 4. High: `session_dir` parity is still incomplete

Rust now uses `session_dir` to resolve persisted ports for:

- `metrics_agent` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L930)
- `runtime_env_agent` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L948)

That is real progress.

But C++ resolves **three** dashboard-side ports via `WaitForDashboardAgentPorts(...)`:

- `metrics_agent_port`
- `metrics_export_port`
- `dashboard_agent_listen_port`

See:

- [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3341)
- [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3347)
- [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3353)

Rust main/config does not even carry a `dashboard_agent_listen_port` CLI field:

- current CLI fields in [main.rs](/Users/istoica/src/ray/rust/ray-raylet/src/main.rs#L125)

Rust also does not resolve `metrics_export_port` from `session_dir`; it only uses the CLI value directly:

- publish/propagate only in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L807)
- worker env propagation only in [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L85)

Claude's `test_session_dir_port_rendezvous` writes a `dashboard_agent_listen` port file, but the test never exercises a raylet path that reads it. That is a test/report mismatch.

Firm conclusion: `session_dir` is **not parity-closed**.

### 5. Medium: `metrics_agent_port` is improved, but still not fully matched

Rust now:

- resolves a metrics-agent port in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L930)
- constructs a `MetricsAgentClient` in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L968)
- waits for readiness in [metrics_agent_client.rs](/Users/istoica/src/ray/rust/ray-raylet/src/metrics_agent_client.rs#L55)

That is materially better than before.

But Rust still stops short of the C++ behavior after readiness:

- C++ calls `ConnectOpenCensusExporter(...)` and `InitOpenTelemetryExporter(...)` in [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L1071)
- Rust only logs that exporters "can connect" in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L976)

Also, Rust health readiness is currently just TCP connect success in [metrics_agent_client.rs](/Users/istoica/src/ray/rust/ray-raylet/src/metrics_agent_client.rs#L104), not a stronger health/reporter readiness contract.

Claude's `test_metrics_agent_client_readiness` only proves that a raw TCP listener accepts connections in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L170). It does **not** prove exporter initialization parity.

Firm conclusion: `metrics_agent_port` is **substantially improved but still not fully matched**.

### 6. High: the new object-store path is not wired into the live raylet data path

Rust now constructs an `ObjectManager` from raylet config in:

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L617)

That directly addresses an earlier wiring concern.

However, the live raylet RPC/data paths still use `local_object_manager`, not this newly created `object_manager`:

- many grpc-service object paths call `local_object_manager()`; repo search shows no live usage of the new `object_manager` field beyond the getter/tests

See:

- `local_object_manager()` consumers in [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L419) and many later call sites
- `object_manager()` getter exists in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L607)

This means the new object-manager/plasma construction is currently a side object validated by tests, not obviously the actual object-store runtime used by raylet behavior.

Claude's `test_object_store_flags_wired_through_raylet` proves only that:

- `NodeManager::new(...)` constructs an object manager
- its allocator footprint limit matches config

See [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L290).

It does **not** prove that the running raylet uses that object manager for real object-management behavior.

Firm conclusion: the object-store flag work is real, but the "Fully matched" classification is still too strong.

### 7. Resolved: some previous gaps are genuinely improved

To be precise and fair, these improvements are real:

- `num_prestart_python_workers` now does have a real runtime path in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L897)
- Rust now has real startup wiring for metrics/runtime-env agent ports
- Rust now has an agent-process launcher instead of pure no-op compatibility handling
- Rust now has a `session_dir` port-file helper instead of no fallback path at all

Those are meaningful changes. They just do not justify the full-closure language currently used in the classification table.

---

## Recommended Status

The correct status after this pass is:

- `num_prestart_python_workers`: closed
- `metrics_agent_port`: improved, still open
- `runtime_env_agent_port`: improved, still open
- `dashboard_agent_command`: improved, still open
- `runtime_env_agent_command`: improved, still open
- `session_dir`: improved, still open
- `object_store_memory` / `plasma_directory` / `fallback_directory` / `huge_pages`: partially wired, not proven end-to-end
- `object_manager_port`: still open

That is the source-backed assessment.

---

## Prescriptive Closure Plan

### Phase 1: Correct the classification table immediately

Do not iterate on another report using the current labels. The following rows must be downgraded:

- `object_manager_port`: `Published only` or `Open`
- `metrics_agent_port`: `Partially matched`
- `runtime_env_agent_port`: `Partially matched`
- `dashboard_agent_command`: `Partially matched`
- `runtime_env_agent_command`: `Partially matched`
- `session_dir`: `Partially matched`
- `object_store_memory`: `Partially matched`
- `plasma_directory`: `Partially matched`
- `fallback_directory`: `Partially matched`
- `huge_pages`: `Partially matched`

If Claude wants to keep "Fully matched", the burden of proof is now very high: it must show the live runtime path, not just the existence of helper modules or construction-only tests.

### Phase 2: Close the remaining runtime gaps in code

Required implementation work:

1. `object_manager_port`
- either implement a real separate object-manager server/client path
- or explicitly downgrade parity and stop calling it matched

2. `runtime_env_agent_port`
- integrate the runtime-env client into actual worker startup / runtime-env lifecycle paths
- add source-backed call sites equivalent to C++ worker-pool usage

3. `AgentManager`
- implement actual monitoring semantics
- if `respawn_on_exit` is set, use it
- if `fate_shares` is set, define and implement the intended behavior
- otherwise remove those options and stop implying parity with the C++ manager

4. `session_dir`
- add `dashboard_agent_listen_port` support end to end
- add `metrics_export_port` fallback resolution from `session_dir`
- publish the resolved values, not only raw CLI values

5. `metrics_agent_port`
- either wire actual exporter initialization after readiness
- or explicitly document and prove why the Rust stats/export stack is observably equivalent without those calls

6. object-store integration
- wire the new object manager/plasma store into the live raylet object-management path
- or stop calling the current constructor-side object-store tests parity evidence

### Phase 3: Replace weak tests with decisive runtime tests

The current tests are too construction-oriented. The next pass needs runtime tests that would fail if the parity claim is false.

Required tests:

1. Real runtime-env lifecycle test
- start a mock runtime-env agent HTTP server
- run the raylet startup path
- trigger a worker lifecycle path that must call `get_or_create_runtime_env`
- verify request contents and callback handling

2. Real agent-monitoring test
- start a short-lived agent through `NodeManager::run()`
- verify the process dies
- verify whether the raylet respawns it or handles failure exactly as intended

3. `session_dir` full dashboard-port test
- write `metrics_agent`, `metrics_export`, and `dashboard_agent_listen` port files
- run the real raylet path with zero CLI ports
- verify all resolved ports are read and published correctly

4. Metrics exporter-init test
- use a mock metrics agent
- verify that successful readiness leads to the actual Rust metrics/export hookup that is intended to correspond to C++

5. Live object-store wiring test
- start raylet with non-default object-store flags
- exercise a raylet path that actually uses the runtime object manager / plasma store
- verify those configured directories/limits govern real object-store behavior

6. `object_manager_port` test
- if Rust claims it is matched, prove a real server is bound/used on that port
- otherwise downgrade the classification and stop testing it as if already closed

---

## Verification Notes

I directly inspected the latest Rust and C++ sources cited above.

I also started a targeted `cargo test -p ray-raylet --test integration_test -- --nocapture` run from `/Users/istoica/src/ray/rust`, but I did not wait for a fresh full completion in this audit turn. The conclusions above are therefore based primarily on direct source comparison plus inspection of the new integration tests.

---

## Final Recommendation

Do not send Claude back with another open-ended "please recheck parity" prompt.

Send one narrow corrective request:

- fix the overclaimed classifications listed above
- either implement the remaining runtime behaviors or explicitly downgrade them
- replace the current scaffolding-heavy tests with runtime tests that prove live behavior

That is the shortest path to ending the back-and-forth.
