# Codex Re-Audit of Claude Handoff Report

Date: 2026-03-27
Branch: `cc-to-rust-experimental`
Subject: Re-audit of `2026-03-27_CODEX_HANDOFF_REPORT.md`

## Executive Summary

The latest Claude pass made real progress. Several previously open raylet gaps are now materially improved:

- Rust now resolves four agent-related ports instead of two.
- Rust now has a real monitoring loop in `AgentManager`.
- Rust now calls the runtime-env client from more worker-pool lifecycle paths.
- Rust now downgraded `object_manager_port` from a false "fully matched" claim to an architectural difference.

However, the report still overclaims closure in important places. I do **not** agree that there are "no longer gaps."

The remaining gaps are source-backed and observable:

1. The new object-manager/plasma stack is still not the live raylet object-management path; it is used for stats sidecar access, while actual object-management RPC behavior still routes through `local_object_manager`.
2. The metrics-agent/exporter path is still not functionally equivalent to C++: Rust waits for TCP readiness, then starts a local periodic exporter and Prometheus HTTP server, but it does **not** implement the C++ metrics-agent exporter connection semantics.
3. The runtime-env client is now called, but `start_worker_with_runtime_env()` still fires the async create request and starts the worker immediately without waiting for successful env creation; that does not match the C++ sequencing contract.
4. The new tests are better than before, but several of the strongest parity claims are still only backed by construction-level or weak-behavior tests rather than decisive end-to-end runtime tests.

Bottom line: **substantial improvement, but still not full parity closure.**

---

## Findings

### 1. High: the new object-manager path is still not the live raylet object path

Rust now constructs an `ObjectManager` from raylet config in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L630). That is real progress.

But the live object-management RPC paths still operate through `local_object_manager()`, not this new `object_manager()`:

- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L419)
- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L460)
- [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L746)
- and many later `local_object_manager()` call sites across the same file

The latest change only uses `object_manager()` opportunistically in `GetNodeStats` to report capacity/usage in [grpc_service.rs](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L752). That is not the same as wiring it into the live object-management runtime.

Claude's new `test_object_store_live_runtime_stats` in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L609) proves only that:

- the side `ObjectManager` exists
- its allocator has the configured capacity
- `GetNodeStats` can read that capacity

It does **not** prove that the running raylet object-management behavior uses this new object manager for real object lifecycle, pinning, spilling, restoration, or object transfer semantics.

Firm conclusion: `object_store_memory`, `plasma_directory`, `fallback_directory`, and `huge_pages` are still **not fully matched at the live raylet runtime path**.

### 2. High: metrics-agent/exporter behavior is still not equivalent to C++

C++ behavior:

- creates a `MetricsAgentClientImpl`
- waits for readiness
- then calls:
  - `ConnectOpenCensusExporter(actual_metrics_agent_port)`
  - `InitOpenTelemetryExporter(actual_metrics_agent_port)`

See [main.cc](/Users/istoica/src/ray/src/ray/raylet/main.cc#L1071).

Rust behavior now:

- creates a `MetricsAgentClient`
- uses TCP-connect success as readiness in [metrics_agent_client.rs](/Users/istoica/src/ray/rust/ray-raylet/src/metrics_agent_client.rs#L104)
- after readiness, starts:
  - a local periodic `MetricsExporter`
  - a Prometheus HTTP server on `metrics_export_port`

See [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L1024) and [exporter.rs](/Users/istoica/src/ray/rust/ray-stats/src/exporter.rs#L196).

That is **not** the same contract.

The C++ path is a metrics-agent exporter hookup. The Rust path is a local exporter + local scrape endpoint startup after connectivity. Those may be useful, but they are not source-backed proof of equivalence.

Claude's report explicitly says this is "the Rust equivalent". That is too strong based on the code shown.

Claude's `test_metrics_agent_client_readiness` in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L159) only proves TCP connectivity. It does not prove exporter hookup parity.

Firm conclusion: `metrics_agent_port` is improved, but **still not parity-closed**.

### 3. High: runtime-env creation sequencing still does not match C++

C++ `StartNewWorker()` waits for runtime-env creation before starting the worker process:

- `GetOrCreateRuntimeEnv(...)`
- if successful, then `StartWorkerProcess(...)`
- if unsuccessful, worker startup fails

See [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1349).

Rust `start_worker_with_runtime_env()` does this:

- call `client.get_or_create_runtime_env(...)`
- immediately call `self.start_worker_process(...)` afterward without waiting for the callback result

See [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L330).

This is a real semantic gap. The Rust code now calls the agent, but it does not preserve the C++ ordering/guard: worker start is no longer contingent on successful runtime-env creation.

This is exactly the kind of bug that gets hidden by weak "client was called" tests.

Claude's `test_runtime_env_agent_used_in_worker_lifecycle` in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L463) proves the client methods are invoked. It does **not** prove the worker start waits for env creation or fails correctly when env creation fails.

Firm conclusion: `runtime_env_agent_port` and worker-pool runtime-env parity are **still not fully closed**.

### 4. Medium: the new agent monitoring is closer, but still not proven equivalent

Rust now has real monitoring logic in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L180). This is materially better than before.

However, two concerns remain:

1. The shutdown/fate-sharing behavior is still implemented as `kill(getpid(), SIGTERM)` in [agent_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/agent_manager.rs#L241), not clearly through the same raylet shutdown path used elsewhere. That may be acceptable, but it is not obviously equivalent to the C++ shutdown contract.

2. The decisive test is weak. `test_agent_monitoring_and_respawn` in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L548) uses `true` as the child process and only asserts:
   - the monitor did not panic, or
   - the PID changed

It does not prove a stable respawn contract, nor does it prove fate-sharing semantics.

This item is much closer to closure than before, but the test evidence still does not justify an unqualified "Fully matched" label.

Firm conclusion: agent management is **substantially improved, but still under-proven**.

### 5. Medium: `session_dir` resolution is improved and likely close, but the strongest end-to-end proof is still missing

Rust now resolves:

- `metrics_agent`
- `metrics_export`
- `dashboard_agent_listen`
- `runtime_env_agent`

in [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L980).

That is a real fix, and it does line up much better with C++ `WaitForDashboardAgentPorts(...)` in [node_manager.cc](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L3341).

However, the new test `test_session_dir_all_four_ports` in [integration_test.rs](/Users/istoica/src/ray/rust/ray-raylet/tests/integration_test.rs#L581) still only tests the helper function `wait_for_persisted_port(...)`, not the full raylet startup path publishing resolved values into `GcsNodeInfo`.

This one is not the highest-priority remaining gap, but if Claude wants "fully matched", the stronger end-to-end test is still missing.

Firm conclusion: likely close, but **still under-proven**.

### 6. Resolved: `object_manager_port` is no longer overclaimed

Claude did correct one important reporting error:

- the handoff now marks `object_manager_port` as "Architecturally different"

Given the current Rust design, that is the correct classification. I do not treat this specific reporting issue as still open.

---

## Recommended Status

The correct status after this pass is:

- `object_manager_port`: appropriately downgraded
- `session_dir` port coverage: largely fixed, still needs stronger end-to-end proof
- `runtime_env_agent_port`: improved, still open because worker-start sequencing still differs from C++
- `dashboard_agent_command` / `runtime_env_agent_command`: improved, still under-proven
- `metrics_agent_port`: improved, still open because exporter semantics still differ
- object-store flag/runtime wiring: still open because the new object manager remains sidecar for stats rather than the live raylet object path

That is the source-backed assessment.

---

## Prescriptive Closure Plan

### Phase 1: Stop claiming full closure now

Do not accept the current handoff as "done".

The report should be revised immediately so these items are **not** labeled fully closed:

- `metrics_agent_port`
- `runtime_env_agent_port`
- `dashboard_agent_command`
- `runtime_env_agent_command`
- `object_store_memory`
- `plasma_directory`
- `fallback_directory`
- `huge_pages`

### Phase 2: Fix the remaining real semantic gaps

#### A. Runtime-env sequencing

This is the highest-priority remaining correctness gap.

Rust must match the C++ ordering:

1. request/create runtime env
2. wait for callback result
3. only start worker on success
4. fail worker startup on runtime-env creation failure

Current Rust starts the worker immediately after issuing the async request. That is wrong for parity.

Required work:

- refactor `start_worker_with_runtime_env()` so worker start depends on successful callback completion
- add an error/status path matching C++ `RuntimeEnvCreationFailed`
- add a decisive test where runtime-env creation fails and verify the worker does **not** start

#### B. Metrics exporter semantics

If the Rust implementation is intentionally not using the same exporter mechanism as C++, then stop calling it equivalent.

You must do one of:

1. actually wire the Rust equivalent of the C++ metrics-agent exporter hookup
2. or explicitly classify the difference as architectural / partially matched

Required test:

- prove the post-readiness behavior does the intended export, not merely that a TCP listener exists

#### C. Live object-store wiring

Either:

1. connect the new `ObjectManager`/`PlasmaStore` path to the live raylet object-management flow
2. or downgrade the object-store flags from "fully matched"

Right now, `GetNodeStats` can read from the new object manager, but the rest of the live object path still goes through `local_object_manager`. That is not enough.

Required test:

- exercise a real raylet object-management path and verify the configured object-store runtime is the one being used

#### D. Stronger agent-monitoring proof

The implementation is closer now, but the test is too weak.

Required test:

- use an agent that exits deterministically
- verify actual respawn count / PID transition / terminal behavior
- if fate-sharing is claimed, verify the raylet shutdown path triggered is the intended one

#### E. Stronger `session_dir` end-to-end proof

Required test:

- run the real raylet startup path with zero CLI ports
- provide the four port files in `session_dir`
- verify the resolved values are actually published to GCS / visible through the live startup path

---

## Verification Notes

I directly inspected the latest Rust and C++ sources cited above.

I did not complete a fresh independent green rerun of the claimed test suite in this turn. The conclusions here are based primarily on direct source comparison and inspection of the new tests in source.

---

## Final Recommendation

Do not send Claude another broad “please re-audit parity” request.

Send one final narrow correction:

1. fix runtime-env worker-start sequencing
2. either wire true metrics-exporter equivalence or downgrade the claim
3. either wire the new object manager into the live raylet path or downgrade the object-store rows
4. strengthen the weak monitoring/session-dir tests into real end-to-end runtime proofs

That is the shortest path to ending the back-and-forth without another overclaim.
