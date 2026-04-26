# Rust GCS vs C++ GCS Parity Recheck

Date: 2026-04-20
Repository: `/Users/istoica/src/cc-to-rust/ray`
Scope: fresh end-to-end source review of the current Rust GCS against the current C++ GCS, with the bar set to functional identity and drop-in replacement behavior.

## Executive Summary

The Rust GCS is substantially closer to the C++ GCS than in the earlier reviews. The previous blocker classes around:

- `start_with_listener()` listener installation
- lifecycle listener coverage
- `RayConfig` initialization / `config_list`
- `RaySyncerService`
- periodic raylet `GetResourceLoad` polling
- `DrainNode` parity
- health-check semantics

are now materially addressed in the current tree.

However, the Rust GCS is still **not yet functionally identical** to the C++ GCS, and I would still **not** treat it as a drop-in replacement.

The remaining gaps are narrower but still real:

1. `GetTaskEvents` semantics still differ from C++ in externally visible ways.
2. Rust task-event storage still ignores several C++ `RayConfig`-driven limits and cleanup behaviors.
3. Rust still omits the C++ usage-stats and metrics-exporter initialization path, even though the binary accepts `metrics_agent_port`.

These are not stylistic differences. They change observable RPC behavior, operational behavior, or both.

## Review Method

I re-read the current Rust and C++ implementations directly, focusing on:

- binary startup / config initialization
- gRPC services actually registered
- listener installation and background loops
- persisted-state initialization
- task-event ingestion and `GetTaskEvents`
- metrics / usage side effects initialized by `GcsServer`

I also cross-checked the existing C++ task-manager test coverage to make sure the remaining gaps are not just hypothetical.

I did **not** rerun the full workspace test suite in this pass. The conclusions below are based on direct source comparison, plus the already-verified closure of earlier blockers.

## What Now Matches Well

The following earlier mismatches now look genuinely closed in the current tree:

- `start_with_listener()` and `start()` share the same listener/install path.
- Worker/job/node lifecycle listeners now include the C++-equivalent actor, placement-group, task-manager, and pubsub side effects.
- `config_list` is base64-decoded strictly and used to initialize `ray_config` before the runtime is built.
- Rust now registers `RaySyncer` and wires the resource-view path.
- Rust now runs a periodic raylet `GetResourceLoad` pull loop.
- Rust `DrainNode` now preempts actors, forwards to raylet, and propagates rejection/error correctly.
- Rust health-check behavior now matches the C++ “GCS-side task must respond” contract.
- Worker state is persisted through `WorkerTable` and queries read from storage.
- Actor and placement-group managers are no longer the earlier “immediate success” stubs.

This is real progress. The gap list is much shorter than in the earlier reports.

## Findings

### 1. Critical: `GetTaskEvents` semantics still differ from C++

The current Rust `GcsTaskManager::get_task_events()` is still not behaviorally identical to the C++ `HandleGetTaskEvents`.

#### 1a. Rust still does not implement `task_filters`

C++ supports `filters.task_filters` both in the fast-path candidate selection and in the full predicate path:

- `ray/src/ray/gcs/gcs_task_manager.cc:436-447`
- `ray/src/ray/gcs/gcs_task_manager.cc:501-512`

Rust only does an index preselection for `job_filters`, then applies:

- `exclude_driver`
- `task_name_filters`
- `state_filters`
- `actor_filters`

Relevant Rust code:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:475-498`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:509-583`

There is no Rust handling for `filters.task_filters` at all.

Why this matters:

- C++ clients can query task events by task ID with equality / inequality predicates.
- Rust currently ignores those filters and returns results based on the other filters only.

This is a direct RPC contract mismatch.

#### 1b. Rust can return task events that C++ suppresses

C++ explicitly skips task events that do not have `task_info()`:

- `ray/src/ray/gcs/gcs_task_manager.cc:491-495`

Rust does not have the equivalent unconditional early return. In Rust, an event without `task_info` is still returned unless one of the later filters happens to require `task_info`:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:509-583`

Why this matters:

- C++ and Rust can return different result sets for the same `GetTaskEvents` request, especially for lifecycle/profile-only events that arrived before the task-definition event merged in.

That is externally visible behavior, not just an internal implementation detail.

#### 1c. Rust does not match C++ invalid-predicate handling

C++ wraps filtering in a `try`/`catch` and returns `InvalidArgument` on an invalid predicate:

- `ray/src/ray/gcs/gcs_task_manager.cc:584-618`

Rust currently hard-codes predicate handling as:

- `0 = EQUAL`
- `1 = NOT_EQUAL`

and silently treats any other value as “no mismatch found, continue”:

- task-name filters: `task_manager.rs:527-542`
- state filters: `task_manager.rs:546-564`
- actor filters: `task_manager.rs:567-580`

Why this matters:

- malformed client input that C++ rejects can be silently accepted by Rust, producing different results and different status codes.

#### Severity

Critical.

`GetTaskEvents` is a public RPC surface, and these are direct behavioral mismatches.

### 2. High: Rust task-event storage still misses several C++ storage/config behaviors

Even where Rust task-event behavior is closer than before, it still does not implement several C++ storage rules.

#### 2a. Rust does not enforce `task_events_max_num_profile_events_per_task`

C++ truncates per-task profile events to the configured cap:

- `ray/src/ray/gcs/gcs_task_manager.cc:179-191`
- config knob: `ray/src/ray/common/ray_config_def.h:529`
- covered by C++ tests: `ray/src/ray/gcs/tests/gcs_task_manager_test.cc:412-422`, `1732-1743`

Rust merges profile events without any equivalent truncation:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:175-181`

There is no Rust config entry for `task_events_max_num_profile_events_per_task`, and no truncation path.

Impact:

- Rust can retain and return arbitrarily larger profile-event payloads than C++.
- dropped-profile-event counters will diverge from C++.

#### 2b. Rust does not honor `task_events_max_num_task_in_gcs` through `RayConfig`

C++ constructs the task manager with `RayConfig::instance().task_events_max_num_task_in_gcs()`:

- `ray/src/ray/gcs/gcs_task_manager.cc:42-45`
- config knob: `ray/src/ray/common/ray_config_def.h:498`

Rust constructs the task manager from `GcsServerConfig.max_task_events`, which defaults to `100_000` but is not wired to `ray_config`:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:107`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:137`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:296`

Rust `ray-config` does not define `task_events_max_num_task_in_gcs` at all:

- `ray/src/ray/rust/gcs/crates/ray-config/src/lib.rs:216-261`

Impact:

- `config_list` and `RAY_task_events_max_num_task_in_gcs` can affect C++ task-event retention but do not affect Rust retention.

That breaks the configuration contract of the binary.

#### 2c. Rust does not implement job-summary GC or the dropped-attempt tracking cap

C++ periodically runs `GcJobSummary()`:

- `ray/src/ray/gcs/gcs_task_manager.cc:50-52`
- implementation: `ray/src/ray/gcs/gcs_task_manager.h:263-267`

C++ also enforces `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`:

- `ray/src/ray/gcs/gcs_task_manager.cc:777-788`
- config knob: `ray/src/ray/common/ray_config_def.h:506`
- C++ test setup: `ray/src/ray/gcs/tests/gcs_task_manager_test.cc:424-434`

Rust has neither:

- no periodic GC hook in `GcsTaskManager`
- no equivalent tracking cap in `JobTaskSummary`
- no `ray-config` entries for these knobs

Relevant Rust storage definitions:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:36-44`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:87-102`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:233-264`

Impact:

- dropped-attempt bookkeeping can diverge over time
- stale per-job summary state can accumulate differently from C++
- aggregate drop counters can diverge from C++ under long-lived workloads

#### Severity

High.

These differences affect observable task-event payloads and the `config_list` contract.

### 3. High: Rust still omits the C++ usage-stats and metrics-exporter initialization path

The C++ GCS initializes both usage-stats plumbing and the event/metrics exporter connection:

- `InitUsageStatsClient()`:
  - `ray/src/ray/gcs/gcs_server.cc:629-636`
- `InitMetricsExporter(...)`:
  - eager path when `config_.metrics_agent_port > 0`: `ray/src/ray/gcs/gcs_server.cc:325-328`
  - deferred path when the head node registers: `ray/src/ray/gcs/gcs_server.cc:831-842`
  - exporter connect/setup: `ray/src/ray/gcs/gcs_server.cc:950-972`

The C++ usage-stats client is then threaded into:

- `GcsWorkerManager`
- `GcsActorManager`
- `GcsPlacementGroupManager`
- `GcsTaskManager`

via:

- `ray/src/ray/gcs/gcs_server.cc:633-636`

Rust has no equivalent initialization path. The binary parses `metrics_agent_port` and logs it:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:133-136`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:238-255`

But there is:

- no `metrics_agent_port` field in `GcsServerConfig`
- no Rust `InitMetricsExporter` equivalent
- no head-node-registration metrics-exporter hook
- no usage-stats client
- no equivalent `SetUsageStatsClient(...)` wiring in Rust managers

Evidence of the missing Rust side:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:102-126`
- no Rust hits for `UsageStats`, `SetUsageStatsClient`, or `InitMetricsExporter` in `ray/src/ray/rust/gcs`

Why this matters:

- the Rust binary accepts the same startup flag surface but does not honor the same metrics/export side effects
- operational observability and usage reporting will differ from the C++ GCS
- the dynamic head-node path in C++ is completely absent in Rust

This is not the most core RPC-path difference, but it is still part of the real process behavior that existing deployments rely on.

#### Severity

High.

### 4. Medium: C++ function-manager reference tracking still has no Rust counterpart

C++ still initializes a `GCSFunctionManager` and threads it into the job and actor managers:

- `InitFunctionManager`: `ray/src/ray/gcs/gcs_server.cc:624-627`
- job manager construction: `ray/src/ray/gcs/gcs_server.cc:466-478`
- actor manager construction: `ray/src/ray/gcs/gcs_server.cc:518-533`

The C++ job and actor managers then use it for job-reference tracking:

- job manager: `ray/src/ray/gcs/gcs_job_manager.cc:38`, `136`, `175`
- actor manager: `ray/src/ray/gcs/actor/gcs_actor_manager.cc:731`, `1114`, `1730`

Rust has no corresponding function-manager type or wiring:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs:27-55`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:114-204`
- no Rust hits for `FunctionManager`, `AddJobReference`, or `RemoveJobReference`

I am classifying this below the task-event and metrics findings because I did not validate a current user-facing failure from it in this pass. But it is still a real architectural mismatch in job/actor-associated cleanup semantics.

#### Severity

Medium.

## Overall Verdict

The Rust GCS is much closer than in the earlier reviews, and most of the old blocker list is now genuinely closed.

But the current implementation is still **not yet functionally identical** to the C++ GCS, and it still **should not yet be treated as a drop-in replacement**.

The remaining work is now much more focused than before:

1. finish `GetTaskEvents` parity
2. finish the remaining task-event storage/config parity
3. implement the metrics/usage side effects of the C++ GCS process
4. decide whether the `GCSFunctionManager` path needs a Rust equivalent for full cleanup parity

## Recommended Next Steps

### 1. Close `GetTaskEvents` parity first

Implement the missing Rust behaviors in `gcs-managers/src/task_manager.rs`:

- add `filters.task_filters` handling
- match C++ behavior of suppressing events without `task_info`
- reject invalid predicates with `InvalidArgument`

This is the cleanest remaining public-RPC mismatch.

### 2. Port the remaining task-event `RayConfig` knobs and storage rules

Add Rust equivalents for:

- `task_events_max_num_task_in_gcs`
- `task_events_max_num_profile_events_per_task`
- `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`

Then implement:

- per-task profile-event truncation
- dropped-attempt tracking cap
- periodic job-summary GC

### 3. Decide whether Rust must fully mirror C++ metrics/usage side effects

If the goal is true drop-in replacement, the answer is yes.

At minimum Rust should:

- honor `metrics_agent_port`
- initialize the event/metrics exporter path when configured
- mirror the C++ delayed initialization when the head node registers
- decide whether usage-stats client behavior is in scope for parity and wire it explicitly

## Final Assessment

Earlier reports identified broad control-plane gaps; most of those are now fixed.

The current remaining parity problem is no longer “the Rust GCS is broadly incomplete.” It is narrower:

- task-events still differ in important ways
- some process-level metrics/usage behavior is still missing

That is enough to keep the drop-in verdict negative today.
