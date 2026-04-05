# Claude Code Prompt: Finish the Remaining Raylet Parity Gaps Without Another Overclaim

Stop. You are still not done.

The latest handoff made real progress, but it still overclaims closure. We are not doing another round of “mostly equivalent,” “architecturally similar,” or “the helper exists so the row is closed.” If the live runtime semantics do not match C++, then the gap is still open. Period.

Codex re-audited the current Rust and C++ sources and found that there are still real remaining gaps. You need to fix them in code or explicitly downgrade the parity claim. Do not come back with another “fully matched” table unless the source and the tests prove it end to end.

## What Is Still Wrong

### 1. Runtime-env worker startup sequencing still does not match C++

This is the most important remaining correctness gap.

C++ behavior:
- `StartNewWorker()` calls `GetOrCreateRuntimeEnv(...)`
- waits for the callback
- only starts the worker process on success
- if runtime-env creation fails, worker startup fails

See:
- `src/ray/raylet/worker_pool.cc:1349-1394`

Rust behavior today:
- `start_worker_with_runtime_env()` issues `get_or_create_runtime_env(...)`
- then immediately calls `self.start_worker_process(...)`
- worker start is not gated on successful runtime-env creation

See:
- `rust/ray-raylet/src/worker_pool.rs:330-367`

This is not a minor nuance. It is a semantic mismatch. The current Rust implementation can start a worker even when runtime-env setup has not completed or has failed. C++ does not do that.

The current tests are too weak. They only prove the client methods were called. They do **not** prove the worker start is correctly blocked on runtime-env success.

### 2. The new object-manager/plasma path is still not the live raylet object path

Rust now constructs an `ObjectManager` from raylet config. That is real progress.

But the live raylet object-management paths still primarily use `local_object_manager()`, not this new `object_manager()`:
- object-management RPC handling still routes through `local_object_manager()` in `rust/ray-raylet/src/grpc_service.rs`
- the new object manager is currently used for stats-side reads in `GetNodeStats`, not as the main live object-management runtime

See:
- constructor: `rust/ray-raylet/src/node_manager.rs:630-682`
- stats-side use: `rust/ray-raylet/src/grpc_service.rs:745-783`
- live object paths still on `local_object_manager()`: many call sites in `rust/ray-raylet/src/grpc_service.rs`

That means the current “object store flags are fully matched” claim is still too strong. Right now you have a side object that proves configuration can be threaded into an allocator. That is not the same as proving the real raylet runtime uses it.

### 3. Metrics/exporter semantics still do not match C++

C++ behavior after metrics-agent readiness:
- `ConnectOpenCensusExporter(actual_metrics_agent_port)`
- `InitOpenTelemetryExporter(actual_metrics_agent_port)`

See:
- `src/ray/raylet/main.cc:1071-1075`

Rust behavior after readiness:
- create a local `MetricsExporter`
- start periodic export
- start a Prometheus HTTP server on `metrics_export_port`

See:
- `rust/ray-raylet/src/node_manager.rs:1024-1076`

This may be useful, but it is **not** source-backed proof that the Rust path is equivalent to the C++ metrics-agent/exporter hookup. It is a different implementation with different semantics unless you prove otherwise.

The current test only proves TCP connectivity to a mock listener. That is nowhere near enough.

### 4. Agent monitoring is improved, but still under-proven

You added real monitoring logic. Good. But the current test is still weak:
- it starts `true`
- waits
- asserts the monitor did not panic or that the PID changed

That is not a decisive proof of parity-level monitoring semantics.

If you want to claim parity here, you need tests that prove:
- actual respawn attempts
- terminal behavior after repeated failures
- fate-sharing shutdown behavior if you claim to implement it

### 5. `session_dir` handling is much better, but the strongest end-to-end proof is still missing

You now resolve all four relevant ports. That is real progress.

But your strongest new test still exercises the helper function directly rather than the full raylet startup path publishing the resolved values through the live runtime path.

That does not automatically mean the implementation is wrong. It does mean the current proof is not yet strong enough for another overconfident closure claim.

## Your Assignment

You must do one of two things for each of the remaining gaps:

1. implement the missing runtime behavior so it actually matches C++
2. or explicitly downgrade the classification and stop claiming parity

You are NOT allowed to do any of the following:
- do not say “fully matched” because a helper module exists
- do not say “fully matched” because a config field is wired through
- do not say “fully matched” because a side object is constructed
- do not say “fully matched” because a mock test proves a function was called
- do not say “equivalent” unless the runtime behavior is actually demonstrated

Another overclaim is worse than a precise downgrade.

## Required Work

### PHASE 1: Fix runtime-env sequencing first

This is the highest-priority fix.

You must change the Rust worker startup flow so it matches C++:

1. request/create the runtime env
2. wait for the callback result
3. only start the worker process if runtime-env creation succeeded
4. if creation fails, do not start the worker and surface the failure

That means:
- `start_worker_with_runtime_env()` must no longer call `start_worker_process()` unconditionally
- the success callback must trigger the worker start
- the failure callback must prevent worker start and propagate an error state

If the current API shape makes that awkward, refactor it. This is not optional.

Required decisive test:
- mock runtime-env client that returns failure
- call the worker-start path
- assert the worker is **not** started
- this test must fail on the current implementation

Also add:
- success-path test proving the worker starts only after runtime-env creation succeeds
- ordering test proving no worker start occurs before the callback

### PHASE 2: Either wire the object-manager path into the live raylet runtime or downgrade the claim

Right now the new object manager is not clearly the main runtime object path.

You must do one of:

1. integrate the new `ObjectManager` / `PlasmaStore` path into the actual raylet object-management flow
2. or downgrade the object-store rows from “fully matched”

If you choose option 1:
- identify the live object-management operations that still use `local_object_manager()`
- either migrate them to the new object-manager/plasma-backed runtime
- or make the new object-manager the backing implementation behind those operations

If you choose option 2:
- explicitly state that the object-store configuration is only partially wired
- stop claiming live parity

Required decisive test if you keep parity claims:
- start the real raylet runtime with non-default object-store flags
- exercise a real object-management path
- verify the actual live object runtime honors those settings

Constructor-only tests do not count anymore.

### PHASE 3: Fix or downgrade the metrics/exporter parity claim

Right now you are still calling the Rust metrics behavior “equivalent” to C++ without proving that.

You must do one of:

1. implement the real Rust equivalent of the C++ metrics-agent exporter hookup
2. or explicitly downgrade the claim to “architecturally different” / “partially matched”

If you keep a parity claim, your final report must explain exactly why:
- `MetricsExporter + start_metrics_server`
is observably equivalent to:
- `ConnectOpenCensusExporter + InitOpenTelemetryExporter`

If you cannot prove that, do not claim it.

Required decisive test:
- not just TCP readiness
- a test that proves post-readiness metrics export behavior actually matches the intended contract

### PHASE 4: Strengthen the agent monitoring proof

The implementation is closer, but the test still does not justify a full-closure claim.

Required decisive tests:

1. Respawn test
- use a process that exits deterministically
- verify respawn attempts actually occur
- verify PID changes or explicit respawn count

2. Max-failure test
- force repeated respawn failures
- verify the terminal behavior matches your intended design

3. Fate-sharing test
- if you claim fate-sharing semantics, prove the raylet shutdown path that should occur
- do not hide behind “sends SIGTERM to self” unless you prove that is the intended equivalent

### PHASE 5: Strengthen the `session_dir` end-to-end proof

You improved the helper and the resolution logic. Now prove the full startup path.

Required decisive test:
- write all four port files into `session_dir`
  - `metrics_agent`
  - `metrics_export`
  - `dashboard_agent_listen`
  - `runtime_env_agent`
- start the real raylet startup path with zero CLI ports
- verify the resolved values are actually consumed and published in the live runtime path

Testing the helper in isolation is not enough anymore.

## Required Final Report Format

Your final response must include a table with these columns:
- item
- previous claim
- actual Rust behavior before this pass
- actual Rust behavior after this pass
- C++ behavior
- final status: fully matched / partially matched / architecturally different / unsupported
- exact source references
- exact tests proving the claim

You must explicitly answer these questions:

1. Does Rust now wait for successful runtime-env creation before starting a worker, matching C++?
2. If runtime-env creation fails, does Rust now correctly avoid starting the worker?
3. Is the new `ObjectManager`/`PlasmaStore` path now part of the live raylet object-management runtime, or still mostly a side object?
4. Is the Rust metrics/exporter behavior actually equivalent to the C++ metrics-agent exporter hookup, or is it still different?
5. Do the agent monitoring tests now prove real respawn/fate-sharing behavior instead of just showing the monitor did not panic?
6. Does the full raylet startup path now consume and publish all four `session_dir`-resolved ports correctly?

## Completion Standard

Do not say “fully matched” for any remaining disputed row unless:
- the live runtime code path exists
- the behavior is exercised by a decisive test
- the test would have failed before your fix

If you cannot close a gap in this pass, downgrade it explicitly. That is acceptable. Another overclaim is not.

Do not come back with another optimistic summary. Fix the runtime semantics or downgrade the claims honestly.
