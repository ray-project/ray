# Codex Review — `cc-to-rust-experimental` Backend Port

Date: 2026-03-25
Branch: `cc-to-rust-experimental`
Reviewer: Codex

## Executive Summary

This branch is a large and serious port of Ray backend subsystems from C++ to
Rust. The amount of implementation work is substantial:

- new Rust crates for GCS, raylet, object manager, core worker, RPC, stats,
  pubsub, syncer, common utilities, and Python bindings
- a large body of Rust unit/integration tests
- multiple rounds of parity remediation reports and targeted fixes

However, after reviewing the branch carefully, I do **not** believe the Rust
backend is yet proven to be functionally equivalent to the C++ backend as a
whole.

The branch contains both:

1. areas that now appear parity-complete or close to parity
2. other areas with clear source-backed semantic differences, open gap markers,
   or startup/runtime contract mismatches

The strongest conclusion I can support is:

**The Rust port is substantial and increasingly credible, but it is still not a
fully demonstrated drop-in functional replacement for the C++ backend.**

The highest-risk remaining issues are:

1. Rust `raylet` binary startup still ignores many C++ runtime flags that are
   wired into the real C++ backend
2. Rust Plasma client semantics differ materially from C++ in multi-process IPC
   and blocking `get` behavior
3. distributed ownership / object-location subscription parity is still not
   convincingly closed overall

---

## Scope And Method

This review was not a line-by-line diff of every file added in the branch.
That would be impractical in one pass given the size of the port.

Instead, I reviewed:

1. branch scope and subsystem layout
2. existing status/parity reports
3. the most risk-sensitive runtime paths:
   - GCS
   - raylet
   - object manager / Plasma
   - core worker ownership/object-location logic
4. concrete C++ vs Rust control-flow differences in startup and runtime behavior

I also used the project’s own status documents as evidence when those reports
matched the current source state.

Important constraint:

This report is a source-backed technical review, not a full independent rerun of
the complete branch test matrix.

---

## High-Level Assessment

### What looks strong

1. The GCS event-export parity work (`GCS-6`) now appears to be in much better
   shape after the recent rounds. The specific `enable_ray_event` gaps that were
   previously open appear closed in the current Rust source.
2. The Rust test suite is extensive and, on paper, larger than the C++ suite.
3. The port includes real subsystem implementations, not thin wrappers or stubs.
4. The Python/native compatibility surface has clearly improved relative to
   earlier audit snapshots.

### What is still not proven

1. Whole-backend functional equivalence
2. full distributed ownership and object-location semantics
3. full raylet startup/runtime parity
4. full object-store / Plasma semantics in realistic multi-process operation

The branch’s own earlier status report still says the backend is not yet
functionally equivalent overall:

- `rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md`

And the code still contains source-backed evidence consistent with that broader
conclusion.

---

## Findings

### 1. High: Rust `raylet` binary startup ignores many C++ runtime inputs

This is a real functional divergence in the production startup path.

#### Rust behavior

The Rust `raylet` binary accepts many C++ CLI flags but explicitly treats them
as compatibility-only and discards them:

- `rust/ray-raylet/src/main.rs:75-185`
- `rust/ray-raylet/src/main.rs:248-275`

Examples discarded by `let _ = (...)`:

- `object_manager_port`
- `min_worker_port`
- `max_worker_port`
- `worker_port_list`
- `maximum_startup_concurrency`
- `metrics_agent_port`
- `metrics_export_port`
- `runtime_env_agent_port`
- `object_store_memory`
- `dashboard_agent_command`
- `runtime_env_agent_command`
- `plasma_directory`
- `fallback_directory`
- `head`
- `num_prestart_python_workers`
- and others

The Rust `RayletConfig` does not carry most of these fields:

- `rust/ray-raylet/src/node_manager.rs:285-300`

#### C++ behavior

The C++ `raylet` wires these inputs into real runtime configuration:

- `src/ray/raylet/main.cc:602-665`

Examples:

- `runtime_env_agent_port`
- `metrics_agent_port`
- `metrics_export_port`
- worker port range/list
- `dashboard_agent_command`
- `runtime_env_agent_command`
- `object_manager_port`
- `object_store_memory`
- `plasma_directory`
- `fallback_directory`

These are not cosmetic flags in C++.

#### Why this matters

This means the real Rust raylet startup path is not yet equivalent to the real
C++ startup contract.

This is especially important because:

1. CLI/startup parity is part of backend replaceability
2. ignored config often turns into silent runtime divergence
3. several of these flags affect worker spawning, object store layout, metrics,
   runtime env, and process topology

#### Suggested tests

Add startup-conformance tests that launch the Rust `raylet` binary with the same
flags as C++ and verify the resulting runtime state reflects them.

Minimum tests:

1. `object_manager_port` is actually used
2. `min_worker_port` / `max_worker_port` / `worker_port_list` constrain worker
   binding behavior
3. `object_store_memory` is required and enforced
4. `dashboard_agent_command` and `runtime_env_agent_command` are required or
   wired equivalently if the architecture still depends on them
5. `metrics_agent_port` / `runtime_env_agent_port` propagate into published node
   info and any dependent logic

#### Recommendation

Do not claim whole-backend parity while the Rust `raylet` binary still accepts
C++ flags it does not honor.

---

### 2. High: Rust Plasma client is not functionally equivalent to the C++ Plasma client

This is a major subsystem-level difference.

#### Rust behavior

The Rust Plasma client explicitly uses an in-process direct store reference
instead of the C++ socket IPC model:

- `rust/ray-object-manager/src/plasma/client.rs:13-15`
- `rust/ray-object-manager/src/plasma/client.rs:73-75`

The file says:

- direct reference to `PlasmaStore`
- Unix socket IPC path can be added later
- intended simplification for in-process communication

That is not just an implementation detail. It changes process-boundary behavior.

#### C++ behavior

The C++ Plasma client is socket/message based and supports the actual external
store communication model:

- `src/ray/object_manager/plasma/client.cc`
- `src/ray/object_manager/plasma/client.h`

#### Additional concrete semantic mismatch

The Rust `get()` path still does not implement blocking wait semantics for
`timeout_ms > 0` when the object is absent:

- `rust/ray-object-manager/src/plasma/client.rs:351-352`

It explicitly says:

- object not found and timeout > 0
- TODO: implement blocking wait with store notification
- returns `None`

The C++ client does implement real `GetBuffers(...)` request/wait behavior:

- `src/ray/object_manager/plasma/client.cc:291-360`

#### Why this matters

This is a concrete runtime semantic difference, not merely a different code
style:

1. multi-process access semantics differ
2. blocking `get` semantics differ
3. object-store integration realism differs

That affects one of the most central backend contracts in Ray.

#### Suggested tests

Add explicit cross-process conformance tests:

1. separate-process writer/reader through the store socket path
2. blocking `get(timeout_ms > 0)` waits until object seal or timeout
3. object availability transitions from missing → created → sealed are observed
   correctly by an external client
4. disconnected/reconnected client behavior matches C++
5. multi-client refcount and release semantics match C++

#### Recommendation

Do not treat the current Plasma client as functionally equivalent until the
socket/multi-process and blocking-wait semantics are covered.

---

### 3. High: Distributed ownership / object-location subscription parity is still not convincingly closed

This remains one of the largest correctness-risk areas.

#### Evidence from Rust source

The Rust core worker gRPC service still contains an explicit gap marker:

- `rust/ray-core-worker/src/grpc_service.rs:1458-1470`

It states that the C++ implementation has:

- distributed pub/sub for object location tracking
- subscribe
- initial snapshot delivery
- incremental broadcast
- unsubscribe
- owner-death propagation

and that Rust currently has:

- synchronous point-in-time queries
- unidirectional update batches

The broader branch status report also still classifies distributed ownership and
reference semantics as open overall:

- `rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md:265-287`

#### Why I am not calling this closed

There is some Rust-side subscription-related code:

- `rust/ray-core-worker/src/core_worker.rs:377-431`

and some local object directory subscription logic:

- `rust/ray-object-manager/src/object_directory.rs:1-153`

But the presence of partial subscription machinery is not enough to prove
equivalence to the C++ ownership/object-directory protocol.

The C++ object directory contract is richer:

- callback IDs
- owner address aware subscription
- unsubscribe by callback ID
- failure callback semantics
- snapshot/incremental semantics
- owner-death/object-deleted distinction

See:

- `src/ray/object_manager/object_directory.h:31-107`
- `src/ray/object_manager/ownership_object_directory.cc:320-380`
- `src/ray/core_worker/core_worker.cc:3805-3854`

The Rust `ObjectDirectory` is a much simpler local callback registry:

- `rust/ray-object-manager/src/object_directory.rs:39-153`

That is a meaningful semantic difference unless proven otherwise end-to-end.

#### Suggested tests

Add distributed ownership/object-location conformance tests spanning owner,
borrower, object manager, and subscriber behavior.

Minimum tests:

1. initial snapshot on first subscribe
2. incremental add/remove location updates after subscribe
3. unsubscribe stops future updates without affecting other subscribers
4. owner death triggers the correct failure path
5. object-deleted vs owner-died failure modes are distinguished correctly
6. multiple subscribers for the same object receive consistent sequences
7. late subscriber receives current snapshot, not only future increments

#### Recommendation

Treat distributed ownership/object-location parity as still high risk until
there is an end-to-end conformance suite proving it.

---

### 4. Medium: The branch’s own broad status documents still contradict any whole-backend equivalence claim

The strongest project-wide status document I checked still says the backend is
not yet functionally equivalent overall:

- `rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md`

Key still-open categories listed there:

1. distributed ownership / reference counting / borrower protocol
2. raylet worker lease / request lifecycle / drain / shutdown parity
3. object transfer / fetch / resend / spill / restore parity
4. remaining core worker semantic gaps
5. remaining GCS control-plane and persistence gaps

This document may be older than some later targeted fixes, but I did not find
enough source-backed evidence to overturn its broad conclusion for the branch as
a whole.

#### Recommendation

Before claiming whole-backend equivalence, the project needs a refreshed
branch-level status report that supersedes the March 18 conclusion with
source-backed closure of those subsystem-wide items, not just targeted findings.

---

## Areas That Look Better / Lower Risk

I want to be explicit that this review is not dismissing the work.

Positive findings:

1. GCS `enable_ray_event` parity work appears materially improved and likely
   closed based on the recent rounds
2. the Rust branch has broad subsystem coverage and substantial implementation
3. the Python/native boundary appears much stronger than in earlier review
   snapshots
4. the branch has a serious testing culture and parity tracking discipline

Those are real strengths.

---

## Recommended Test Program

If the goal is to establish real functional equivalence, I would prioritize the
following test program.

### A. Real binary startup conformance

For `gcs_server` and especially `raylet`:

1. launch C++ and Rust binaries with identical CLI/config inputs
2. capture published node info / runtime config / spawned subprocess behavior
3. diff the observable startup behavior

Most important for Rust `raylet`:

1. worker port range/list handling
2. object manager port wiring
3. object store memory enforcement
4. dashboard/runtime env agent command handling
5. metrics/runtime env port propagation

### B. Plasma / object-store conformance

1. multi-process reader/writer tests
2. blocking `get` timing tests
3. create/seal/get/release/delete lifecycle parity
4. spill/restore and timeout behavior parity
5. separate-client refcount interaction

### C. Distributed ownership / object-location conformance

1. owner snapshot on subscribe
2. incremental location updates
3. unsubscribe correctness
4. owner death propagation
5. object deletion propagation
6. wrong-recipient suppression
7. multiple borrowers and late subscribers

### D. Raylet lifecycle conformance

1. worker lease request / return / retry behavior
2. drain / reject semantics
3. shutdown interaction with worker stats and in-flight leases
4. runtime env agent and dashboard agent dependencies

### E. Branch-level black-box cluster conformance

Run the same cluster scenarios against:

1. C++ backend
2. Rust backend

Compare:

1. task and actor completion behavior
2. object availability and wait/get semantics
3. failure handling
4. node add/remove behavior
5. event / metrics / error propagation

This is the only credible way to move from “many targeted parity fixes” to
“branch is functionally equivalent overall.”

---

## Final Conclusion

My current judgment is:

**The `cc-to-rust-experimental` branch is a substantial backend port, but it is
still not proven functionally equivalent to the original C++ backend overall.**

The most concrete remaining differences I verified are:

1. Rust `raylet` startup ignores many C++ runtime flags that affect real backend
   behavior
2. Rust Plasma client semantics differ materially from C++ in multi-process and
   blocking-wait behavior
3. distributed ownership/object-location parity is still not convincingly closed

So the right message is:

**serious progress, but not yet a fully demonstrated drop-in replacement**
