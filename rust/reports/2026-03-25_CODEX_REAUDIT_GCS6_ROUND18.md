# Codex Re-Audit — GCS-6 Round 18

Date: 2026-03-25
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Subject: verification of `2026-03-25_PARITY_FIX_REPORT_GCS6_ROUND18.md`

## Executive Summary

Round 18 made real progress:

1. `metrics_agent_port` is now wired through the real Rust binary startup path
2. Rust now has a late-init path keyed off head-node registration
3. the stale file-fallback comment was corrected

However, after checking the actual C++ and Rust sources, I do **not** agree with
the report's claim that `enable_ray_event` has reached FULL parity.

Two observable semantic gaps still remain:

1. **Late-init zero-port handling is wrong**: the Rust late-init path consumes the
   one-shot initialization flag before checking whether the head node actually
   reported a usable port. C++ does not.
2. **Buffered events can be dropped before exporter attach**: Rust starts the
   periodic flush loop immediately when `enable_ray_event=true`, even with no
   sink attached; `flush_inner()` drains the buffer and loses events. C++ does
   not start exporting until `StartExportingEvents()` is called after exporter
   readiness succeeds, so events remain buffered.

Because of these two issues, FULL parity is still not proven.

The correct status after Round 18 is:

**startup plumbing fixed, late-init partially fixed, full parity still open**

---

## What I Verified

### Rust fixes that are real

I verified the following changes directly in the Rust source:

1. `metrics_agent_port` is now passed from CLI into `GcsServerConfig`
   - `rust/ray-gcs/src/main.rs:123-144`
2. `GcsServer` now contains an atomic exporter-init flag
   - `rust/ray-gcs/src/server.rs:121-123`
3. Rust now has a head-node late-init listener
   - `rust/ray-gcs/src/server.rs:373-437`
4. the stale file-fallback comment in `GrpcEventAggregatorSink` was corrected
   - `rust/ray-observability/src/export.rs:531-532`

These are genuine improvements over Round 17.

### C++ reference behavior re-verified

I also re-checked the relevant C++ control flow:

- startup-known port initialization
  - `src/ray/gcs/gcs_server.cc:317-318`
- late initialization when head node reports the real port
  - `src/ray/gcs/gcs_server.cc:824-827`
- exactly-once guard inside `InitMetricsExporter`
  - `src/ray/gcs/gcs_server.cc:940-943`
- export loop only started after readiness succeeds
  - `src/ray/gcs/gcs_server.cc:953-959`

Those C++ semantics remain the correct parity target.

---

## Findings

### 1. High severity: Rust consumes the init flag before checking `metrics_agent_port > 0`

This is a real C++ vs Rust semantic mismatch.

#### Rust behavior

In the new late-init listener:

- Rust first checks `is_head_node`
- then immediately does `init_flag.swap(true, ...)`
- only after that does it read `node_info.metrics_agent_port`
- if the port is `<= 0`, it logs and returns

Source:

- `rust/ray-gcs/src/server.rs:390-409`

That means a head-node registration with `metrics_agent_port=0` permanently burns
the one-shot init path, even though no exporter was actually initialized.

#### C++ behavior

C++ does **not** do that.

The head-node callback checks:

1. `node->is_head_node()`
2. `!metrics_exporter_initialized_.load()`
3. `actual_port > 0`
4. only then calls `InitMetricsExporter(actual_port)`

Source:

- `src/ray/gcs/gcs_server.cc:824-827`

And the atomic `exchange(true)` occurs **inside** `InitMetricsExporter(...)`, not
before the `actual_port > 0` test:

- `src/ray/gcs/gcs_server.cc:940-943`

#### Why this matters

If the head node first registers with `metrics_agent_port=0` and later a valid
port becomes available, C++ still has a remaining path to initialize the exporter.
Rust does not. Rust has already consumed the init flag.

This is observable runtime divergence.

#### Why the Round 18 report is incorrect here

The report says:

> "Head node with port 0: flag is set (consumed the one-shot), but no sink. C++
> also sets the flag via exchange(true) ..."

That is false.

C++ does **not** set the flag in the `port == 0` branch because it never enters
`InitMetricsExporter(...)`.

#### Required fix

The late-init listener must only attempt the atomic one-shot after confirming
all preconditions that C++ checks outside `InitMetricsExporter`:

1. `is_head_node == true`
2. `metrics_exporter_initialized == false`
3. `metrics_agent_port > 0`

Only then should it claim the init slot and attempt gRPC connect.

#### Required tests

Add both:

1. `head node with port 0 does not consume future valid init`
   - start with no startup port
   - register head node with `metrics_agent_port=0`
   - confirm no sink and init flag still false
   - register head node again with a live valid port
   - confirm sink attaches and events export

2. `non-head with any port does not consume future head-node valid init`
   - this is mostly already implied, but should be proven in the same sequence style

---

### 2. High severity: Rust starts periodic flushing before exporter attach and drops buffered events

This is the most serious remaining parity gap.

#### Rust behavior

When `enable_ray_event=true`, Rust starts the periodic flush loop immediately,
regardless of whether a sink has been attached:

- `rust/ray-gcs/src/server.rs:296-313`

That loop calls:

- `event_exporter.flush()`

And `flush_inner()` does:

1. `take()` the full buffer
2. if no sink is attached, return `0`
3. the taken events are gone

Source:

- `rust/ray-observability/src/export.rs:741-764`

Specifically:

- `std::mem::take(&mut *buffer)` drains the buffer
- `match &*sink { None => 0 }` drops the events on the floor

#### C++ behavior

C++ does not start the export loop until readiness succeeds:

- `ray_event_recorder_->StartExportingEvents()` is called only in the successful
  `WaitForServerReady(...)` callback
- `src/ray/gcs/gcs_server.cc:953-959`

Before that point, the recorder still buffers events; it does not periodically
drain them into nowhere.

`RayEventRecorder::AddEvents(...)` continues buffering while enabled:

- `src/ray/observability/ray_event_recorder.cc:146-167`

and `ExportEvents()` only runs once `StartExportingEvents()` has scheduled it:

- `src/ray/observability/ray_event_recorder.cc:36-49`

#### Why this matters

In Rust today, if:

1. `enable_ray_event=true`
2. startup does not know the port, or late attach has not happened yet
3. actor events are generated
4. the periodic interval fires before sink attach

then those buffered events are dropped permanently.

In C++, those events remain buffered until exporting actually starts or shutdown
flushes them.

This is a direct semantic mismatch.

#### Why Round 18 tests do not prove parity here

The new late-init tests hide this issue by using:

- `ray_events_report_interval_ms: 60_000`

for the late-init scenarios, then attaching the sink well before the interval fires.

Examples:

- `rust/ray-gcs/src/server.rs:1961-1964`
- `rust/ray-gcs/src/server.rs:2047-2050`
- `rust/ray-gcs/src/server.rs:2100-2103`

Those tests do not exercise the actual divergent case:

- events buffered before sink attach
- interval fires while no sink exists
- later successful attach

#### Required fix

Rust must stop treating `enable_ray_event=true` as equivalent to “export loop should run now.”

To match C++:

1. do **not** start the periodic flush loop until the exporter is actually ready
2. in practice, that means start it only after gRPC sink attach succeeds
3. if late-init succeeds, start the loop at that moment
4. if attach never succeeds, keep buffering; do not drain into `None`

The cleanest parity-preserving shape is:

1. extract a helper similar to `start_metrics_exporter(port)` or
   `attach_event_exporter_sink_and_start_loop(...)`
2. call that helper from:
   - startup-known-port path
   - late head-node path
3. only mark the periodic task as running after sink attach succeeds

If the current exporter abstraction is kept, `flush_inner()` must also stop
dropping buffered events when no sink exists. But the closer parity match is to
avoid starting the loop until attach succeeds, just as C++ does.

#### Required tests

At minimum add these:

1. `buffered events survive pre-attach interval and export after late attach`
   - startup with no metrics-agent port
   - short flush interval (for example 10 ms)
   - emit actor events before any sink exists
   - wait for multiple intervals
   - verify buffer still contains events or at least that they later export
   - register head node with live port
   - verify pre-attach events are exported after attach

2. `startup unreachable port does not start destructive periodic draining`
   - startup with configured but unreachable port
   - short interval
   - emit events
   - wait
   - verify events remain buffered until shutdown, matching C++ best-effort buffering semantics

3. `late attach starts periodic loop exactly once`
   - confirm no loop before attach
   - attach successfully
   - verify loop begins
   - repeated head-node registration does not start an extra loop

---

## Secondary Issues

### 3. The claimed Round 18 companion re-audit report is missing

The Round 18 report says:

- `2026-03-25_BACKEND_PORT_REAUDIT_GCS6_ROUND18.md`

but that file is not present in `rust/reports/`.

This is not a runtime parity bug, but it is a process problem. If a report claims
auditor signoff, the referenced audit artifact should exist.

---

## What Round 18 Did Actually Fix

To be precise and fair, these items should now be considered improved or fixed:

1. CLI/config startup plumbing for `metrics_agent_port`
2. existence of a late-init path in Rust at all
3. removal of stale fallback documentation

That is meaningful progress.

But those fixes are not enough for full parity because the remaining semantics are
still observably different under realistic timing/order conditions.

---

## Prescriptive Closure Plan

### Phase 1: fix late-init zero-port semantics

Owner: `rust/ray-gcs`

Required code changes:

1. In the node-added late-init listener, move the `port <= 0` guard before
   consuming the atomic init flag.
2. Preserve C++ structure:
   - non-head node: return
   - already initialized: return
   - port <= 0: log info, return
   - otherwise claim init slot and proceed
3. Ensure the init flag remains false after a head-node registration with
   `metrics_agent_port=0`.

Required tests:

1. `zero_port_does_not_consume_future_valid_init`
2. `valid_late_init_after_zero_port_exports_events`

Acceptance criteria:

- head node with zero port leaves exporter still eligible for future late-init
- later valid port successfully attaches sink and exports events

### Phase 2: fix premature export-loop startup and pre-attach event loss

Owner: `rust/ray-gcs` and possibly `rust/ray-observability`

Required code changes:

1. Do not start the periodic actor-event flush loop in `initialize()` merely
   because `enable_ray_event=true`.
2. Start the loop only after successful gRPC sink attach.
3. Share the startup and late-init attach logic through one helper so loop-start
   semantics stay identical in both paths.
4. Make the helper idempotent with respect to both:
   - exporter initialization
   - periodic task creation
5. Review shutdown to ensure it still:
   - rejects new events
   - flushes remaining buffered events
   - aborts the periodic task if it exists

Optional hardening:

6. Consider making `EventExporter::flush_inner()` preserve the buffer when no
   sink is attached. That is a good safety guard even if Phase 2's loop changes
   make the bad path unreachable in normal flow.

Required tests:

1. `pre_attach_events_not_lost_before_late_init`
2. `no_periodic_drain_without_sink`
3. `successful_attach_starts_loop_once`
4. `shutdown_flushes_pre_attach_buffer_after_late_init`

Acceptance criteria:

- no event loss before exporter attach
- periodic flush loop behavior matches C++ start timing closely enough to be
  defensible

### Phase 3: rerun parity evidence and update reports

Owner: `rust/ray-gcs`, `rust/reports`

Required work:

1. Rerun targeted tests for:
   - startup-known port
   - late-init with valid port
   - zero-port then valid-port sequence
   - pre-attach buffering across periodic intervals
   - unreachable port behavior
2. Update the parity report only after the above tests pass.
3. Add the missing Round 18 re-audit companion file if the workflow expects it.

Acceptance criteria:

- report language matches the actual runtime semantics
- no remaining timing/order gap is left untested

---

## Final Status

My conclusion after direct source comparison is:

**GCS-6 is still not fully closed.**

Precise status:

- startup CLI/config wiring: fixed
- late-init path existence: fixed
- late-init zero-port semantics: open
- pre-attach buffering/export-loop semantics: open
- stale comment cleanup: fixed

So the correct Round 18 disposition is:

**important progress, but still not FULL parity**
