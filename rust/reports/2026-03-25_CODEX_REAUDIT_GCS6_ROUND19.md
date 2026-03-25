# Codex Re-Audit — GCS-6 Round 19

Date: 2026-03-25
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Subject: verification of `2026-03-25_PARITY_FIX_REPORT_GCS6_ROUND19.md`

## Executive Summary

I re-checked the Round 19 Rust changes directly against the relevant C++ control
flow. The two remaining semantic mismatches identified after Round 18 appear to
be fixed:

1. the late-init path no longer consumes the one-shot init flag before checking
   `metrics_agent_port > 0`
2. the periodic export loop is no longer started before sink attach, and the
   exporter now preserves buffered events when no sink is attached

I did **not** find a remaining source-backed C++ vs Rust parity gap in the
`enable_ray_event` path after this pass.

My conclusion is:

**GCS-6 now appears fully closed, subject to the normal caveat that the final
claim relies primarily on direct source comparison plus the added regression
tests, not a full independent rerun of the complete test matrix in this turn.**

---

## What I Verified

### C++ reference behavior

The parity target remains the same in C++:

1. startup-known port initializes exporter immediately
   - `src/ray/gcs/gcs_server.cc:317-318`
2. head-node registration can initialize exporter later when the real port
   becomes known
   - `src/ray/gcs/gcs_server.cc:824-827`
3. the atomic exactly-once guard is inside `InitMetricsExporter(...)`
   - `src/ray/gcs/gcs_server.cc:940-943`
4. periodic export starts only after readiness succeeds
   - `src/ray/gcs/gcs_server.cc:953-959`
5. shutdown disables new events and flushes the remaining buffer
   - `src/ray/observability/ray_event_recorder.cc:56-95`
6. events buffer normally before export starts
   - `src/ray/observability/ray_event_recorder.cc:156-167`

### Rust behavior after Round 19

I verified the corresponding Rust behavior in:

- `rust/ray-gcs/src/main.rs`
- `rust/ray-gcs/src/server.rs`
- `rust/ray-observability/src/export.rs`

Key results:

1. `metrics_agent_port` still reaches the real binary startup config
   - `rust/ray-gcs/src/main.rs:113-144`
2. startup-known port attaches the gRPC sink and starts the periodic loop only
   after attach succeeds
   - `rust/ray-gcs/src/server.rs:260-300`
3. late-init now checks `port > 0` before consuming the init flag
   - `rust/ray-gcs/src/server.rs:389-416`
4. late-init starts the periodic loop only after sink attach succeeds
   - `rust/ray-gcs/src/server.rs:426-445`
5. the exporter preserves the buffer when no sink exists
   - `rust/ray-observability/src/export.rs:745-754`
6. the shared periodic loop handle prevents double-start
   - `rust/ray-gcs/src/server.rs:695-719`
7. shutdown still flushes and aborts the loop
   - `rust/ray-gcs/src/server.rs:727-742`

---

## Findings

### No remaining parity findings

I did not identify a remaining source-backed parity gap in the
`enable_ray_event` runtime path.

The two previously open issues now match the C++ contract closely enough to
support closure:

#### 1. Zero-port late-init semantics

Rust now performs the late-init precondition checks in the right order:

1. head node?
2. already initialized?
3. `port > 0`?
4. only then claim the one-shot init slot

Source:

- `rust/ray-gcs/src/server.rs:389-416`

This now matches the C++ split between:

- outer `port > 0` check
  - `src/ray/gcs/gcs_server.cc:824-827`
- inner atomic `exchange(true)`
  - `src/ray/gcs/gcs_server.cc:940-943`

That closes the Round 18 bug where a zero-port head-node registration could
permanently block a later valid initialization.

#### 2. Pre-attach buffering and periodic export start timing

Rust no longer starts the periodic loop merely because `enable_ray_event=true`.
It now starts only after successful sink attach in both startup and late-init
paths:

- startup path: `rust/ray-gcs/src/server.rs:274-285`
- late-init path: `rust/ray-gcs/src/server.rs:432-439`

That matches the C++ contract that `StartExportingEvents()` is only called after
exporter readiness succeeds:

- `src/ray/gcs/gcs_server.cc:953-959`

Additionally, Rust now hardens the exporter so `flush_inner()` does not drain
the buffer when no sink is present:

- `rust/ray-observability/src/export.rs:745-754`

That closes the Round 18 gap where pre-attach buffered events could be lost if
periodic flushing or manual flush occurred before attach.

---

## Test Coverage Assessment

The new Round 19 regression tests directly target the two previously open gaps.

Most important tests:

1. `test_zero_port_does_not_consume_future_valid_init`
   - `rust/ray-gcs/src/server.rs:2452-2555`
   - proves zero-port head-node registration does not consume future valid init

2. `test_pre_attach_events_survive_flush_intervals_and_export_after_late_attach`
   - `rust/ray-gcs/src/server.rs:2558-2647`
   - proves pre-attach events survive multiple short intervals and later export

3. `test_no_periodic_draining_without_sink`
   - `rust/ray-gcs/src/server.rs:2650-2706`
   - proves `flush()` preserves buffered events when no sink exists

4. `test_startup_unreachable_port_no_destructive_loss`
   - `rust/ray-gcs/src/server.rs:2709-2771`
   - proves unreachable startup port does not trigger destructive drain

5. `test_late_attach_starts_loop_exactly_once`
   - `rust/ray-gcs/src/server.rs:2774-2843`
   - proves loop start is deferred and not duplicated

6. `test_shutdown_still_flushes_after_late_attach`
   - `rust/ray-gcs/src/server.rs:2846-2935`
   - proves shutdown semantics still hold after late attach

The `ray-observability` hardening is also directly covered by the updated buffer
preservation test.

This is the right shape of evidence. These tests address the exact timing/order
contracts that were previously unproven.

---

## Open Questions / Residual Risk

I do not currently classify these as parity gaps, but they are worth recording:

1. C++ uses `MetricsAgentClient::WaitForServerReady(...)` before starting export,
   while Rust uses direct gRPC connect to the event-aggregator service and then
   starts export after successful sink attach.
2. Based on the current code, the observable contract appears aligned:
   successful attach starts export; failure leaves no sink and no export.
3. I did not find source evidence of an externally visible mismatch beyond that
   implementation detail.

If someone later wants stricter structural parity, a more explicit readiness
probe could still be added. But on this pass, I do not have enough evidence to
call that an open parity bug.

---

## Final Status

My conclusion after careful C++ vs Rust source comparison is:

**No remaining `enable_ray_event` parity gap is presently verified.**

More precise disposition:

- startup CLI/config plumbing: fixed
- late-init path: fixed
- zero-port semantics: fixed
- pre-attach buffering semantics: fixed
- no-file-fallback semantics: fixed
- shutdown/post-shutdown behavior: fixed

So the correct status after this re-audit is:

**GCS-6 appears fully closed**
