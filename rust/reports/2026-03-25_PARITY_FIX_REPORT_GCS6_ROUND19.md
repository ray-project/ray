# Round 19: Fix Two Remaining `enable_ray_event` Semantic Mismatches

**Date:** 2026-03-25
**Author:** Claude
**Reviewer:** Codex
**Branch:** `cc-to-rust-experimental`

## Summary

Round 18 closed three real gaps (binary plumbing, late-init path, stale comments) but introduced two new semantic mismatches. This round fixes both.

**Bug 1 — Init flag consumed before port check:** The late-init callback did `swap(true)` before checking `port > 0`. A head node with `port=0` permanently burned the one-shot, blocking later valid initialization. C++ checks port first, then exchanges inside `InitMetricsExporter`.

**Bug 2 — Premature periodic flush loop:** The periodic export loop started unconditionally when `enable_ray_event=true`, even before any sink was attached. `flush_inner()` drained the buffer via `std::mem::take` and discarded events when `sink == None`. C++ only starts `StartExportingEvents()` after readiness succeeds.

---

## Files Changed

| File | What changed |
|------|-------------|
| `rust/ray-gcs/src/server.rs` | Fix flag ordering, defer periodic loop to post-attach, shared handle slot, 6 new regression tests, 2 existing tests updated |
| `rust/ray-observability/src/export.rs` | Harden `flush_inner()` to preserve buffer when no sink; update `test_flush_without_sink` |
| `rust/ray-gcs/src/main.rs` | (Round 18, unchanged this round) |

---

## Bug 1: Init Flag Consumed Before Port Check

### Root cause

```rust
// OLD (Round 18) — WRONG ordering:
if init_flag.swap(true, AcqRel) { return; }  // flag consumed HERE
let port = node_info.metrics_agent_port;
if port <= 0 { return; }  // too late — flag already burned
```

### C++ reference

```
gcs_server.cc:822-827 (precondition checks):
  if (node->is_head_node() && !metrics_exporter_initialized_.load()) {
    int actual_port = node->metrics_agent_port();
    if (actual_port > 0) {
      InitMetricsExporter(actual_port);  // exchange(true) happens INSIDE here
    } else {
      RAY_LOG(INFO) << "Metrics agent not available...";
    }
  }

gcs_server.cc:940-943 (inside InitMetricsExporter):
  if (metrics_exporter_initialized_.exchange(true, ...)) {
    return;  // already initialized
  }
```

C++ checks all preconditions (head node, port > 0) BEFORE calling `InitMetricsExporter`. The `exchange(true)` only fires inside `InitMetricsExporter`, which is only called when `port > 0`.

### Fix (server.rs late-init listener)

```rust
// NEW — correct C++ ordering:
if !node_info.is_head_node { return; }                        // 1. head only
if init_flag.load(Acquire) { return; }                        // 2. already init? (optimization)
if node_info.metrics_agent_port <= 0 { log; return; }        // 3. port > 0? (flag NOT consumed)
if init_flag.swap(true, AcqRel) { return; }                  // 4. claim one-shot (inside "InitMetricsExporter")
tokio::spawn(/* connect + set_sink + start_loop */);
```

### Proof: `test_zero_port_does_not_consume_future_valid_init`
1. Register head node with `port=0` → no sink, flag NOT consumed
2. Register head node with valid port → sink attaches, events flow through gRPC

This test **would fail** on Round 18 code because the flag was consumed at step 1.

---

## Bug 2: Premature Periodic Flush Destroys Buffered Events

### Root cause (two parts)

**Part A — Loop starts too early:**
```rust
// OLD — starts unconditionally when enable_ray_event=true:
let handle = tokio::spawn(async move {
    loop { ticker.tick().await; exporter.flush(); }
});
self.periodic_flush_handle = Some(handle);
// This runs even when no sink is attached
```

**Part B — `flush_inner()` drains buffer into void:**
```rust
// OLD flush_inner():
let events = std::mem::take(&mut *buffer);  // buffer is now empty
// ...
match &*sink {
    Some(s) => s.flush(&grouped),
    None => 0,  // events gone forever
}
```

### C++ reference

```
gcs_server.cc:953-959 (inside InitMetricsExporter callback):
  metrics_agent_client_->WaitForServerReady([this, ...](const Status &status) {
    if (status.ok()) {
      ray_event_recorder_->StartExportingEvents();  // ← loop starts HERE
    }
  });
```

C++ only calls `StartExportingEvents()` after the readiness probe succeeds. Events buffered before that point remain in the buffer until the loop starts.

### Fix (two parts)

**Part A — Defer loop to post-attach (server.rs):**

New helper method `start_periodic_flush_loop()`:
```rust
fn start_periodic_flush_loop(
    exporter: &Arc<EventExporter>,
    flush_interval_ms: u64,
    handle_slot: &Arc<Mutex<Option<JoinHandle<()>>>>,
) {
    let mut slot = handle_slot.lock();
    if slot.is_some() { return; }  // already running — no double-start
    let handle = tokio::spawn(/* flush loop */);
    *slot = Some(handle);
}
```

Called from both paths, ONLY after `set_sink()` succeeds:
- **Startup path:** After `GrpcEventAggregatorSink::connect()` → `set_sink()` → `start_periodic_flush_loop()`
- **Late-init path:** Inside `tokio::spawn` after `connect()` → `set_sink()` → `start_periodic_flush_loop()`

The `periodic_flush_handle` is now `Arc<Mutex<Option<JoinHandle>>>` so the late-init callback (which captures it by clone) can store the handle.

**Part B — Harden `flush_inner()` (export.rs):**

```rust
fn flush_inner(&self) -> usize {
    // Check sink BEFORE draining buffer
    {
        let sink = self.sink.lock();
        if sink.is_none() { return 0; }  // preserve buffer
    }
    // Only drain buffer if sink exists
    let events = std::mem::take(&mut *buffer);
    // ...
}
```

This is defense-in-depth: even if the loop timing is correct, the buffer is never destroyed without a sink to receive the events.

### Proof: `test_pre_attach_events_survive_flush_intervals_and_export_after_late_attach`
1. `ray_events_report_interval_ms = 10` (very short — provokes the race)
2. Emit events before any sink exists
3. Wait 100ms (10+ potential flush intervals)
4. Assert events are **still buffered** (not drained)
5. Late-attach via head-node registration with valid port
6. Assert events are exported through gRPC

This test **would fail** on Round 18 code because the periodic loop would drain the buffer at the first 10ms tick.

### Proof: `test_no_periodic_draining_without_sink`
1. Short flush interval, no sink
2. Emit events
3. Manually call `flush()` — returns 0
4. Assert `buffer_len() == 1` (buffer preserved, not drained)
5. Wait 100ms, assert still buffered

---

## All New/Updated Tests

### 6 New Regression Tests

| # | Test | Bug proven fixed | Would fail on Round 18? |
|---|------|-----------------|------------------------|
| 1 | `test_zero_port_does_not_consume_future_valid_init` | Bug 1: flag ordering | Yes — flag consumed at port=0 |
| 2 | `test_pre_attach_events_survive_flush_intervals_and_export_after_late_attach` | Bug 2: premature flush | Yes — events drained at first tick |
| 3 | `test_no_periodic_draining_without_sink` | Bug 2: flush_inner hardening | Yes — buffer drained by manual flush |
| 4 | `test_startup_unreachable_port_no_destructive_loss` | Bug 2: startup variant | Yes — events drained by loop |
| 5 | `test_late_attach_starts_loop_exactly_once` | No double-loop | No (new behavior to verify) |
| 6 | `test_shutdown_still_flushes_after_late_attach` | Shutdown semantics after late-init | No (new behavior to verify) |

### 2 Updated Existing Tests

| Test | Change |
|------|--------|
| `test_gcs_server_stop_stops_periodic_actor_event_export_loop` | Now uses mock aggregator (loop only starts after sink attach) |
| `test_gcs_server_stop_cancels_or_quiesces_periodic_flush_task` | Same — needs mock aggregator for loop to exist |

### 1 Updated `ray-observability` Test

| Test | Change |
|------|--------|
| `test_flush_without_sink` | Now asserts `buffer_len() == 1` (buffer preserved, not drained) |

### 1 Updated Existing Test (Round 18)

| Test | Change |
|------|--------|
| `test_late_init_head_node_with_zero_port_does_not_initialize` | Added assertions: `metrics_exporter_initialized() == false`, `periodic_flush_running() == false` |

---

## Test Results

```
$ cargo test -p ray-gcs --lib
  519 unit tests:      PASS (including all 32 server::tests)

$ cargo test -p ray-gcs --test integration_test --test stress_test
   18 integration:     PASS
    5 stress:          PASS

$ cargo test -p ray-observability --lib
   51 unit tests:      PASS

Total: 593 passed, 0 failed
```

---

## Commands Run

```
cargo check -p ray-gcs -p ray-observability                        # compilation check
cargo test -p ray-gcs --lib server::tests                          # server unit tests
cargo test -p ray-gcs --lib server::tests::test_pre_attach         # targeted regression test
cargo test -p ray-gcs -p ray-observability --lib                   # all unit tests
cargo test -p ray-gcs --test integration_test --test stress_test   # integration + stress
```

---

## C++ ↔ Rust Parity Matrix (Updated)

| Behavior | C++ Source | Rust Source | Match? |
|----------|-----------|-------------|--------|
| Startup init when port > 0 | `gcs_server.cc:317-318` | `server.rs:252-286` | Yes |
| Late init on head-node register | `gcs_server.cc:822-832` | `server.rs:372-447` | Yes |
| **Port check BEFORE flag consume** | `gcs_server.cc:824-827` | `server.rs:397-416` | **Yes (fixed)** |
| Atomic exactly-once flag | `gcs_server.h:315` | `server.rs:122` | Yes |
| `exchange(true)` inside init only | `gcs_server.cc:941` | `server.rs:255` + `server.rs:416` | Yes |
| Zero port does NOT consume flag | `gcs_server.cc:826` (no call to InitMetricsExporter) | `server.rs:406-414` (return before swap) | **Yes (fixed)** |
| Failure → no export, no file | `gcs_server.cc:958-964` | `server.rs:275-284` + `server.rs:435-441` | Yes |
| **Loop starts after readiness only** | `gcs_server.cc:958` (StartExportingEvents) | `server.rs:270` + `server.rs:432` | **Yes (fixed)** |
| **Pre-attach events preserved** | C++ doesn't start loop before readiness | `export.rs:743-751` (sink check before drain) | **Yes (fixed)** |
| No file fallback | No file sink in event path | No file sink in event path | Yes |
| Shutdown flush | `gcs_server.cc:324-331` | `server.rs:698-742` | Yes |
| Post-shutdown rejection | `gcs_server.cc (enabled_=false)` | `export.rs:848-871` | Yes |
| Binary CLI → config | `gcs_server_main.cc` | `main.rs:123-143` | Yes |

---

## Direct Answers to Required Questions

**1. Does a head-node registration with `metrics_agent_port=0` still allow later valid initialization?**

Yes. The init flag is NOT consumed when port ≤ 0 (server.rs:406-414 returns before line 416's `swap`). Test `test_zero_port_does_not_consume_future_valid_init` proves this: port=0 → flag stays false → valid port → sink attaches → events flow.

**2. Can pre-attach events survive multiple periodic intervals and still export after late attach?**

Yes. Two defenses:
- The periodic flush loop does not start until after sink attach (server.rs:270/432 — `start_periodic_flush_loop` called only after `set_sink`)
- Even if `flush()` is called manually without a sink, `flush_inner()` preserves the buffer (export.rs:743-751 — sink check before `mem::take`)

Test `test_pre_attach_events_survive_flush_intervals_and_export_after_late_attach` proves this with a 10ms interval, 100ms wait, then late attach → events exported.

**3. Does Rust now start periodic exporting only after exporter attach, matching C++ semantics?**

Yes. `start_periodic_flush_loop()` is called exclusively inside the success branches of both init paths:
- Startup: server.rs:268-272 (after `set_sink` in the `Ok(sink)` arm)
- Late-init: server.rs:430-434 (after `set_sink` in the `Ok(sink)` arm inside `tokio::spawn`)

The shared `Arc<Mutex<Option<JoinHandle>>>` prevents double-start. Tests `test_no_periodic_draining_without_sink` and `test_late_attach_starts_loop_exactly_once` prove this.

**4. Is there still any observable C++ vs Rust gap in the `enable_ray_event` path?**

One minor non-gap: C++ uses `MetricsAgentClient::WaitForServerReady` (an async health RPC) before `StartExportingEvents`. Rust uses `tonic::connect()` which establishes a TCP connection (equivalent readiness verification). The observable behavior is identical: unreachable endpoint → no sink → no events → no loop. This is not a parity gap — the contract is the same.

**5. What exact code paths and tests prove that?**

| Contract point | Code path | Tests |
|---------------|-----------|-------|
| Startup port → sink + loop | server.rs:252-286 | `test_startup_known_port_sets_initialized_flag`, `test_actor_ray_event_path_uses_real_aggregator_service_client` |
| Late-init port → sink + loop | server.rs:372-447 | `test_late_init_head_node_with_port_initializes_exporter`, `test_late_attach_starts_loop_exactly_once` |
| Port=0 → no flag, no sink, no loop | server.rs:406-414 | `test_late_init_head_node_with_zero_port_does_not_initialize`, `test_zero_port_does_not_consume_future_valid_init` |
| Pre-attach buffer preserved | export.rs:743-751 | `test_no_periodic_draining_without_sink`, `test_pre_attach_events_survive_flush_intervals_and_export_after_late_attach` |
| No loop before attach | server.rs:688-720 (helper) | `test_startup_unreachable_port_no_destructive_loss`, `test_no_periodic_draining_without_sink` |
| Shutdown flush after late attach | server.rs:730-742 | `test_shutdown_still_flushes_after_late_attach` |
| No double-init, no double-loop | server.rs:416 + 694-696 | `test_late_init_no_double_initialization`, `test_late_attach_starts_loop_exactly_once` |
