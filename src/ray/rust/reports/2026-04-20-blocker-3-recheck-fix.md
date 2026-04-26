# 2026-04-20 — Blocker 3 recheck fix: correct field read + real aggregator client

## Problem

`2026-04-20-blocker-3-recheck.md` confirmed that most of Blocker 3
was closed (usage-stats wiring, eager init, config surface) but
flagged two remaining items:

1. **Field-parity bug in the deferred head-node path.** C++
   `GcsServer` reads `node->metrics_agent_port()` at
   `gcs_server.cc:835`. Rust read `node.metrics_export_port`. These
   are two separate proto fields — tag 30 (the dashboard-agent gRPC
   port) vs. tag 9 (the HTTP metrics-scrape port). `raylet/main.cc:1115-1116`
   populates them independently. The Rust deferred path would silently
   fail whenever the head node reported only `metrics_agent_port` —
   which is the common case.
2. **Exporter only dials a raw `Channel`.** C++
   `InitMetricsExporter` also constructs an `EventAggregatorServiceClient`
   so subsequent event-forwarding code can call `AddEvents` without
   redialling (`gcs_server.cc:955-961`). The Rust exporter dialed
   and dropped the channel, so there was no cached client.

Severity: the first is a direct C++-parity bug; the second is the
"not yet fully identical side effects" scope caveat the recheck
report called out.

---

## Fix

### 1. Read `metrics_agent_port` in the deferred path

File: `crates/gcs-server/src/lib.rs`.

`start_metrics_exporter_on_head_node_register` now reads
`node.metrics_agent_port` instead of `node.metrics_export_port`.
Matches C++ `gcs_server.cc:835` byte-for-byte. Inline comment calls
out the two proto fields explicitly so a future reader doesn't
regress.

### 2. Cache a live `EventAggregatorServiceClient` on success

File: `crates/gcs-managers/src/metrics_exporter.rs`.

- New field `event_aggregator_client:
  parking_lot::Mutex<Option<EventAggregatorServiceClient<Channel>>>`.
- On a successful dial, the spawned connect task now builds
  `EventAggregatorServiceClient::new(channel)` and stores it on the
  exporter — not just logs readiness. Parity with C++
  `event_aggregator_client_->Connect(port)` +
  `metrics_agent_client_` construction at `gcs_server.cc:955-961`.
- New public accessor `event_aggregator_client() ->
  Option<EventAggregatorServiceClient<Channel>>` for event-forwarding
  code. Cloning the client is cheap (tonic clients wrap a cloneable
  `Channel`), so this hands back an owned handle without locking
  the caller against subsequent reinitialisation.
- Module-level doc-comment updated to reflect the new scope: the
  aggregator client *is* now wired; the remaining unshipped bits
  (OpenCensus / OpenTelemetry exporters) are called out as a separate
  workstream with a concrete hook point inside the successful-connect
  branch.

### 3. Documentation

The inline comment inside
`start_metrics_exporter_on_head_node_register` now names the two
proto fields (tag 9 vs. tag 30), names the C++ line, and explains
why reading the wrong field silently breaks the common case. That's
the primary regression guard for anyone editing this block.

---

## Tests

### Updated — `metrics_exporter_initializes_on_head_node_register`

The pre-existing test used `metrics_export_port: 1`, so it *passed*
while exercising the wrong field. It now uses `metrics_agent_port`
and the doc-comment calls out the historical mismatch so future
readers can trace back why this test is the field-parity canary.

### New — `metrics_exporter_inits_when_only_metrics_agent_port_is_set`

Positive regression: `metrics_agent_port: 1`, `metrics_export_port: 0`
on a head node must still flip `is_initialized()`. This is the
common production configuration and the exact failure mode the
recheck report called out. A revert of the field-read fix makes
this test fail.

### New — `metrics_exporter_does_not_init_when_only_metrics_export_port_is_set`

Negative regression: `metrics_agent_port: 0`, `metrics_export_port: 1`
on a head node must NOT fire init. `metrics_export_port` is a
metrics-scrape port (HTTP) that has no relationship to the event
aggregator. Before the fix this test would have incorrectly
initialised the exporter.

### New (unit) — `connect_success_caches_aggregator_client`

Splits the prior `connect_success_logs_ready` test. After the dial
succeeds, `exporter.event_aggregator_client()` must return
`Some(client)`. Regression guard for anyone removing the store step
of the expanded exporter.

### New (unit) — `connect_failure_leaves_aggregator_client_unset`

Pair: a failed dial must leave the client `None`, so event-forwarding
code that checks for `Some` doesn't silently retry against a dead
channel.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    244 passed   (+1 vs previous, exporter test split)
gcs-server:       40 passed   (+2 vs previous)
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions.

---

## Before → after behaviour

| Scenario                                                           | Before                                             | After                                                 |
| ------------------------------------------------------------------ | -------------------------------------------------- | ----------------------------------------------------- |
| Head node registers with `metrics_agent_port=9876`, `metrics_export_port=0` | No init (Rust read the wrong field)              | Exporter inits, matches C++ `gcs_server.cc:835`       |
| Head node registers with `metrics_agent_port=0`, `metrics_export_port=9876` | Spurious init on the wrong port                    | No init (parity with C++)                             |
| Successful dial after eager or deferred init                       | Channel dropped; no cached client                  | `EventAggregatorServiceClient` cached; reachable via `event_aggregator_client()` |
| Failed dial                                                        | Channel error logged                                | Same, plus `event_aggregator_client() == None` (no stale client left behind) |

---

## Scope and caveats

What's included:

- Complete parity with the C++ field read
  (`node->metrics_agent_port()`).
- Cached `EventAggregatorServiceClient` after a successful dial — the
  concrete gRPC client the GCS would use to forward events.
- Positive and negative regression tests pinning the correct field.
- Paired tests (success caches / failure doesn't) on the exporter
  itself.

What's deliberately out of scope (unchanged from the previous report):

- **OpenCensus / OpenTelemetry exporters.** The Rust GCS doesn't have
  either stack. The successful-connect branch inside the exporter is
  the hook point for a future workstream. The recheck report flagged
  this as an accepted temporary scope choice; this commit doesn't
  change that, but it does narrow the scope: with the aggregator
  client now cached, the OC/OTel wiring is the only remaining
  observable gap.
- **AWS testing.** Not needed. All three test paths (eager,
  agent-port-only deferred, export-port-only deferred) are driven
  in-process via `node_manager.add_node`. AWS would only exercise
  tonic transport, not the field-read parity this commit closes.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs`:
  - `start_metrics_exporter_on_head_node_register` reads
    `node.metrics_agent_port`.
  - Existing deferred-init test updated to set `metrics_agent_port`;
    two new regression tests added (positive and negative field-parity).
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/metrics_exporter.rs`:
  - New `event_aggregator_client` field + accessor.
  - Connect task builds and caches
    `EventAggregatorServiceClient::new(channel)` on success.
  - Unit-test split: `connect_success_caches_aggregator_client` and
    `connect_failure_leaves_aggregator_client_unset`.
  - Module doc-comment updated to reflect the expanded scope.
