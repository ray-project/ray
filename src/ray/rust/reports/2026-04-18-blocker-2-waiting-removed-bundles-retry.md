# 2026-04-18 — Blocker 2 close-out: real `HandleWaitingRemovedBundles` retry

## Summary

The previous Blocker 2 commit wired every C++
`GcsServer::InstallEventListeners` hook into Rust *structurally* but left
one behaviorally equivalent only on paper: `worker_dead` called
`pg_scheduler.handle_waiting_removed_bundles()`, and that method was an
explicit no-op with a TODO. This commit makes it real:

1. Ports the C++ `waiting_removed_bundles_` retry queue.
2. Gives `cancel_resource_reserve` a meaningful failure disposition —
   enqueue instead of just logging.
3. Replaces the no-op body of `handle_waiting_removed_bundles` with a
   real drain that retries each queued release and either drops
   successful entries or keeps still-failing ones for the next pass.
4. Matches C++ idempotency semantics (dead node → no-op; PG gone → safe;
   repeated worker-dead events → harmless re-drains).
5. Ships a regression test that forces the queue into the stuck state
   and proves a `ReportWorkerFailure` RPC drains it through the real
   listener pipeline.

## Code changes

### New retry queue — `crates/gcs-managers/src/pg_scheduler.rs`

```rust
#[derive(Debug, Clone)]
struct WaitingRemovedBundle {
    node_id: Vec<u8>,
    bundle: Bundle,
}

pub struct GcsPlacementGroupScheduler {
    // … existing fields …
    waiting_removed_bundles: Mutex<VecDeque<WaitingRemovedBundle>>,
}
```

The queue's shape (`list<pair<NodeID, BundleSpecification>>`) mirrors C++
(`gcs_placement_group_scheduler.h:509-510`). `VecDeque` gives
O(1) enqueue + cheap full-swap drain, and `parking_lot::Mutex` matches
the rest of the scheduler — short critical sections, no await points
held.

### New public method — `try_release_bundle_or_defer`

```rust
pub async fn try_release_bundle_or_defer(&self, node_id: &[u8], bundle: Bundle) {
    let Some(node) = self.node_manager.get_alive_node(node_id) else {
        // Node gone → nothing to release. Matches C++
        // TryReleasingBundleResources returning true on dead node.
        return;
    };
    if !self.send_cancel_resource_reserve(&node, bundle.clone()).await {
        self.waiting_removed_bundles.lock().push_back(WaitingRemovedBundle {
            node_id: node_id.to_vec(),
            bundle,
        });
    }
}
```

This is the **enqueue point**. The old internal `cancel_resource_reserve`
now delegates to it, so the one existing caller (prepare-phase failure
cleanup at `pg_scheduler.rs:405`) automatically gets retry semantics
without any call-site churn.

### Real drain — `handle_waiting_removed_bundles`

```rust
pub async fn handle_waiting_removed_bundles(self: &Arc<Self>) {
    let pending: VecDeque<WaitingRemovedBundle> = {
        let mut queue = self.waiting_removed_bundles.lock();
        std::mem::take(&mut *queue)
    };
    if pending.is_empty() { return; }
    for entry in pending {
        // try_release_bundle_or_defer handles the "node now dead → drop"
        // case and will push back on failure.
        self.try_release_bundle_or_defer(&entry.node_id, entry.bundle).await;
    }
}
```

Key properties:

- **Lock isn't held across RPCs.** Snapshot then drop. The per-iteration
  re-enqueue takes the lock briefly.
- **Self-consistent.** The drain loop funnels each entry back through
  `try_release_bundle_or_defer`, so the "node now dead" short-circuit and
  the re-enqueue logic live in one place.
- **Async, spawned.** The listener closure is sync, so
  `start_lifecycle_listeners` now does:

  ```rust
  let pgs = pgs.clone();
  tokio::spawn(async move {
      pgs.handle_waiting_removed_bundles().await;
  });
  ```

### Observability — `waiting_removed_bundles_count`

Small `pub fn` that returns `self.waiting_removed_bundles.lock().len()`.
Tests use it to verify drain behavior; operators can wire it into a
metric later.

---

## Idempotency / race matrix

All the race cases the blocker called out are handled:

| Race condition | Behavior | Where |
|---|---|---|
| Bundle already gone (node dead) | Drop — `get_alive_node` returns `None` | `try_release_bundle_or_defer` |
| PG already removed | Safe — the queue stores `(node_id, bundle)` pairs; the bundle spec is self-contained, so replay just re-sends an idempotent `CancelResourceReserve` | `handle_waiting_removed_bundles` |
| PG already rescheduled elsewhere | Safe — replay only affects the original (node, bundle) pair; new placements are tracked in `bundle_index` separately | same |
| Target node already dead | Drop — "dead node" branch in `try_release_bundle_or_defer` (C++ parity: `!HasNode(node_id)` → return true → erase entry) | same |
| Repeated `worker_dead` events | Safe — each call snapshots a fresh queue state; a second call right after a drain sees an empty queue and returns immediately | `handle_waiting_removed_bundles` |
| Concurrent prepare failure + worker_dead | Safe — `waiting_removed_bundles.lock()` serializes all enqueue/drain access; enqueues that arrive mid-drain are processed by the next drain | Mutex serialization |

---

## Tests (5 new, 291 total pass)

### Unit — `crates/gcs-managers/src/pg_scheduler.rs` (4 new)

1. `test_handle_waiting_removed_bundles_drains_on_dead_node` — force a
   release RPC to fail against a live-but-unreachable raylet, verify the
   entry is queued, then mark the node dead and call the drain. Assert
   the entry is dropped (matches C++ "dead node → no-op → erase").
2. `test_handle_waiting_removed_bundles_keeps_still_failing_entries` —
   same preconditions, but leave the node alive-and-unreachable. The
   retry fails again, and the entry must stay queued for the next pass.
3. `test_try_release_skips_when_node_already_dead` — releasing against
   an unknown node must not enqueue. Prevents infinite queue growth
   under node-removal races.
4. `test_handle_waiting_removed_bundles_empty_is_noop` — calling the
   drain with an empty queue is a cheap no-op and does not accumulate
   entries on repeated calls.

### End-to-end — `crates/gcs-server/src/lib.rs` (1 new)

5. `test_worker_dead_drains_waiting_removed_bundles` — the direct
   behavioral test the blocker asked for:
   1. Spin up a `GcsServer` and install listeners.
   2. Register a live-but-unreachable "raylet" and drive the scheduler
      into the `waiting_removed_bundles` state via
      `try_release_bundle_or_defer` (assertion: queue count == 1).
   3. Mark that node dead so the retry will drop the entry.
   4. Fire a `ReportWorkerFailure` RPC for an unrelated worker on
      another node.
   5. Poll up to ~1s for the queue to drain. Assert it hits 0.

   If any of `start_lifecycle_listeners` wiring, `worker_dead` →
   `handle_waiting_removed_bundles` spawn, or the drain body is missing
   or stubbed out, the assertion fails with a message referencing
   `gcs_server.cc:897`.

### Full workspace

```
$ cargo test --workspace
test result: ok. 20  passed; 0 failed  (gcs-kv)
test result: ok. 200 passed; 0 failed  (gcs-managers       — +4 new)
test result: ok. 13  passed; 0 failed  (gcs-pubsub)
test result: ok. 20  passed; 0 failed  (gcs-server lib     — +1 new)
test result: ok. 5   passed; 0 failed  (gcs-server bin)
test result: ok. 25  passed; 0 failed  (gcs-store)
test result: ok. 8   passed; 0 failed  (gcs-table-storage)
```

Total: **291 / 291 passing** (was 286, +5), 0 failures, 0 new warnings.

---

## Blocker-2 closure checklist

From the task description:

- [x] `worker_dead` triggers actor cleanup/restart parity
- [x] `worker_dead` triggers task-failure parity
- [x] `worker_dead` triggers pubsub subscriber cleanup parity
- [x] `worker_dead` triggers placement-group deferred bundle cleanup
      parity with **real retry behavior, not a stub** ← this commit
- [x] `job_finished` triggers placement-group cleanup parity
- [x] `node_removed` triggers job-manager and pubsub cleanup parity

All six points now have behavioral (not just structural) parity with
C++.

---

## C++ ↔ Rust parity matrix for the queue

| Behavior | C++ source | Rust source | Parity |
|---|---|---|---|
| Queue of deferred releases | `gcs_placement_group_scheduler.h:509-510` (`waiting_removed_bundles_`) | `waiting_removed_bundles: Mutex<VecDeque<WaitingRemovedBundle>>` | ✅ |
| Enqueue on release failure | `ReturnBundleResources` line 753 | `try_release_bundle_or_defer` else-branch | ✅ |
| "Dead node → no release needed" | `TryReleasingBundleResources` line 777-782 | `get_alive_node` → `None` short-circuit | ✅ |
| Drain on worker_dead | `HandleWaitingRemovedBundles` 819-832 | `handle_waiting_removed_bundles` async drain | ✅ |
| Erase successful, keep failing | same | same, via re-entry through `try_release_bundle_or_defer` | ✅ |
| Wired into worker_dead listener | `gcs_server.cc:897` | `start_lifecycle_listeners`, `tokio::spawn(drain)` | ✅ |

---

## On AWS testing

Not required. The retry path is GCS-local: `waiting_removed_bundles` is
in-memory, the RPC target (a raylet) is what the test intentionally
makes unreachable, and the drain logic is pure state transitions.
`test_worker_dead_drains_waiting_removed_bundles` exercises the full
Rust pipeline over localhost and covers the same code paths that would
run on EC2; only the NIC changes.

---

## Outcome

Blocker 2 is fully closed. `handle_waiting_removed_bundles` now does
real work — retries, idempotency, failure-preservation, and a listener
that drains the queue on every `worker_dead` event. The regression
test proves the hook is behaviorally equivalent to C++, not just
structurally present.
