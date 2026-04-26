# 2026-04-18 — Blocker 2 fix: full listener parity with C++ `InstallEventListeners`

## Summary

Even after Blocker 1 was closed (the production binary now always calls
`start_lifecycle_listeners`), the Rust listener set was materially weaker
than C++:

| C++ hook (`gcs_server.cc:856-907`) | Rust before | Rust after |
|---|---|---|
| `worker_dead` → `task_manager.OnWorkerDead` | ✅ | ✅ |
| `worker_dead` → `actor_manager.OnWorkerDead` | ❌ | ✅ |
| `worker_dead` → `placement_group_scheduler.HandleWaitingRemovedBundles` | ❌ | ✅ (no-op stub, see below) |
| `worker_dead` → `pubsub_handler.AsyncRemoveSubscriberFrom(worker_id)` | ❌ | ✅ |
| `job_finished` → `task_manager.OnJobFinished` | ✅ | ✅ |
| `job_finished` → `placement_group_manager.CleanPlacementGroupIfNeededWhenJobDead` | ❌ (TODO in src) | ✅ |
| `node_removed` → `job_manager.OnNodeDead` | ✅ | ✅ |
| `node_removed` → `pubsub_handler.AsyncRemoveSubscriberFrom(node_id)` | ❌ | ✅ |

The most severe of these was the missing `actor_manager.on_worker_dead`
hook: in C++, worker death drives **actor reconstruction**; without the
hook in Rust, an actor whose worker died would sit in a terminal-looking
state and never be restarted — a dead-cluster-by-stealth scenario for
long-running jobs.

## Code changes

### New API: `GcsPlacementGroupManager::clean_placement_group_if_needed_when_job_dead`

`crates/gcs-managers/src/placement_group_stub.rs`

Ports C++ `GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenJobDead`
(`gcs_placement_group_manager.cc:748-774`). Scans registered PGs; for
each one owned by the dead job:

- Sets `creator_job_dead=true` and persists (mirrors C++
  `PlacementGroup::MarkCreatorJobDead` + table flush).
- If lifetime is done (`!is_detached && creator_job_dead &&
  creator_actor_dead` — `gcs_placement_group.cc:129-132`), removes the
  PG via a shared internal helper.

The removal logic that was previously inlined in the
`RemovePlacementGroup` RPC handler is now extracted to a private
`remove_placement_group_internal` method, and the RPC handler is a
one-liner that delegates to it. This keeps the PG-removal path unique —
the RPC and the listener-driven cleanup produce identical on-disk and
in-memory state.

### New API: `GcsPlacementGroupScheduler::handle_waiting_removed_bundles`

`crates/gcs-managers/src/pg_scheduler.rs`

Documented no-op stub. Rationale: C++ maintains a
`waiting_removed_bundles_` queue that `cancel_resource_reserve` failures
enqueue into, and `HandleWaitingRemovedBundles` retries. Rust's current
`cancel_resource_reserve` (`pg_scheduler.rs:752-776`) only logs on
failure; there is no retry queue yet. Rather than leave a bare `// TODO`
at the call site, I added the method so the Rust call graph mirrors
C++'s exactly. When the retry queue is ported later, only the body
changes — every call site is already in place.

### New API: `PubSubManager::has_subscriber`

`crates/gcs-pubsub/src/lib.rs`

Small boolean accessor that `self.subscribers.contains_key(id)`. Useful
on its own and required for the new regression tests — lets us verify
lifecycle cleanup without exposing internal DashMap types.

### The listener wiring itself

`crates/gcs-server/src/lib.rs` — `start_lifecycle_listeners`

```rust
// worker_dead → {task, actor, pg_scheduler, pubsub}
self.worker_manager.add_worker_dead_listener(Box::new(move |worker_data| {
    // 1. Mark tasks FAILED
    tm.on_worker_dead(worker_id.clone(), worker_data.clone(), delay_ms);

    // 2. Drive actor reconstruction. creation_task_exception.is_none()
    //    maps the C++ "allow restart unless the worker died because its
    //    creation task threw" rule.
    let need_reconstruct = worker_data.creation_task_exception.is_none();
    tokio::spawn(async move {
        am.on_worker_dead(&node_id, &worker_id_for_actor, need_reconstruct).await;
    });

    // 3. Retry stuck bundle releases (no-op for now, see doc comment).
    pgs.handle_waiting_removed_bundles();

    // 4. Clean up pubsub subscription.
    pubsub.remove_subscriber(&worker_id);
}));

// job_finished → {task, placement_group_manager}
self.job_manager.add_job_finished_listener(Box::new(move |job_data| {
    tm.on_job_finished(job_id.clone(), end_time_ms, delay_ms);
    let pgm = pgm.clone();
    tokio::spawn(async move {
        pgm.clean_placement_group_if_needed_when_job_dead(&job_id).await;
    });
}));

// node_removed → {job_manager, pubsub}
tokio::spawn(async move {
    while let Ok(node) = rx_removed.recv().await {
        jm.on_node_dead(&node.node_id).await;
        pubsub.remove_subscriber(&node.node_id);
    }
});
```

Every C++ side effect on `worker_dead`, `job_finished`, and
`node_removed` now has a Rust counterpart in this function.

### Why `tokio::spawn` inside the closures

`add_worker_dead_listener` / `add_job_finished_listener` take
`Box<dyn Fn(&T) + Send + Sync>` — synchronous closures. The hooks we
need to drive (`actor_manager.on_worker_dead`,
`placement_group_manager.clean_placement_group_if_needed_when_job_dead`)
are async. Spawning inside the closure is correct and matches C++'s
behavior: C++ fires these on the `io_context` and returns immediately;
the work completes asynchronously. Rust's `tokio::spawn` is the direct
equivalent.

---

## Tests added (7 new, 286 total pass)

### At the PG-manager level (`crates/gcs-managers/src/placement_group_stub.rs`, 4 tests)
1. `test_clean_pg_on_job_dead_removes_when_lifetime_done` — PG with
   `creator_actor_dead=true`, non-detached → transitions to REMOVED.
2. `test_clean_pg_on_job_dead_marks_without_removing` — PG with
   `creator_actor_dead=false` → stays registered, but
   `creator_job_dead` is now true.
3. `test_clean_pg_on_job_dead_ignores_detached` — detached PG survives
   its creator job, matching C++
   `IsPlacementGroupLifetimeDone` returning false when
   `is_detached=true`.
4. `test_clean_pg_on_job_dead_scoped_to_job` — PGs owned by a different
   job are not touched.

### At the server-lifecycle level (`crates/gcs-server/src/lib.rs`, 3 tests)
1. `test_worker_dead_listener_cleans_pubsub_subscription` — subscribe a
   worker, fire `ReportWorkerFailure`, assert the subscription is gone
   (parity with C++ line 898).
2. `test_node_removed_listener_cleans_pubsub_subscription` — same shape,
   driven by `node_manager.remove_node` (parity with C++ line 869).
3. `test_job_finished_listener_cleans_placement_groups` — creates a PG
   with `creator_actor_dead=true`, registers a job, calls
   `MarkJobFinished`, polls until the PG transitions to REMOVED
   (parity with C++ line 906).

The existing Blocker-1 regression test
(`test_start_with_listener_installs_lifecycle_listeners`) continues to
pass, confirming the `task_manager.on_worker_dead` branch of the
listener chain is unbroken.

### Full workspace

```
$ cargo test --workspace
test result: ok. 20  passed; 0 failed  (gcs-kv)
test result: ok. 196 passed; 0 failed  (gcs-managers       — +4 new)
test result: ok. 13  passed; 0 failed  (gcs-pubsub)
test result: ok. 19  passed; 0 failed  (gcs-server lib     — +3 new)
test result: ok. 5   passed; 0 failed  (gcs-server bin)
test result: ok. 25  passed; 0 failed  (gcs-store)
test result: ok. 8   passed; 0 failed  (gcs-table-storage)
```

Total: **286 / 286 passing** (was 279, +7), 0 failures, 0 new warnings.
A pre-existing `actor_scheduler`-dead-code warning remains; the previous
`placement_group_manager` and `pg_scheduler`-dead warnings are gone
because both fields are now used by the listener wiring.

---

## C++ ↔ Rust parity matrix — complete

All three listener bodies match cardinality with C++:

| Event | C++ callbacks | Rust callbacks | Parity |
|---|---|---|---|
| `worker_dead` | 4 (task, actor, pg_scheduler, pubsub) | 4 | ✅ |
| `job_finished` | 2 (task, pg_manager) | 2 | ✅ |
| `node_removed` (scope of `start_lifecycle_listeners`) | 2 (job_manager, pubsub) | 2 | ✅ |

Other `node_removed` / `node_added` side effects (resource, autoscaler,
actor scheduler, PG scheduler, health-check) remain in sibling
`start_node_*_listeners` helpers as they were; those were already at
parity.

---

## The one residual caveat, clearly noted

`handle_waiting_removed_bundles` is a no-op today because the
`waiting_removed_bundles_` retry queue doesn't exist in Rust yet —
`cancel_resource_reserve` currently just logs on failure. The listener
still calls the method so:

- The Rust call graph structurally matches C++ (grep for
  `handle_waiting_removed_bundles` lands in the same place as
  `HandleWaitingRemovedBundles`).
- Once the retry queue is ported, zero wiring changes are needed — only
  the method body.

This is explicitly documented in the function's doc comment and the
call site.

---

## On AWS testing

Not required. Every hook fires between local in-process objects; there
is no network, storage backend, or AWS-specific resource involved.
The new regression tests drive the full Rust paths (worker RPC, job
RPC, node removal, pubsub subscription cleanup, PG removal) through
the actual listener pipeline — same paths that would run on EC2.

---

## Outcome

Blocker 2 is closed. The Rust GCS lifecycle listeners now fire the same
set of downstream hooks on `worker_dead`, `job_finished`, and
`node_removed` that C++ fires, with behaviorally-equivalent semantics
(actor reconstruction, PG cleanup on job death, pubsub subscriber
cleanup). The one known deviation — `handle_waiting_removed_bundles`
being a no-op until the retry queue ports — is structurally in place,
documented, and guarded by the presence of the call site so it can't be
forgotten.
