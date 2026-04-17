# RFC: Gang-Aware Traffic Management

Gang-aware routing, pausing, and restructuring for Ray Serve gang-scheduled deployments.

---

## Motivation

Today, routers see replicas as independent entities — `RunningReplicaInfo` carries no gang identity. There is no way to:

- Pause traffic to an entire gang when a member fails.
- Continue serving with the remaining healthy members while a failed member is replaced.
- Invoke a collective operation (e.g., EPLB expert redistribution) on healthy members before resuming traffic.

This RFC introduces a pause flag on `RunningReplicaInfo`, and a controller-driven restructure mechanism via `reconfigure_gang`.

---

## Concepts

- **Gang**: a group of replicas that operate collectively. All members are co-scheduled via placement groups.
- **Healthy gang**: all members are RUNNING at target version.
- **Unhealthy gang**: a gang with a faulty member that has not yet been cordoned from traffic.
- **Degraded gang**: a gang whose healthy member count is below `gang_size`, but the faulty member has been cordoned and the remaining members have been restructured. A degraded gang can serve requests.
- **Restructure**: a collective operation invoked on healthy members after membership changes (e.g., EPLB expert redistribution). Traffic is paused during restructure and resumes once it completes.

**Gang lifecycle:**

```
Healthy gang
  → member fails → Unhealthy gang
  → pause traffic, restructure → Degraded gang (serving)
  → replacement replica joins
  → pause traffic, restructure → Healthy gang (serving)
```

---

## Foundational Contract: `RunningReplicaInfo`

This RFC adds a single new field:

```python
@dataclass(frozen=True)
class RunningReplicaInfo:
    # ... existing fields (replica_id, node_id, actor_name, etc.) ...

    is_paused: bool = False
    """When True, the replica is healthy but should not receive new traffic.
       Used during gang restructure."""
```

**Gotchas**
- `is_paused` must be included in `RunningReplicaInfo.__post_init__`'s hash computation so LongPoll broadcast de-duplication does not miss pause/unpause transitions.
- `DeploymentReplica` carries the durable `_is_paused` bit (read by `get_running_replica_info()` each broadcast) since `RunningReplicaInfo` is frozen and reconstructed every tick.

### Router-side filter

`RequestRouter.update_replicas()` partitions the broadcast into active and paused. Only active replicas populate `_replicas_list`, `_replica_id_set`, so `choose_replicas()` never sees paused ones. Paused wrappers stay in `_replicas` so pause→unpause cycles reuse cached actor handles.

### Applicability

This RFC applies only to `runtime_failure_policy=RESTART_REPLICA`. Under the default `RESTART_GANG` policy, any health-check failure tears down the entire gang atomically, so there are no surviving members to pause or restructure. `is_paused` remains a no-op on `RESTART_GANG` gangs and on non-gang deployments.

### Why not reuse existing graceful shutdown?

Graceful shutdown is **terminal and replica-side**: the replica moves to STOPPING, is removed from the broadcast, drains, and exits. There is no "resume."

`is_paused` is fundamentally different:

- **Reversible** — clear the flag and traffic resumes.
- **Router-only** — the replica actor is unaware, stays RUNNING.
- **Replica stays in the broadcast** — wrappers reused, no add/remove churn.

---

## End-to-End Lifecycle

The sections below define each mechanism in detail. This section shows how they compose into a complete failure-and-recovery cycle for a gang with `world_size=4` where rank 2 fails.

Each "tick" is one iteration of `DeploymentStateManager.update()`, which runs these steps in order:

1. `check_and_update_replicas()` + `check_and_update_deployment_actors()`: health checks, state transitions, gang failure detection. **New work happens here:** set/clear `is_paused`, fire `reconfigure_gang`, poll pending `reconfigure_gang` ObjectRefs.
2. `check_curr_status()`: initial status check.
3. Drain nodes → `migrate_replicas_on_draining_nodes()`.
4. `_reserve_gang_placement_groups()`: atomically reserve PGs for new gangs.
5. `scale_deployment_replicas()`: upscale/downscale to match target.
6. `check_curr_status()`: update status after scaling.
7. `schedule()`: place STARTING replicas, stop STOPPING replicas.
8. `broadcast_running_replicas_if_changed()` + `broadcast_deployment_config_if_changed()`: push `RunningReplicaInfo` (including updated `is_paused`) to routers via LongPoll.
9. Record metrics + cleanup deleted deployments.

Pause/unpause and `reconfigure_gang` orchestration all happen in step 1. The pause state is not observable by routers until step 8 broadcasts it later in the same tick.

### Failure Path (healthy → degraded)

```
Tick N   [check_and_update]  Health check detects rank 2 failed in gang "abc"
                             Controller sets is_paused=True on ALL gang "abc" replicas
                             Controller updates GangContext on ranks 0, 1, 3 (removes dead rank 2)
                             Controller fires reconfigure_gang(new_context) on ranks 0, 1, 3
                             Stores ObjectRefs in _pending_gang_reconfigure[gang_id]
         [broadcast]         LongPoll broadcast → router stops sending traffic to gang "abc"

Tick N+1 [check_and_update]  check_obj_ref_ready_nowait(refs) → still pending, skip

Tick N+2 [check_and_update]  check_obj_ref_ready_nowait(refs) → all ready (collective done)
                             Controller sets is_paused=False on ranks 0, 1, 3
         [broadcast]         LongPoll broadcast → router resumes traffic to degraded gang [0, 1, _, 3]
```

### Recovery Path (degraded → healthy)

```
Tick M   [scale]             target_replicas > current → new replica created for rank 2 (STARTING)

Tick M+K [check_and_update]  New rank 2 transitions STARTING → RUNNING
                             Controller sets is_paused=True on ALL gang "abc" replicas (0, 1, 2_new, 3)
                             Controller updates GangContext on ALL ranks (adds new rank 2)
                             Controller fires reconfigure_gang(new_context) on ALL ranks
         [broadcast]         LongPoll broadcast → router pauses traffic

Tick M+K+J
         [check_and_update]  refs all ready (collective done)
                             Controller sets is_paused=False on all ranks
         [broadcast]         LongPoll broadcast → router resumes traffic to healthy gang [0, 1, 2, 3]
```

---

## 1. Pause and Resume

Temporarily suppress new traffic to a set of replicas without stopping them.

### Triggers

**Pause** when:
- A gang replica fails its health check (all gang members paused).
- A replacement replica becomes RUNNING and is ready to rejoin (all gang members paused for restructure).

**Resume** when:
- Restructure completes after a member is cordoned (healthy members resume; failed member stays paused).
- Restructure completes after a replacement joins (all members resume).

### Contract

```python
class DeploymentState:
    def pause_replicas(self, replica_ids: List[ReplicaID]) -> None:
        """
        Sets RunningReplicaInfo.is_paused=True. On the next LongPoll
        broadcast, the router stops selecting these replicas for new
        requests.
        """

    def resume_replicas(self, replica_ids: List[ReplicaID]) -> None:
        """Clear the paused flag for a list of replicas."""
```

### Alternatives Considered

Gang-level `active_replica_ids: Set[ReplicaID]` on the gang, where only replicas in the set receive traffic. This requires the router to understand gang structure and look up gang membership on every request. `is_paused` on individual replicas is simpler — the router just skips paused replicas in `update_replicas()` without knowing anything about gangs.

---

## 2. Gang Restructure via `reconfigure_gang`

After traffic is paused, healthy replicas may need to perform a collective operation before the gang can resume serving. The controller orchestrates this by calling a user-defined `reconfigure_gang` method on each healthy replica, following the same pattern as the existing `reconfigure()` mechanism.

### User-Facing API

```python
@serve.deployment(gang_scheduling_config=GangSchedulingConfig(gang_size=4))
class MyModel:
    async def reconfigure_gang(self, gang_context: GangContext):
        """
        Optional: Called by Serve on ALL healthy replicas simultaneously
        when gang membership changes. `gang_context` carries the new
        membership (updated `member_replica_ids`, same `gang_id`,
        rank unchanged for surviving replicas).
        """
        self._gang_context = gang_context  # user-controlled
```

### Mechanism

The controller already calls `reconfigure()` on replicas via `actor_handle.reconfigure.remote(...)` (`deployment_state.py:1150`), stores the `ObjectRef` in `_ready_obj_ref`, and polls it with `check_obj_ref_ready_nowait` each tick (`deployment_state.py:1291`). `reconfigure_gang` follows the same pattern, but uses a dedicated object-ref slot (`_gang_reconfigure_ref`) so it cannot collide with the init/config-update `_ready_obj_ref`:

1. **Compute new context**: Controller builds a fresh `GangContext` per surviving replica with updated `member_replica_ids` (dead rank removed, or new rank inserted) and the same `gang_id` / `pg_name`. `rank` is preserved for surviving replicas; the new replica keeps the rank it was created with. The controller's in-memory `DeploymentReplica._gang_context` is overwritten at the same time so future `get_running_replica_info()` calls see the new membership.
2. **Fire**: Controller calls `replica.actor_handle.reconfigure_gang.remote(new_gang_context)` on each healthy replica, passing the new context as an argument (not mutating the replica actor's init-time copy in place). All calls dispatch in the same loop iteration so replicas enter the collective simultaneously. Refs are stored in a new per-deployment `_pending_gang_reconfigure: Dict[str, List[ObjectRef]]` keyed by `gang_id`.
3. **Poll**: Each subsequent tick, step 1 of `update()` iterates `_pending_gang_reconfigure` and uses `check_obj_ref_ready_nowait` on each ref. The controller never blocks.
4. **Complete**: When all ObjectRefs for a `gang_id` resolve, the controller clears `is_paused` on its healthy members, drops the entry from `_pending_gang_reconfigure`, and step 8 broadcasts the unpaused state.

Replica-side, `ServeReplica` gains a `reconfigure_gang(new_context)` handler that (a) updates its own cached `GangContext`, (b) invokes the user's `reconfigure_gang` if defined, and (c) returns. The user method is optional — when absent, Serve still performs step (a) so the replica's view of gang membership stays consistent.

### Timing: Pause Propagation vs. Collective Start

There is a small window between when the controller sets `is_paused=True` / fires `reconfigure_gang` and when the router processes the LongPoll update. During this window, the router may still dispatch a few requests. This is acceptable as long as the client could retry based on retryable errors thrown by Ray Serve. In-flight request re-routing is out-of-scope for this RFC.

### Why in Serve, Not the Application

The orchestration (pause → collective → resume) belongs in Serve:

- Serve owns `is_paused` and the LongPoll broadcast.
- Serve holds the actor handles needed to fire `remote()` calls.
- Serve's reconciliation loop provides non-blocking ObjectRef polling.
- Serve knows *when* membership changes (health check failure, new replica RUNNING).

The *content* of the collective belongs in the application, i.e. the user implements `reconfigure_gang()`.

### Alternatives Considered

- **Replica-initiated**: A replica detects the failure and coordinates with its gang peers directly. But the controller can't track completion, and replicas have no way to know when all peers are paused.
- **Application handles pause/resume externally**: Serve only provides primitives; the application orchestrates the lifecycle, but the application must reach into Serve internals for pause/resume with no reliable way to detect membership changes.
- **LongPoll channel from controller to replicas**: Push gang context updates via a new LongPoll subscription. Adds new propagation infrastructure with no completion tracking; direct actor calls with ObjectRefs are simpler.
