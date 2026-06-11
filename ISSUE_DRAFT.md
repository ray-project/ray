# [RFC][Train] Worker health & preemption lifecycle: from binary running/fault to a controllable fault-tolerance substrate

## Motivation

Pre-training runs are **long and brittle**. A frontier run occupies thousands of GPUs for weeks, and at that scale something is almost always degrading: a flaky NIC, a throttled or overheating GPU, a slow storage path, a preempted spot node. Today Ray Train's only answer to a single such fault is to **tear down the entire worker group and checkpoint-restart the whole job** — wasting `N_workers × (time since last checkpoint)` accelerator-hours for one bad node.

That all-or-nothing response is acceptable only if faults are rare. On modern racks (e.g. GB300) they aren't, which is **why this matters now**: the components that can fail per node have grown by orders of magnitude over the A100 generation, and the most expensive failures aren't even hard crashes — a job keeps running while one GPU silently overheats and throttles, or one rank staggers, collapsing throughput without ever tripping failure handling.

Ray Train has a single controller that already watches every worker — so in principle it could spot the one bad node and fix just that node: checkpoint it, replace it with a spare, and avoid scheduling work back onto broken hardware, instead of restarting all N workers. The problem is what the controller can see. Today each worker only reports two things — "am I still running?" and "did I hit an error?" ([`WorkerStatus`](python/ray/train/v2/_internal/execution/worker_group/worker.py)) — so the controller only ever learns about a worker once it has already crashed, and the only move it has is to tear the whole group down and start over. A worker that's still running but clearly in trouble (a GPU overheating, one rank falling behind) looks perfectly healthy until it dies.

This isn't starting from zero. Ray Train already has a mature recovery toolkit: periodic checkpointing with `FailureConfig(max_failures=...)`, mid-epoch resume via `DatasetCheckpointConfig`, elastic training with `num_workers=(min, max)`, and an in-progress TorchFT integration. But every one of these is **reactive** — it only kicks in *after* a worker has already died — and they all treat every death the same. None of them uses the advance warning the cloud gives before a spot reclaim, lets survivors finish in-flight work instead of being torn down alongside the dead rank, or tells a routine preemption apart from a real bug like an OOM. This RFC adds the *before* and the *after* on top of that existing foundation, rather than replacing any of it.

### One lifecycle, two signals

Both **node preemption** and **silent GPU degradation** are the same problem shape — a per-worker signal the framework can't see, that should drive a *targeted* response rather than a full restart. They share one lifecycle:

**detect a per-worker signal → take graceful, targeted action → reschedule with the right classification.**

| Signal | Source | **Before** — detect | **At** — graceful action | **After** — reschedule & classify |
|--------|--------|------|------|------|
| `DRAINING` (preemption) | Ray Core `get_draining_nodes()` / cloud notice | surface to UDF via `preemption_status()` | drain window; survivors JIT-checkpoint | `PreemptionError` on its own budget; quarantine drained node |
| `DEGRADED` (overheat, slowness, stagger) | user health probe (e.g. pynvml) | echo `WorkerHealth` on poll | checkpoint + proactively evict the bad rank | quarantine the node; swap in a spare |
| `UNHEALTHY` (probe failure) | user health probe | echo `WorkerHealth` on poll | checkpoint the group | restart, excluding the offending node |
| `HEALTHY` | default | — | continue | — |

Ray Core already provides the `DRAINING` signal, so the proposal **builds the lifecycle end-to-end for preemption first (Phases 1–3)**, then **generalizes the same detect/act/react machinery to user-defined health probes** so `DEGRADED`/`UNHEALTHY` get the same treatment (Phases 4–6).

For preemption specifically, the warning window makes the payoff concrete: spot capacity is 60–90% cheaper, and providers warn in advance — [AWS up to 120s](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html), [GKE ~30s](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/spot-vms#termination-graceful-shutdown) — yet Ray Train ignores it and reacts only to the actual death, giving the *"checkpoint every 1000 steps, died at step 999"* problem.

## Proposed Initiative

Phased and incremental. Each phase is independently mergeable and additive — jobs that don't opt in see no behavioral change. Phases 1–3 land the preemption lifecycle (prototype: [PR #63475](https://github.com/ray-project/ray/pull/63475)); Phases 4–6 generalize it to arbitrary health signals and standby capacity.

### Phase 1 — Surface the preemption signal into the UDF *(Before)*

Add `ray.train.preemption_status() -> Optional[PreemptionInfo]` so the loop can observe, at any step boundary, whether a drain notice arrived and which ranks are doomed vs. survivors. A **`PreemptionWatcher`** actor (one per worker group, managed by a `PreemptionCallback`) polls `get_draining_nodes()` every `DEFAULT_PREEMPTION_POLL_INTERVAL_S = 5.0`s and fans out `mark_preempt(info)` directly to all worker actors; workers store it in a thread-safe `PreemptionContext` and echo it in `WorkerStatus.preemption_info`.

```python
@dataclass(frozen=True)
class PreemptionInfo:
    deadline: float                 # UNIX ts the pod must exit by
    preempted_ranks: list[int]
    preempted_node_ids: list[str]
    @property
    def seconds_remaining(self) -> float: ...

def train_loop_per_worker(config):
    for step, batch in enumerate(train_iterator):
        info = ray.train.preemption_status()
        if info is not None:
            if ray.train.get_context().get_world_rank() in info.preempted_ranks:
                cleanup_local(); ray.train.report({"step": step})   # doomed: skip pointless I/O
            else:
                save_jit_checkpoint(model, step)                    # survivor: JIT checkpoint
            return
        train_step(model, batch)
```

- **Failure-domain-aware fan-out.** Ranks sharing a failure domain are flagged atomically — for TPU slices (fate-shared) via `RAY_NODE_TPU_SLICE_NAME_KEY`, otherwise per-node. A staggered drain never expands the preempted set or invalidates a survivor's JIT checkpoint.
- The watcher is a separate actor so the drain source can later be a pluggable developer API (K8s informer, cloud event queue), not just `get_draining_nodes()` — the seam Phase 4 builds on.

### Phase 2 — Graceful drain for survivors *(At)*

Introduce a **`PreemptingState`**, entered when any worker echoes `preemption_info != None`. Instead of immediate teardown, survivors get a **user-configurable drain window** to finish the current step and flush an in-flight checkpoint; the controller keeps the replacement request asserted so provisioning overlaps with cleanup. Exit when every rank has terminated **OR** `seconds_remaining <= 0`.

This requires resolving the **`ray.train.report()` barrier**: a doomed rank may be SIGKILL'd before reaching `report()`, stranding a valid survivor checkpoint at the consolidation barrier. The design doc evaluates three options (strict barrier + `report(None)` / relax in `PreemptingState` / synthesize empty reports) against a save-time × topology matrix — **the main open question for this phase.**

*Out of scope for v1:* full suspend-and-resume — blocked on resizable Placement Groups, and for TorchTrainer largely covered by the in-progress TorchFT integration, so `PreemptingState` is intentionally **not** layered on TorchFT initially.

### Phase 3 — Classify the failure *(After)*

Add a distinct **`PreemptionError(TrainingFailedError)`** (carrying `preempted_node_ids`, `preempted_ranks`, `deadline_exceeded`, per-rank `worker_failures`) and a new `FailureConfig.max_preemption_failures` (default `-1` = unlimited), consumed independently of `max_failures` (default `0`).

```python
FailureConfig(max_failures=3, max_preemption_failures=-1)   # fail fast on bugs, ride out spot churn
```

Classification at `PreemptingState` exit: any preemption-caused error → `PreemptionError`; other errors → `WorkerGroupError`; clean teardown → synthesized `PreemptionError`. Generalizes the existing Anyscale-side classifier to OSS. **Zero opt-in cost** — even a UDF that never calls `preemption_status()` now gets preemption tagged and retried under its own budget instead of burning `max_failures`.

### Phase 4 — Rich worker health via a pluggable probe

Generalize the watcher pattern into a user-extensible health signal. Three pieces:

**(a) A callback hook to launch a user-defined polling thread.** The user supplies probe callables (e.g. pynvml for GPU telemetry, a step-time monitor for stagger); the framework runs each on a daemon thread inside every worker, so probing never blocks the training thread.

**(b) The probe populates a structured `WorkerHealth`** — replacing the binary view with `HEALTHY | DEGRADED | DRAINING | UNHEALTHY` plus attribution.

**(c) It's echoed on the existing poll path** as a new `WorkerStatus` field — no new control channel; the controller already polls this every 2s.

```python
@dataclass(frozen=True)
class WorkerHealth:
    HEALTHY = "healthy"; DEGRADED = "degraded"; DRAINING = "draining"; UNHEALTHY = "unhealthy"
    status: str = HEALTHY
    reason: Optional[str] = None              # e.g. "gpu_thermal", "step_stagger"
    detail: dict = field(default_factory=dict)

# user probe — polled on a daemon thread inside each worker
def gpu_probe(ctx) -> WorkerHealth:
    import pynvml; pynvml.nvmlInit()
    h = pynvml.nvmlDeviceGetHandleByIndex(ctx.get_local_rank())
    temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)
    if temp >= 90:
        return WorkerHealth(WorkerHealth.DEGRADED, reason="gpu_thermal", detail={"temp_c": temp})
    return WorkerHealth(WorkerHealth.HEALTHY)

trainer = TorchTrainer(
    train_loop_per_worker,
    run_config=RunConfig(health_probes=[gpu_probe], health_probe_interval_s=10.0),
)

# WorkerStatus gains one field, echoed on the existing poll:
#   worker_health: WorkerHealth = WorkerHealth()
```

`PreemptionInfo` becomes one concrete producer of this signal (a probe that reads `get_draining_nodes()` → `DRAINING`). *Open:* thresholding/debounce on the worker (local, cheap) vs. the controller (cross-rank correlation).

### Phase 5 — Controller reactions to non-binary status

When the controller polls in `RunningState`, it reads `worker_health` from each `WorkerStatus` and consults a declarative **`HealthConfig`** to pick a transition — the same poll-driven loop that already detects errors, now with more than two outcomes:

```python
RunConfig(
    health_config=HealthConfig(
        # how gracefully to remove the bad rank — both quarantine the node on the way out
        on_degraded=HealthAction.CHECKPOINT_AND_EVICT,  # still works: checkpoint first, then swap the rank
        on_unhealthy=HealthAction.EVICT_NOW,            # can't trust it: drop it, resume from last checkpoint
        quarantine_nodes=True,   # add the offending node_id to a scheduling deny-list whenever we evict for health
    ),
)
```

| `WorkerHealth` | Action | Node quarantined? | State transition |
|----------------|--------|-------------------|------------------|
| `HEALTHY` | continue | — | stay `RunningState` |
| `DRAINING` | graceful drain (Phase 2) | yes (already leaving) | → `PreemptingState` |
| `DEGRADED` | checkpoint, then evict the rank | **yes** | → `Restarting` (spare-filled group) |
| `UNHEALTHY` | evict now, resume from last checkpoint | **yes** | → `Restarting`, node on deny-list |

Both `DEGRADED` and `UNHEALTHY` quarantine the node — a replacement should never land back on the same sick hardware. The difference is only *how gracefully we leave*: a `DEGRADED` worker still functions, so the controller can take a clean just-in-time checkpoint before evicting it; an `UNHEALTHY` worker may be unusable, so we drop it and fall back to the last checkpoint. The deny-list is plumbed into [scaling_policy](python/ray/train/v2/_internal/execution/scaling_policy/scaling_policy.py) + placement-group creation. This is where automated remediation lives: detect → checkpoint → quarantine → reschedule on healthy resources, no human in the loop.

### Phase 6 — Spare / standby workers

Let users launch with a pool of pre-provisioned **spare workers** beyond the active world size. When a node is quarantined or unschedulable (Phase 5), a warm spare swaps in immediately instead of stalling on cold re-provisioning.

```python
ScalingConfig(num_workers=8, num_spare_workers=2)   # 8 active + 2 warm standby
```

Composes with elastic `num_workers=(min,max)` but distinct: spares are *already provisioned and warm*, trading steady-state cost for near-zero replacement latency. *Open:* pool sizing/cost, whether spares pre-load weights/runtime env, and failure-domain placement (a spare must differ from the node it replaces).

## Design Principles

1. **Incremental** — each phase an independently mergeable PR.
2. **Backward compatible** — additions are opt-in; ignoring them changes nothing.
3. **Low signal latency** — single-digit seconds signal→worker, or the warning window is worthless.
4. **One signal path** — reuse the `poll_status` echo channel; no parallel control planes.
5. **Pluggable sources** — Ray Core today; pynvml / K8s informer / cloud events / custom probes later.
6. **Observability-first** — time-in-`PreemptingState`, JIT success rate, signal latency, quarantined-node counts.

## Open Questions

1. **Sequencing** — land Phases 1–3 ([PR #63475](https://github.com/ray-project/ray/pull/63475)) before designing 4–6?
2. **`WorkerHealth` schema** (Phase 4) — right enum + attribution shape; thresholding on worker vs. controller? Should probes be sync callables or long-lived objects with their own state?
3. **Reaction policy** (Phase 5) — is declarative `HealthConfig` (status → action) enough, or do we need a pluggable `HealthPolicy` like `FailurePolicy`? How does quarantine interact with the autoscaler and elastic resize?
4. **Spare workers** (Phase 6) — in scope for Ray Train, or push to the autoscaler / cluster layer? How are warm spares billed/accounted?
5. **TorchFT boundary** — confirm `PreemptingState` and health-driven eviction stay off the TorchFT path initially (per-step quorum already covers the in-quorum case).

## Related Work

- Prototype: [PR #63475](https://github.com/ray-project/ray/pull/63475) — preemption signal handling (Phases 1–3).
- TorchFT + Ray Train integration (in progress) — complementary in-quorum recovery.
- Existing reactive recovery this builds alongside: elastic `num_workers=(min,max)`, `DatasetCheckpointConfig` mid-epoch resume.

## Environment

- Ray version: master
- Component: Ray Train v2 (`python/ray/train/v2/`)
