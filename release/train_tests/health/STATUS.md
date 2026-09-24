# What is built, what is wired, and what the GPU tests prove

## The shape

The user brings a probe, wraps it in a `HealthPolicy`, and passes it to the run.
Ray Train owns the loop.

```
user code (nccl_ras_health.py)            Ray Train (python/ray/train/health/)
─────────────────────────────             ────────────────────────────────────
NcclRasProbe(ClusterProbe)    ─┐
NcclRasReadyProbe(OnDemand)    ├ HealthPolicy ─ HealthConfig ─ RunConfig(health_config=)
NcclHangEvaluator              ┘                                    │
                                                                    ▼
health.report({...}) ─ TrainContext ─ WorkerStatus.health ─▶ HealthManager
                                                          (probes, evaluators,
                                                           merge → HealthDecision)
                                                                    │
            UserCallback.after_health_decision ◀────────────────────┤
            pre-flight / DIAGNOSE → OnDemandRunner ◀────────────────┤
            REATTEMPT / EVICT → HealthDecisionError → FailurePolicy → restart,
                                evicted nodes excluded by label selector
```

## Layout

```
python/ray/train/health/
  __init__.py      public exports
  probe.py         ProbeResult, Probe | WorkerProbe | NodeProbe + NodeContext |
                   ClusterProbe + ClusterContext | OnDemandProbe + its context
  state.py         WorkerHealth, NodeHealth, HealthState
  decision.py      Action, Cause, HealthDecision, Noop/Reattempt/Evict/Diagnose
  policy.py        Evaluator, HealthPolicy, HealthConfig
  report.py        report()
  exceptions.py    HealthDecisionError
  _internal/
    manager.py     HealthManager, merge_decisions
    on_demand.py   OnDemandRunner (DIAGNOSE and pre-flight dispatch)
    callback.py    HealthCallback (controller integration)
```

Outside that package, only the hooks it needs: `RunConfig.health_config`,
`UserCallback.after_health_decision`, `WorkerStatus.health`,
`TrainContext.report_health` + `TrainFnUtils.report_health`, and the
`HealthCallback` registration in `DataParallelTrainer`.

## Where this differs from the REP

| addition | why |
|---|---|
| `ClusterProbe` with `entity` | NCCL RAS answers for the whole mesh from one query, keyed by communicator. As a `WorkerProbe` it would be N identical queries, and the hung rank is the one that may not answer. |
| `ProbeResult.artifacts`, `OnDemandProbeContext.upload` | diagnostics produce files (stack dumps, `nvidia-smi -q`); the result carries the path, not the bytes |
| `OnDemandProbe.scope` | `py-spy` must attach to the training process, so some diagnostics run in the worker rather than on the node |

## Wired into the runtime

| step | where | what happens |
|---|---|---|
| enable | `RunConfig.health_config` → `DataParallelTrainer` | registers `HealthCallback` when any policy is set |
| pre-flight | `HealthCallback.on_controller_start_worker_group` | before each worker group is scheduled, the `OnDemandProbe`s of `preflight=True` policies run on every alive node that fits a worker and has not been screened yet; a failed check or an `Evict` from that policy's evaluators evicts the node |
| collect: UDF | `health.report()` → `TrainFnUtils.report_health` → `TrainContext` → `WorkerStatus.health` | same path shape as `ray.train.report`; rides the existing status poll, no new RPC, not a barrier |
| collect: cluster | background thread in the callback | polls each `ClusterProbe` on its `interval_s`, off the event loop; a failing probe is logged once and retried |
| decide | `HealthManager.poll_decision()` | builds `HealthState`, runs every evaluator, merges one decision |
| act: hook | `UserCallback.after_health_decision` | fires before any action, including pre-flight evictions |
| act: diagnose | `OnDemandRunner.diagnose` | worker-scoped probes via `worker.execute_async`, node-scoped via a pinned task, all targets in parallel; output under `health_diagnostics/`; results land in the next `HealthState` |
| act: reattempt / evict | `HealthDecisionError(WorkerGroupError)` | same path as a worker error, so `FailureConfig` applies |
| act: evict | `on_controller_start_worker_group` | next worker group gets `ray.io/node-id: !in(...)`; verified on a 2-node CPU cluster |

## Not wired

- `WorkerProbe` execution inside `poll_status`.
- The `NodeMonitor`. Node-scoped on-demand probes and pre-flight run in a task
  pinned to the node, which does not survive a node that cannot schedule work.
  No `NodeProbe` can run.
- `stop_workers` pausing.
- Pre-flight only screens nodes that exist when the worker group is scheduled.
  If rejections leave too few nodes, the run waits for capacity like any other
  unschedulable run; starting on spares is out of scope in the REP.
- `EVICT` excludes the node on restart; it does not resize.

## What the GPU tests prove

| script | proves | does not prove |
|---|---|---|
| `01_nccl_ras.py` | RAS emits parseable JSON under a real and a wedged job | any of our code |
| `02_injected_fault.py` (A) | the merged detector fires — the control | our code |
| `02_injected_fault.py --ported` (B) | pre-flight runs on the GPU nodes; a user policy passed through `RunConfig` detects the hang; `DIAGNOSE` pushes diagnostics at the right ranks and nodes; `REATTEMPT` fails the run through the retry path | `EVICT` on real hardware |
| `03_collective_join.py` | the UDF join keeps a slow step from being called a hang, still fires on a real wedge, and names the group; the control shows a fixed window does not | anything beyond one TP/PP layout |

An idle subgroup, such as a PP group between its all-reduces, never looks
frozen to RAS: a communicator is only mismatched when its ranks' op counts
differ. The false positive 03 reproduces is a rank that is late to a
collective its peers are already in.
