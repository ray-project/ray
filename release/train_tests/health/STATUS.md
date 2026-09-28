# Status: what is built, what is proven, what is next

## The shape

The user brings probes and evaluators as a `HealthPolicy`. The
`HealthManager` turns what they collect into one `HealthDecision` per poll.
The controller decides what to do with it.

```
user code                         Ray Train
─────────                         ─────────
HealthPolicy(probes, evaluators) ─ RunConfig(health_config=) ─▶ TrainController
health.report({...}, step=)  ─ TrainContext ─ WorkerStatus.health ─┐   │ creates
                                                                    ▼   ▼
                               ClusterProbe thread ─────────▶ HealthCallback
                                                              (HealthManager)
                                                                    │ poll_decision()
                                                                    ▼
                                    TrainController._execute_health_decision
                                    ├─ after_health_decision → UserCallback
                                    ├─ DIAGNOSE  → HealthCallback.diagnose()
                                    ├─ REATTEMPT → FailurePolicy (max_failures)
                                    └─ EVICT     → restart via the scaling policy,
                                                   node excluded, no retry used
```

## Layout

```
python/ray/train/health/                 workload-agnostic; could become ray.health
  probe.py        ProbeResult, Probe | WorkerProbe | NodeProbe + NodeContext |
                  ClusterProbe + ClusterContext | OnDemandProbe + its context
  state.py        WorkerHealth, NodeHealth, HealthState
  decision.py     Action, Cause, HealthDecision, Noop/Reattempt/Evict/Diagnose
  policy.py       Evaluator, HealthPolicy, HealthConfig
  _internal/manager.py     HealthManager, merge_decisions
  _internal/on_demand.py   OnDemandRunner (DIAGNOSE and pre-flight dispatch)

python/ray/train/v2/                     Ray Train-specific
  _internal/callbacks/health_callback.py   HealthCallback: collection, pre-flight,
                                           diagnose, node exclusion
  _internal/execution/controller/          acts on decisions
  api/health.py                            report()
  api/exceptions.py                        HealthDecisionError
```

`ray.train.health` re-exports `report` and `HealthDecisionError`. End-user
entry points (`HealthConfig`, `HealthPolicy`, `report`, `HealthDecisionError`)
are `@PublicAPI(stability="alpha")`; the contract probe and evaluator authors
code against is `@DeveloperAPI`, like `UserCallback`. `test_health.py` checks
this.

## REP components

| area | component | status | where / what is missing |
|---|---|---|---|
| Collect | `ProbeResult`, `Probe` | done | `health/probe.py`; adds `artifacts` |
| | `WorkerProbe` | contract only | nothing runs it; `WorkerHealth.probe_results` is always empty |
| | `NodeProbe` | contract only | needs the `NodeMonitor` |
| | `OnDemandProbe` | partial | dispatch works; `stop_workers` only warns; `timeout_s` stops waiting but does not kill |
| | `health.report()` | done | `v2/api/health.py` → `TrainFnUtils.report_health` → `TrainContext` → `WorkerStatus.health` |
| | `NodeMonitor` | not done | node-scoped probes run in a task pinned to the node, which does not survive a node that cannot schedule |
| Decide | `Action`, `Cause`, decisions | done | `health/decision.py` |
| | `HealthManager`, merge rules | done | `health/_internal/manager.py`; suppression covers evicted nodes |
| | `HealthState` + typed reads | done | `health/state.py`; adds a `cluster` section |
| | `Evaluator`, `HealthPolicy`, `HealthConfig` | done | `health/policy.py`, `RunConfig.health_config` |
| Act | the controller acts on the decision | done | `TrainController._execute_health_decision` |
| | `REATTEMPT` through `FailurePolicy` | done | counts against `FailureConfig.max_failures` |
| | `EVICT` through the sizing path | partial | restart via the scaling policy, no retry used, node excluded by label selector; no structured evict event; elastic shrink untested |
| | `DIAGNOSE` | partial | runs and feeds results back; no worker pause; runs synchronously in the control loop |
| | `UserCallback.after_health_decision` | done | a `ControllerCallback` hook, forwarded by `UserCallbackHandler` |
| | `ray.drain_node` (REP milestone 2) | not done | Ray Core |
| Pre-flight | `HealthPolicy(preflight=True)` | done | run by the controller before scheduling; screens nodes alive at that moment |
| Tests | unit | done | `test_health.py`, `test_health_diagnostics.py`, `test_health_callback.py`, `test_health_controller.py` |
| | survives worker death, hung-probe kill | not done | need the `NodeMonitor` |
| | step-time regression < 1% | not measured | |
| Docs | user guide, API reference | not done | |
| Adapter | one real adapter on the public API | done | `nccl_ras_health.py` |

Additions to the REP, and why:

| addition | why |
|---|---|
| `ClusterProbe` and `HealthState.cluster` | some sources answer for the whole run from one query (NCCL RAS, keyed by communicator). As a `WorkerProbe` that is N identical queries, and the hung rank is the one that may not answer. |
| `ProbeResult.artifacts`, `OnDemandProbeContext.upload` | diagnostics produce files; the result carries the path, not the bytes |
| `OnDemandProbe.scope` | `py-spy` must attach to the training process, so some diagnostics run in the worker |

## What is proven

| test | proves |
|---|---|
| unit tests | contracts, merging, state, the manager, on-demand dispatch, pre-flight, callback collection, and how the controller maps each action |
| `local_e2e.py` (laptop, 4 local nodes) | through the real controller: no-config is untouched, broken components do not fail the run, pre-flight rejection, DIAGNOSE → REATTEMPT, EVICT and restart off the node |
| `nccl_hang.py` (4 × A10G) | not yet run in this form. Its predecessor showed the port detects a real hang, diagnoses it and ends in `HealthDecisionError`, about 10s behind the merged detector because it polled every 5s instead of 2s |
| `collective_join.py` (4 × A10G) | not yet run in this form. Its predecessor passed the slow-step scenario, silent through every pause |

## Metrics

Measured with the scripts above, on the same cluster and fault, baseline first.

| metric | today | target |
|---|---|---|
| time to detect a hang | the merged detector's confirm window (600s default), or the collective timeout without it | the same window for the port; `stall_factor × step time` with the join |
| false positives on slow steps | merged: fires once a pause outlasts its window | the join: none while a pause is under the job's threshold |
| attribution | the merged detector names no node | the node of a faulty GPU, from DIAGNOSE |
| false-positive rate on healthy runs | n/a | < 1 per 1,000 run-hours, needs real run-hours to measure |
| step-time overhead | n/a | < 1%, the REP's acceptance bar |

## Risks

1. **The `NodeMonitor` is the largest piece and the REP's central claim.** It
   is what keeps reporting when a worker dies. Every `NodeProbe` and the
   worker-death test wait on it. Give it its own milestone.
2. **Contracts frozen before an outside user tests them.** The NCCL RAS port
   is the only outside user so far, and it changed the contracts three times
   (`ClusterProbe` with `HealthState.cluster`, `artifacts`, `scope`). Port the
   merged callback before declaring the contracts stable.
3. **Thresholds are guesses.** `stall_factor=5` and the confirm windows have no
   false-positive data behind them. Nothing should default to acting until
   healthy run-hours are measured.
4. **A cascade can hide the culprit.** When one rank wedges, groups that wait
   on it freeze too, so a decision can name several communicators. The probe
   has what is needed to name the one lagging rank; the evaluators do not yet.
5. **Blocking work in the control loop.** DIAGNOSE and pre-flight wait for
   their probes inside the controller's step, up to each probe's timeout.
6. **NVSentinel depends on work outside Ray Train**: the platform team's
   Kubernetes setup and a KubeRay change mapping Ray node ids to Kubernetes
   node names.

## Next

1. Run `nccl_hang.py` and `collective_join.py` on the 4 × A10G cluster.
2. Port the merged `NCCLRASCallback` onto `ray.train.health`, with its parser
   tests and real captures, under `python/ray/train/v2/tests/`.
3. Degradation faults and windowed evaluators: inject a rank that gets steadily
   slower, and detect it from a window of `HealthState` history rather than
   one poll.
4. Name the lagging rank in hang decisions.
5. The `NodeMonitor`, then `stop_workers`.
6. Bazel targets for `test_health*.py`.
