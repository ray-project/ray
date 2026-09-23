# What is built, what is not, and what the GPU tests actually cover

## Probe kinds, and where NCCL RAS sits

`NcclRasProbe` is a **`ClusterProbe` with `entity = "communicator"`** — not a
`NodeProbe`, not a `WorkerProbe`.

A RAS report spans every rank in the mesh, is obtained by asking any *one*
rank, and is keyed by communicator. As a `WorkerProbe` it would spawn N
redundant `ncclras` subprocesses for identical data — and the hung rank is
precisely the one that may not answer. As a `NodeProbe`, the same redundancy,
and it is not node-keyed to begin with.

"Runs on the controller" describes the *orchestration*. The `ncclras` binary
itself still has to execute on a node hosting a rank, because RAS listens on
`127.0.0.1:28028` inside each NCCL process. The probe takes its transport as an
injected `query` callable: in production that is `RASPoller.next_result`, which
does `worker.execute_async(run_ncclras, ...)`; in `02_injected_fault.py` it is a
Ray task pinned to a GPU node.

| kind | where it runs | keyed by | implementations today |
|---|---|---|---|
| `WorkerProbe` | in each train worker, every poll | rank | **none** |
| `NodeProbe` | in the node's `NodeMonitor` | node | **none** — the `NodeMonitor` does not exist yet |
| `ClusterProbe` | controller, one read covers all | `entity` | `NVSentinelProbe` (node), `NcclRasProbe` (communicator) |
| `OnDemandProbe` | pushed; `scope` says where | entity | `StackTraceProbe` (worker), `NvidiaSmiProbe` (node), `RasTextReportProbe` (worker) |

## Built

All of this is unit-tested (107 tests) and **none of it is wired into the Train
runtime loop**. It is a library that nothing calls yet.

| area | module | what it does |
|---|---|---|
| contracts | `probe.py` | `ProbeResult` (incl. `artifacts`), the four probe kinds, `ProbeDegraded`, contexts |
| | `state.py` | `WorkerHealth`, `NodeHealth`, `HealthState` + typed reads |
| | `decision.py` | `Action`, `Cause`, `Noop`/`Reattempt`/`Evict`/`Diagnose`, `merge_decisions` |
| | `policy.py` | `Evaluator`, `HealthPolicy`, `HealthConfig` |
| Collect | `report.py` | the `health.report()` accumulator |
| Decide | `adapters/collective_join.py` | **RAS x UDF**: names the frozen communicator, sets the stall threshold in the job's own units |
| pre-flight | `preflight.py` | `PreflightRunner`, screens candidate nodes before the worker group starts |
| Decide | `manager.py` | `HealthManager`: ingest, run cluster probes, merge, lifecycle |
| Act | `diagnostics.py` | `DiagnosticRunner`: targeting, per-probe isolation, pause/resume |
| | `callbacks/health_callback.py` | node-exclusion label selector, manager lifecycle |
| adapters | `nvsentinel.py` | inbound probe + evaluator, outbound health-event builder |
| | `nccl_ras_policy.py` | the ported hang detector |
| | `nccl_ras_diagnostics.py` | the three on-demand checks from #64928 / #66229 |
| | `udf_signals.py` | straggler, SDC, numerical policies |
| injection | `testing/` | `nvrx`, `dcgm`, `nvsentinel`, `symptoms` |

## Not built

Everything that connects the library to a running job — REP phases 3–6 and 9:

- `WorkerStatus.health` — the field does not exist, so nothing a worker
  produces reaches the controller.
- `RunConfig.health_config` — no way for a user to turn any of this on.
- `ray.train.health` — `report()` exists but is not exported.
- `WorkerProbe` execution inside `RayTrainWorker.poll_status`.
- **The `NodeMonitor` actor** — entirely absent. This is why there are no
  `NodeProbe` implementations and why node-scoped diagnostics have nowhere to
  run. It is also the component the REP leans on hardest: the thing that keeps
  reporting when a worker hangs or dies.
- `HealthManager` instantiated in `TrainController`.
- Pre-flight invoked from the startup path. `PreflightRunner` exists and is
  tested, but nothing calls it before scheduling, and with no `NodeMonitor`
  there is nowhere for its node-scoped probes to run.
- `UserCallback.after_health_decision`.
- The controller acting on a decision — `REATTEMPT`, `EVICT`, `DIAGNOSE`.

## What the GPU tests cover

| script | exercises | does **not** exercise |
|---|---|---|
| `00_preflight.py` | image + cluster prerequisites | anything about the health loop |
| `01_nccl_ras.py` | that RAS emits parseable JSON under a real and a wedged job | any of our code |
| `02_injected_fault.py` (A) | the **merged** detector, #64928 — shipped code, not ours | our code entirely; this is the control |
| `02_injected_fault.py` (B) | **Collect + Decide**: `NcclRasProbe` parsing real reports, `NcclHangEvaluator` reaching a `HealthDecision` | **Act**, and the controller integration |

So part B answers *"does our probe understand real RAS output, and does our
evaluator reach the right conclusion from it"*. It drives the `HealthManager`
from a hand-rolled poll loop in the script, because the controller does not yet
call it.

What stays unproven until the wiring lands: that a decision reaches the
controller, that `DIAGNOSE` actually pushes probes at workers and nodes, and
that `EVICT` excludes a node on restart. The last of those is separately
verified on a 2-node CPU cluster in `test_health_node_exclusion.py`, which is
why a GPU adds nothing to it yet.
