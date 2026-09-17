# Ray Train Monitoring & Diagnostics — implementation plan (NVSentinel first)

REP: [`2026-08-11-ray-train-monitoring-and-diagnostics`](https://github.com/ray-project/enhancements/tree/main/reps/2026-08-11-ray-train-monitoring-and-diagnostics)

This plan lands the REP's **Collect → Decide → Act** loop, with NVSentinel as the
first real signal-source adapter (REP acceptance criterion #4).

## Why NVSentinel is the right first adapter

NVSentinel owns everything *below* the node — DCGM, syslog, NVSwitch, NIC, CSP
maintenance — and it already publishes its verdict through standard Kubernetes
primitives, plus it owns repair (GPU reset, reboot, replace). Ray Train owns
everything *above* the node: collective liveness, step progress, numerics.

Neither side alone can localize a silent hang. NVSentinel sees a NIC flap but
not that the job stalled. Ray Train sees a stalled collective but not which of
128 hosts is the culprit. The REP's mock case study *is* this join.

It also exercises both directions of the loop, which is what proves the
contracts are sufficient from outside the framework:

| Direction | Mechanism | Buys us |
|---|---|---|
| Inbound (Collect) | Node conditions / taints / cordon on the `Node` object | Hardware faults in seconds, already localized to a host, with no NVIDIA-side work |
| Outbound (Act) | `PlatformConnector.HealthEventOccurredV1` gRPC | `EVICT` becomes real remediation — cordon, repair — **without** waiting for Ray Core's `drain_node` |

## Three findings that change the shape of the work

### 1. `EVICT` needs no Ray Core change

`bundle_label_selector={"ray.io/node-id": "!in(<id>,...)"}` keeps a placement
group off named nodes. Verified against a live 2-node cluster in
`test_health_node_exclusion.py`. The controller already asks callbacks for a
label selector in `_start_worker_group`, so eviction plugs into an existing
seam. The REP's Milestone 2 (`ray.drain_node`) becomes an optimization, not a
prerequisite.

One bug to fix on the way: `controller.py::_start_worker_group` *overwrites*
the label selector per callback rather than merging, so the last callback to
return one silently wins.

### 2. Out-of-band sources need one new probe kind — `ClusterProbe`

The REP's `NodeProbe` runs in each node's `NodeMonitor`. NVSentinel's data is
published in one place (the Kubernetes API). Running it as a `NodeProbe` would:

- fan one logical read into N API calls (1,024 nodes × 10 s ≈ 100 QPS at the
  apiserver, for data that one list call already returns), and
- go blind on exactly the nodes that died — the case that matters most.

So this plan adds exactly one probe kind: a `ClusterProbe` that runs
controller-side and returns `{entity_id: ProbeResult}`.

**What separates probe kinds is *where* a probe runs and *when*** — that decides
lifecycle, failure domain, and whether it survives the thing it watches. What
its results are keyed by is not a kind, it is a field. A `ClusterProbe`
declares `entity`, and the implementation converts whatever it read into the
same `ProbeResult` shape: NVSentinel keys by node, NCCL RAS keys by
communicator, through the same class.

`entity = "node"` gets one piece of special treatment: results merge into
`HealthState.nodes` alongside every `NodeProbe` sample for the same host, so an
evaluator can join a vendor control plane against an on-host probe with one
lookup. Every other value is opaque to the framework — only the evaluator
reading that probe knows what its keys mean. Evaluators read both the same way:
`state.results(SomeProbe) -> {entity_id: ProbeResult}`.

### 3. The NCCL RAS port is the contract test, and it forced four deltas

`callbacks/nccl_ras.py` (924 lines, default-off) is already Collect -> Decide -> Act built
ad-hoc. Porting it is the cheapest honest test of the contracts: a detector that already
works and whose behavior must not change. Prototyped in
`adapters/nccl_ras_policy.py` + `tests/test_health_nccl_ras_port.py`.

Fits with no contract change: the poller becomes the probe transport (`ncclras` handling,
JSON-schema repair and the op-count diff imported verbatim); the per-communicator
frozen-streak logic becomes an evaluator holding its own history; observe-vs-fail becomes a
constructor argument instead of an env var read inside a callback.

What it broke, all now in the prototype:

| Delta | Why |
|---|---|
| `ClusterProbe.entity` | A RAS report is keyed by *communicator*: it spans every rank, is obtained from any one rank, and decomposes neither per node nor per worker without losing the cross-rank skew that is the whole signal. This needs no new probe kind — the transport is identical to NVSentinel's, so only the key differs. Keying by communicator also turned out to be the *better* port: each communicator gets its own `ProbeResult` with its own verdict, instead of several crammed into one result's `devices` map. |
| `ProbeResult.artifacts` | On a confirmed hang the detector fans `py-spy` out to every rank and uploads `rank_N.log` files. Nothing in `ProbeResult` could say "I produced files, here." |
| `OnDemandProbe.scope = WORKER` | `py-spy` must attach to the training process; the REP's on-demand probe runs in the `NodeMonitor`. Answers the REP's own open question on diagnostic granularity with a yes. A field, not a class: the push mechanism is identical either way. |
| `ProbeDegraded` | A missing `ncclras` binary is not transient and the detector latches itself off. The manager disables raising *evaluators*; probes need the same, distinguished from a retryable failure. |

Two payoffs:

1. **Attribution.** Today a confirmed hang raises `NCCLHangError` (a `WorkerGroupError`), so
   the run retries onto the same nodes -- where a real NIC fault hangs again. In the port,
   RAS names the stalled ranks, `state.node_of(rank)` names their hosts, and NVSentinel's
   result for those hosts says whether the hardware is at fault: found -> `Evict(HARDWARE)`,
   not found -> `Reattempt(NO_PROGRESS)`. A hang alone must stay `NO_PROGRESS`, or a bug in
   user code could cordon healthy hardware.
2. **Wall-clock confirmation.** The callback converts a confirm *duration* to a poll count,
   assuming each drained report is one poll interval apart. True at the defaults (controller
   2s, RAS 15s), but it inverts if the RAS interval is set below the health-check interval,
   and the detector then fires late by a drift that grows all run.

One rule fell out worth writing into the contracts: **a probe that runs on the controller may
hold sample-to-sample state; a probe inside the failure domain may not.** That is what lets
the RAS probe keep the previous report and emit a delta -- four floats per communicator
instead of 30k raw counters at 1,024 ranks -- while a `WorkerProbe`'s history, which would
die with the rank it watches, stays forbidden.

## The integration blocker to settle first: node identity

Ray node id → Kubernetes node name. A Ray worker pod's hostname is the *pod*
name and its IP is the *pod* IP; neither is the Kubernetes node name unless the
pod runs with `hostNetwork`. Everything in the inbound path depends on this
mapping being right — and being wrong means attributing a hardware fault to the
wrong host.

Resolution order implemented in `adapters/nvsentinel.py::resolve_node_names`:

1. Ray node label `ray.io/k8s-node-name` (preferred — set once from the
   downward API `fieldRef: spec.nodeName` in the KubeRay pod template).
2. `kubernetes.io/hostname`, for hostNetwork deployments.
3. No match → **report no evidence**, never guess.

**Action item:** agree the KubeRay pod-template snippet with the KubeRay
maintainers early; it gates the whole inbound path.

## Coordination contract: who evacuates the node

Ray Train and NVSentinel must not both evacuate the same host.

- **Ray Train-initiated.** Ray Train moves its own workers off (restart
  excluding the node), then emits a health event with
  `quarantineOverrides.force=true` (cordon it, nothing else should land) and
  `drainOverrides.skip=true` (don't race my restart). NVSentinel proceeds to
  remediation once our pods are gone.
- **NVSentinel-initiated.** The drain has a grace period (per-namespace
  eviction mode: `immediate` / `allow-completion` / `delete-after-timeout`).
  Ray Train must checkpoint and restart inside it. This maps exactly onto the
  existing preemption path — see Phase 1.

## Phases

Each phase is one reviewable PR. Phases 2–6 are the framework; 1, 7, 8 are
NVSentinel. Phase 1 is deliberately out of order: it ships user-visible value
before the framework lands.

### Phase 0 — Prototype ✅ (branch `train-health-nvsentinel`)

Vertical slice, no runtime path touched. 47 tests passing.

- `execution/health/{probe,state,decision,policy,manager}.py` — the contracts.
- `execution/health/adapters/nvsentinel.py` — inbound probe + evaluator,
  outbound event builder, node identity resolution.
- `execution/health/adapters/nccl_ras_policy.py` — the ported hang detector.
- `callbacks/health_callback.py` — controller wiring + node-exclusion selector.
- `tests/test_health.py`, `tests/test_health_nccl_ras_port.py`,
  `tests/test_health_node_exclusion.py`.

### Phase 1 — NVSentinel drain as an advance signal (quick win, small)

**Independent of everything else. Ship first.**

Today an NVSentinel-initiated node repair kills our pods mid-step and costs up
to a full checkpoint interval. But Ray Train already has a machine for
"this node is going away, you have N seconds": `PreemptionWatcher` →
`mark_preempt` → `PreemptingState` → emergency checkpoint → restart, charged
to the preemption retry budget rather than the user's bug budget.

An `NVSentinelWatcher` actor — same shape as `PreemptionWatcher`, ~150 lines —
watches node conditions and cordons for the run's nodes and calls the existing
`mark_preempt` with a deadline derived from the drain grace period. **No
controller change.**

*Exit:* progress loss on an NVSentinel node repair drops from a checkpoint
interval to near zero.

### Phase 2 — Contracts (no behavior)

`ProbeResult`, the four probe kinds (`WorkerProbe`, `NodeProbe`, `ClusterProbe`,
`OnDemandProbe`), `WorkerHealth`/`NodeHealth`/`HealthState`,
`Action`/`Cause`/`HealthDecision` + merge, `HealthPolicy`/`HealthConfig`,
`RunConfig.health_config = None`. Carries all four deltas the RAS port forced —
cheap now, breaking changes later.

Pure dataclasses plus the merge algorithm and its tests. Nothing on the runtime
path. Ships as `@DeveloperAPI` / alpha per the REP.

### Phase 3 — Collect: worker path

`WorkerStatus.health` (additive), `ray.train.health.report()`, `WorkerProbe`
polling inside `RayTrainWorker.poll_status`, `HealthManager.ingest_worker_health`.
Rides the `poll_status` call the controller already makes — no new RPC.

*Exit:* a `health.report()` from the UDF appears in a controller-side
`HealthState`. Step-time regression within noise.

### Phase 4 — Collect: node path

`NodeMonitor` actor (`num_cpus=0`, pinned) + lifecycle callback modeled on
`PreemptionCallback`, node-probe loop, `poll_health()` ride-along.

*Exit:* `SIGKILL` a worker; node health still reports.

### Phase 5 — Decide

`HealthManager` in the controller, evaluators, merge-by-severity, suppression
of in-flight faults, `UserCallback.after_health_decision`. Wire `NOOP` and
`REATTEMPT` only (reuse `FailurePolicy`).

*Exit:* an injected fault produces a decision and a retry; a raising evaluator
is disabled, not fatal.

### Phase 6 — Act: `EVICT`

Node exclusion via label selector (+ the merge fix above), structured evict
event, hand-off to `ResizeDecision` so eviction composes with elastic sizing
instead of duplicating it.

*Exit:* the group comes back without the target node — fixed world size and
elastic.

### Phase 7 — NVSentinel inbound adapter

`NVSentinelProbe` + `NVSentinelEvaluator` + `nvsentinel_policy()`, RBAC docs,
the KubeRay pod-template snippet.

*Exit:* NVSentinel's own fault-injection demo (`demos/local-fault-injection-demo`,
kind-based, no GPUs needed) drives a Ray Train run to a correct `Evict`.

### Phase 8 — NVSentinel outbound

`build_health_event` + gRPC sink. **Shadow first:** `processingStrategy=STORE_ONLY`
means NVSentinel records Ray Train's verdicts and nothing in the cluster
changes. Flip to `EXECUTE_REMEDIATION` once the decisions are trusted.

*Exit:* a Ray Train-detected NCCL hang shows up as an NVSentinel health event
against the right node.

### Phase 9 — Act: `DIAGNOSE`

On-demand probe push, subprocess isolation with a hard timeout, `stop_workers`
pause/resume, result fed back into the next `HealthState`.

### Phase 10 — NCCL RAS as a `HealthPolicy` (move ahead of 6-8)

Prototyped. It earns its place right after Phase 5 rather than at the end: the
port needs only the contracts and Decide, and it is the thing that tells us
whether those are right *before* four more phases are built on them.

Land at behavior parity behind the existing
`RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR` flag, run both detectors side by side on a
release test, then delete the callback. The attribution half — `Evict` instead
of `Reattempt` — switches on for free once Phase 7 lands.

*Exit:* same detections, same timings as the callback on the same fault
injection; then the callback goes.

### Phase 11 — Pre-flight, docs, release test

`HealthPolicy(preflight=True)` gate before training starts; user guide + API
reference + runnable example; the silent-NIC-hang release test from the REP.

## Open REP questions, answered for this plan

| REP question | Answer here |
|---|---|
| Diagnostic granularity | Node-level suffices for NVSentinel (its data is node-keyed). Per-worker matters for the NCCL proof — Phase 9. |
| Cross-probe aggregation | `HealthManager.ingest_node_health` merges by probe name, so a `NodeMonitor` sample and a node-keyed cluster probe never clobber each other. Probes whose entity is not a node are held separately and never invent a host. |
| Where judgment lives | **Option 2, evaluator-centric.** NVSentinel forces it: the join (condition + no-progress) belongs to neither probe. |
| Metrics history | On the evaluator, per the REP. `NVSentinelEvaluator`'s confirm-streak map is the concrete instance. |

## Risks

| Risk | Mitigation |
|---|---|
| Node identity mapping wrong → fault attributed to the wrong host | Resolution order with a hard fail-closed: no match ⇒ no evidence. Settle the KubeRay snippet in Phase 1. |
| RBAC: cluster-scoped `get`/`list` on `nodes` | One ClusterRole; documented. Cluster probe means only the controller needs it, not every worker pod. |
| Double evacuation (Ray Train + node-drainer racing) | `drainOverrides.skip` on emitted events; Phase 1 handles the reverse direction. |
| Operators cordon nodes for unrelated reasons | `evict_on_cordon` is separable from the hardware path, and a bare cordon is `INFRASTRUCTURE`, never `HARDWARE`. |
| Eviction under gang scheduling may not be grantable | Per the REP, `EVICT` is a request: name the node, emit the event, let the cluster manager escalate. |
| A detector bug takes down training | Evaluators are sandboxed and disabled on raise; cluster probe failures keep the previous snapshot. Tested. |

## Contribution notes

Per `AGENTS.md`: no open PRs cover this (checked `ray-project/ray` open Train
PRs). Each phase is substantive, human-reviewed, tested locally, and its
description must state the duplicate-work check, the test commands and results,
and that AI assistance was used. DCO sign-off (`git commit -s`) on every commit.
