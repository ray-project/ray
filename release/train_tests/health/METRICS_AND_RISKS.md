# Metrics and risks for the health work

Companion to the milestone plan. What we measure to claim the feature works,
and what is most likely to go wrong on the way.

## Outcome metrics — does the feature buy anything

Every one is measurable from `release/train_tests/health/02_injected_fault.py`,
which prints the first two directly. Take the baseline **before** changing
anything, on the same cluster, with the same injected fault.

| metric | today | target | how |
|---|---|---|---|
| **Time to detect** a NCCL hang | the collective timeout (`TORCH_NCCL_TIMEOUT`, 600–1800 s), or 600 s for the merged RAS detector's default confirm window | `stall_factor × median step time` — about 10 s for a 2 s-step job | injection log line → first decision |
| **Time to recover** | detect + teardown + reschedule + reload | detect + restart, excluding the bad node | injection → first completed step after restart |
| **Lost progress** | up to one checkpoint interval | unchanged by this work; call it out so nobody claims it | last checkpointed step vs resumed step |
| **Wasted GPU-hours per incident** | (detect + recover) × world size | the same formula with a much smaller first term | derived |
| **False-positive rate** | n/a (nothing fires) | **< 1 per 1,000 healthy run-hours** | count decisions on runs with nothing injected |
| **Attribution accuracy** | n/a (a hang names no node) | of injected hardware faults, the % where the named node is the injected one | inject on a known node, compare `target_nodes` |

Two of those deserve emphasis.

**Time to detect is the headline, and it is a ratio, not an absolute.** A
60× improvement on a 1,024-GPU run is ~10 GPU-hours per incident. With the
504-GPU study's rate of 17 incidents in 55 days, that is the number to put in
front of anyone asking whether this is worth doing.

**False positives are what kill adoption, and we have no data on ours.** The
same study measured 0.84 false positives per day from metric-based detection.
Our straggler and numerical thresholds (`slow_ratio=1.5`, `outlier_ratio=5.0`,
`pinned_windows=3`, `stall_factor=5.0`) are guesses. Nothing should default to
`fail` until this has been measured on healthy runs, and the measurement needs
real run-hours, not a test.

## Delivery metrics — is the plan on track

| metric | target | why it is the right one |
|---|---|---|
| **Breaking changes to `@DeveloperAPI` after the NVIDIA unblock date** | **0** | The entire point of the contracts milestone is to stop being a moving target for another team. One breaking change after they start costs them a rewrite and costs us the relationship. |
| **Contract deltas found by the NCCL RAS port** | found *before* the unblock, not after | The port is the only thing that tests the contracts from outside. It found four in the prototype (`entity`, `artifacts`, `scope=WORKER`, `ProbeDegraded`); the next one found after the announcement is a broken promise. |
| **Lines deleted from `callbacks/nccl_ras.py`** | > 300 | If porting a detector to the framework does not delete its bespoke plumbing, the abstraction did not earn itself. |
| **Adapter size** | an adapter is < 400 lines and imports only public contracts | Proxy for whether a third party can actually write one. |
| **Step-time regression with a representative policy set** | < 1 % | The REP's own acceptance criterion. Collection must never perturb training. |

## Risks

Ordered by how much they would cost, not how likely they are.

### 1. Contracts are declared stable before anything has tested them

The plan has the NCCL RAS port *after* the contracts milestone. That is the
wrong way round: the port is the only thing that exercises the contracts from
outside the framework, and in the prototype it forced four changes. Freezing
the API and *then* discovering a fifth means NVIDIA rewrites.

**Mitigation:** make the port a gate on the contracts milestone rather than the
milestone after it. The port already exists in the prototype, so this is a
review-and-merge cost, not a build cost.

### 2. "API/Contracts" is not enough to unblock anyone

Types alone let NVIDIA *write* a probe. They do not let NVIDIA *run* one. For
that, the minimum is `RunConfig.health_config`, `WorkerStatus.health`, and a
`HealthManager` the controller actually calls. None of the three exists.

**Mitigation:** either widen the contracts milestone to include that minimal
runnable path, or say plainly in the hand-off that they can compile against it
but not execute until the next milestone. The first is better and is perhaps
two days of work; the second at least avoids a surprise.

### 3. The `NodeMonitor` is a milestone hiding inside a bullet

"Collect: WorkerProbe/NodeProbe/ClusterProbe/OnDemandProbe" reads as one line
of work. `ClusterProbe` is controller-side and nearly free. `NodeProbe` needs a
per-node pinned actor with a lifecycle across worker-group restarts, a
background poll loop, and subprocess isolation with hard timeouts for
diagnostics. It is the single largest component in the REP and the one its
central claim rests on — the thing that still reports when a worker dies.

**Mitigation:** give it its own milestone. Note what does *not* depend on it:
NVSentinel is a `ClusterProbe`, so the NVSentinel case study can land without
it. What does depend on it: every `NodeProbe`, node-scoped diagnostics
(`nvidia-smi`), and all of pre-flight.

### 4. The NVSentinel milestone depends on two things we do not control

The Platform team's Kubernetes work, and a KubeRay pod-template change for the
Ray-node-id → Kubernetes-node-name mapping. Everything inbound rests on that
mapping, and a cross-repo change has its own review cycle.

**Mitigation:** open the KubeRay conversation in week one, not week four. Keep
NVSentinel's own kind-based fault-injection demo as the fallback — it runs on a
laptop with no GPU and no platform work, and proves the adapter end to end.

### 5. Every fixture is invented

`_parse_nvidia_smi` was written against text nobody has seen a GPU produce. The
RAS schema parser is inherited from merged code, but our probe's reduction of
it is not.

**Mitigation:** `01_nccl_ras.py` in week one. One real capture, healthy and
wedged, pinned as fixtures. Expect at least one key name to be wrong.

### 6. The interesting policies are not on the critical path

The straggler, SDC and numerical policies are the strongest argument for why
this belongs in Ray Train — and none of them is needed to unblock NVIDIA or to
port the RAS detector. They are the most likely thing to quietly consume the
contracts milestone.

**Mitigation:** keep them in the prototype as evidence, and schedule them after
the case studies. They validate the contracts for free by existing.

### 7. No GPU time is budgeted

The plan has no row for running on hardware, and two of the three milestones
cannot be called done without it.

**Mitigation:** a standing 2 × 4-GPU cluster for the duration, and an explicit
verification step in each case-study milestone.
