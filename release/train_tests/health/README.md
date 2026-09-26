# Ray Train health loop — the NCCL RAS case study

The REP's Collect → Decide → Act loop, exercised the way a user would use it:
the user brings a probe, wraps it in a `HealthPolicy`, and passes it to the
training run. Ray Train's `HealthManager` runs the probes and evaluators inside
the controller and acts on the `HealthDecision` they produce.

```python
import ray.train.health as health
from ray.train import RunConfig
from nccl_ras_health import nccl_ras_policy   # user code, lives here

trainer = TorchTrainer(
    train_func,
    run_config=RunConfig(
        health_config=health.HealthConfig(policies=[nccl_ras_policy()]),
    ),
)

# inside train_func, optional:
health.report({"step_time_s": dt, "tp_rank": tp}, step=step)
```

Two halves, deliberately separated:

| | where | what |
|---|---|---|
| **framework** | `python/ray/train/health/` | public contracts (probes, `HealthState`, `HealthDecision`, `Evaluator`, `HealthPolicy`, `HealthConfig`, `report()`), and under `_internal/` the `HealthManager`, `OnDemandRunner` and controller callback |
| **user code** | `release/train_tests/health/nccl_ras_health.py` | `NcclRasProbe`, the `NcclRasReadyProbe` pre-flight check, `NcclHangEvaluator`, `CollectiveHangEvaluator`, the #66229 diagnostics, and the two policy factories |

`nccl_ras_health.py` imports only `ray.train.health`, which is the test that
the public contracts are enough to write a real policy from outside Ray Train.

## Getting your changes onto the cluster

The image installs a Ray wheel, so `import ray.train` resolves to the wheel,
not your checkout. Symlink the installed `ray/train` to your working tree once:

```bash
git pull && git checkout <your-branch>
python python/ray/setup-dev.py -y --allow train
```

Every edit under `python/ray/train/` then takes effect on the next
`ray.init()`. Verify it took:

```bash
python -c "import ray.train, os; print(os.path.realpath(ray.train.__file__))"
```

> The symlink exists only on the head node. That covers the controller and the
> driver. Code that must run *inside a training worker* on another node needs a
> new wheel or `runtime_env={"py_modules": [...]}`. `nccl_ras_health.py` avoids
> this: the scripts ship it by value with
> `ray.cloudpickle.register_pickle_by_value`.

## Unit tests (no GPU)

```bash
pytest python/ray/train/v2/tests/test_health*.py           # framework
pytest release/train_tests/health/test_nccl_ras_health.py  # user-side policy
```

The second one parses real captures from an A10G cluster in `data/`.

## The GPU steps

Run them in order. Each one fails fast if the previous was not really passing.

```bash
python release/train_tests/health/01_nccl_ras.py         # is RAS producing data?
python release/train_tests/health/02_injected_fault.py   # does anything react?
python release/train_tests/health/03_collective_join.py  # does it react for the right reason?
```

### Pre-flight

There is no separate pre-flight script. Both policies set `preflight=True`, so
before the first worker group is scheduled on a node, the controller runs
`NcclRasReadyProbe` there: `ncclras` ≥ 2.28, in-process NCCL ≥ 2.28, and
whether `py-spy` can capture native stacks (reported, not failed). A failing
node is evicted, which you see as an `EVICT` from `after_health_decision` and a
`ray.io/node-id: !in(...)` selector on the worker group. A passing run logs
`[Health] pre-flight passed on N node(s)`.

### 01_nccl_ras.py

Starts a real all-reduce job and queries `ncclras` on every GPU node while it
runs, saving `ncclras -f json`, `ncclras -f text` and `nvidia-smi -q` per node.

```bash
python release/train_tests/health/01_nccl_ras.py            # healthy job
python release/train_tests/health/01_nccl_ras.py --hang     # one rank wedged
```

Keep the `--hang` output: that is the input the probe actually parses, and it
is what the fixtures in `data/` came from.

A bare `ncclras` on an idle node always fails with *"Connection refused …
Failed to connect to the NCCL RAS service"*. That is correct: RAS is not a
daemon. Its threads live inside the NCCL processes, so the service exists only
while a job runs, and only on nodes hosting a rank.

### 02_injected_fault.py

One rank stops calling the collective while staying alive, so its op count
falls a step behind and every peer blocks inside the all-reduce.

```bash
python release/train_tests/health/02_injected_fault.py            # A
python release/train_tests/health/02_injected_fault.py --ported   # B
```

**A** is the merged detector (#64928), no new code. It should raise
`NCCLHangError`. Run it first — if it does not fire, the cluster is the
problem, not this change.

**B** is the same detection brought as a user policy through
`RunConfig(health_config=...)`. Expect, in the controller log, the pre-flight
line, then a `DIAGNOSE` (stack dumps, `nvidia-smi`, the RAS text report, pushed
at the stalled ranks and their nodes, with output paths under
`health_diagnostics/`), then a `REATTEMPT` that fails the run with
`HealthDecisionError`. The script's
`UserCallback.after_health_decision` prints each decision as it happens.

Pass criterion for the port: B confirms at the same time A does. Detection
latency is the confirm window in both; the port changes what happens *after*
detection, not how fast it is.

Knobs: `--workers`, `--hang-rank` (keep it non-zero), `--hang-step`,
`--poll-s`, `--confirm-s`.

### 03_collective_join.py

Why the UDF signal belongs in the same loop as the probe. RAS flags a
communicator that is mismatched and not advancing. A hang looks like that — and
so does a *legitimately slow step*: one rank pauses before the collective (a
checkpoint save, a GC pause, a slow shard) and its peers wait inside the
all-reduce until it arrives. A fixed confirm window is right for one job's step
time and wrong for another's.

The job builds real TP and PP subgroups over 4 ranks and reports its layout
and normal step time with `health.report()`. `CollectiveHangEvaluator` sets the
stall threshold to `stall_factor × median step_time_s`, and names the frozen
communicator by the ranks' reported coordinates.

```bash
python release/train_tests/health/03_collective_join.py --no-hang                   # phase 1
python release/train_tests/health/03_collective_join.py                             # phases 1 + 2
python release/train_tests/health/03_collective_join.py --no-hang --merged-control  # control
```

- **phase 1** — rank 1 pauses 18s before its TP all-reduce every 5 steps.
  Threshold is 5 × 6s = 30s, so the policy must stay silent. The script counts
  frozen-communicator observations and reports *INCONCLUSIVE* rather than PASS
  if it never saw one, so silence cannot pass vacuously.
- **phase 2** — rank 1 leaves its TP all-reduce for good. Must fire, and the
  decision must name the TP group.
- **control** — the same phase 1 under the merged detector with a fixed window
  shorter than the pause. It should fire: that is the false positive the join
  avoids.

Needs an even worker count (tp=2) and at least 4 ranks.

## Reference

- `STATUS.md` — what is built, what is wired, what each script proves.
- `CLUSTER_SETUP.md` — cluster shape, GPU types, the Dockerfile.
- `METRICS_AND_RISKS.md` — milestone metrics and risks.
