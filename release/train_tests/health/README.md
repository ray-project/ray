# Ray Train health: release tests

These scripts run the REP's Collect → Decide → Act loop on real GPUs, with NCCL
RAS as the case study. They answer three questions:

1. **Baseline.** What does today's merged NCCL RAS hang detector
   (`NCCLRASCallback`, #64928) do on a real hang, and what does it emit?
2. **Parity.** Does the same detection, written as a user policy on
   `ray.train.health`, catch the same hang just as fast?
3. **Why in Ray.** Does joining the probe with the training loop's own metrics
   do something the probe alone cannot?

`STATUS.md` has what is built, the REP component table, metrics and risks.

| file | what it is |
|---|---|
| `nccl_hang.py` | questions 1 and 2: one hang, both detectors, compared |
| `collective_join.py` | question 3: slow step vs hang, with and without the join |
| `local_e2e.py` | the whole loop on a laptop with toy probes, no GPU |
| `harness.py` | the injected faults and the `Timeline` that records what happened |
| `nccl_ras_health.py` | the NCCL RAS policy, written as a user would; imports only `ray.train.health` |

## What gets injected

Both faults are in `harness.py` and print their own description.

| fault | what it does | what RAS sees | right answer |
|---|---|---|---|
| `LeaveCollective(rank, at_step)` | the rank stops calling the collective and sleeps, staying alive | the communicator is MISMATCH, every rank RUNNING, op counts frozen | a hang: act |
| `SlowStep(rank, every, pause_s)` | the rank pauses before its collective every few steps, like a checkpoint save | the same, for `pause_s` | not a hang: stay silent |

`SlowStep` is the interesting one: for the length of the pause it is
indistinguishable from the start of a hang. Only the job knows its own step
time.

Next faults to add are gradual degradations rather than hangs (a rank getting
steadily slower, a GPU starting to throttle), to test evaluators that keep a
window of `HealthState` history instead of reacting to one poll.

## The `Timeline`

`harness.Timeline` is a controller callback. It records, with a timestamp,
every health decision and every error the controller acts on, from either
detector. Faults record when they fire, so each script prints something like:

```
  timeline (seconds after the fault fired):
       -8.0s  RUNNING      worker group started
       +0.0s  INJECT       rank 1 stops calling the collective at step 100 ...
      +21.9s  DIAGNOSE     1 of 1 NCCL communicators (...) made no progress for 20s; ...
      +24.1s  REATTEMPT    ...; diagnostics found no hardware fault, so this is software or data
      +24.1s  SHUTTINGDOWN HealthDecisionError: ...
```

## `nccl_hang.py`: baseline and parity

```bash
python release/train_tests/health/nccl_hang.py           # merged, then health
python release/train_tests/health/nccl_hang.py merged    # one of them
```

Same job, same `LeaveCollective(rank=1, at_step=100)`, same timing for both:
RAS polled every 2s, a hang confirmed after 20s with no progress.

| | merged `NCCLRASCallback` | `ray.train.health` |
|---|---|---|
| turned on by | `RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1` + env vars | `RunConfig(health_config=HealthConfig([nccl_ras_ready_policy(), nccl_ras_policy()]))` |
| before training | nothing | `nccl_ras_ready_policy()`: `NcclRasReadyProbe` on every GPU node |
| on a confirmed hang | writes `hang_detector/stack_traces` and `hang_detector/nccl_ras`, raises `NCCLHangError` | `DIAGNOSE`: stacks on the stalled ranks, `nvidia-smi` and the RAS text report on their nodes, under `health_diagnostics/` |
| then | the failure policy retries or ends the run | `REATTEMPT` if the GPUs are clean, `EVICT` if one is not; retries follow `FailureConfig` |
| error if it ends | `NCCLHangError` | `HealthDecisionError`, carrying the decision |

It prints both timelines, the artifact files each one wrote, and PASS if both
detected the hang within a few seconds of each other.

## `collective_join.py`: the join

```bash
python release/train_tests/health/collective_join.py              # all three
python release/train_tests/health/collective_join.py slow_step    # one
```

The job has real TP and PP subgroups over 4 ranks, plus one barrier on the
default group so NCCL creates the world communicator the probe uses to map
subgroup ranks to global ranks. It calls
`health.report({"tp_rank", "pp_rank", "step_time_s"}, step=...)`.
`CollectiveHangEvaluator` sets the stall threshold to 5 × the reported step
time (30s), names a frozen communicator by the ranks' reported coordinates, and
diagnoses on the nodes involved before deciding.

| scenario | fault | detector | pass |
|---|---|---|---|
| `slow_step` | `SlowStep`: 18s pause every 5 steps | our policy | no decision, although RAS reports frozen communicators on every pause |
| `slow_step_merged` | the same | merged, fixed 10s window | fires: the false positive the join avoids |
| `wedge` | `LeaveCollective` at step 6 | our policy | fires, and the decision names TP group `[0, 1]` (PP `[1, 3]` freezes too, waiting on the same rank) |

About 10 minutes for all three.

## On a laptop

`local_e2e.py` starts a 4-node Ray cluster locally and runs the real controller
through five scenarios with toy probes: no config, broken components,
pre-flight rejection, DIAGNOSE → REATTEMPT from a stalled `health.report()`,
and EVICT from a node-keyed `ClusterProbe`. About 100s. It needs Ray Core from a
wheel and `ray/train` from this checkout:

```bash
python -m venv ~/health-venv && source ~/health-venv/bin/activate
pip install "ray[train] @ https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-macosx_12_0_arm64.whl"
python python/ray/setup-dev.py -y --allow train
python release/train_tests/health/local_e2e.py
```

Run it from any directory except `/tmp`: Ray's `/tmp/ray` shadows the package.

## Unit tests

```bash
pytest python/ray/train/v2/tests/test_health*.py
```

`test_health_controller.py` and `test_health_node_exclusion.py` need a local
Ray; the rest need nothing.

## The GPU cluster

**Shape: 4 × 1-GPU nodes** (A10G, T4 or L4; nothing here depends on the
architecture). Four ranks is the floor for the TP × PP layout in
`collective_join.py`, and one rank per node puts every node-scoped diagnostic
on a different host. A CPU head node is fine.

**NCCL ≥ 2.28 on both sides.** `ncclras -f json` appeared in 2.28. The client
binary comes from the apt `libnccl-dev` package, not the pip wheel; the library
the training process loads comes from the torch wheel. `torch==2.11.0+cu128`
pins NCCL 2.28.9, so no `LD_PRELOAD` is needed. RAS is on by default since
2.24; nothing needs to be set. Check both:

```bash
ncclras --version 2>&1
python -c "import torch; print(torch.cuda.nccl.version())"
```

The pre-flight probe checks the same two versions on every node, so a node
with an old stack is evicted before training and the log says why.

**The image.** `anyscale/ray:nightly-py312-cu128` ships `ncclras` 2.25.1, and
an unpinned `pip install torch` pulls a CUDA 13 build. Verified on Anyscale:

```dockerfile
FROM anyscale/ray:nightly-py312-cu128

RUN pip uninstall -y ray && \
    pip install <your-ray-wheel-url> && \
    python -c "import ray"

RUN pip install "torch==2.11.0+cu128" \
      --index-url https://download.pytorch.org/whl/cu128

# The ncclras client is an 18 KB binary in libnccl-dev that links only libc.
# Extract it rather than going through apt: the Anyscale base has no NVIDIA
# apt repo, runs as a non-root user, and its builder's apt cache ignores
# added repos.
ARG NCCL_VER=2.28.9-1+cuda12.9
ARG NCCL_REPO=https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64
RUN cd /tmp && \
    wget -q "${NCCL_REPO}/libnccl-dev_${NCCL_VER}_amd64.deb" && \
    dpkg-deb --fsys-tarfile "libnccl-dev_${NCCL_VER}_amd64.deb" \
      | sudo tar -xf - -C / ./usr/bin/ncclras && \
    rm -f /tmp/libnccl-dev_*.deb

# Fail the build, not the run. `&&`, not `;`, so the check counts, and `2>&1`
# because `ncclras --version` writes to stderr.
RUN ncclras --version 2>&1 | grep -E '2\.(2[89]|3[0-9])\.' && \
    python -c "import torch; v=torch.cuda.nccl.version(); assert v[:2]>=(2,28), v"
```

Use `ubuntu2404` in `NCCL_REPO` if the base is 24.04.

**Your checkout on the cluster.** The image has a Ray wheel. Point its
`ray/train` at your checkout once, on the head node:

```bash
python python/ray/setup-dev.py -y --allow train
python -c "import ray.train, os; print(os.path.realpath(ray.train.__file__))"
```

The controller and driver then run your code. The scripts ship `harness.py`
and `nccl_ras_health.py` to the workers by value.

**Storage.** The scripts write to `/mnt/cluster_storage` when it exists, so
diagnostics written on any node can be listed from the driver. Without shared
storage each node keeps its own files.

**Stack dumps and ptrace.** `py-spy` has to attach to the training process.
With Ubuntu's default `ptrace_scope=1` and no `SYS_PTRACE` capability, stacks
fall back to Python-only, which loses the native frames a NCCL hang sits in.
The pre-flight probe reports this per node; it does not fail the node.
