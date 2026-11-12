# Ray Train health: GPU tests

Scripts that run the Ray Train health stack (`ray.train.health`, #67671 to
this PR) on a real GPU cluster, with the faults injected on purpose. They print
a timeline of what the controller decided and PASS or FAIL.

| file | what it is |
|---|---|
| `gpu_faults.py` | GPU faults mocked as GPU health events: a pre-flight all-reduce, a slow and hot node diagnosed and reattempted, a fatal XID evicted |
| `nccl_hang.py` | one NCCL hang, caught by the merged `NCCLRASCallback` and by the same detection on `ray.train.health`, compared; and a slow step that must not fire |
| `gr00t_recovery.py` | GR00T 2's straggler detection and recovery: stalls, stragglers and a NaN loss, diagnosed, then reattempted or evicted, resuming from a checkpoint |
| `progress_health.py` | the progress checks GR00T would run: phase-aware stall detection, and straggler scores diagnosed before evicting |
| `gpu_health.py` | the GPU probes and evaluators, and the mocked GPU health events |
| `nccl_ras_health.py` | NCCL RAS hang detection on `ray.train.health`, from the 7/n probes |
| `harness.py` | the injected faults, and the `Timeline` that records what the controller did |
| `check_cluster.py` | checks the running cluster: the installed `ray/train` against this checkout's, and each node's GPUs, `py-spy` and NCCL RAS |
| `sync_ray_train.py` | installs this checkout's `ray/train` on every node of the running cluster |

## Cluster

An Anyscale workspace with a CPU head node and 4 worker nodes of 1 A10G each
(`g5.xlarge`), at a fixed size, so the GPUs are up when a script starts. The
scripts train one worker per GPU in the cluster, here 4, one per node. Any
other shape works too, such as 2 nodes of 4 GPUs. `--workers` or
`HEALTH_TEST_WORKERS` sets another number: 3 workers on the 4 nodes keep one
node spare, as in GR00T's standby nodes.

The image needs a Ray nightly close to this branch's master, since the cluster's
Ray can't be swapped by `pip install` once it runs. `nccl_hang.py` also needs
NCCL 2.28.7 or newer, with its `ncclras` client, on every node. `torch<=2.10`
bundles NCCL 2.27.5, so install the system NCCL too, built for the image's
CUDA. For example:

```dockerfile
FROM anyscale/ray:nightly-py311-cu128
# `apt-cache madison libnccl2` lists the versions; pick one built for this CUDA.
RUN sudo apt-get update && sudo apt-get install -y --allow-change-held-packages \
    libnccl2=<version> libnccl-dev=<version>
RUN pip install torch
```

`nccl_hang.py` preloads the system NCCL into the workers when it's newer than
torch's (`harness.nccl_runtime_env()`); an older one lacks symbols torch needs.
`check_cluster.py` prints both versions. A node without `ncclras`, or with an
NCCL older than 2.28, fails the pre-flight check, which evicts it and says why.
`gpu_faults.py` and `gr00t_recovery.py` need neither.

## Setup

The image's Ray needs this branch's `ray/train`: build the image from a wheel
of this branch, or install it on the running cluster with `sync_ray_train.py`.
In the workspace terminal:

```bash
cd ~ && git clone --depth 1 --branch ray-train-health/health-gpu-tests https://github.com/ray-project/ray.git
cd ~/ray && python release/train_tests/health/check_cluster.py
```

`check_cluster.py` says whether the installed `ray/train` matches this
checkout's, and for each node, the GPUs torch sees, `py-spy`, and whether
`nccl_hang.py`'s NCCL RAS pre-flight would pass. If `ray/train` differs,
`sync_ray_train.py` installs this checkout's `python/ray/train` into the
image's Ray on every node, and keeps the image's copy as `ray/train.orig`;
rerun it after nodes join or after `git pull`. Every change on the health stack
is Python under `ray/train`, so nothing needs building.

## Run

```bash
python release/train_tests/health/gpu_faults.py             # workflow, xid79
python release/train_tests/health/nccl_hang.py               # merged vs health on a hang
python release/train_tests/health/nccl_hang.py health slow_step
python release/train_tests/health/gr00t_recovery.py         # every GR00T scenario
```

To keep a run's whole output, every worker's lines included and without color
codes, write it to the shared storage as well:

```bash
mkdir -p /mnt/cluster_storage/health_logs
RAY_DEDUP_LOGS=0 RAY_COLOR_PREFIX=0 python -u release/train_tests/health/gr00t_recovery.py 2>&1 | tee /mnt/cluster_storage/health_logs/gr00t_recovery_$(date +%Y%m%d_%H%M%S).log
```

`gpu_faults.py --local` and `gr00t_recovery.py --local` run the same scenarios
on a local cluster of 4 single-CPU nodes, with gloo, to check the scripts on a
laptop. Artifacts go to `/mnt/cluster_storage/<run name>/`, which every node
shares.

### `gpu_faults.py workflow`

1. Pre-flight, with training stopped (`HealthCheck(requires_idle_workers=True)`):
   `AllReduceProbe` all-reduces a one over every worker's process group, and
   `EvictNodesFailingAllReduce` passes it.
2. Training starts. At step 20 rank 0 injects a thermal event on its node A,
   with `relative_speed` 0.5 and `temp_c` 92.
3. In flight, `GpuSpeedProbe` sees A slow, and `DiagnoseASlowNode` returns a
   `Diagnose`.
4. On demand, `GpuTemperatureProbe` finds A at 92C, and `ReattemptOnAHotNode`
   returns `Reattempt`.
5. With `max_failures=0` the run ends with `HealthCheckError`.

```
       -3.4s  RUNNING      the attempt's training started
       +0.0s  INJECT       GPU event on node fc9959fe: {'checkName': 'GpuThermalWatch', ...}
       +2.6s  DIAGNOSE     nodes ['fc9959fe'] run slower than the others
       +4.6s  REATTEMPT    node fc9959fe is 92C: {'fc9959fe': 92.0}
       +4.6s  ERRORED      A health check decided to Reattempt: node fc9959fe is 92C: ...
  decisions ['DIAGNOSE', 'REATTEMPT'], ended with HealthCheckError: PASS
```

### `gpu_faults.py xid79`

The last rank injects XID 79 (GPU fallen off the bus, `RESTART_VM`) on its
node B. `EvictOnFatalGpuEvent` evicts B, outside the retry budget, and the
worker group restarts. Nothing drains B, so the restart can land on it again;
a node is evicted once, so training then finishes. With a drain hooked to
`after_health_decision`, the restart would land elsewhere.

### `nccl_hang.py`

Both detectors get the same job and timing: RAS polled every 2s, a hang
confirmed after 20s without progress.

| fault | what it does | pass |
|---|---|---|
| `hang` | `LeaveCollective`: rank 1 stops calling the all-reduce at step 100 and sleeps, so RAS sees a mismatched communicator with every rank RUNNING | both detect it, within a few seconds of each other |
| `slow_step` | `SlowStep`: rank 1 pauses 10s before its collective every 40 steps | neither fires |

| | merged `NCCLRASCallback` | `ray.train.health` |
|---|---|---|
| turned on by | `RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1` | `HealthConfig(preflight_checks=[nccl_ras_preflight_check()], inflight_checks=[nccl_hang_check(...)])` |
| before training | nothing | `NCCLRASReadyProbe` on every node |
| suspected hang | logs, and fetches `ncclras -f text` on the controller | `Diagnose` with `NCCLRASTextProbe`, logged; training continues |
| confirmed hang | Flight Recorder, `nvidia-smi`, RAS history and stacks, one after another; raises `NCCLHangError` | `Diagnose` with all four at once, saved under `health_diagnostics/`; evicts a node whose `nvidia-smi` shows a fault, else reattempts |
| error if it ends | `NCCLHangError` | `HealthCheckError` carrying the hang message |

### `gr00t_recovery.py`

The recovery workflow from NVIDIA's "GR00T 2 Training Straggler Detection &
Recovery", on `ray.train.health`. Every scenario runs the same config: the
all-reduce pre-flight, and `progress_health.stall_check()` and
`straggler_check()` in flight. Each rank calls
`report_progress(step, phase, loss, straggler_score)` every step. The job saves
a checkpoint every 40 steps and resumes from the latest one, with
`max_failures=1`.

| GR00T step | here |
|---|---|
| monitor progress | `report_progress()` through `ray.train.health.report()`. A worker that dies fails the controller's poll and takes the existing retry. |
| detect and investigate | `ProgressEvaluator`: a rank whose step and phase haven't changed for longer than its phase's timeout. `DiagnoseStragglers`: a straggler score below 0.7. Both return a `Diagnose` that reads the ranks' nodes' GPU health events and temperatures; a stall's also takes every rank's stack trace. |
| restore capacity | `Evict` when the diagnosis finds a GPU fault |
| ready in Ray | the all-reduce pre-flight, at the start of each attempt |
| restart and resume | `Reattempt` or `Evict` restarts every rank, and the job resumes from `ray.train.get_checkpoint()` |
| verify recovery | `ProgressEvaluator` again, after its `on_reset()`: the steps advance within the init phase's timeout, and the loss stays finite |
| fall back | once `max_failures` runs out, `fit()` raises `HealthCheckError` |

| scenario | fault, in the first attempt | expected |
|---|---|---|
| `stall` | rank 1 stops after step 50, alive | DIAGNOSE, no GPU fault, REATTEMPT, resume from step 40 |
| `checkpoint_pause` | rank 0 takes 25s to save the step 40 checkpoint | no decision: longer than the train phase's timeout (10s), within the checkpoint phase's (60s) |
| `straggler` | rank 1 reports a straggler score of 0.5 from step 50 | DIAGNOSE, no GPU fault, training continues |
| `straggler_gpu_fault` | the last rank reports 0.5 from step 50, and its node gets XID 48 | DIAGNOSE, EVICT, resume from the latest checkpoint: step 40, or 80 if training reached it while diagnosing |
| `nan_loss` | rank 0's loss is NaN from step 50 | REATTEMPT, resume from step 40 |

A stall's stack traces go to `<storage>/<run name>/health_diagnostics/`. Each
attempt's `START` line lists its nodes. With `--workers 3` on the 4 nodes, one
node is spare: compare the nodes before and after a restart. Nothing drains an
evicted node, so the restart can land on it again rather than on the spare.

The scores here are injected. With NVRx, a rank's score could be its relative
GPU performance score from NVRx's straggler report
(`gpu_relative_perf_scores`), which every rank gets when the detector doesn't
gather the report on rank 0 only; check the names against your NVRx version.

## Mocking GPU health events

The events have the fields of NVSentinel's `HealthEvent`
(`data-models/protobufs/health_event.proto`): `checkName`, `isFatal`,
`isHealthy`, `errorCode`, `entitiesImpacted`, `recommendedAction`. As in
NVSentinel, an event is fatal when its recommended action isn't `NONE`
(`gpu_health.xid_event()` maps a few XIDs), and one with `isHealthy` clears it.
`harness.inject_gpu_event(node_id, event)` writes one on a node, from a task
pinned there, and the probes in `gpu_health.py` read it on their next poll.

How the other projects fake GPU faults, and where they'd fit here:

| project | technique | here |
|---|---|---|
| NVSentinel | `dcgmi test --inject --gpuid 0 -f 230 -v 79` into a real or fake (`NVML_INJECTION_MODE`) DCGM; JSON health events posted to its platform connector | the event schema and the fatal rule; a DCGM-backed probe could read the real injection |
| NVRx | `inject_fault()`: `torch.cuda._sleep(1 << 62)` hangs a GPU, `a[b] = 0` makes a sticky CUDA error, `SIGSTOP` hangs a rank; health checks tested by patching `pynvml` | `LeaveCollective` for a hang RAS can see |
| TorchFT | `EventInjector.fail_at(rank, step)`, `_abort_process_group()` | faults fire at a rank and step, once per run |

An NVRx-style `torch.cuda._sleep` hang would be a useful next fault: every rank
issues the same collectives, so the RAS op counts likely match, and the run
would show what the hang detector can't see.
