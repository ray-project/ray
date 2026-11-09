# Ray Train health: GPU tests

Scripts that run the Ray Train health stack (`ray.train.health`, #67671 to
this PR) on a real GPU cluster, with the faults injected on purpose. They print
a timeline of what the controller decided and PASS or FAIL.

| file | what it is |
|---|---|
| `gpu_faults.py` | GPU faults mocked as GPU health events: a pre-flight all-reduce, a slow and hot node diagnosed and reattempted, a fatal XID evicted |
| `nccl_hang.py` | one NCCL hang, caught by the merged `NCCLRASCallback` and by the same detection on `ray.train.health`, compared; and a slow step that must not fire |
| `gpu_health.py` | the GPU probes and evaluators, and the mocked GPU health events |
| `nccl_ras_health.py` | NCCL RAS hang detection on `ray.train.health`, from the 7/n probes |
| `harness.py` | the injected faults, and the `Timeline` that records what the controller did |
| `sync_ray_train.py` | installs this checkout's `ray/train` on every node of the running cluster |

## Cluster

An Anyscale workspace with a CPU head node and 2 worker nodes of 4 GPUs each
(for example `g5.12xlarge`), at a fixed size: the scripts train 8 workers, one
per GPU. Set `HEALTH_TEST_WORKERS` for another size.

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

`nccl_hang.py` preloads the system NCCL into the workers
(`harness.nccl_runtime_env()`). A node without `ncclras` fails the pre-flight
check, which evicts it and says why. `gpu_faults.py` needs neither.

## Setup

In the workspace terminal, clone outside the workspace directory
(`/home/ray/default`), so the workspace doesn't sync the repo to every node:

```bash
cd ~ && git clone --depth 1 --branch ray-train-health/health-gpu-tests https://github.com/ray-project/ray.git
cd ~/ray && python release/train_tests/health/sync_ray_train.py
```

`sync_ray_train.py` installs this branch's `python/ray/train` into the image's
Ray on every node. Every change on the health stack is Python under
`ray/train`, so nothing needs building. It keeps the image's copy as
`ray/train.orig`. Rerun it after nodes join or after `git pull`.

## Run

```bash
python release/train_tests/health/gpu_faults.py             # workflow, xid79
python release/train_tests/health/nccl_hang.py               # merged vs health on a hang
python release/train_tests/health/nccl_hang.py health slow_step
```

`gpu_faults.py --local` runs the same scenarios on a local 2-node CPU cluster,
with gloo, to check the scripts on a laptop. Artifacts go to
`/mnt/cluster_storage/<run name>/`, which every node shares.

### `gpu_faults.py workflow`

1. Pre-flight, with training stopped (`HealthCheck(stop_workers=True)`):
   `AllReduceProbe` all-reduces a one over the 8 workers' process group, and
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
worker group restarts. Nothing drains B, so the restart lands on it again; a
node is evicted once, so training then finishes. With a drain hooked to
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
