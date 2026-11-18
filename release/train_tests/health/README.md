# Ray Train health: GPU tests

Scripts that run the Ray Train health stack (`ray.train.health`, #67671 to
this PR) on a real GPU cluster, with the faults injected on purpose. They print
a timeline of what the controller decided and PASS or FAIL.

| file | what it is |
|---|---|
| `gpu_faults.py` | GPU faults mocked as GPU health events: a pre-flight all-reduce, a slow and hot node diagnosed and reattempted, a fatal XID evicted |
| `nccl_hang.py` | one NCCL hang, caught by the NCCL hang detector on `ray.train.health` (#67916) and by the `NCCLRASCallback` it replaces, compared; and a slow step that must not fire |
| `gr00t_recovery.py` | GR00T 2's straggler detection and recovery: stalls, stragglers and a NaN loss, diagnosed, then reattempted or evicted, resuming from a checkpoint |
| `progress_health.py` | the progress checks GR00T would run: phase-aware stall detection, and straggler scores diagnosed before evicting |
| `gpu_health.py` | the GPU probes and evaluators, and the mocked GPU health events |
| `harness.py` | the injected faults, and the `Timeline` that records what the controller did |

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
`ncclras` must be on the nodes' `PATH`, since the detector runs it on each
node's `NodeMonitor`. Without it, or with an NCCL older than 2.28, the detector
turns itself off with a warning and the hang isn't caught. `gpu_faults.py` and
`gr00t_recovery.py` need neither.

## Setup

The image's Ray needs this branch's `ray/train`: build the image from a wheel
of this branch. In the workspace terminal:

```bash
cd ~ && git clone --depth 1 --branch ray-train-health/health-gpu-tests https://github.com/ray-project/ray.git
cd ~/ray
```

## Run

```bash
python release/train_tests/health/gpu_faults.py             # workflow, xid79
python release/train_tests/health/nccl_hang.py               # callback vs health on a hang
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
2. Training starts. At step 20 rank 0 injects DCGM's thermal clocks event on
   its node A (`GpuThermalWatch`, `DCGM_FR_CLOCKS_EVENT_THERMAL`, not fatal),
   with `relative_speed` 0.5 and `temp_c` 92.
3. In flight, `GpuSpeedProbe` sees A slow, and `DiagnoseASlowNode` returns a
   `Diagnose`.
4. On demand, `GpuTemperatureProbe` finds A at 92C and the other nodes at
   their real temperature, and `ReattemptOnAHotNode` returns `Reattempt`.
5. With `max_failures=0` the run ends with `HealthCheckError`.

```
       -2.0s  RUNNING      the attempt's training started
       +0.0s  INJECT       GPU event on node 643d9058: {'checkName': 'GpuThermalWatch', ...}
       +2.0s  DIAGNOSE     nodes ['643d9058'] run slower than the others
       +4.0s  REATTEMPT    node 643d9058 is 92C: {'34c197d1': 29.0, '53de389d': 28.0, '643d9058': 92.0, 'c74b3ccb': 29.0}
      +39.2s  ERRORED      A health check decided to Reattempt: node 643d9058 is 92C: ...
  decisions ['DIAGNOSE', 'REATTEMPT'], ended with HealthCheckError: PASS
```

### `gpu_faults.py xid79`

The last rank injects XID 79 (GPU fallen off the bus) on its node B, as
NVSentinel's syslog monitor reports it: `SysLogsXIDError`, `RESTART_BM`, fatal.
`EvictOnFatalGpuEvent` evicts B, outside the retry budget, and the worker group
restarts. `RESTART_BM` tells NVSentinel to cordon, drain and reboot B; Ray
Train can't reboot a node, and `Evict` is its part: take the job off B before
the drain kills its worker. Nothing drains B here, so the restart can land on
it again; a node is evicted once, so training then finishes. With a drain hooked to
`after_health_decision`, the restart would land elsewhere.

### `nccl_hang.py`

Both detectors get the same job and timing: RAS polled every 2s, a hang
confirmed after 20s without progress, and `RAY_TRAIN_NCCL_RAS_ACTION=fail`.

| fault | what it does | pass |
|---|---|---|
| `hang` | `LeaveCollective`: rank 1 stops calling the all-reduce at step 100 and sleeps, so RAS sees a mismatched communicator with every rank RUNNING | both end the run with their error, within a few seconds of each other, and both upload the four diagnostics |
| `slow_step` | `SlowStep`: rank 1 pauses 10s before its collective every 40 steps | neither fires |

| | `NCCLRASCallback` (master) | `ray.train.health` (#67916) |
|---|---|---|
| turned on by | `RunConfig(callbacks=[NCCLRASCallback()])`, since #67916 the env var no longer registers it | `RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1`: the controller adds the detector's check to the run |
| where `ncclras` runs | on the first worker that answers | on each node's `NodeMonitor` |
| suspected hang | logs, and fetches `ncclras -f text` on the controller | `Diagnose` with `NCCLRASTextProbe`, logged; training continues |
| confirmed hang | Flight Recorder, `nvidia-smi`, RAS history and stacks, one after another, uploaded to `hang_detector/` | `Diagnose` with all four at once, uploaded to `hang_detector/`, then `Reattempt` |
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
| detect and investigate | `ProgressEvaluator`: a rank whose step and phase haven't changed for longer than its phase's timeout. `DiagnoseStragglers`: a straggler score below 0.75, NVRx's default. Both return a `Diagnose` that reads the ranks' nodes' GPU health events and temperatures; a stall's also takes every rank's stack trace. |
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
| `straggler_gpu_fault` | the last rank reports 0.5 from step 50, because its GPU is past its hardware slowdown temperature: NVSentinel's `GpuThermalMarginWatch`, fatal | DIAGNOSE, EVICT, resume from the latest checkpoint: step 40, or 80 if training reached it while diagnosing |
| `nan_loss` | rank 0's loss is NaN from step 50 | REATTEMPT, resume from step 40 |

A stall's stack traces go to `<storage>/<run name>/health_diagnostics/`. Each
attempt's `START` line lists its nodes. With `--workers 3` on the 4 nodes, one
node is spare: compare the nodes before and after a restart. Nothing drains an
evicted node, so the restart can land on it again rather than on the spare.

The scores here are injected. With NVRx, a rank's score could be its relative
GPU performance score from NVRx's straggler report
(`gpu_relative_perf_scores`), which every rank gets when the detector doesn't
gather the report on rank 0 only; check the names against your NVRx version.

## What each scenario mocks, and why

Each row is a failure the health checks act on. "Without a health check" is
Ray Train with no `HealthConfig` facing the real failure: the mocked events
change nothing unless a check reads them. The times are from the 4 × A10G
runs, counted from when the fault fired. Every fault fires once per run, at a
set rank and step, as TorchFT's `EventInjector.fail_at(rank, step)` does.

| use case / error pattern | script | mocked as, and from | probe → evaluator | without a health check | with it |
|---|---|---|---|---|---|
| a bad GPU or link before training starts | every attempt of `gpu_faults.py workflow` and `gr00t_recovery.py` | nothing injected: GR00T's "ready in Ray" step, run on healthy nodes | `AllReduceProbe` → `EvictNodesFailingAllReduce`, with training stopped | training starts on the node; its first collective fails, or hangs until the NCCL timeout | passes before every attempt |
| a GPU throttled by heat | `gpu_faults.py workflow` | NVSentinel's report of DCGM's thermal clocks event (not fatal, dropped by default), at half speed and 92C | `GpuSpeedProbe` → `DiagnoseASlowNode`, then `GpuTemperatureProbe` → `ReattemptOnAHotNode` | nothing raises: every step waits on the slow GPU | DIAGNOSE +2.0s, REATTEMPT +4.0s |
| a GPU fallen off the bus (XID 79) | `gpu_faults.py xid79` | NVSentinel's syslog report of XID 79: `RESTART_BM`, fatal | `GpuHealthEventProbe` → `EvictOnFatalGpuEvent` | the rank's CUDA calls fail and the run retries against `max_failures`, maybe on the same node; or the other ranks hang until the NCCL timeout | EVICT +1.6s, training again at +5.6s, outside `max_failures` |
| a rank stops calling the collective | `nccl_hang.py` | `LeaveCollective` (#66247), like TorchFT's `fail_at` and NVRx's `SIGSTOP` fault | `NCCLRASProbe` → `NCCLHangEvaluator` → `Diagnose` with the RAS text, `nvidia-smi`, Flight Recorder and stack probes → `ConfirmedHangEvaluator` | hangs until the NCCL timeout; the detector is off by default | confirmed, then REATTEMPT, naming rank 1's host and pid (times to be rerun on #67916) |
| an expected pause that isn't a hang: a slow step, or a long checkpoint save | `nccl_hang.py health slow_step`; `gr00t_recovery.py checkpoint_pause` | `SlowStep` (#66247): rank 1 pauses 10s every 40 steps. Rank 0 takes 25s to save a checkpoint (GR00T doc: a timeout per phase) | `NCCLHangEvaluator`, whose 20s confirmation outlasts the pause; `ProgressEvaluator`, whose checkpoint phase has a 60s timeout | nothing, correctly; with one stall timeout for every phase, the save would be restarted | no decision |
| a rank alive but not progressing | `gr00t_recovery.py stall` | rank 1 sleeps after step 50 (GR00T doc) | `report_progress()` → `ProgressEvaluator` → `Diagnose` with stacks, GPU events and temperatures → `DecideOnStall` | hangs until the NCCL timeout, then retries against `max_failures`, with no stack traces | DIAGNOSE +10.8s, REATTEMPT +14.8s, resumed from step 40 |
| a slow rank on healthy GPUs | `gr00t_recovery.py straggler` | rank 1 reports an NVRx relative performance score of 0.5 | `DiagnoseStragglers` → `Diagnose` with GPU events and temperatures → `EvictOnConfirmedGpuFault` | NVRx reports the rank, and nothing acts | DIAGNOSE +0.7s, no fault, training continues at +4.7s |
| a slow rank on a GPU past its slowdown temperature | `gr00t_recovery.py straggler_gpu_fault` | score 0.5, and NVSentinel's `GpuThermalMarginWatch` event: `CONTACT_SUPPORT`, fatal | as above | every step waits on the slow GPU | DIAGNOSE, then EVICT, outside `max_failures` |
| a NaN loss | `gr00t_recovery.py nan_loss` | rank 0's loss is NaN from step 50 (GR00T doc: verify the loss stays finite) | `ProgressEvaluator` | training goes on with NaN weights and checkpoints them | REATTEMPT +0.7s, resumed from step 40 |

The NCCL timeout is Ray Train's `TorchConfig.timeout_s`, 30 minutes; PyTorch's
own default is [10 minutes](https://docs.pytorch.org/docs/2.15/distributed.html#torch.distributed.init_process_group).

### How often these happen

- Fatal GPU faults are the most common interruption. In Llama 3's 54-day
  pre-training run, faulty GPUs caused 148 of the 419 unexpected
  interruptions and HBM3 memory 72 ([Table 5](https://arxiv.org/abs/2407.21783)).
  In Alibaba's 4,096-GPU job, ECC and NVLink errors caused 27.5% of 40 crashes
  in a month ([Table I](https://arxiv.org/abs/2406.04594)).
- XID 79 itself is rare: 10 in 895 days on NCSA Delta's A100s, none in 146
  days on its H100s ([Table 1](https://arxiv.org/abs/2503.11901)). PCIe errors
  often come with it: on Meta's RSC-1 and RSC-2 clusters, 43% and 63% of the
  PCIe errors co-occur with an XID 79 ([Kokolis et al. §III](https://arxiv.org/abs/2410.21680)).
- Slow GPUs are rare, and costly. MegaScale found about 0.5% of machines
  substantially slower ([§5.1](https://arxiv.org/abs/2402.15627)); in one of
  FALCON's case studies, a GPU ran 20% slower than its peers at nearly 70C
  ([§3.2](https://arxiv.org/abs/2410.12588)); Llama 3 saw a 1-2% swing in
  throughput with the time of day, from the heat.
- Most stragglers aren't hardware, so a low score is diagnosed, not evicted on.
  Across 3,079 ByteDance jobs, problems in one or a few workers caused most of
  the slowdown in only 1.7% of the straggling ones. The job itself was far more
  often the cause: the last pipeline stage caused most of the slowdown in 39.3%
  of jobs, and uneven sequence lengths affected 21.4%
  ([§5](https://arxiv.org/abs/2505.05713)).
- A hang is silent until something times out. A hung ByteDance job writes no
  logs until the NCCL timeout, 30 or 60 minutes
  ([ByteRobust §2.3](https://arxiv.org/abs/2509.16293)); at Alibaba, a hang
  lasted up to 30 minutes, until the elastic agent killed the processes
  ([C4 §IV-B](https://arxiv.org/abs/2406.04594)).

### Where the mocks come from

The GPU health events have the fields of NVSentinel's
[`HealthEvent`](https://github.com/NVIDIA/NVSentinel/blob/ca526447ce038c69f6527b574088788c547a4c93/data-models/protobufs/health_event.proto#L118-L140)
and the values its monitors set (`gpu_health.xid_event()`,
`gpu_health.dcgm_event()`). As in NVSentinel, an event is fatal when its
recommended action isn't `NONE`, and one with `isHealthy` clears it.
`harness.inject_gpu_event(node_id, event)` writes one on a node, from a task
pinned there, and the probes in `gpu_health.py` read it on their next poll.

| source | what it gave these tests |
|---|---|
| NVSentinel | the event schema; the XID actions from [NVIDIA's XID catalog](https://docs.nvidia.com/deploy/xid-errors/analyzing-xid-catalog.html), as its [syslog monitor maps them](https://github.com/NVIDIA/NVSentinel/blob/ca526447ce038c69f6527b574088788c547a4c93/health-monitors/syslog-health-monitor/pkg/common/common.go#L36-L103); the DCGM codes and [their actions](https://github.com/NVIDIA/NVSentinel/blob/ca526447ce038c69f6527b574088788c547a4c93/distros/kubernetes/nvsentinel/charts/gpu-health-monitor/files/dcgmerrorsmapping.csv); its tests inject XID 79 [into the kernel log](https://github.com/NVIDIA/NVSentinel/blob/ca526447ce038c69f6527b574088788c547a4c93/tests/syslog_health_monitor_test.go#L275-L279) and [into DCGM](https://github.com/NVIDIA/NVSentinel/blob/ca526447ce038c69f6527b574088788c547a4c93/tests/gpu_health_monitor_test.go#L304-L307) |
| NVRx | the [relative performance score](https://github.com/NVIDIA/nvidia-resiliency-ext/blob/67255611d37fea82d26e3e14bd32aa02f39c40a6/docs/source/straggler_det/usage_guide.rst#L16-L25): the fastest rank's median time over this rank's, so 0.5 is twice as slow, with a [default threshold of 0.75](https://github.com/NVIDIA/nvidia-resiliency-ext/blob/67255611d37fea82d26e3e14bd32aa02f39c40a6/src/nvidia_resiliency_ext/attribution/straggler/reporting.py#L84-L89); its [`inject_fault()`](https://github.com/NVIDIA/nvidia-resiliency-ext/blob/67255611d37fea82d26e3e14bd32aa02f39c40a6/src/nvidia_resiliency_ext/shared_utils/inject_fault.py#L33-L45) hangs a rank with `SIGSTOP` or a GPU with `GPU_SLEEP` |
| TorchFT | faults at a rank and step, once per run, as its [`EventInjector.fail_at`](https://github.com/pytorch/torchft/blob/43e3b6d3557194ff1eea3c3217e94ef7f6308bc2/torchft/manager_integ_test.py#L115-L131) does |
| GR00T 2 | NVIDIA's "GR00T 2 Training Straggler Detection & Recovery" doc: progress and phase timeouts, slow-rank reports as evidence rather than proof, standby nodes, and checking the loss after a restart |
| #66247 | `LeaveCollective` and `SlowStep`, the hang and the slow step NCCL RAS must tell apart |

An NVRx-style `GPU_SLEEP` hang would be a useful next fault: every rank
issues the same collectives, so the RAS op counts likely match, and the run
would show what the hang detector can't see.
