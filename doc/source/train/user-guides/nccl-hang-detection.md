---
myst:
  html_meta:
    description: "Detect hung NCCL collectives in Ray Train before the PyTorch watchdog times out, capture per-rank stack traces and NCCL diagnostics, and optionally fail the run so the failure policy restarts it."
---

(train-nccl-hang-detection)=

# Detect NCCL hangs

:::{note}
Ray Train added the NCCL hang detector in Ray 2.60. It's experimental and off by default. Ray 2.60 captures stack traces and the NCCL RAS report and history on a hang. The `nvidia-smi` snapshots and PyTorch Flight Recorder dumps described in {ref}`train-nccl-hang-diagnostics` are available from Ray 2.61.
:::

A distributed training job hangs when the ranks of a NCCL communicator disagree about which collective comes next. For example, if one rank takes a code path that skips an `all_reduce`, calls an extra one, or launches a collective with a different shape, then every other rank waits for it. As a result, the job makes no progress, and the only signal is the PyTorch NCCL watchdog, which aborts the process group after `TorchConfig(timeout_s)`, 30 minutes by default. By then the processes are gone, so you can't see where each rank was stuck, and you have to rerun the job with manual monitoring to find out.

The NCCL hang detector watches the collective counts of every rank and acts while the ranks are still alive. When a communicator's ranks disagree and none of them makes progress for a configurable duration, the detector logs a warning, captures diagnostics from every worker, and optionally fails the run so that the {ref}`failure policy <train-fault-tolerance>` restarts it.

This guide shows how to enable the detector, what it does during a hang, how to read the diagnostics it captures, and which hangs it can't detect.

## How does the detector work?

NCCL ships a Reliability, Availability, and Serviceability (RAS) subsystem that runs a monitoring thread inside every NCCL process. The threads form a mesh across the ranks of a job and track, for each communicator, how many collectives of each type every rank has launched. The `ncclras` command-line client queries that mesh from any one rank and returns the state of every rank it can reach.

The detector runs on the Ray Train controller and polls `ncclras` on one of the training workers every `RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S` seconds. It doesn't touch the training function and adds no work to the training step. A single poll shows a snapshot, and ranks are legitimately skewed by stragglers, data loading, and checkpointing, so the detector compares consecutive polls and only treats a communicator as hung when both of the following hold across the whole confirm window:

- Its ranks report different launch counts for at least one collective type, and every rank is still running.
- No rank advanced any collective count since the previous poll.

Any progress on any rank resets that communicator. The detector tracks each communicator on its own, so it confirms a stuck sub-group even while the world communicator is healthy.

## Requirements

- NCCL 2.28 or later, with `ncclras` on the `PATH` of the training workers. RAS exists from NCCL 2.24, but the detector parses the JSON output that `ncclras -f json` added in 2.28. If the binary is missing or doesn't support `-f`, the detector logs one warning and disables itself for the rest of the run. Training continues unaffected.
- PyTorch with the NCCL backend. The detector reads the NCCL process groups that `torch.distributed` creates. It doesn't apply to Gloo, XLA, or JAX backends.
- `py-spy` on the workers, for native stack traces. Detection works without it, but the stack traces it captures on a hang are Python-only. See {ref}`train-nccl-hang-diagnostics` for what each diagnostic needs.

## Enable the detector

Set `RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1` on the driver before you construct the trainer. The trainer registers the detector as a built-in callback, and the training function needs no changes:

```{literalinclude} ../doc_code/nccl_hang_detection.py
:language: python
:start-after: __enable_start__
:end-before: __enable_end__
```

By default the detector runs in `observe` mode. On a confirmed hang it logs a warning, captures diagnostics, and leaves the job running until the PyTorch watchdog times out. Use `observe` mode first, so that a false positive can't fail a healthy run.

### Fail the run on a confirmed hang

Set `RAY_TRAIN_NCCL_RAS_ACTION=fail` to raise a {class}`~ray.train.NCCLHangError` on a confirmed hang instead. `NCCLHangError` is a {class}`~ray.train.WorkerGroupError`, so the {ref}`failure policy <train-worker-fault-tolerance>` treats it like any other worker failure. Each confirmed hang uses one retry from `FailureConfig(max_failures)`, and Ray Train restarts the run from the latest checkpoint. A restart helps when the hang comes from flaky hardware, such as a faulty NIC, because the new workers can land on different nodes. When the run exhausts its retries, `trainer.fit()` raises the `NCCLHangError`.

```{literalinclude} ../doc_code/nccl_hang_detection.py
:language: python
:start-after: __fail_start__
:end-before: __fail_end__
```

:::{caution}
The detector only has value if it confirms a hang before the PyTorch watchdog aborts the job. Keep `RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S` well below `TorchConfig(timeout_s)`. With the defaults of 600 seconds and 1800 seconds, the detector confirms and has captured its diagnostics about 20 minutes before the watchdog fires. If you lower `timeout_s` below the confirm duration, PyTorch aborts the ranks first, and the detector only ever logs suspicions.
:::

## Configure the detector

The detector reads the `RAY_TRAIN_*` variables once on the driver, when the trainer constructs it, and validates them eagerly. An invalid value raises a `ValueError` when you construct the trainer.

```{list-table} NCCL hang detector environment variables
:header-rows: 1

* - Environment variable
  - Default
  - Description
* - `RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR`
  - `0`
  - Set to `1` to register the detector. Read on the driver.
* - `RAY_TRAIN_NCCL_RAS_ACTION`
  - `observe`
  - What to do on a confirmed hang. `observe` logs the hang and captures diagnostics. `fail` also raises {class}`~ray.train.NCCLHangError`. Read on the driver.
* - `RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S`
  - `15`
  - Seconds between `ncclras` queries. Read on the driver.
* - `RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S`
  - `600`
  - Seconds a communicator must stay frozen before the detector confirms a hang. The detector rounds it up to a whole number of polls, 40 with the defaults. Read on the driver.
* - `NCCL_RAS_ADDR`
  - unset
  - NCCL's own `host:port` for the RAS listener. Only set it if you've changed it from NCCL's default. Ray Train propagates it from the driver to the workers and passes it to `ncclras`.
* - `TORCH_FR_BUFFER_SIZE`
  - unset
  - Number of collectives PyTorch's Flight Recorder keeps per rank. Set it to also capture Flight Recorder dumps on a hang. See {ref}`train-nccl-hang-flight-recorder`. Ray Train propagates it from the driver to the workers.
```

The time from the first frozen poll to a confirmed hang is at least the confirm duration and usually a little more. The first frozen poll needs a previous report to compare against, and the controller only acts on a report at its next health check, which runs every few seconds.

## What happens during a hang?

The detector escalates in three stages, all logged on the Ray Train controller:

1. **Suspected.** When a communicator has been frozen for about 60 seconds, the detector logs a `WARNING` naming the communicator and, in `fail` mode, how long until it raises. It keeps monitoring, because a transient stall looks the same at this point.
1. **Still suspected.** About every 120 seconds, it logs a `WARNING` listing every still-frozen communicator and how long each one has stalled, followed by the human-readable `ncclras` report at `INFO` level. The report lists every rank, its status, and its collective counts per communicator, so you can see which ranks fell behind.
1. **Confirmed.** When a communicator stays frozen for the full confirm duration, the detector logs the `ncclras` report, captures the diagnostics described in the next section, and then either logs a summary in `observe` mode or raises {class}`~ray.train.NCCLHangError` in `fail` mode. The summary lists where each diagnostic was uploaded.

If a suspected communicator resumes progress at any stage, the detector logs that it recovered and resets its streak.

(train-nccl-hang-diagnostics)=

## Diagnostics

On a confirmed hang, the detector captures four diagnostics while the ranks are still alive and uploads them to the run's {ref}`storage path <persistent-storage-guide>` under `<storage_path>/<name>/hang_detector/`. Each tool writes into its own subdirectory, and a tool that isn't available on a rank writes a one-line file saying why, so a gap is never silent:

```text
<storage_path>/<name>/hang_detector/
├── stack_traces/
│   ├── rank_0.log
│   └── rank_<i>.log
├── nccl_ras/
│   ├── ncclras_report.txt
│   └── ncclras_<timestamp>.json
├── nvidia_smi/
│   └── node_<ip>.log
└── flight_recorder/
    ├── rank_0.json
    └── rank_<i>.json
```

Stack traces, the RAS report and history, and `nvidia-smi` snapshots are on by default. Flight Recorder dumps are opt-in, because they need a buffer armed before training starts.

### Stack traces

The detector runs `py-spy dump --native` on every worker and writes the native plus Python stack of every thread to `stack_traces/rank_<i>.log`. This is the diagnostic that shows *where* each rank is: which ranks are blocked inside NCCL waiting for a peer, and what the peer is doing instead.

It needs `py-spy` installed on the workers, and the `SYS_PTRACE` capability if the workers run in containers. Without `py-spy`, the file starts with a line saying why and holds a Python-only traceback of every thread, which shows the Python frames but can't show a rank blocked inside a NCCL kernel.

### NCCL RAS report and history

The detector writes the human-readable `ncclras -f text` report at confirmation time to `nccl_ras/ncclras_report.txt`, and the JSON report from each poll leading up to the hang to `nccl_ras/ncclras_<timestamp>.json`. The history covers the whole confirm window plus a short margin before it, so you can see when each rank's counts stopped advancing and which collective type diverged first. This diagnostic needs nothing beyond `ncclras` itself.

### `nvidia-smi` snapshots

The detector runs `nvidia-smi -q` on one worker per node and writes the output to `nvidia_smi/node_<ip>.log`: power, temperature, clocks, utilization, and ECC state of every GPU on that node at the moment of the hang. It needs `nvidia-smi` on the nodes, which the NVIDIA driver installs.

(train-nccl-hang-flight-recorder)=

### PyTorch Flight Recorder dumps

PyTorch's Flight Recorder keeps a ring buffer of the most recent collectives on every rank: the process group, sequence number, state, input and output shapes and dtypes, and the call stack that issued each one. The detector dumps each rank's buffer to `flight_recorder/rank_<i>.json`. It's the only diagnostic that shows a mismatch in shape, dtype, or call order, because RAS counts launches and doesn't compare arguments.

To capture Flight Recorder dumps, set `TORCH_FR_BUFFER_SIZE` on the driver before you construct the trainer, as in the preceding `fail` mode example. Ray Train propagates it to the workers, and PyTorch arms the buffer when it creates the process group. The detector can't switch the buffer on retroactively in a hung process, so if you set it after the run starts, the dump holds no collectives.

Flight Recorder shows the exact collective in flight on each rank with its arguments. You can see a shape or dtype mismatch, and a rank that issued an extra or a different collective, without reproducing the hang.

Weigh that against its cost before you turn it on. Every rank keeps the buffer in host memory and records every collective, which adds a small per-collective overhead on the critical path. A buffer that's too small for the number of collectives per step rolls over before the hang and loses the earlier context. A few thousand entries is a reasonable starting point for most training loops.

### Read the diagnostics

Work through the diagnostics in this order. Each one narrows down what the previous one found:

1. Find the communicator and the ranks that fell behind in `nccl_ras/ncclras_report.txt`. The report lists every rank, its status, and its collective counts per communicator. The ranks with the lowest counts on the mismatched communicator stopped launching first. Then open the `ncclras_<timestamp>.json` history to see when the counts diverged, and whether they drifted apart slowly, which points at a straggler, or stopped at once, which points at a divergent code path.
1. Find where each rank is stuck in `stack_traces/rank_<i>.log`. Compare the main thread across ranks. The ranks that are blocked inside a NCCL kernel or a `torch.distributed` call are waiting. The rank whose main thread is somewhere else, such as a data loader, a checkpoint upload, or a branch that only some ranks take, is usually the one that skipped the collective.
1. Check the collective's arguments in `flight_recorder/rank_<i>.json`, if you armed it. Compare the last entry of each rank. Matching sequence numbers with different shapes or dtypes is a mismatch. A rank whose last entry has a higher sequence number than its peers launched an extra collective. A file that holds only a `dump_error` key says why the rank couldn't produce a dump.
1. Rule hardware out with `nvidia_smi/node_<ip>.log`. A GPU with pending ECC errors, a throttled clock, or zero utilization while its peers are busy points at the device rather than the code.

The ranks in the `ncclras` report are NCCL communicator ranks. For the world communicator they match the Ray Train world ranks in the file names. For a sub-group, such as a tensor-parallel or pipeline-parallel group, they're the rank's position within that group.

## Limitations

The detector catches hangs where ranks disagree about launch counts. It doesn't detect the following:

- **Symmetric in-collective hangs.** If every rank launches the same collective and the fabric wedges mid-operation, all counts match and the detector sees no mismatch. The PyTorch watchdog is still the only signal for these.
- **A rank that exited early.** The detector only evaluates a communicator while all of its ranks report `RUNNING`. A rank that crashed or finalized alongside frozen peers is skipped, because Ray Train's health check fails the worker group first.
- **Hangs in communicators that the polled rank can't reach.** RAS only knows about the ranks reachable through the communicators that each process created. If a job never creates a communicator that spans all ranks, a query on one rank reports only its own subset. Every common parallelism strategy creates a world process group, so this rarely applies in practice.

The detector can also report a false positive:

- **A legitimately idle, skewed communicator.** A communicator that sits with mismatched counts and no progress for the whole confirm window looks like a hang. A pipeline-parallel send and receive group waiting on a slow stage can do this. In `fail` mode, that fails the run. This false-positive mode is the reason the detector defaults to `observe`. If your workload has communicators that idle for long periods, raise `RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S` above that idle time or stay in `observe` mode.

The `NCCLHangError` doesn't attribute the hang to a specific rank. The `ncclras` report and the diagnostics in the preceding section are the way to find it.
