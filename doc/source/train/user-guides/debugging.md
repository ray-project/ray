---
myst:
  html_meta:
    description: "Debug Ray Train runs that fail or hang: reproduce on a small cluster, read per-rank errors and logs, set breakpoints with the Ray Distributed Debugger, and find where a hung run is stuck."
---

(train-debugging)=

# Debug failures and hangs

Ray Train runs your training function in training workers, which are Ray actors in separate processes and often on separate nodes. A Ray Train controller actor starts the workers, polls them, and decides whether to retry or raise when something goes wrong. So an error or a hang in your training function happens in a process that your driver script doesn't own, and you debug it with Ray's distributed tools rather than with a debugger attached to your script.

This guide describes how to reproduce a failure or hang on a small cluster, find the output and logs of each worker and of the controller, and debug a run that fails with an error or one that hangs.

{ref}`train-debugging-env-vars` lists the timeouts and switches that this guide mentions. {ref}`train-debugging-agents` lists the non-interactive commands that suit a coding agent.

(train-debugging-reproduce)=

## Reproduce the problem on a small cluster

Debug with the same trainer and scaling configuration that fails, scaled down as far as the problem still reproduces. Every run goes through the same controller, worker group, and `ray.train` code paths at any size, so a run with one or two workers on your laptop behaves like the production run, apart from scale.

- Use few workers. Set `ScalingConfig(num_workers=1)` to debug training logic and `num_workers=2` to debug anything that involves more than one rank, such as collectives, `ray.train.report` synchronization, or rank-dependent code paths.
- Use CPUs when the bug isn't GPU specific. With `use_gpu=False`, `TorchTrainer` sets up the process group with the Gloo backend, so distributed code runs on a laptop without GPUs.
- Start a local Ray cluster. `trainer.fit()` calls `ray.init()` for you if Ray isn't running, which starts a single-node cluster on your machine. To simulate more resources than your machine has, call `ray.init(num_cpus=<n>)` before `fit()`.
- Surface the first error. `FailureConfig(max_failures)` defaults to `0`, so the run raises on the first worker failure. If your configuration retries failures, set `max_failures=0` while you debug so that a retry doesn't hide the error behind a restart.

The following example runs a two-worker, CPU-only version of a training function on a local cluster:

```{literalinclude} ../doc_code/debugging.py
:language: python
:start-after: __reproduce_start__
:end-before: __reproduce_end__
```

To unit test a training function, write the test the same way: start a small local cluster in a fixture, run a trainer with one or two CPU workers, and assert on the {class}`~ray.train.Result` or on the exception that `fit()` raises.

(train-debugging-logs)=

## Find the output and logs

Ray Train writes output from the training function and from its own components to the console and to log files.

### Console output

Ray forwards the stdout and stderr of every worker and of the controller to the driver console. Each line starts with the actor class name and process ID, such as `(RayTrainWorker pid=61382)` or `(TrainController pid=61363)`. The prefix doesn't include the worker rank. The {ref}`Ray Train log files <train-debugging-log-files>` record the rank on every line, so filter them to read the output of one rank. To map the process IDs in the console to ranks, use the table that the controller prints each time it starts a worker group:

```text
(TrainController pid=61363) Started training worker group of size 2:
(TrainController pid=61363) - (ip=127.0.0.1, pid=61383) world_rank=0, local_rank=0, node_rank=0
(TrainController pid=61363) - (ip=127.0.0.1, pid=61382) world_rank=1, local_rank=1, node_rank=0
```

Ray deduplicates identical lines that several processes log, and prints them once with a `[repeated Nx across cluster]` suffix. When every rank logs the same message, the deduplicated line shows the prefix of only one of them. To see every line from every rank, set `RAY_DEDUP_LOGS=0` before you start Ray. See {ref}`log-deduplication`.

The controller prints its state transitions to the console, such as `Requesting resources`, `Attempting to start training worker group`, and `Started training worker group`, and a `[FailurePolicy]` summary each time a failure occurs. Use these lines to tell which stage a run is in.

(train-debugging-log-files)=

### Ray Train log files

Ray Train writes JSON-formatted log files to the `logs/train/` directory of the Ray session directory on each node. On a single-node cluster, that's `/tmp/ray/session_latest/logs/train/` by default. See {ref}`logging-directory`.

```{list-table}
:header-rows: 1
:widths: 40 60

* - File
  - Contents
* - `ray-train-app-controller-<worker_id>.log`
  - Ray Train controller logs at the level that `LoggingConfig(log_level)` sets.
* - `ray-train-sys-controller-<worker_id>.log`
  - All Ray Train controller logs, including `DEBUG`.
* - `ray-train-app-worker-<worker_id>.log`
  - Ray Train logs and `print` output from one worker, at the level that `LoggingConfig(log_level)` sets.
* - `ray-train-sys-worker-<worker_id>.log`
  - All Ray Train logs from one worker, including `DEBUG`.
```

Each line of a worker log file includes the `run_name`, `world_rank`, `local_rank`, and `node_rank` of the worker, along with Ray IDs such as `node_id` and `actor_id`. Use them to filter the logs of one rank, for example with `jq`:

```bash
cat /tmp/ray/session_latest/logs/train/ray-train-app-worker-*.log \
    | jq -r 'select(.world_rank == 1) | "\(.asctime) \(.levelname) \(.message)"'
```

To print Ray Train's internal `DEBUG` logs to the console and the application log files, set the log level in the run configuration:

```python
from ray.train import LoggingConfig, RunConfig

run_config = RunConfig(logging_config=LoggingConfig(log_level="DEBUG"))
```

Workers on other nodes write their log files to that node's session directory. To read them from the head node, use the **Logs** view of the Ray dashboard or the `ray logs` command. See {ref}`observability-getting-started` and {ref}`configure-logging`.

(train-debugging-errors)=

## Debug a run that fails with an error

When training fails, `trainer.fit()` raises a {class}`~ray.train.TrainingFailedError`. The subclass tells you where the failure happened:

```{list-table}
:header-rows: 1
:widths: 30 70

* - Exception
  - Meaning
* - {class}`~ray.train.WorkerGroupError`
  - One or more workers failed. The training function raised an exception, a worker process died, or a worker stopped responding to the controller's health checks. `worker_failures` maps each failed world rank to its exception.
* - {class}`~ray.train.ControllerError`
  - The controller failed, for example because a callback raised. `controller_failure` holds the underlying exception.
* - {class}`~ray.train.PreemptionError`
  - The cluster preempted a node that a worker ran on. See {ref}`train-preemption-fault-tolerance`.
```

The message of a `WorkerGroupError` includes a traceback snippet for each failed rank:

```text
ray.train.WorkerGroupError: Training failed due to worker errors:
[Rank 1 Error Snippet]:
Traceback (most recent call last):
  File "train.py", line 8, in train_func
    raise ValueError("boom on rank 1")
ValueError: boom on rank 1
```

Ray Train truncates long snippets with `... (Output truncated. See individual worker logs for full details) ...`. In that case, find the full traceback in the console output of that rank, or in its `ray-train-app-worker-*.log` file.

To inspect the failures in code, catch the exception:

```python
import ray.train

try:
    result = trainer.fit()
except ray.train.WorkerGroupError as e:
    for rank, error in e.worker_failures.items():
        print(f"Rank {rank} failed: {error!r}")
except ray.train.ControllerError as e:
    print(f"Controller failed: {e.controller_failure!r}")
```

Look first at the rank that failed first. A failure on one rank often causes errors on the others, such as a collective that times out or a connection that the failed process closed, and these secondary errors are rarely the root cause.

(train-debugging-breakpoints)=

### Set breakpoints with the Ray Distributed Debugger

To step through the training function on a worker, use the {ref}`Ray Distributed Debugger <ray-distributed-debugger>`, a VS Code extension that attaches to paused Ray tasks and actors. Install and configure it as that guide describes, then call `breakpoint()` in the training function. Pause only one rank, so that you know which worker the debugger attaches to:

```python
import ray.train


def train_func():
    rank = ray.train.get_context().get_world_rank()
    for step in range(10):
        if rank == 0 and step == 3:
            breakpoint()
        ...
```

When a worker reaches the breakpoint, its console output shows `Ray debugger is listening on <ip>:<port>` and `Waiting for debugger to attach`, and the paused worker appears in the **Paused Tasks** list of the extension.

While one worker is paused, the other workers keep running until they block, for example in a collective or in `ray.train.report`, where they wait for the paused rank. Several timeouts keep running while you debug:

- The PyTorch process group times out a blocked collective after `TorchConfig(timeout_s)`, 30 minutes by default.
- The controller fails a worker that doesn't respond to health checks for `RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S`, 10 minutes by default.
- If you set `RAY_TRAIN_COLLECTIVE_TIMEOUT_S`, `ray.train.report` fails after that many seconds.

For a long debugging session, raise these timeouts so that the run isn't torn down while you inspect it.

:::{note}
Post-mortem debugging with `RAY_DEBUG_POST_MORTEM=1` doesn't pause a training worker when the training function raises. Ray Train catches the exception in the worker so that it can report the failure to the controller. As a result, Ray never sees an unhandled exception in the actor. To inspect the state at the point of failure, catch the exception in the training function and call `breakpoint()` in the `except` block.
:::

(train-debugging-hangs)=

## Debug a run that hangs

A run that hangs makes no progress and raises no error. First find out which stage it's stuck in, from the last controller lines in the console or from the `ray_train_controller_state` metric. See {ref}`train-metrics`. The following sections cover each stage.

(train-debugging-hang-scheduling)=

### Stuck before the workers start

If the last controller lines are `Requesting resources` or `Attempting to start training worker group` and no `Started training worker group` line follows, the cluster can't place the workers. Common causes are the following:

- The scaling configuration requests more resources than the cluster has, such as more GPUs than any node, or a resource type that no node provides.
- The cluster is autoscaling and new nodes aren't up yet.
- A `label_selector` or `placement_strategy="STRICT_SPREAD"` constraint can't be satisfied.

Run `ray status` to compare the resource demand with the cluster resources, and see the {ref}`Ray Train scaling configuration guide <train_scaling_config>`. If the workers don't start within `RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S`, 60 seconds by default, the controller logs a `WorkerGroupStartupTimeoutError` and a `[FailurePolicy] RETRY` line, then tries again. It retries with no limit on the number of attempts. On an autoscaling cluster, raise this timeout if node startup takes longer.

:::{caution}
On an autoscaling cluster, Ray Train compares the request with the maximum resources of the cluster configuration and fails right away with `Insufficient cluster resources to launch training workers` if the cluster can never satisfy it. On a cluster without an autoscaler, such as a local cluster, Ray Train can't tell an infeasible request from a temporary shortage, so the run repeats the startup timeout indefinitely. If the `WorkerGroupStartupTimeoutError` lines repeat, compare the request with the output of `ray status`.
:::

(train-debugging-hang-report)=

### Stuck in `ray.train.report`

`ray.train.report` is a barrier: every worker must call it the same number of times. If one rank skips a call, for example because it reports inside a condition that's true only on some ranks, the other ranks wait for it. While they wait, Ray Train logs a warning every `RAY_TRAIN_COLLECTIVE_WARN_INTERVAL_S` seconds, 60 by default, that names the missing ranks:

```text
(SynchronizationActor pid=61784) The workers have been waiting for 60.00 s for the following ranks to join the `ray.train.report` call: [1].
```

Check the stack trace of each missing rank to find where it is, as the next section shows. To fail the run instead of waiting indefinitely, set `RAY_TRAIN_COLLECTIVE_TIMEOUT_S`. Ray Train then raises a `BroadcastCollectiveTimeoutError` that lists the missing ranks.

(train-debugging-hang-stacks)=

### Stuck in the training function

To find where a worker is stuck, dump its stack trace while it hangs. Use any of the following tools, which all run `py-spy` on the worker process:

- In the **Actors** view of the Ray dashboard, find the `RayTrainWorker` actors and click **Stack Trace**. See {ref}`observability-debug-hangs`.
- Run `ray stack` on a node to dump the stack traces of all Ray worker processes on that node.
- Run `py-spy dump --pid <pid>` on the node of a worker, with the process ID from the worker group table. Add `--native` on Linux to include C++ and CUDA frames, which show whether a worker is blocked inside a NCCL kernel or in other native code.

Ray Train runs the training function in a thread named `TrainingThread(<function name>)`, so look for that thread in the dump. Compare the stacks of all ranks. Ranks that wait in different collectives, or in the same collective at different steps, point to rank-dependent control flow, such as a conditional `all_reduce` or a different number of batches per rank.

For GPU training with NCCL, also use the following:

- Enable the {ref}`NCCL hang detector <train-nccl-hang-detection>`, which detects ranks that disagree about the next collective and captures stack traces and NCCL diagnostics from every worker before the PyTorch watchdog tears them down.
- Lower `TorchConfig(timeout_s)` so that a hung collective fails in minutes instead of 30 minutes.
- Turn on NCCL and PyTorch distributed logging on the workers through the runtime environment:

  ```python
  import ray

  ray.init(
      runtime_env={
          "env_vars": {
              "NCCL_DEBUG": "INFO",
              "TORCH_DISTRIBUTED_DEBUG": "DETAIL",
          }
      }
  )
  ```

  `TORCH_DISTRIBUTED_DEBUG=DETAIL` checks that every rank calls the same collective with the same shapes, at a cost to performance, so use it only while you debug.

If the training function waits on data, see {ref}`train-debugging-data-loading-bottlenecks`.

(train-debugging-hang-unresponsive)=

### Workers stop responding

If a worker process stops responding, for example because it's stuck in native code that holds the Python global interpreter lock (GIL), the controller can't poll it. After `RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S`, 10 minutes by default, the controller marks the worker as failed with a `WorkerHealthCheckTimeoutError`, and the failure policy retries or raises. A worker that's killed, for example by the out-of-memory killer, fails its health check right away. Check `ray logs raylet.out` and the dashboard for out-of-memory events. See {ref}`ray-oom-monitor`.

(train-debugging-env-vars)=

## Debugging environment variables

Set the following environment variables in the environment of the driver before you construct the trainer. Ray Train propagates them to the controller and to the workers.

```{list-table}
:header-rows: 1
:widths: 40 15 45

* - Environment variable
  - Default
  - Effect
* - `RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S`
  - `60`
  - Seconds to wait for all workers to start before the controller tries again.
* - `RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S`
  - `600`
  - Seconds a worker can stay unresponsive before the controller marks it as failed.
* - `RAY_TRAIN_COLLECTIVE_WARN_INTERVAL_S`
  - `60`
  - Seconds between warnings that name the ranks that haven't reached `ray.train.report`.
* - `RAY_TRAIN_COLLECTIVE_TIMEOUT_S`
  - Unset, no timeout
  - Seconds before `ray.train.report` fails with a `BroadcastCollectiveTimeoutError`.
* - `RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR`
  - `0`
  - Set to `1` to enable the {ref}`NCCL hang detector <train-nccl-hang-detection>`.
```

The following settings aren't Ray Train environment variables, but they also help with debugging:

- `RAY_DEDUP_LOGS=0` turns off log deduplication. Set it before Ray starts.
- `TorchConfig(timeout_s)` sets the PyTorch process group timeout, 1800 seconds by default.
- `NCCL_DEBUG` and `TORCH_DISTRIBUTED_DEBUG` turn on NCCL and PyTorch distributed logging. Ray Train doesn't propagate them, so pass them through `runtime_env={"env_vars": {...}}`.

(train-debugging-agents)=

## Debug with a coding agent

A coding agent can't attach an interactive debugger, but it can run the following commands and read their output, which covers most of this guide:

- `python train.py 2>&1 | tee run.log` to capture the console output, including the worker group table and the per-rank error snippets.
- `ray status` to compare resource demand with cluster resources when the workers don't start.
- `ray stack` or `py-spy dump --pid <pid>` to dump the stack traces of hung workers.
- `jq` over `/tmp/ray/session_latest/logs/train/*.log` to filter logs by `world_rank` or `levelname`.
- `ray list actors --filter "class_name=RayTrainWorker"` to list the training workers with their process IDs and nodes.

Have the agent reproduce the problem with one or two CPU workers first, as {ref}`train-debugging-reproduce` describes, so that each attempt takes seconds. Also set `RAY_TRAIN_COLLECTIVE_TIMEOUT_S` and a low `TorchConfig(timeout_s)`, so that a hang turns into an error the agent can read instead of a command that never returns.
