---
myst:
  html_meta:
    description: "Tune hyperparameters for a Ray Train run with Ray Tune, covering per-trial resources, concurrency limits, and metric reporting."
---

(train-tune)=

# Hyperparameter tuning with Ray Tune

:::{important}
This guide shows how to integrate Ray Train and Ray Tune to tune hyperparameters for distributed training runs with Ray Train V2. Ray Train V2 is available starting in Ray 2.43 when you set the environment variable `RAY_TRAIN_V2_ENABLED=1`. This guide assumes that you've set this environment variable.

For information about the deprecation and migration, see {ref}`train-tune-deprecation`.
:::


You can combine Ray Train with Ray Tune to run hyperparameter sweeps over distributed training runs. This combination is often useful for a small sweep over critical hyperparameters before you launch a long run with the best-performing hyperparameters on all available cluster resources.

## Quickstart

The following example uses these components:

* {class}`~ray.tune.Tuner` launches the tuning job, which runs trials of `train_driver_fn` with different hyperparameter configurations.
* `train_driver_fn` takes in a hyperparameter configuration, instantiates a `TorchTrainer` or another framework's trainer, and launches the distributed training job.
* {class}`~ray.train.ScalingConfig` defines the number of training workers and resources per worker for a single Ray Train run.
* `train_fn_per_worker` is the Python code that executes on each distributed training worker for a trial.

```{literalinclude} ../doc_code/train_tune_interop.py
:language: python
:start-after: __quickstart_start__
:end-before: __quickstart_end__
```


## What does Ray Tune provide?

Ray Tune provides utilities for the following tasks:

* {ref}`Defining hyperparameter search spaces <tune-search-space-tutorial>` and {ref}`launching multiple trials concurrently <tune-parallel-experiments-guide>` on a Ray cluster.
* {ref}`Using search algorithms <tune-search-alg>`.
* {ref}`Early stopping runs based on metrics <tune-stopping-guide>`.

This guide focuses only on the integration layer between Ray Train and Ray Tune. For details on using Ray Tune, see the {ref}`Ray Tune documentation <tune-main>`.


(configuring-resources-for-multiple-trials)=

## Configure resources for multiple trials

Ray Tune launches multiple trials that each {ref}`run a user-defined function in a remote Ray actor <tune-function-api>`. Each trial gets a different sampled hyperparameter configuration.

When you use Ray Tune by itself, trials do computation directly inside the Ray actor. For example, each trial could request 1 GPU and run single-process model training within the remote actor. When you use Ray Train inside Ray Tune functions, the Tune trial doesn't do extensive computation inside this actor. Instead, it acts as a driver process that launches and monitors the Ray Train workers running elsewhere.

Ray Train requests its own resources through the {class}`~ray.train.ScalingConfig`. For details, see {ref}`train_scaling_config`.

```{figure} ../images/hyperparameter_optimization/train_without_tune.png
:align: center

A single Ray Train run. The next figure shows how Ray Tune adds a layer of hierarchy to this tree of processes.
```


```{figure} ../images/hyperparameter_optimization/train_tune_interop.png
:align: center

Ray Train runs launched from within Ray Tune trials.
```


### Limit the number of concurrent Ray Train runs

A Ray Train run starts only when it can acquire resources for all of its workers at once. As a result, multiple Tune trials that spawn Train runs compete for the logical resources available in the Ray cluster.

If a cluster resource such as GPUs is limited, you can't run training for all hyperparameter configurations concurrently. Because the cluster has enough resources for only a handful of concurrent trials, set {class}`tune.TuneConfig(max_concurrent_trials) <ray.tune.TuneConfig>` on the Tuner to limit the number of in-flight Train runs so that no trial starves for resources.

```{literalinclude} ../doc_code/train_tune_interop.py
:language: python
:start-after: __max_concurrent_trials_start__
:end-before: __max_concurrent_trials_end__
```


For example, consider a fixed-size cluster with 128 CPUs and 8 GPUs.

* The `Tuner(param_space)` runs a grid search over four hyperparameter configurations, defined by `param_space={"train_loop_config": {"batch_size": tune.grid_search([8, 16, 32, 64])}}`.
* Each Ray Train run trains with four GPU workers, as set by `ScalingConfig(num_workers=4, use_gpu=True)`. Because the cluster has only 8 GPUs, only two Train runs can acquire their full set of resources at a time.
* However, the cluster has many CPUs, and each Ray Tune trial requests 1 CPU by default, so Ray Tune launches all four trials immediately. That launches two extra Ray Tune trial processes whose inner Ray Train runs wait for resources until one of the other trials finishes. While Train waits for resources, it emits noisy, repetitive log messages. If the total number of hyperparameter configurations is large, there might also be an excessive number of Ray Tune trial processes.
* To fix this issue, set `Tuner(tune_config=tune.TuneConfig(max_concurrent_trials=2))`. Only two Ray Tune trial processes then run at a time. Calculate this number from the limiting cluster resource and the amount of that resource each trial requires.


### Advanced: Set Train driver resources

By default, the Train driver runs as a Ray Tune function with 1 CPU. Ray Tune schedules these functions on any node in the cluster that has free logical CPU resources.

:::{tip}
If you launch longer-running training jobs or use spot instances, run the Tune functions that act as the Ray Train driver process on *safe nodes*, which are at lower risk of going down. For example, don't schedule them on preemptible spot instances, and don't run them on the same nodes as training workers. A safe node could be the head node or a dedicated CPU node in your cluster.
:::

Placing the driver on a safe node matters because the Ray Train driver process handles fault tolerance for the worker processes, which are more likely to fail. Nodes that run Train workers can crash because of spot preemption or other errors that come from your model training code.

* If a Train worker node dies, the Ray Train driver process on a different node is still alive and can handle the error gracefully.
* If the driver process dies, all Ray Train workers exit ungracefully, and some of the run state might not be fully committed.

One way to place the Train driver on safe nodes is to set custom resources on certain node types and configure the Tune functions to request those resources.

```{literalinclude} ../doc_code/train_tune_interop.py
:language: python
:start-after: __trainable_resources_start__
:end-before: __trainable_resources_end__
```


(reporting-metrics-and-checkpoints)=

## Report metrics and checkpoints

Ray Train and Ray Tune both provide utilities to upload and track checkpoints through the {func}`ray.train.report <ray.train.report>` and {func}`ray.tune.report <ray.tune.report>` APIs. For details, see the {ref}`train-checkpointing` user guide.

If the Ray Train workers report checkpoints, you don't need to save another Ray Tune checkpoint at the Train driver level, because the driver doesn't hold any extra training state. The Ray Train driver process already snapshots its status periodically to the configured `storage_path`. For details, see {ref}`train-job-driver-fault-tolerance`.

To access the checkpoints from the Tuner output, append the checkpoint path as a metric. The provided {class}`~ray.tune.integration.ray_train.TuneReportCallback` does this. It propagates reported Ray Train results to Ray Tune, where the checkpoint path is attached as a separate metric.


### Advanced: Fault tolerance

To recover when the Ray Tune trials that run the Ray Train driver process crash, enable trial fault tolerance on the Ray Tune side with {class}`ray.tune.Tuner(run_config=ray.tune.RunConfig(failure_config)) <ray.tune.FailureConfig>`.

Fault tolerance on the Ray Train side is configured and handled separately. For details, see the {ref}`train-fault-tolerance` user guide.

```{literalinclude} ../doc_code/train_tune_interop.py
:language: python
:start-after: __fault_tolerance_start__
:end-before: __fault_tolerance_end__
```


(train-with-tune-callbacks)=

(advanced-using-ray-tune-callbacks)=

### Advanced: Use Ray Tune callbacks

Pass Ray Tune callbacks into {class}`ray.tune.RunConfig(callbacks) <ray.tune.RunConfig>` at the Tuner level.

If you depend on the behavior of built-in or custom Ray Tune callbacks, run Ray Train as a single-trial Tune run and pass the callbacks to the Tuner.

If any callback depends on reported metrics, pass {class}`ray.tune.integration.ray_train.TuneReportCallback` to the trainer callbacks. This callback propagates results to the Tuner.


```{testcode}
:skipif: True

import ray.tune
from ray.tune.integration.ray_train import TuneReportCallback
from ray.tune.logger import TBXLoggerCallback


def train_driver_fn(config: dict):
    trainer = TorchTrainer(
        ...,
        run_config=ray.train.RunConfig(..., callbacks=[TuneReportCallback()])
    )
    trainer.fit()


tuner = ray.tune.Tuner(
    train_driver_fn,
    run_config=ray.tune.RunConfig(callbacks=[TBXLoggerCallback()])
)
```


(train-tune-deprecation)=

## `Tuner(trainer)` API deprecation

The `Tuner(trainer)` API takes a Ray Train trainer instance directly. It's deprecated as of Ray 2.43 and will be removed in a future release.

### Motivation

This API change decouples the responsibilities of Ray Train and Ray Tune, and it makes hyperparameter and run configuration more explicit and flexible.

### Migration steps

To migrate from the `Tuner(trainer)` API to the function-based pattern, do the following:

1. Enable the environment variable `RAY_TRAIN_V2_ENABLED=1`.
1. Replace `Tuner(trainer)` with a function-based approach that launches Ray Train inside a Tune trial.
1. Move your training logic into a driver function that Tune calls with different hyperparameters.

### Additional resources

* [Train V2 Ray Enhancement Proposal (REP)](https://github.com/ray-project/enhancements/blob/main/reps/2024-10-18-train-tune-api-revamp/2024-10-18-train-tune-api-revamp.md): Technical details about the API change
* [Train V2 migration guide](https://github.com/ray-project/ray/issues/49454): Full migration guide for Train V2
* {ref}`train-tune-deprecated-api`: Documentation for the old API
