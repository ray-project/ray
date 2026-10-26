---
myst:
  html_meta:
    description: "Control Tune parallelism and resources: request GPUs per trial, run distributed training inside trials, and limit trial concurrency."
---

(tune-parallelism)=

# A guide to parallelism and resources for Ray Tune

The resources each trial requests and the resources available to Ray Tune, as `ray.cluster_resources()` reports them, determine how many trials run in parallel. By default, each trial requests 1 CPU and 0 GPUs.

By default, Tune runs `N` concurrent trials, where `N` is the number of CPU cores on your machine.

```python
# If you have 4 CPUs on your machine, this will run 4 concurrent trials at a time.
tuner = tune.Tuner(
    trainable,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()
```

To override the per-trial resources, use {func}`tune.with_resources <ray.tune.with_resources>`. Specify the resource request as either a dictionary or a {class}`PlacementGroupFactory <ray.tune.execution.placement_groups.PlacementGroupFactory>` object. In either case, Tune tries to start a placement group for each trial.

```python
# If you have 4 CPUs on your machine, this will run 2 concurrent trials at a time.
trainable_with_resources = tune.with_resources(trainable, {"cpu": 2})
tuner = tune.Tuner(
    trainable_with_resources,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()

# If you have 4 CPUs on your machine, this will run 1 trial at a time.
trainable_with_resources = tune.with_resources(trainable, {"cpu": 4})
tuner = tune.Tuner(
    trainable_with_resources,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()

# Fractional values are also supported, (i.e., {"cpu": 0.5}).
# If you have 4 CPUs on your machine, this will run 8 concurrent trials at a time.
trainable_with_resources = tune.with_resources(trainable, {"cpu": 0.5})
tuner = tune.Tuner(
    trainable_with_resources,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()

# Custom resource allocation via lambda functions are also supported.
# If you want to allocate gpu resources to trials based on a setting in your config
trainable_with_resources = tune.with_resources(trainable,
    resources=lambda config: {"gpu": 1} if config["use_gpu"] else {"gpu": 0})
tuner = tune.Tuner(
    trainable_with_resources,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()
```

Tune allocates the CPUs and GPUs that you specify in `tune.with_resources` to each trial. Even if a trial can't be scheduled yet, Tune still tries to start its placement group. If not enough resources are available and you use the Ray cluster launcher, the placement group request triggers {ref}`autoscaling behavior <cluster-index>`.

You can also request memory, in bytes, with the `"memory"` key, and request custom resources.

If your trainable starts more remote workers, pass a placement group factory object to request resources for them. See the {class}`PlacementGroupFactory documentation <ray.tune.execution.placement_groups.PlacementGroupFactory>` for details. The same applies if you use other libraries built on Ray, such as Modin. If you don't set resources correctly, the cluster might deadlock and hang.

:::{note}
Tune uses the resources you specify this way only to schedule trials. It doesn't automatically enforce them on your trainable. Make sure your trainable has enough resources to run, for example by setting `n_jobs` on a scikit-learn model to match.
:::

## Use GPUs in Tune

To use GPUs, set `gpu` in `tune.with_resources(trainable, resources_per_trial)`. Setting `gpu` also sets `CUDA_VISIBLE_DEVICES` for each trial automatically.

```python
# If you have 8 GPUs, this will run 8 trials at once.
trainable_with_gpu = tune.with_resources(trainable, {"gpu": 1})
tuner = tune.Tuner(
    trainable_with_gpu,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()

# If you have 4 CPUs and 1 GPU on your machine, this will run 1 trial at a time.
trainable_with_cpu_gpu = tune.with_resources(trainable, {"cpu": 2, "gpu": 1})
tuner = tune.Tuner(
    trainable_with_cpu_gpu,
    tune_config=tune.TuneConfig(num_samples=10)
)
results = tuner.fit()
```

For an example, see the {doc}`Keras MNIST example </tune/examples/tune-mnist-keras>`.

:::{warning}
If you don't set `gpu`, the `CUDA_VISIBLE_DEVICES` environment variable is set to an empty value, so the trial can't access any GPUs.
:::

:::{tip}
A new trial might occasionally run into GPU memory issues. The cause might be that the previous trial didn't clean up its GPU state fast enough. To avoid this, you can use {func}`tune.utils.wait_for_gpu <ray.tune.utils.wait_for_gpu>`.
:::

(tune-dist-training)=

## Run distributed training with Tune

To tune distributed training jobs, you can use Ray Tune with Ray Train. Tune runs multiple trials in parallel, and each trial runs distributed training with Ray Train.

For more details, see {ref}`Ray Train Hyperparameter Optimization <train-tune>`.

## Limit concurrency in Tune

To limit the number of trials that run concurrently, set `max_concurrent_trials` in {class}`TuneConfig <ray.tune.tune_config.TuneConfig>`.

The actual parallelism can be lower than `max_concurrent_trials`, because it depends on how many trials fit in the cluster at once. For example, if each trial requires 16 GPUs, your cluster has 32 GPUs, and `max_concurrent_trials=10`, the `Tuner` can only run two trials concurrently.

```python
from ray.tune import TuneConfig

config = TuneConfig(
    # ...
    num_samples=100,
    max_concurrent_trials=10,
)
```
