---
myst:
  html_meta:
    description: "Validate checkpoints asynchronously so training continues while validation runs, with TorchTrainer and Ray Data approaches and subcluster isolation."
---

(train-validating-checkpoints)=

# Validate checkpoints asynchronously

To monitor training progress, you can validate the model periodically during training. The standard approach alternates between training and validation within the training loop. Ray Train can instead validate the model asynchronously in a separate Ray task, which does the following:

* Runs validation in parallel without blocking the training loop.
* Runs validation on different, potentially cheaper hardware than training. Validation doesn't require optimizer states or gradients, so it can use 2-4x less GPU memory.
* Uses {ref}`autoscaling <vms-autoscaling>` to launch machines you specify only for the duration of the validation.
* Continues training immediately after you save a checkpoint with partial metrics, such as loss, and receives validation metrics, such as accuracy, as soon as they're available. If the initial and validated metrics share a key, the validated metrics overwrite the initial metrics.

## When to use async validation

Use asynchronous validation instead of alternating between training and validation in the same training loop in the following scenarios:

* **Validation takes a large percentage of total training time.** Running validation asynchronously overlaps it with training, which can substantially reduce end-to-end wall-clock time.
* **Cheaper GPUs are available for validation.** Validation doesn't require optimizer states or gradients, so it can use 2-4x less GPU memory than training. If you have a pool of cheaper GPUs or an autoscaling setup that can provision them, async validation can run on those cheaper machines instead of occupying your expensive training GPUs.
* **Training throughput stops scaling linearly with more workers.** As the worker count increases, allreduce overhead grows and limits training speed, so doubling workers no longer doubles throughput. Validation scales more linearly because it requires no gradient synchronization, so asynchronous validation can use otherwise idle cluster capacity without affecting training.

To find out whether async validation helps your workload, run both approaches and compare. The following tutorial shows how to switch to async validation.

## Tutorial

First, define a `validation_fn` that takes a {class}`ray.train.Checkpoint` to validate and any number of JSON-serializable keyword arguments. The function returns a dictionary of metrics from that validation. The following example is for teaching purposes only. It's impractical because the validation task always runs on CPU. For a more realistic example, see {ref}`train-distributed-validate-fn`.

```{literalinclude} ../doc_code/asynchronous_validation.py
:language: python
:start-after: __validation_fn_simple_start__
:end-before: __validation_fn_simple_end__
```

:::{note}
In this example, the validation dataset is a `ray.data.Dataset` object, which isn't JSON-serializable. The example captures it in the `validation_fn` closure instead of passing it as a keyword argument.
:::

:::{warning}
Don't pass large objects to the `validation_fn`. Ray Train runs it as a Ray task and serializes all captured variables. Instead, package large objects in the `Checkpoint` and access them from shared storage later. See {ref}`train-checkpointing`.
:::

Next, register your `validation_fn` with your trainer. Set the trainer's `validation_config` argument to a {class}`~ray.train.v2.api.report_config.ValidationConfig` object that contains your `validation_fn` and any default keyword arguments to pass to it.

Then, in your rank 0 worker's training loop, call {func}`ray.train.report` with `validation` set to `True`. Ray Train then runs your `validation_fn` with the default keyword arguments you passed to the trainer. To override some of those defaults, set `validation` to a {class}`~ray.train.v2.api.report_config.ValidationTaskConfig` object instead. Its keyword arguments override the matching keyword arguments you passed to the trainer. If `validation` is `False`, Ray Train doesn't run validation.

```{literalinclude} ../doc_code/asynchronous_validation.py
:language: python
:start-after: __validation_fn_report_start__
:end-before: __validation_fn_report_end__
```

Finally, after training finishes, access your checkpoints and their associated metrics with the {class}`ray.train.Result` object. See {ref}`train-inspect-results` for details.

(train-distributed-validate-fn)=

## Write a distributed validation function

The preceding `validation_fn` runs in a single Ray task. To improve its performance, spawn more Ray tasks or actors with one of the following approaches:

* Creating a {class}`ray.train.torch.TorchTrainer` that only does validation, not training.
* Using {func}`ray.data.Dataset.map_batches` to calculate metrics on a validation set.

### Choose an approach

Use `TorchTrainer` if any of the following apply:

* You want to keep your existing validation logic and avoid migrating to Ray Data. With the training function API, you can fully customize the validation loop to match your current setup.
* Your validation code depends on running within a PyTorch process group. For example, your metric aggregation logic uses collective communication calls, or your model parallelism setup requires cross-GPU communication during the forward pass.
* You want a more consistent training and validation experience. The `map_batches` approach runs multiple Ray Data Datasets in a single Ray cluster, and the Ray team is still working on better support for this pattern.

Use `map_batches` if any of the following apply:

* You care about validation performance. Preliminary benchmarks show that `map_batches` is faster.
* You prefer Ray Data's native metric aggregation APIs over PyTorch, where you must implement aggregation manually with low-level collective operations or rely on third-party libraries such as [`torchmetrics`](https://lightning.ai/docs/torchmetrics/stable).

### Example: Validation with Ray Train `TorchTrainer`

The following `validation_fn` uses a `TorchTrainer` to calculate average cross-entropy loss on a validation set. Note the following about this example:

* You typically use `TorchTrainer` for training, but you can also use it for validation, as this example does. Training and validation can then have different resource requirements, such as A100 GPUs for training and A10G GPUs for validation.
* The validation training function returns its metrics directly from worker 0 instead of calling `ray.train.report`. Access them through `result.return_value`. As with `ray.train.report`, these values can't contain `torch` tensors, so convert them to Python objects first, for example with `.item()`.

```{literalinclude} ../doc_code/asynchronous_validation.py
:language: python
:start-after: __validation_fn_torch_trainer_start__
:end-before: __validation_fn_torch_trainer_end__
```

### Example: Validation with Ray Data `map_batches`

The following `validation_fn` uses {func}`ray.data.Dataset.map_batches` to calculate average accuracy on a validation set. To learn how to use `map_batches` for batch inference, see {ref}`batch_inference_home`.

```{literalinclude} ../doc_code/asynchronous_validation.py
:language: python
:start-after: __validation_fn_map_batches_start__
:end-before: __validation_fn_map_batches_end__
```

(isolating-training-and-validation-with-subclusters)=

## Isolate training and validation with subclusters

By default, when training and validation run concurrently on the same Ray cluster, they compete for the same nodes. To give each phase its own slice of the cluster, such as A100s for training and A10Gs for validation, label your worker pools with a `ray-subcluster` value and pin each Dataset to its subcluster. See {ref}`data_concurrent_execution` for background and compute-config setup.

The pattern differs slightly between the `TorchTrainer` and `map_batches` versions of `validation_fn`, because only the `TorchTrainer` version goes through `ray.train.DataConfig`.

For the `TorchTrainer` version, set the validation Dataset's selector through the sub-trainer's `dataset_config`:

```python
from ray.data import ExecutionOptions

def validation_fn(checkpoint, ...) -> dict:
    trainer = ray.train.torch.TorchTrainer(
        ...,
        datasets={"validation": validation_dataset},
        dataset_config=ray.train.DataConfig(
            execution_options={
                "validation": ExecutionOptions(
                    label_selector={"ray-subcluster": "validation"}
                ),
            },
        ),
    )
    ...
```

The `map_batches` version doesn't take a `DataConfig`. Instead, construct `validation_dataset` inside a `DataContext.current()` block. This block sets the selector on the Dataset at construction, and every downstream operator inherits it:

```python
ctx = ray.data.DataContext.get_current().copy()
ctx.execution_options.label_selector = {"ray-subcluster": "validation"}
with ray.data.DataContext.current(ctx):
    validation_dataset = ray.data.read_parquet(...)

def validation_fn(checkpoint) -> dict:
    eval_res = validation_dataset.map_batches(...)
    ...
```

On the training side, a Ray Train pipeline needs the selector in the following two places, which cover different phases and aren't redundant:

1. **At Dataset construction**, through the `DataContext.current()` context manager, so that construction-time tasks, such as Parquet schema inference and file listing, land on training nodes.
1. **In the trainer's** `dataset_config`, because at training start, Ray Train replaces `ds.context.execution_options` wholesale with the per-dataset entry from `DataConfig`. If you don't restate an option in `DataConfig.execution_options`, including `label_selector`, Ray Train drops it, and per-worker ingest loses its pinning.

```python
from ray.data import ExecutionOptions

def run_trainer() -> ray.train.Result:
    # (1) Pin construction-time tasks.
    ctx = ray.data.DataContext.get_current().copy()
    ctx.execution_options.label_selector = {"ray-subcluster": "training"}
    with ray.data.DataContext.current(ctx):
        train_dataset = ray.data.read_parquet(...)

    # (2) Pin per-worker ingest. Train replaces ds.context options
    # wholesale, so the selector must be restated here.
    trainer = ray.train.torch.TorchTrainer(
        ...,
        datasets={"train": train_dataset},
        dataset_config=ray.train.DataConfig(
            datasets_to_split=["train"],
            execution_options={
                "train": ExecutionOptions(
                    label_selector={"ray-subcluster": "training"}
                ),
            },
        ),
    )
    ...
```

:::{note}
*Interleaved* validation reuses the training workers to validate on a separate "validation" Dataset inside the same `TorchTrainer`. For interleaved validation, pass both Datasets to `datasets={...}` and give each one an entry in `DataConfig.execution_options` to scope it to its own subcluster:

```python
from ray.data import ExecutionOptions

dataset_config = ray.train.DataConfig(
    datasets_to_split=["train", "validation"],
    execution_options={
        "train": ExecutionOptions(
            label_selector={"ray-subcluster": "training"}
        ),
        "validation": ExecutionOptions(
            label_selector={"ray-subcluster": "validation"}
        ),
    },
)
```
:::

(tuning-asynchronous-validation)=

## Tune asynchronous validation

The following section describes how to tune asynchronous validation so that validation and training overlap.

(overlapping-validation-and-training)=

### Overlap validation and training

Asynchronous validation is most beneficial when training and validation fully overlap. If one finishes before the other, some workers sit idle. With {ref}`autoscaling <vms-autoscaling>`, you can start workers only for the duration of validation, which narrows the gap but doesn't fully eliminate it.

Tune the following settings to overlap validation and training as closely as possible:

* **Number of workers**: Adjust the number of validation workers relative to training workers so that the two phases overlap.
* **Batch size**: A larger batch size typically improves throughput, but it can hurt training convergence and might cause out-of-memory (OOM) errors.
* **Validation frequency**: Choose a validation cadence and dataset size that balance overlap with training. Validating too frequently or over too many rows can create a long validation tail.

:::{caution}
Breaking early from a Ray Data iterator might leak resources. A fix is planned for a future release.
:::

## Checkpoint metrics lifecycle

During the training loop, the following happens to your checkpoints and metrics:

1. You report a checkpoint with initial metrics, such as training loss, and a {class}`~ray.train.v2.api.report_config.ValidationTaskConfig` object that contains the keyword arguments to pass to the `validation_fn`.
1. Ray Train asynchronously runs your `validation_fn` with that checkpoint and configuration.
1. When the validation task completes, Ray Train associates the metrics that your `validation_fn` returns with that checkpoint.
1. After training finishes, you can access your checkpoints and their associated metrics with the {class}`ray.train.Result` object. See {ref}`train-inspect-results` for details.

```{figure} ../images/checkpoint_metrics_lifecycle.png
How Ray Train populates checkpoint metrics during training and how you access them after training.
```

## Experiment tracking

In standard {ref}`experiment tracking with Ray Train <train-experiment-tracking-native>`, you create, log to, and finish the experiment tracking run from the rank 0 training worker. Asynchronous validation complicates this because a separate Ray task computes the validation metrics outside the training worker.

Most modern experiment tracking configurations, such as [W&B distributed training](https://docs.wandb.ai/models/track/log/distributed-training#track-all-processes-to-a-single-run), support writing to the same run from different threads or processes. Others, such as the [MLflow fluent API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.html), might not.

(writing-to-the-same-run)=

### Write to the same run

If your experiment tracking library supports writing to the same run from different processes, start the run from the rank 0 training worker, then join it from the validation task and log validation metrics directly.

::::{tab-set}
:::{tab-item} W&B
```{literalinclude} ../doc_code/asynchronous_validation.py
:language: python
:start-after: __exp_tracking_same_run_wandb_start__
:end-before: __exp_tracking_same_run_wandb_end__
```
:::

:::{tab-item} MLflow `MlflowClient`
```{literalinclude} ../doc_code/asynchronous_validation.py
:language: python
:start-after: __exp_tracking_same_run_mlflow_start__
:end-before: __exp_tracking_same_run_mlflow_end__
```
:::
::::

### Reliability

If experiment tracking logging fails, for example because of a transient network error, retry it in one of two ways:

1. **Wrap your logging calls in a `try`/`except` block** inside the `validation_fn` and retry manually with your experiment tracker's API.
1. **Use** {func}`ray.train.get_all_reported_checkpoints` **periodically during training** to retrieve all reported checkpoints and their associated metrics, then re-log any missing entries to your experiment tracker.

(writing-to-different-runs)=

### Write to different runs

If your experiment tracking library doesn't support writing to the same run from different processes, the validation task must start a new run each time it logs validation metrics. Many tracking libraries can group related runs so that training and validation runs stay associated.

::::{tab-set}
:::{tab-item} W&B
Use [W&B run grouping](https://docs.wandb.ai/models/runs/grouping) to group the training run and validation runs together.
:::

:::{tab-item} MLflow
Use [MLflow parent and child runs](https://mlflow.org/docs/latest/ml/traditional-ml/tutorials/hyperparameter-tuning/part1-child-runs/#adapting-for-parent-and-child-runs) to group the training run and validation runs together.
:::
::::
