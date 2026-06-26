(data_concurrent_execution)=

# Run multiple Datasets in one cluster

When two or more Ray Data Datasets share a single Ray cluster, they compete for the same pool of nodes by default. That competition can cause unwanted contention — one Dataset's reads can starve a second Dataset's GPU stage, autoscaling decisions get muddled, and runtime becomes a function of whatever else happens to be running.

Ray Data lets you assign each Dataset to its own **subcluster** — a labeled subset of nodes that only that Dataset uses. Subclusters give you smooth, predictable execution for concurrent Datasets and a natural way to express "this Dataset runs here, that one runs there."

Common use cases:

- **Asynchronous validation during training.** A training Dataset feeds the trainer. A validation Dataset feeds a separate validation task on different hardware. See {ref}`train-validating-checkpoints` for the Ray Train integration.
- **Multitenancy on a shared workspace.** Several Datasets — different users, different pipelines, or different stages of one workflow — share one Anyscale workspace and don't disturb each other.

## How it works

Each Dataset carries an `ExecutionOptions.label_selector` (a `Dict[str, str]`) that Ray Data attaches to every task and actor the Dataset launches. The autoscaling coordinator buckets nodes by the value at the reserved label key `"ray-subcluster"` and only places a Dataset's work on nodes whose label matches.

## Configuration

There are two steps.

### 1. Label your worker nodes

In your Anyscale [compute config](https://docs.anyscale.com/reference/compute-config-api/), add a `labels` block to each worker pool. Use the reserved key `ray-subcluster` to mark which subcluster each pool belongs to.

```yaml
worker_nodes:
  - name: train-workers
    instance_type: g5.xlarge
    min_nodes: 2
    max_nodes: 4
    labels:
      ray-subcluster: training
  - name: validation-workers
    instance_type: g4dn.xlarge
    min_nodes: 0
    max_nodes: 2
    labels:
      ray-subcluster: validation
```

Subcluster values are arbitrary strings (`"training"`, `"validation"`, `"tenant_a"`, `"team-blue"`) — pick whatever makes sense for your workload.

### 2. Tag each Dataset with a `label_selector`

Copy the current `DataContext`, set the selector on the copy, and apply the copy temporarily with the `DataContext.current()` context manager. Construct your Dataset inside the `with` block:

```python
import ray

ctx = ray.data.DataContext.get_current().copy()
ctx.execution_options.label_selector = {"ray-subcluster": "tenant_a"}

with ray.data.DataContext.current(ctx):
    # Tasks launched during construction (reads, schema inference) read
    # the temporary context. ``Dataset.context`` is a deep copy of the
    # current context, so the new Dataset keeps the selector after the
    # ``with`` block exits.
    dataset = ray.data.read_parquet("s3://my-bucket/tenant_a/")
```

:::{important}
Mutating `ray.data.DataContext.get_current()` in place permanently affects every subsequent Dataset in the same driver process. Use the `DataContext.current()` context manager to scope each Dataset's selector to its own construction block.

Set the selector *before* creating the Dataset, not after. Tasks Ray Data spawns during construction (for example, the parquet read tasks that infer the schema) read the current context, so setting `dataset.context.execution_options.label_selector` after the fact doesn't retroactively re-route them.
:::

## Example: two Datasets, two subclusters

```python
import ray
import threading


def make_dataset(subcluster: str, path: str) -> ray.data.Dataset:
    ctx = ray.data.DataContext.get_current().copy()
    ctx.execution_options.label_selector = {"ray-subcluster": subcluster}
    with ray.data.DataContext.current(ctx):
        return ray.data.read_parquet(path)


# Construct each Dataset in the main thread so the temporary contexts
# don't race on the process-global ``_default_context``.
ds_a = make_dataset("tenant_a", "s3://my-bucket/tenant_a/")
ds_b = make_dataset("tenant_b", "s3://my-bucket/tenant_b/")

# Then run them concurrently. ds_a's tasks only land on
# ray-subcluster=tenant_a nodes; ds_b's only on
# ray-subcluster=tenant_b nodes.
threading.Thread(target=lambda: ds_a.materialize()).start()
threading.Thread(target=lambda: ds_b.materialize()).start()
```

## Ray Train integration

When you wire the Datasets into a `TorchTrainer` (or any `DataParallelTrainer`), `ray.train.DataConfig` is the more ergonomic entry point — it takes a per-dataset `ExecutionOptions` map. See {ref}`train-validating-checkpoints` for the full pattern, including how to set the training-side selector through `DataConfig` and the validation-side selector inside your `validation_fn`.

## API reference

- {class}`ray.data.ExecutionOptions` — see the `label_selector` parameter.
- {class}`ray.data.DataContext` — the per-process Ray Data configuration the `execution_options` live on.
- {class}`ray.train.DataConfig` — accepts a `Dict[str, ExecutionOptions]` so each Train dataset can carry its own selector.
