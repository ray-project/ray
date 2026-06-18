(data_concurrent_execution)=

# Run multiple Datasets in one cluster

When two or more Ray Data Datasets share a single Ray cluster, they compete
for the same pool of nodes by default. That competition can cause unwanted
contention — one Dataset's reads can starve a second Dataset's GPU stage,
autoscaling decisions get muddled, and runtime becomes a function of
whatever else happens to be running.

Ray Data lets you assign each Dataset to its own **subcluster** — a labeled
subset of nodes that only that Dataset uses. Subclusters give you smooth,
predictable execution for concurrent Datasets and a natural way to express
"this Dataset runs here, that one runs there."

Common use cases:

- **Asynchronous validation during training.** A training Dataset feeds the
  trainer; a validation Dataset feeds a separate validation task on
  different hardware. See {ref}`train-validating-checkpoints` for the Ray
  Train integration.
- **Multitenancy on a shared workspace.** Several Datasets — different
  users, different pipelines, or different stages of one workflow — share
  one Anyscale workspace and don't disturb each other.

## How it works

Each Dataset carries an `ExecutionOptions.label_selector` (a
`Dict[str, str]`) that Ray Data attaches to every task and actor the Dataset
launches. The autoscaling coordinator buckets nodes by the value at the
reserved label key `"ray-subcluster"` and only places a Dataset's work on
nodes whose label matches.

## Configuration

There are two steps.

### 1. Label your worker nodes

In your Anyscale [compute config](https://docs.anyscale.com/reference/compute-config-api/),
add a `labels` block to each worker pool. Use the reserved key
`ray-subcluster` to mark which subcluster each pool belongs to.

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

Subcluster values are arbitrary strings (`"training"`, `"validation"`,
`"tenant_a"`, `"team-blue"`, etc.) — pick whatever makes sense for your
workload.

### 2. Tag each Dataset with a `label_selector`

Set the selector on the **global** `DataContext` *before* creating the
Dataset:

```python
import ray

# Set the global DataContext first.
ctx = ray.data.DataContext.get_current()
ctx.execution_options.label_selector = {"ray-subcluster": "tenant_a"}

# Now create the Dataset. ``Dataset.context`` is a deep copy of the global
# ``DataContext`` taken at construction time, so it inherits the selector
# automatically — you don't need to also set ``dataset.context``.
dataset = ray.data.read_parquet("s3://my-bucket/tenant_a/")
```

:::{important}
Set the global `DataContext` selector *before* creating the Dataset, not
after. Tasks Ray Data spawns during construction (for example, the parquet
read tasks that infer the schema) read the global context directly, so
setting `dataset.context.execution_options.label_selector` after the fact
doesn't retroactively re-route them.
:::

If you have more than one Dataset in the same driver, set the global
context to each Dataset's subcluster *just before* you create that Dataset.

## Example: two Datasets, two subclusters

```python
import ray
import threading

ctx = ray.data.DataContext.get_current()

# Tenant A: subcluster "tenant_a".
ctx.execution_options.label_selector = {"ray-subcluster": "tenant_a"}
ds_a = ray.data.read_parquet("s3://my-bucket/tenant_a/")

# Tenant B: subcluster "tenant_b". Set the global context again
# right before creating ds_b.
ctx.execution_options.label_selector = {"ray-subcluster": "tenant_b"}
ds_b = ray.data.read_parquet("s3://my-bucket/tenant_b/")

# Run them concurrently. ds_a's tasks only land on
# ray-subcluster=tenant_a nodes; ds_b's only on
# ray-subcluster=tenant_b nodes.
threading.Thread(target=lambda: ds_a.materialize()).start()
threading.Thread(target=lambda: ds_b.materialize()).start()
```

## Ray Train integration

When the Datasets are wired into a `TorchTrainer` (or any
`DataParallelTrainer`), `ray.train.DataConfig` is the more ergonomic entry
point — it takes a per-dataset `ExecutionOptions` map. See
{ref}`train-validating-checkpoints` for the full pattern, including how to
set the training-side selector through `DataConfig` and the
validation-side selector inside your `validation_fn`.

## API reference

- {class}`ray.data.ExecutionOptions` — see the `label_selector` parameter.
- {class}`ray.data.DataContext` — the per-process Ray Data configuration
  the `execution_options` live on.
- {class}`ray.train.DataConfig` — accepts a `Dict[str, ExecutionOptions]`
  so each Train dataset can carry its own selector.
