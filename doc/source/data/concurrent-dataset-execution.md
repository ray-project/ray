---
myst:
  html_meta:
    description: "Run multiple Datasets in one Ray cluster without contention by labeling worker nodes and tagging each Dataset with a label selector."
---

(data_concurrent_execution)=

# Run multiple Datasets in one cluster

When two or more Datasets share a single Ray cluster, they compete for the same pool of nodes by default. That competition can cause contention. One Dataset's reads can starve a second Dataset's GPU stage, autoscaling decisions get muddled, and runtime depends on whatever else happens to be running.

To avoid contention, assign each Dataset to its own *subcluster*, a labeled subset of nodes that only that Dataset uses. Subclusters make execution predictable for concurrent Datasets, and they give you a direct way to say "this Dataset runs here, that one runs there."

Subclusters fit use cases such as the following:

- **Asynchronous validation during training**: A training Dataset feeds the trainer. A validation Dataset feeds a separate validation task on different hardware. See {ref}`train-validating-checkpoints` for the Ray Train integration.
- **Multitenancy on a shared workspace**: Several Datasets share one Anyscale workspace without disturbing each other. The Datasets can belong to different users, different pipelines, or different stages of one workflow.

## How do subclusters work?

Each Dataset carries an `ExecutionOptions.label_selector`, a `Dict[str, str]` that Ray Data attaches to every task and actor the Dataset launches. The autoscaling coordinator buckets nodes by the value at the reserved label key `"ray-subcluster"` and only places a Dataset's work on nodes whose label matches.

## Assign each Dataset to a subcluster

Assigning a Dataset to a subcluster takes two steps.

### 1. Label your worker nodes

Label each worker node with the reserved key `ray-subcluster` to mark which subcluster it belongs to. See {ref}`labels` for how to configure labels. Depending on your deployment, set labels in the cluster YAML config, in KubeRay, or with `ray start --labels`.

The following example sets the labels in a Ray cluster YAML config:

```yaml
available_node_types:
  train_workers:
    min_workers: 2
    max_workers: 4
    labels:
      ray-subcluster: training
    node_config:
      InstanceType: g5.xlarge
  validation_workers:
    min_workers: 0
    max_workers: 2
    labels:
      ray-subcluster: validation
    node_config:
      InstanceType: g4dn.xlarge
```

Subcluster values are arbitrary strings, such as `"training"`, `"validation"`, `"tenant_a"`, or `"team-blue"`. Pick whatever makes sense for your workload.

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

Set the selector *before* creating the Dataset, not after. Tasks that Ray Data spawns during construction, such as the Parquet read tasks that infer the schema, read the current context. Setting `dataset.context.execution_options.label_selector` afterward doesn't re-route those tasks.
:::

## Example: Two Datasets, two subclusters

The following example constructs two Datasets, each with its own subcluster selector, and then materializes them concurrently in separate threads.

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

(ray-train-integration)=

## Use subclusters with Ray Train

When you pass the Datasets to a `TorchTrainer` or any other `DataParallelTrainer`, `ray.train.DataConfig` is the more convenient entry point. It takes a per-dataset `ExecutionOptions` map. See {ref}`train-validating-checkpoints` for the full pattern, including how to set the training-side selector through `DataConfig` and the validation-side selector inside your `validation_fn`.

## API reference

See the following classes for the full API:

- {class}`ray.data.ExecutionOptions`: See the `label_selector` parameter.
- {class}`ray.data.DataContext`: The per-process Ray Data configuration that holds `execution_options`.
- {class}`ray.train.DataConfig`: Accepts a `Dict[str, ExecutionOptions]` so each Train dataset can carry its own selector.
