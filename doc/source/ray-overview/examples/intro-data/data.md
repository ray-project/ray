# Introduction to Ray Data

This template provides a comprehensive introduction to **Ray Data** — a scalable data processing library for AI workloads built on [Ray](https://docs.ray.io/en/latest/). You will learn what Ray Data is, why it matters for ML pipelines, and how to use its core APIs hands-on with an MNIST image classification example.

In the first half, we'll walk through the core Ray Data workflow — loading, transforming, and persisting data — with concepts explained along the way. The second half covers advanced topics you can explore as your workloads grow.

**Core:**
- **Part 1:** When to Use Ray Data
- **Part 2:** Loading Data
- **Part 3:** Lazy Execution
- **Part 4:** Transforming Data with `map_batches`
- **Part 5:** Stateful Transformations and Batch Inference
- **Part 6:** Data Operations — Groupby, Aggregation
- **Part 7:** Observability
- **Part 8:** Materializing and Persisting Data

**Advanced topics:**
- **Part 9:** Preprocessing and Expressions
- **Part 10:** Shuffling
- **Part 11:** Resource Management and Performance Tuning
- **Part 12:** Fault Tolerance and Checkpointing
- **Summary and Next Steps**

## Imports

```python
!pip install -q "torch==2.10.0" "torchvision==0.25.0"
```

```python
import numpy as np
import torch
from torchvision.transforms import Compose, ToTensor, Normalize
from matplotlib import pyplot as plt

import ray
```

### Note on Storage

Throughout this tutorial, we use `/mnt/cluster_storage` to represent a shared storage location. In a multi-node cluster, Ray workers on different nodes cannot access the head node's local file system. Use a [shared storage solution](https://docs.anyscale.com/configuration/storage#shared) accessible from every node.

## Part 1: When to Use Ray Data

Consider using Ray Data when your project meets one or more of these criteria:

| **Challenge** | **Ray Data Solution** |
|---------------|----------------------|
| **Operating on large datasets** | Distributes data loading and processing across a Ray cluster with streaming execution |
| **Feeding data into distributed training** | Streams data directly to [Ray Train](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html) workers with configurable batch sizes — no intermediate disk writes needed. See the **Introduction to Ray Train** template for a hands-on walkthrough. |
| **Running batch inference at scale** | Maximizes GPU utilization by streaming data through model inference actors |
| **Building reliable data pipelines** | Leverages Ray Core's fault-tolerance mechanisms — retries, checkpointing, and recovery |

Ray Data features a **streaming execution engine** that processes data in a pipelined fashion across a heterogeneous cluster of CPUs and GPUs. You write everything in native Python — your existing functions and libraries (NumPy, PyTorch, etc.) work as-is — and Ray Data handles the distribution, avoiding full materialization in memory and keeping all hardware utilized.

To understand why this matters, consider the difference between traditional batch processing and streaming:

**Traditional batch processing** completes one stage fully before starting the next, leading to idle resources:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/cko-2025-q1/batch-processing.png" width="80%" alt="Traditional Batch Processing">

**Streaming pipelining** overlaps stages, keeping all hardware (CPUs and GPUs) busy simultaneously:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/cko-2025-q1/pipelining.png" width="80%" alt="Streaming Model Pipelining">

This is critical for GPU-heavy workloads: while the GPU runs inference on one batch, the CPU can preprocess the next batch.

## Part 2: Loading Data

Ray Data provides built-in connectors for common formats — Parquet, CSV, JSON, images, text — as well as sources such as HuggingFace Datasets, Delta Lake, Iceberg, and in-memory objects (NumPy, Pandas, PyTorch). See the full list in the [Input/Output docs](https://docs.ray.io/en/latest/data/api/input_output.html) and the [data loading guide](https://docs.ray.io/en/latest/data/loading-data.html).

Under the hood, Ray Data uses Ray tasks to read data from remote storage. It creates read tasks proportional to the number of CPUs in your cluster, and each task produces output **blocks**:

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/dataset-read-many-to-many.png" width="90%">

Let's load MNIST image data from S3:

```python
ds = ray.data.read_images(
    "s3://anyscale-public-materials/ray-ai-libraries/mnist/50_per_index/",
    include_paths=True,
)
```

A **Dataset** is a distributed collection of **blocks** — contiguous subsets of rows stored as PyArrow tables. Blocks are distributed across the cluster and processed in parallel. They live in the **Ray object store** — shared memory distributed across cluster nodes — so data passes between operators through zero-copy reads. When the object store fills up, Ray spills blocks to disk automatically.

<img src="https://docs.ray.io/en/latest/_images/dataset-arch.svg" width="90%"/>

Since a Dataset is a list of Ray object references, it can be freely passed between Ray tasks, actors, and libraries.

## Part 3: Lazy Execution

In Ray Data, most transformations are **lazy** — they build an execution plan rather than running immediately. The plan executes only when you call a method that *consumes* or *materializes* the dataset.

**Execution-triggering methods include:**

| **Category** | **Methods** |
|-------------|------------|
| Small samples | `take_batch()`, `take()`, `show()` |
| Full materialization | `materialize()` |
| Write to storage | `write_parquet()`, `write_csv()`, etc. |
| Aggregations | `count()`, `mean()`, `min()`, `max()`, `sum()`, `std()` |
| Joins and shuffles | `union()`, `random_shuffle()`, `sort()` |

To materialize a small subset for inspection, use `take`:

```python
sample = ds.take(5)

fig, axes = plt.subplots(1, 5, figsize=(12, 3))
for ax, row in zip(axes, sample):
    label = row["path"].split("/")[-2]
    ax.imshow(row["image"], cmap="gray")
    ax.set_title(f"Label: {label}")
    ax.axis("off")
plt.tight_layout()
plt.show()
```

This lazy execution model lets Ray Data optimize the full pipeline before running it. When you chain transformations (e.g., `ds.map_batches(f1).map_batches(f2).write_parquet(...)`), Ray Data builds a **logical plan** of your declared operations, then produces an optimized **physical plan**. A key optimization is **operator fusion** — adjacent compatible operators are merged into a single task, reducing data movement between stages. You can use `ds.explain()` to inspect the execution plan.

For a deeper dive into Ray Data internals, see the [Data Internals guide](https://docs.ray.io/en/latest/data/data-internals.html) and the [Key Concepts page](https://docs.ray.io/en/latest/data/key-concepts.html).

## Part 4: Transforming Data with `map_batches`

The primary transformation API in Ray Data is `map_batches()`. It applies a user-defined function to each batch of data in parallel.

A batch is a `dict[str, np.ndarray]` by default. Your function receives a batch and returns a transformed batch:

```python
def normalize(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """Normalize MNIST images to [-1, 1] range using torchvision transforms."""
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    batch["image"] = [transform(image) for image in batch["image"]]
    return batch
```

```python
ds_normalized = ds.map_batches(normalize)  # Lazy — not executed yet
```

Let's verify the transformation works on a small batch:

```python
normalized_batch = ds_normalized.take_batch(batch_size=10)

for image in normalized_batch["image"]:
    assert image.shape == (1, 28, 28)  # channel, height, width
    assert image.min() >= -1 and image.max() <= 1  # normalized range
```

```python
# Check the value ranges after normalization
print(f"Shape: {normalized_batch['image'][0].shape}")
print(f"Min value: {normalized_batch['image'][0].min():.4f}")
print(f"Max value: {normalized_batch['image'][0].max():.4f}")
```

**Key `map_batches` parameters:**

| **Parameter** | **Purpose** |
|--------------|-------------|
| `batch_size` | Number of rows per batch (default: `None` = entire block) |
| `batch_format` | `"default"`/`"numpy"` (dict), `"pandas"`, or `"pyarrow"` |
| `num_cpus` / `num_gpus` | Resources per worker |
| `fn_kwargs` | Keyword arguments to pass to your function |
| `compute` | Execution strategy — `TaskPoolStrategy` for functions, `ActorPoolStrategy` for classes |

**When to use `map` vs `map_batches`:** Use `map_batches` when the underlying computation is vectorized — for example, NumPy array operations, PyArrow transforms, or GPU inference. If your logic is row-by-row standard Python (such as string parsing or feature extraction), use `map` instead, which applies a function to each row individually and avoids the overhead of batch formatting.

## Part 5: Stateful Transformations and Batch Inference

In the previous section, we specified a normalization step where individual tasks did not require maintaining any state — a stateless operation. It is however common that operations require some pre-stored state. For example, in batch inference, you want to load a model once and reuse it across many batches. Ray Data supports this via **callable classes** passed to `map_batches`:

- `__init__`: Initialize expensive state (load model, set up connections)
- `__call__`: Process each batch using the initialized state

Here's an MNIST classifier that loads a pre-trained PyTorch model and runs inference:

```python
class MNISTClassifier:
    def __init__(self, model_path: str):
        self.model = torch.jit.load(model_path)
        self.model.eval()

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        images = torch.tensor(batch["image"]).float()

        with torch.no_grad():
            logits = self.model(images).numpy()

        batch["predicted_label"] = np.argmax(logits, axis=1)
        return batch
```

Download the model to shared storage, then apply the classifier:

```python
!aws s3 cp --no-sign-request s3://anyscale-public-materials/ray-ai-libraries/mnist/model/model.pt /mnt/cluster_storage/model.pt
```

```python
ds_preds = ds_normalized.map_batches(
    MNISTClassifier,
    fn_constructor_kwargs={"model_path": "/mnt/cluster_storage/model.pt"},
    num_cpus=1,
    batch_size=100,
    compute=ray.data.ActorPoolStrategy(size=1),
)
```

**Note:** We pass the class *uninitialized* — Ray Data creates the actor and passes `fn_constructor_kwargs` to `__init__`. This ensures the model is loaded once per worker, not once per batch. To run on GPUs instead, add `num_gpus=1` and move the model/tensors to CUDA. For more on batch inference patterns, see the [Batch Inference guide](https://docs.ray.io/en/latest/data/batch_inference.html).

Let's verify predictions:

```python
preds = ds_preds.take(5)
for p in preds:
    digit = p["path"].split("/")[-2]
    print(f"True label: {digit}, Predicted: {p['predicted_label']}")
```

**Scaling up:** Increase the pool size to use more workers. For example, `compute=ray.data.ActorPoolStrategy(size=4)` creates a fixed pool of 4 actors, or `compute=ray.data.ActorPoolStrategy(min_size=1, max_size=4)` creates an autoscaling pool.

## Part 6: Materializing and Persisting Data

Now that we've built a multi-step pipeline (read → normalize → inference), let's inspect the execution plan Ray Data created:

```python
ds_preds.explain()
```

By default, Ray Data streams data lazily. This is the right point to **materialize** `ds_preds` — execute the pipeline fully and cache the results in the Ray object store (distributed across the cluster, spilling to disk if needed).

Materializing here also releases the `ActorPoolStrategy` worker: as long as `ds_preds` is an unexecuted lazy plan, the GPU actor stays alive. Once materialized, the actor is freed and the GPU node can scale down.

**When to materialize:**
- When you need to reuse the same dataset multiple times (avoids re-computation)
- When downstream operations require the full dataset (e.g., `groupby`, `random_shuffle`)
- When using `ActorPoolStrategy` — to release the actor pool after execution

**When NOT to materialize:**
- For streaming pipelines where data flows through once (training ingest, write-to-sink)
- When the dataset is too large to fit in the object store

```python
ds_preds = ds_preds.materialize()
```

```python
# Check materialized dataset stats
print(ds_preds.stats())
```

### Persisting to storage

Write processed data to persistent storage using any of Ray Data's write functions. Ray Data supports writing to Parquet, CSV, JSON, TFRecords, and more. See the [Input/Output docs](https://docs.ray.io/en/latest/data/api/input_output.html) for the full list.

```python
ds_preds.write_parquet("/mnt/cluster_storage/mnist_preds")
```

## Part 7: Data Operations

### Adding Labels

Let's add ground truth labels to our MNIST dataset by extracting them from the image paths. Since this is row-by-row Python string parsing, we use `map` instead of `map_batches`:

```python
def add_label(row: dict[str, any]) -> dict[str, any]:
    row["ground_truth_label"] = int(row["path"].split("/")[-2])
    return row


ds_labeled = ds_normalized.map(add_label)
```

```python
# Verify labels were added
ds_labeled.take(1)
```

### Groupby and Map Groups

Use `groupby()` to group data by a key and `map_groups()` to apply per-group transformations. Here, we compute per-label accuracy using `ds_preds` from Part 5:

```python
def compute_accuracy(group: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    return {
        "accuracy": [np.mean(group["predicted_label"] == group["ground_truth_label"])],
        "ground_truth_label": group["ground_truth_label"][:1],
    }
```

```python
accuracy_by_label = (
    ds_preds
    .map(add_label)
    .groupby("ground_truth_label")
    .map_groups(compute_accuracy)
    .sort("ground_truth_label")
    .to_pandas()
)
accuracy_by_label
```

### Aggregations

Ray Data provides built-in aggregation functions: `count()`, `max()`, `mean()`, `min()`, `sum()`, `std()`. See the [aggregation API docs](https://docs.ray.io/en/latest/data/api/grouped_data.html#ray.data.aggregate.AggregateFn) for details.

```python
mean_accuracy = ds_preds.map(add_label).map_batches(compute_accuracy).mean(on="accuracy")
print(f"Mean accuracy across all digits: {mean_accuracy:.4f}")
```

**Key takeaway:** `groupby` + `map_groups` lets you apply per-group logic, while built-in aggregation functions cover common statistical operations.

## Part 8: Observability

Ray Data provides several tools for understanding what your pipeline is doing:

- `ds.stats()` — programmatic breakdown of per-operator throughput and timing
- `ds.explain()` — view the execution plan
- **Ray Dashboard** — cluster utilization, per-operator metrics, object store usage
- **Anyscale Metrics tab** — GPU utilization, memory, network, disk I/O

Use `set_name()` to label your dataset in the Ray Dashboard:

```python
ds_preds.set_name("mnist_predictions")
```

The **Ray Workloads** view shows each operator in your pipeline, its status, row counts, and throughput — so you can quickly identify which stage is the bottleneck:

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/ray-data-workloads.png" width="90%" alt="Ray Data Workloads view showing per-operator status and throughput">

The **Ray Dashboard Metrics** tab gives you time-series charts for bytes and blocks generated per second, rows processed per second, object store memory usage and more — useful for spotting throughput drops or memory pressure over time:

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/ray-data-dashboard.png" width="90%" alt="Ray Dashboard Metrics showing throughput and object store memory">

For detailed guidance, see the [Anyscale monitoring and debugging guide](https://docs.anyscale.com/monitoring).

You've built a complete Ray Data pipeline — from loading through persistence. The following sections cover advanced topics organized by theme, and can be read in any order.

## Additional APIs

*Ray Data offers higher-level tools beyond `map` and `map_batches`.*

### Part 9: Preprocessing and Expressions

#### Preprocessors

When a transform needs to compute statistics over the full dataset first (such as mean and standard deviation for normalization), Ray Data provides built-in Preprocessors that follow the familiar scikit-learn `fit` / `transform` pattern. See the [Preprocessor API reference](https://docs.ray.io/en/latest/data/api/preprocessor.html) for the full list of built-ins. You can also chain them with `Chain`:

```python
from ray.data.preprocessors import StandardScaler, Chain, LabelEncoder

# Example with tabular data — fit computes statistics, transform applies them
preprocessor = Chain(
    StandardScaler(columns=["feature_a", "feature_b"]),
    LabelEncoder(label_column="label"),
)
# preprocessor = preprocessor.fit(ds_tabular)
# ds_processed = preprocessor.transform(ds_tabular)
```

#### Expressions API

Before writing a custom `map` or `map_batches` function, check if the **Expressions API** can handle it for you. Expressions are built-in helper functions for common column operations — string manipulation, list operations, struct field access, arithmetic, and more — so you don't have to write boilerplate UDFs yourself.

Use `col()` to reference columns, `lit()` for literal values, and chain operations with namespaces such as `.str`, `.list`, and `.struct`:

```python
from ray.data.expressions import col

# Add an uppercase column without writing a custom function
ds_example = ray.data.from_items([{"name": "alice"}, {"name": "bob"}, {"name": "charlie"}])
ds_example = ds_example.with_column("upper_name", col("name").str.upper())
ds_example.show()
```

Expressions also support arithmetic (`col("a") + col("b")`), comparisons (`col("score") > 0.5`), and custom UDFs through the `@udf` decorator. See the full [Expressions API reference](https://docs.ray.io/en/latest/data/api/expressions.html) for the complete list of available operations.

## Scaling and Performance

*As your data and cluster grow, these tools help you stay fast.*

### Part 10: Shuffling

Ray Data offers three shuffle strategies with increasing randomness and cost:

| **Strategy** | **API** | **Randomness** | **Cost** |
|-------------|---------|---------------|----------|
| File-based shuffle on read | `read_images(..., shuffle="files")` | Low | Low |
| Block order shuffle | `ds.randomize_block_order()` | Medium | Medium |
| Global row shuffle | `ds.random_shuffle()` | High | High |

#### Pull-based vs push-based shuffle

Under the hood, global shuffle (`random_shuffle()`, `sort()`) uses a **sort-based shuffle** with two available implementations:

| | **Pull-based (default)** | **Push-based** |
|---|---|---|
| **How it works** | Classic MapReduce-style: all map tasks run first, then reduce tasks pull and combine outputs | Pipelined: map and merge tasks overlap in rounds, with a final reduce stage |
| **Best for** | Smaller datasets (<1 TB, <1000 blocks) | Large datasets (>1 TB, >1000 blocks) |
| **Trade-off** | Simpler, well-tested | Better throughput and memory for large shuffles |

Try push-based shuffle if your dataset has more than 1000 blocks or is larger than 1 TB in size:

```python
ctx = ray.data.DataContext.get_current()
ctx.shuffle_strategy = "sort_shuffle_push_based"  # Or set RAY_DATA_PUSH_BASED_SHUFFLE=1
```

### Part 11: Resource Management and Performance Tuning

Ray Data's **ResourceManager** tracks CPU, GPU, heap memory, and object store usage across all operators. It dynamically allocates budgets to keep the pipeline balanced.

- **Per-operator resources:** Set `num_cpus`, `num_gpus`, and `memory` on `map_batches` to control what each worker gets
- **Backpressure:** Ray Data automatically throttles upstream operators when downstream operators can't keep up, preventing OOM errors and disk spilling
- **Autoscaling:** Ray Data can request more cluster resources when operators are bottlenecked:
  - *Reactive autoscaling* (default): triggers when operators stall waiting for resources
  - *Proactive autoscaling*: triggers at 75% utilization threshold, requesting whole nodes

When your pipeline isn't performing as expected, follow this systematic approach:

1. **Establish a baseline** — measure single-operator throughput in isolation
2. **Scale up** — run the full pipeline on the target cluster size
3. **Root cause analysis** — check for GPU under-utilization, disk spilling, or OOM errors
4. **Iterate** — change one parameter at a time and measure impact

#### Block size tuning

Block size is an important performance lever. Larger blocks reduce scheduling overhead and improve throughput, but increase memory usage per task. Smaller blocks improve parallelism and reduce per-task memory, but add more scheduling overhead. Start with the defaults and adjust based on your workload.

```python
ctx = ray.data.DataContext.get_current()
ctx.target_max_block_size = 128 * 1024 * 1024  # 128 MB max block size
ctx.target_min_block_size = 1 * 1024 * 1024     # 1 MB min block size
```

#### Eager free

By default, Ray Data relies on Python garbage collection and Ray reference counting to free block memory. Enabling eager free releases block references from the object store as soon as they're no longer needed, reducing memory pressure in streaming pipelines:

```python
ctx = ray.data.DataContext.get_current()
ctx.eager_free = True  # Or set RAY_DATA_EAGER_FREE=1
```

#### Polars for sort operations

You can speed up `sort()` and operations that sort internally (such as `map_groups()`) by enabling Polars as the sorting backend. This can significantly improve performance for large tabular datasets. When enabled, Ray Data uses Polars instead of PyArrow for the internal sorting step. This doesn't affect other operations such as `map_batches`.

```python
ctx = ray.data.DataContext.get_current()
ctx.use_polars_sort = True  # Requires: pip install polars
```

See the [execution configurations docs](https://docs.ray.io/en/latest/data/execution-configurations.html) and [performance tips](https://docs.ray.io/en/latest/data/performance-tips.html) for more tuning guidance.

## Reliability

*For production pipelines that need to handle failures.*

### Part 12: Fault Tolerance and Checkpointing

Ray Data provides fault tolerance at two levels:

#### Worker-level retry

If a worker task fails (e.g., OOM, transient network error), Ray Data automatically retries the task. Configure retry behavior:

```python
ctx = ray.data.DataContext.get_current()
ctx.retried_io_errors = [IOError, ConnectionError]  # Retry on these errors
ctx.max_errored_blocks = 5  # Allow up to 5 failed blocks before aborting
```

#### Job-level checkpointing

For recovering from driver failures, head node crashes, or job pre-emptions, RayTurbo Data provides **job-level checkpointing**:

- Checkpoints are written after each block reaches the sink:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-data-deep-dive/ray_data_checkpointing_storing.png" alt="Ray Data Checkpoint Storing Flow" width="80%">

- On restart, the pipeline skips already-processed rows by matching an ID column:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-data-deep-dive/ray_data_checkpointing_restore.png" alt="Ray Data Checkpoint Restore Flow" width="80%">

```python
from ray.anyscale.data.checkpoint import CheckpointConfig

ctx = ray.data.DataContext.get_current()
ctx.checkpoint_config = CheckpointConfig(
    id_column="row_id",
    checkpoint_path="/mnt/cluster_storage/ray_data_checkpoint/",
    delete_checkpoint_on_success=True,
)
```

## Summary and Next Steps

In this template, you learned:

- **What** Ray Data is — a scalable, streaming data processing library for AI workloads
- **Why** to use it — streaming execution across heterogeneous CPU/GPU clusters for batch inference, training data ingest, and reliable data pipelines
- **How** to use it — loading data, transforming with `map` and `map_batches`, stateful batch inference, data operations, materialization, observability, and persistence
- **Advanced topics** — additional transform APIs, Ray Train integration, shuffling strategies, performance tuning, and fault tolerance

### Next Steps

1. **[Ray Data User Guide](https://docs.ray.io/en/latest/data/user-guide.html)** — In-depth guides for specific use cases
2. **[Batch Inference Guide](https://docs.ray.io/en/latest/data/batch_inference.html)** — Detailed patterns for scaling inference
3. **[Ray Data + Ray Train Integration](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html)** — Feeding Ray Data into distributed training
4. **[Performance Tips](https://docs.ray.io/en/latest/data/performance-tips.html)** — Tuning for production workloads
5. **[Ray Data API Reference](https://docs.ray.io/en/latest/data/api/api.html)** — Complete API documentation
