# Introduction to Ray Data

This template provides a comprehensive introduction to **Ray Data** — a scalable data processing library for AI workloads built on [Ray](https://docs.ray.io/en/latest/). You will learn what Ray Data is, why it matters for ML pipelines, and how to use its core APIs hands-on with an MNIST image classification example.

**Assumed knowledge:** You should be comfortable with PyTorch, NumPy, and standard Python data processing patterns.

**Here is the roadmap for this template:**

- **Part 1:** When to Use Ray Data
- **Part 2:** Loading Data
- **Part 3:** Lazy Execution
- **Part 4:** Transforming Data with `map_batches`
- **Part 5:** Stateful Transformations and Batch Inference
- **Part 6:** Data Preprocessing
- **Part 7:** Data Operations — Filtering, Groupby, Aggregation, Shuffling
- **Part 8:** Materializing Data
- **Part 9:** Persisting Data
- **Part 10:** Architecture Overview
- **Part 11:** Resource Management and Autoscaling
- **Part 12:** Observability and Performance Tuning
- **Part 13:** Fault Tolerance and Checkpointing
- **Part 14:** Ray Data in Production
- **Summary and Next Steps**

---

## Imports

```python
import numpy as np
import torch
from torchvision.transforms import Compose, ToTensor, Normalize

import ray
```

---

### Note on Storage

Throughout this tutorial, we use `/mnt/cluster_storage` to represent a shared storage location. In a multi-node cluster, Ray workers on different nodes cannot access the head node's local file system. Use a [shared storage solution](https://docs.anyscale.com/configuration/storage#shared) accessible from every node.

---

## Part 1: When to Use Ray Data

Use Ray Data to load and preprocess data for distributed ML workloads. Ray Data is the **last-mile bridge** from storage or ETL pipeline outputs to distributed applications and libraries in Ray.

Consider using Ray Data when your project meets one or more of these criteria:

| **Challenge** | **Ray Data Solution** |
|---------------|----------------------|
| **Operating on large datasets** (>10 TB) | Distributes data loading and processing across a Ray cluster with streaming execution |
| **Feeding data into distributed training** | Streams data to training processes with configurable batch sizes across heterogeneous CPU/GPU resources |
| **Running batch inference at scale** | Maximizes GPU utilization by streaming data through model inference actors |
| **Building reliable data pipelines** | Leverages Ray Core's fault-tolerance mechanisms — retries, checkpointing, and recovery |

Ray Data features a **streaming execution engine** that processes data in a pipelined fashion across a heterogeneous cluster of CPUs and GPUs. This avoids materializing the full dataset in memory and keeps all hardware utilized.

<img src="https://docs.ray.io/en/latest/_images/dataset-loading-1.svg" width="60%"/>

**When to use something else:** If your workload is primarily SQL-based analytics, or if you need a mature SQL query engine, consider Spark or another SQL-focused system. Ray Data excels when your pipeline involves deep learning models, GPU compute, or needs tight integration with the Ray ecosystem (Ray Train, Ray Serve, Ray Tune).

---

## Part 2: Loading Data

Ray Data supports a variety of IO connectors to load data from different sources:

| **Source** | **API** |
|------------|---------|
| Parquet files (S3, GCS, local) | `ray.data.read_parquet("s3://...")` |
| Images | `ray.data.read_images("s3://...")` |
| CSV, JSON, text | `ray.data.read_csv(...)`, `ray.data.read_json(...)`, `ray.data.read_text(...)` |
| HuggingFace Datasets | `ray.data.from_huggingface(hf_dataset)` |
| In-memory (NumPy, Pandas, PyTorch) | `ray.data.from_numpy(...)`, `ray.data.from_pandas(...)`, `ray.data.from_torch(...)` |
| Lakehouses (Databricks, Iceberg) | `ray.data.read_databricks_tables(...)` |

See the full list in the [Input/Output docs](https://docs.ray.io/en/latest/data/api/input_output.html), and review further options in the [data loading guide](https://docs.ray.io/en/latest/data/loading-data.html).

Under the hood, Ray Data uses Ray tasks to read data from remote storage. It creates read tasks proportional to the number of CPUs in your cluster, and each task produces output **blocks**:

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-summit/rag-app/dataset-read-cropped-v2.svg" width="500px">

Let's load MNIST image data from S3:

```python
ds = ray.data.read_images(
    "s3://anyscale-public-materials/ray-ai-libraries/mnist/50_per_index/",
    include_paths=True,
)
ds
```

A **Dataset** is a distributed collection of **blocks** — contiguous subsets of rows stored as PyArrow tables. Blocks are distributed across the cluster and processed in parallel.

<img src="https://docs.ray.io/en/latest/_images/dataset-arch.svg" width="50%"/>

Since a Dataset is a list of Ray object references, it can be freely passed between Ray tasks, actors, and libraries.

---

## Part 3: Lazy Execution

In Ray Data, most transformations are **lazy** — they build an execution plan rather than running immediately. The plan executes only when you call a method that *consumes* or *materializes* the dataset.

**Execution-triggering methods include:**

| **Category** | **Methods** |
|-------------|------------|
| Small samples | `take_batch()`, `take()`, `show()` |
| Full materialization | `materialize()` |
| Write to storage | `write_parquet()`, `write_csv()`, etc. |
| Aggregations | `count()`, `mean()`, `min()`, `max()`, `sum()`, `std()` |

To materialize a small subset for inspection, use `take_batch`:

```python
sample = ds.take_batch(batch_size=5)
sample.keys()  # dict_keys(['image', 'path'])
```

This lazy execution model lets Ray Data optimize the full pipeline before running it — including operator fusion, resource allocation, and backpressure management.

---

## Part 4: Transforming Data with `map_batches`

The primary transformation API in Ray Data is `map_batches()`. It applies a user-defined function to each batch of data in parallel.

A batch is a `dict[str, np.ndarray]` by default. Your function receives a batch and returns a transformed batch:

```python
def normalize(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """Normalize MNIST images to [-1, 1] range using torchvision transforms."""
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    batch["image"] = [transform(image) for image in batch["image"]]
    return batch


ds_normalized = ds.map_batches(normalize)
ds_normalized  # Lazy — not executed yet
```

Let's verify the transformation works on a small batch:

```python
normalized_batch = ds_normalized.take_batch(batch_size=10)

for image in normalized_batch["image"]:
    assert image.shape == (1, 28, 28)  # channel, height, width
    assert image.min() >= -1 and image.max() <= 1  # normalized range
```

**Key `map_batches` parameters:**

| **Parameter** | **Purpose** |
|--------------|-------------|
| `batch_size` | Number of rows per batch (default: `None` = entire block) |
| `batch_format` | `"default"`/`"numpy"` (dict), `"pandas"`, or `"pyarrow"` |
| `num_cpus` / `num_gpus` | Resources per worker |
| `fn_kwargs` | Keyword arguments to pass to your function |
| `compute` | Execution strategy — `TaskPoolStrategy` for functions, `ActorPoolStrategy` for classes |

To learn more, see the [`map_batches` API reference](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html) and the [Transforming Data guide](https://docs.ray.io/en/latest/data/transforming-data.html).

> **API Update (Ray 2.53+):** The `concurrency` parameter is **deprecated**. Use the `compute` argument instead:
> - `compute=ray.data.ActorPoolStrategy(size=N)` for a fixed actor pool
> - `compute=ray.data.ActorPoolStrategy(min_size=M, max_size=N)` for autoscaling
> - `compute=ray.data.TaskPoolStrategy(size=N)` to limit concurrent tasks

---

## Part 5: Stateful Transformations and Batch Inference

For operations like batch inference, you want to load a model once and reuse it across many batches. Ray Data supports this via **callable classes** passed to `map_batches`:

- `__init__`: Initialize expensive state (load model, set up connections)
- `__call__`: Process each batch using the initialized state

Here's an MNIST classifier that loads a pre-trained PyTorch model and runs GPU inference:

```python
class MNISTClassifier:
    def __init__(self, model_path: str):
        self.model = torch.jit.load(model_path)
        self.model.to("cuda")
        self.model.eval()

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        images = torch.tensor(batch["image"]).float().to("cuda")

        with torch.no_grad():
            logits = self.model(images).cpu().numpy()

        batch["predicted_label"] = np.argmax(logits, axis=1)
        return batch
```

Download the model to shared storage, then apply the classifier:

```python
!aws s3 cp s3://anyscale-public-materials/ray-ai-libraries/mnist/model/model.pt /mnt/cluster_storage/model.pt
```

```python
ds_preds = ds_normalized.map_batches(
    MNISTClassifier,
    fn_constructor_kwargs={"model_path": "/mnt/cluster_storage/model.pt"},
    num_gpus=1,
    batch_size=100,
    compute=ray.data.ActorPoolStrategy(size=1),  # 1 GPU worker
)
```

**Note:** We pass the class *uninitialized* — Ray Data creates the actor and passes `fn_constructor_kwargs` to `__init__`. This ensures the model is loaded once per worker, not once per batch. For more on batch inference patterns, see the [Batch Inference guide](https://docs.ray.io/en/latest/data/batch_inference.html).

Let's verify predictions:

```python
batch_preds = ds_preds.take_batch(100)
batch_preds.keys()  # dict_keys(['image', 'path', 'predicted_label'])
```

**Scaling up:** To use multiple GPUs, increase the pool size. For example, `compute=ray.data.ActorPoolStrategy(size=4)` creates a fixed pool of 4 GPU actors, or `compute=ray.data.ActorPoolStrategy(min_size=1, max_size=4)` creates an autoscaling pool.

---

## Part 6: Data Preprocessing

Ray Data provides three approaches to data preprocessing, each suited to different use cases:

| **Approach** | **When to Use** | **Example** |
|-------------|----------------|-------------|
| **Stateless functions** (`map_batches` with a function) | Simple transforms that don't require shared state | Normalization, feature extraction, label encoding |
| **Stateful callable classes** (`map_batches` with a class) | Transforms requiring expensive initialization (model, connection) | Batch inference, embedding generation |
| **Preprocessors** (fit/transform pattern) | Transforms that need a pass over the dataset to compute statistics | StandardScaler, MinMaxScaler, OneHotEncoder |

### Built-in Preprocessors

Ray Data includes built-in Preprocessors that follow the familiar scikit-learn `fit` / `transform` pattern. Here's a general example with tabular data (not our MNIST pipeline, which uses `map_batches` for image transforms):

```python
from ray.data.preprocessors import StandardScaler, Chain, Concatenator, LabelEncoder

# Example: fit a StandardScaler on a tabular dataset
# scaler = StandardScaler(columns=["feature_col"])
# scaler = scaler.fit(ds_tabular)
# ds_scaled = scaler.transform(ds_tabular)
```

### Chaining Preprocessors

Combine multiple preprocessors into a pipeline using `Chain`:

```python
# Example: chain multiple preprocessors for tabular data
# preprocessor = Chain(
#     StandardScaler(columns=["feature_col"]),
#     Concatenator(output_column_name="features", exclude=["label"]),
#     LabelEncoder(label_column="label"),
# )
# preprocessor = preprocessor.fit(ds_tabular)
# ds_processed = preprocessor.transform(ds_tabular)
```

For custom preprocessing logic, you can subclass `Preprocessor` and implement `_fit()` and `_transform_pandas()` / `_transform_numpy()`. See the [Preprocessor API reference](https://docs.ray.io/en/latest/data/api/preprocessor.html) for details and the full list of built-in preprocessors.

---

## Part 7: Data Operations

### Adding Labels

Let's add ground truth labels to our MNIST dataset by extracting them from the image paths:

```python
def add_label(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    batch["ground_truth_label"] = [int(path.split("/")[-2]) for path in batch["path"]]
    return batch


ds_labeled = ds_normalized.map_batches(add_label)
```

### Groupby and Map Groups

Use `groupby()` to group data by a key and `map_groups()` to apply per-group transformations. Here, we compute per-label accuracy using `ds_preds` from Part 5:

```python
def compute_accuracy(group: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    return {
        "accuracy": [np.mean(group["predicted_label"] == group["ground_truth_label"])],
        "ground_truth_label": group["ground_truth_label"][:1],
    }


accuracy_by_label = (
    ds_preds
    .map_batches(add_label)
    .groupby("ground_truth_label")
    .map_groups(compute_accuracy)
    .to_pandas()
)
accuracy_by_label
```

### Aggregations

Ray Data provides built-in aggregation functions: `count()`, `max()`, `mean()`, `min()`, `sum()`, `std()`. See the [aggregation API docs](https://docs.ray.io/en/latest/data/api/grouped_data.html#ray.data.aggregate.AggregateFn) for details.

```python
mean_accuracy = ds_preds.map_batches(add_label).map_batches(compute_accuracy).mean(on="accuracy")
mean_accuracy
```

**Key takeaway:** `groupby` + `map_groups` lets you apply per-group logic, while built-in aggregation functions cover common statistical operations.

### Shuffling

Ray Data offers three shuffle strategies with increasing randomness and cost:

| **Strategy** | **API** | **Randomness** | **Cost** |
|-------------|---------|---------------|----------|
| File-based shuffle on read | `read_images(..., shuffle="files")` | Low | Low |
| Block order shuffle | `ds.randomize_block_order()` | Medium | Medium |
| Global row shuffle | `ds.random_shuffle()` | High | High |

```python
# File-based shuffle — randomize which files are read first
ds_shuffled = ray.data.read_images(
    "s3://anyscale-public-materials/ray-ai-libraries/mnist/50_per_index/",
    shuffle="files",
)

# Block order shuffle — randomize block ordering in memory
ds_block_shuffled = ds_preds.randomize_block_order()

# Global shuffle — full row-level randomization (most expensive)
ds_row_shuffled = ds_preds.random_shuffle()
```

---

## Part 8: Materializing Data

By default, Ray Data streams data lazily. You can **materialize** a dataset to eagerly execute the full pipeline and store results in the Ray object store (distributed across the cluster, spilling to disk if needed).

```python
ds_materialized = ds_preds.materialize()
ds_materialized
```

**When to materialize:**
- When you need to reuse the same dataset multiple times (avoids re-computation)
- When downstream operations require the full dataset (e.g., `groupby`, `random_shuffle`)

**When NOT to materialize:**
- For streaming pipelines where data flows through once (training ingest, write-to-sink)
- When the dataset is too large to fit in the object store

Use `set_name()` to label your dataset in the Ray Dashboard for observability:

```python
ds_preds.set_name("mnist_predictions")
```

---

## Part 9: Persisting Data

Write processed data to persistent storage using any of Ray Data's write functions:

```python
ds_preds.write_parquet("/mnt/cluster_storage/mnist_preds")
```

Ray Data supports writing to Parquet, CSV, JSON, TFRecords, and more. See the [Input/Output docs](https://docs.ray.io/en/latest/data/api/input_output.html) for the full list.

```python
# Cleanup
!rm -rf /mnt/cluster_storage/mnist_preds
```

---

Now that we've covered Ray Data's core APIs hands-on, let's look under the hood at how it works. The following sections are conceptual — they'll help you reason about performance, debug issues, and tune your pipelines.

## Part 10: Architecture Overview

### Streaming Execution

Ray Data uses a **streaming execution model** rather than traditional stage-by-stage batch processing.

**Traditional batch processing** completes one stage fully before starting the next, leading to idle resources:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/cko-2025-q1/batch-processing.png" width="800" alt="Traditional Batch Processing">

**Streaming pipelining** overlaps stages, keeping all hardware (CPUs and GPUs) busy simultaneously:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/cko-2025-q1/pipelining.png" width="800" alt="Streaming Model Pipelining">

This is critical for GPU-heavy workloads: while the GPU runs inference on one batch, the CPU can preprocess the next batch.

### Blocks and the Object Store

- A **Dataset** is a distributed collection of **blocks** (PyArrow tables by default)
- Blocks live in the **Ray object store** — shared memory distributed across cluster nodes (typically 30% of node memory)
- When the object store fills up, Ray spills blocks to disk automatically
- Data passes between operators via zero-copy reads from the object store

### Operators, Planning, and Fusion

When you chain transformations (e.g., `ds.map_batches(f1).map_batches(f2).write_parquet(...)`), Ray Data builds a **logical plan** and optimizes it:

1. **Logical plan** — your declared operations
2. **Physical plan** — optimized execution graph with operator fusion
3. **Operator fusion** — adjacent compatible operators are merged into a single task, reducing data movement

Use `ds.explain()` to inspect the execution plan:

```python
ds_preds.explain()
```

For a deeper dive into Ray Data internals, see the [Data Internals guide](https://docs.ray.io/en/latest/data/data-internals.html) and the [Key Concepts page](https://docs.ray.io/en/latest/data/key-concepts.html).

---

## Part 11: Resource Management and Autoscaling

Ray Data's **ResourceManager** tracks CPU, GPU, heap memory, and object store usage across all operators. It dynamically allocates budgets to keep the pipeline balanced.

**Key concepts:**

- **Per-operator resources:** Set `num_cpus`, `num_gpus`, and `memory` on `map_batches` to control what each worker gets
- **Backpressure:** Ray Data automatically throttles upstream operators when downstream operators can't keep up, preventing OOM errors and disk spilling
- **Autoscaling:** Ray Data can request more cluster resources when operators are bottlenecked:
  - *Reactive autoscaling* (default): triggers when operators stall waiting for resources
  - *Proactive autoscaling*: triggers at 75% utilization threshold, requesting whole nodes

To configure resource limits, use the `DataContext`:

```python
ctx = ray.data.DataContext.get_current()

# Example: set target block sizes to control memory usage
ctx.target_max_block_size = 128 * 1024 * 1024  # 128 MB max block size
```

See the [execution configurations docs](https://docs.ray.io/en/latest/data/execution-configurations.html) and [performance tips](https://docs.ray.io/en/latest/data/performance-tips.html) for tuning guidance.

---

## Part 12: Observability and Performance Tuning

When your Ray Data pipeline isn't performing as expected, follow this systematic approach:

1. **Establish a baseline** — measure single-operator throughput in isolation
2. **Scale up** — run the full pipeline on the target cluster size
3. **Root cause analysis** — check for:
   - GPU under-utilization (slow CPU preprocessing starving the GPU)
   - Disk spilling (object store memory pressure)
   - OOM errors (batch size too large)
4. **Iterate** — change one parameter at a time and measure impact

**Useful tools:**
- `ds.stats()` — programmatic breakdown of per-operator throughput and timing
- `ds.explain()` — view the execution plan
- **Ray Dashboard** — cluster utilization, per-operator metrics, object store usage
- **Anyscale Metrics tab** — GPU utilization, memory, network, disk I/O

For detailed guidance, see the [Anyscale monitoring and debugging guide](https://docs.anyscale.com/monitoring).

---

## Part 13: Fault Tolerance and Checkpointing

Ray Data provides fault tolerance at two levels:

### Worker-Level Retry (Open Source)

If a worker task fails (e.g., OOM, transient network error), Ray Data automatically retries the task. Configure retry behavior:

```python
ctx = ray.data.DataContext.get_current()
ctx.retried_io_errors = [IOError, ConnectionError]  # Retry on these errors
ctx.max_errored_blocks = 5  # Allow up to 5 failed blocks before aborting
```

### Job-Level Checkpointing (RayTurbo / Anyscale)

For recovering from driver failures, head node crashes, or job pre-emptions, RayTurbo Data provides **job-level checkpointing**:

- Checkpoints are written after each block reaches the sink:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-data-deep-dive/ray_data_checkpointing_storing.png" alt="Ray Data Checkpoint Storing Flow" width="800">

- On restart, the pipeline skips already-processed rows by matching an ID column:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-data-deep-dive/ray_data_checkpointing_restore.png" alt="Ray Data Checkpoint Restore Flow" width="800">

```python
# Note: This import is Anyscale-specific (RayTurbo Data). Not available in open-source Ray.
from ray.anyscale.data.checkpoint import CheckpointConfig

ctx = ray.data.DataContext.get_current()
ctx.checkpoint_config = CheckpointConfig(
    id_column="row_id",
    checkpoint_path="/mnt/cluster_storage/ray_data_checkpoint/",
    delete_checkpoint_on_success=True,
)
```

> **Note:** Job-level checkpointing is an Anyscale/RayTurbo feature. It requires a pipeline shaped as: **read** -> **map operations** -> **write**. See the [fault tolerance docs](https://docs.ray.io/en/latest/data/fault-tolerance.html) for details on both open-source and Anyscale fault tolerance.

---

## Part 14: Ray Data in Production

Ray Data is used in production at scale by leading AI companies:

- **Netflix** — multi-modal inference pipelines processing millions of items. [Watch their Ray Summit 2024 talk](https://raysummit.anyscale.com/flow/anyscale/raysummit2024/landing/page/sessioncatalog/session/1722028596844001bCg0).
- **Pinterest** — last-mile data processing for recommendation model training with heterogeneous cluster disaggregation. [Read their engineering blog](https://medium.com/pinterest-engineering/last-mile-data-processing-with-ray-data-629affbf34ff).
- **Runway AI** — scaling ML workloads for AI-driven filmmaking. [See this interview](https://siliconangle.com/2024/10/02/runway-transforming-ai-driven-filmmaking-innovative-tools-techniques-raysummit/).
- **Spotify** — ML platform built on Ray Data for batch inference. [Read their engineering blog](https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/).
- **ByteDance** — offline inference with multi-modal LLMs across 200 TB of data. [Read the case study](https://www.anyscale.com/blog/how-bytedance-scales-offline-inference-with-multi-modal-llms-to-200TB-data).

---

## Summary and Next Steps

In this template, you learned:

- **What** Ray Data is — a scalable, streaming data processing library for AI workloads
- **Why** to use it — heterogeneous CPU/GPU clusters, batch inference, training data ingest, fault tolerance
- **How** to use it — loading data with IO connectors, transforming with `map_batches`, stateful transforms for batch inference, preprocessing pipelines, data operations, materialization, and persistence
- **Architecture** — streaming execution, blocks, operator fusion, resource management, backpressure, autoscaling
- **Operations** — observability, fault tolerance, and checkpointing

### Next Steps

1. **[Ray Data User Guide](https://docs.ray.io/en/latest/data/user-guide.html)** — In-depth guides for specific use cases
2. **[Batch Inference Guide](https://docs.ray.io/en/latest/data/batch_inference.html)** — Detailed patterns for scaling inference
3. **[Ray Data + Ray Train Integration](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html)** — Feeding Ray Data into distributed training
4. **[Performance Tips](https://docs.ray.io/en/latest/data/performance-tips.html)** — Tuning for production workloads
5. **[Ray Data API Reference](https://docs.ray.io/en/latest/data/api/api.html)** — Complete API documentation

---

## Cleanup

```python
# Clean up any resources
!rm -f /mnt/cluster_storage/model.pt
```
