---
myst:
  html_meta:
    description: "Tune Ray Data performance: batch transforms, Polars sorts, read block and resource tuning, Parquet projection pushdown, and memory reduction."
---

(data_performance_tips)=

# Advanced: Performance tips and tuning

This page describes how to tune Ray Data performance for transforms, reads, memory usage, and execution.

(optimizing-transforms)=

## Optimize transforms

The following sections describe how to speed up transforms.

(batching-transforms)=

### Batch transforms

If your transformation is vectorized, as most NumPy or pandas operations are, use {meth}`~ray.data.Dataset.map_batches` instead of {meth}`~ray.data.Dataset.map`. It's faster.

If your transformation isn't vectorized, there's no performance benefit.

(enabling-polars-for-sort-operations)=

### Enable Polars for sort operations

To speed up {meth}`~ray.data.Dataset.sort` and operations that sort internally, such as {meth}`~ray.data.grouped_data.GroupedData.map_groups`, enable Polars:

```{testcode}
import ray

ctx = ray.data.DataContext.get_current()
ctx.use_polars_sort = True
```

When you enable this flag, Ray Data uses Polars instead of PyArrow for the internal sorting step, which can improve performance for large tabular datasets. This flag doesn't affect other operations such as {meth}`~ray.data.Dataset.map_batches`.

(optimizing-reads)=

## Optimize reads

The following sections describe how to tune reads.

(read_output_blocks)=
(tuning-output-blocks-for-read)=

### Tune output blocks for reads

By default, Ray Data automatically selects the number of output blocks for a read according to the following procedure:

- The `override_num_blocks` parameter that you pass to Ray Data's {ref}`read APIs <loading-data-api>` specifies the number of output blocks, which equals the number of read tasks to create.
- If a {func}`~ray.data.Dataset.map` or {func}`~ray.data.Dataset.map_batches` follows the read, Ray Data usually fuses the map with the read. In that case, `override_num_blocks` also determines the number of map tasks.

Ray Data chooses the default number of output blocks by applying the following heuristics in order:

1. Start with the default value of 200. To override it, set {class}`DataContext.read_op_min_num_blocks <ray.data.context.DataContext>`.
1. Apply the minimum block size, which defaults to 1 MiB. If the number of blocks would make blocks smaller than this threshold, reduce the number of blocks to avoid the overhead of tiny blocks. To override the threshold, set {class}`DataContext.target_min_block_size <ray.data.context.DataContext>` in bytes.
1. Apply the maximum block size, which defaults to 128 MiB. If the number of blocks would make blocks larger than this threshold, increase the number of blocks to avoid out-of-memory errors during processing. To override the threshold, set {class}`DataContext.target_max_block_size <ray.data.context.DataContext>` in bytes.
1. Account for available CPUs. Increase the number of blocks to use all available CPUs in the cluster. Ray Data sets the number of read tasks to at least 2x the number of available CPUs.

In some cases, tune the number of blocks manually to optimize your application. For example, the following code batches multiple files into the same read task to avoid creating blocks that are too large.

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Pretend there are two CPUs.
ray.init(num_cpus=2)

# Repeat the iris.csv file 16 times.
ds = ray.data.read_csv(["s3://anonymous@ray-example-data/iris.csv"] * 16)
print(ds.materialize())
```

```{testoutput}
:options: +MOCK

MaterializedDataset(
   num_blocks=4,
   num_rows=2400,
   ...
)
```

Suppose that you want to read all 16 files in parallel. For example, you might expect the autoscaler to add CPUs to the cluster, or you might want the downstream operator to transform each file's contents in parallel. To get this behavior, set the `override_num_blocks` parameter. In the following code, the number of output blocks equals `override_num_blocks`:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Pretend there are two CPUs.
ray.init(num_cpus=2)

# Repeat the iris.csv file 16 times.
ds = ray.data.read_csv(["s3://anonymous@ray-example-data/iris.csv"] * 16, override_num_blocks=16)
print(ds.materialize())
```

```{testoutput}
:options: +MOCK

MaterializedDataset(
   num_blocks=16,
   num_rows=2400,
   ...
)
```

When you use the default auto-detected number of blocks, Ray Data attempts to cap each task's output at {class}`DataContext.target_max_block_size <ray.data.context.DataContext>` bytes. However, Ray Data can't perfectly predict the size of each task's output, so each task might produce one or more output blocks. As a result, the total number of blocks in the final {class}`~ray.data.Dataset` might differ from the specified `override_num_blocks`. In the following example, the code sets `override_num_blocks=1` manually, but the one task still produces multiple blocks in the materialized Dataset:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Pretend there are two CPUs.
ray.init(num_cpus=2)

# Generate ~400MB of data.
ds = ray.data.range_tensor(5_000, shape=(10_000, ), override_num_blocks=1)
print(ds.materialize())
```

```{testoutput}
:options: +MOCK

MaterializedDataset(
   num_blocks=3,
   num_rows=5000,
   schema={data: ArrowTensorTypeV2(shape=(10000,), dtype=int64)}
)
```

Currently, Ray Data can assign at most one read task per input file. So if the number of input files is smaller than `override_num_blocks`, Ray Data caps the number of read tasks at the number of input files. To make sure that downstream transforms can still run with the desired number of blocks, Ray Data splits the read tasks' outputs into a total of `override_num_blocks` blocks and prevents fusion with the downstream transform. In other words, Ray Data materializes each read task's output blocks to Ray's object store before the consuming map task runs. For example, the following code runs {func}`~ray.data.read_csv` with only one task, but Ray Data splits its output into four blocks before it runs the {func}`~ray.data.Dataset.map`:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Pretend there are two CPUs.
ray.init(num_cpus=2)

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv").map(lambda row: row)
print(ds.materialize().stats())
```

```{testoutput}
:options: +MOCK

...
Operator 1 ReadCSV->SplitBlocks(4): 1 tasks executed, 4 blocks produced in 0.01s
...

Operator 2 Map(<lambda>): 4 tasks executed, 4 blocks produced in 0.3s
...
```

To turn off this behavior so that Ray Data can fuse the read and map operators, set `override_num_blocks` manually. For example, the following code sets `override_num_blocks` equal to the number of files:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Pretend there are two CPUs.
ray.init(num_cpus=2)

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv", override_num_blocks=1).map(lambda row: row)
print(ds.materialize().stats())
```

```{testoutput}
:options: +MOCK

...
Operator 1 ReadCSV->Map(<lambda>): 1 tasks executed, 1 blocks produced in 0.01s
...
```

(tuning_read_resources)=

### Tune read resources

By default, Ray requests 1 CPU per read task, so one read task per CPU can run concurrently. For datasources that benefit from more IO parallelism, reserve fewer CPUs for each read task. For example, use `ray.data.read_parquet(path, num_cpus=0.25)` to run up to four read tasks per CPU.

(parquet_column_pruning)=
(parquet-column-pruning-projection-pushdown)=

### Prune Parquet columns with projection pushdown

By default, {func}`ray.data.read_parquet` reads all columns in the Parquet files into memory. If you need only a subset of the columns, specify the list of columns explicitly when you call {func}`ray.data.read_parquet`. This technique, called projection pushdown, avoids loading unnecessary data. It's more efficient than calling {func}`~ray.data.Dataset.select_columns`, because column selection is pushed down to the file scan.

```{testcode}
import ray

# Read just two of the five columns of the Iris dataset.
ds = ray.data.read_parquet(
    "s3://anonymous@ray-example-data/iris.parquet",
).select_columns(["sepal.length", "variety"])

print(ds.schema())
```

```{testoutput}
Column        Type
------        ----
sepal.length  double
variety       string
```

(data_memory)=
(reducing-memory-usage)=

## Reduce memory usage

The following sections describe how to reduce memory usage.

(avoiding-object-spilling)=

### Avoid object spilling

Ray Data stores a Dataset's intermediate and output blocks in Ray's object store. Although Ray Data attempts to minimize object store usage with {ref}`streaming execution <streaming_execution>`, the working set can still exceed the object store capacity. In that case, Ray begins spilling blocks to disk, which can slow execution significantly or even cause out-of-disk errors.

Spilling is expected in some cases, in particular when the Dataset's total size is larger than the object store capacity and one of the following is true:

- You use an {ref}`all-to-all shuffle operation <optimizing_shuffles>`.
- You call {meth}`ds.materialize() <ray.data.Dataset.materialize>`.

Otherwise, tune your application to avoid spilling. Manually increase the {ref}`read output blocks <read_output_blocks>`, or change your application code so that each task reads less data.

:::{note}
This is an active area of development. If your Dataset causes spilling and you don't know why, [file a Ray Data issue on GitHub](https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2Ctriage%2Cdata&projects=&template=bug-report.yml&title=[data]+).
:::

(handling-too-small-blocks)=

### Handle too-small blocks

When different operators of your Dataset produce different-sized outputs, you might end up with tiny blocks, which can hurt performance and even cause crashes from excessive metadata. Use {meth}`ds.stats() <ray.data.Dataset.stats>` to check that each operator's output blocks are at least 1 MB each, and ideally larger than 100 MB.

If your blocks are smaller than this, repartition them into larger blocks. You can do this in two ways:

1. If you need control over the exact number of output blocks, use {meth}`ds.repartition(num_partitions) <ray.data.Dataset.repartition>`. This is an {ref}`all-to-all operation <optimizing_shuffles>`, and it materializes all blocks into memory before performing the repartition.
1. If you don't need control over the exact number of output blocks and want only to produce larger blocks, use {meth}`ds.map_batches(lambda batch: batch, batch_size=batch_size) <ray.data.Dataset.map_batches>` and set `batch_size` to the desired number of rows per block. This approach runs in a streaming fashion and avoids materialization.

When you use {meth}`ds.map_batches() <ray.data.Dataset.map_batches>`, Ray Data coalesces blocks so that each map task can process at least `batch_size` rows. The chosen `batch_size` is a lower bound on the task's input block size, but it doesn't necessarily determine the task's final *output* block size.

The following code uses both strategies to coalesce 10 tiny blocks of one row each into one larger block of 10 rows:

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Pretend there are two CPUs.
ray.init(num_cpus=2)

# 1. Use ds.repartition().
ds = ray.data.range(10, override_num_blocks=10).repartition(1)
print(ds.materialize().stats())

# 2. Use ds.map_batches().
ds = ray.data.range(10, override_num_blocks=10).map_batches(lambda batch: batch, batch_size=10)
print(ds.materialize().stats())
```

```{testoutput}
:options: +MOCK

# 1. ds.repartition() output.
Operator 1 ReadRange: 10 tasks executed, 10 blocks produced in 0.33s
...
* Output num rows: 1 min, 1 max, 1 mean, 10 total
...
Operator 2 Repartition: executed in 0.36s

        Suboperator 0 RepartitionSplit: 10 tasks executed, 10 blocks produced
        ...

        Suboperator 1 RepartitionReduce: 1 tasks executed, 1 blocks produced
        ...
        * Output num rows: 10 min, 10 max, 10 mean, 10 total
        ...


# 2. ds.map_batches() output.
Operator 1 ReadRange->MapBatches(<lambda>): 1 tasks executed, 1 blocks produced in 0s
...
* Output num rows: 10 min, 10 max, 10 mean, 10 total
```

(configuring-execution)=

## Configure execution

The following section describes how to configure execution resources and locality.

(configuring-resources-and-locality)=

### Configure resources and locality

By default, Ray Data sets the CPU and GPU limits to the cluster size. It conservatively sets the object store memory limit to 1/4 of the total object store size to avoid the possibility of disk spilling.

Customize these limits in the following scenarios:

- When you run multiple concurrent jobs on the cluster, lower limits can avoid resource contention between the jobs.
- When you want to fine-tune the memory limit to maximize performance.
- When you load data into training jobs, set the object store memory to a low value, such as 2 GB, to limit resource usage.

Configure execution options with the global DataContext. The options apply to future jobs launched in the process:

```
ctx = ray.data.DataContext.get_current()
ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
    cpu=10,
    gpu=5,
    object_store_memory=10e9,
)
```

## Make execution reproducible

The following section describes how to make execution deterministic.

(deterministic-execution)=

### Enable deterministic execution

To enable deterministic execution, set `preserve_order` to `True`, as in the following code. This setting might decrease performance, but it ensures that block ordering is preserved through execution. The flag defaults to `False`.

```
# By default, this is set to False.
ctx.execution_options.preserve_order = True
```
