---
myst:
  html_meta:
    description: "Implementation internals of Ray Data for advanced users and contributors: the execution model, blocks, shuffle algorithms, scheduling, and the memory model."
---

(datasets_scheduling)=

# Ray Data internals

This guide describes the implementation of Ray Data. It's for advanced users and Ray Data developers.

For a gentler introduction to Ray Data, see {ref}`Quickstart <data_quickstart>`.

(dataset_concept)=

## Key concepts

This section describes the core concepts in the Ray Data implementation.

### What are datasets and blocks?

This section describes the dataset API and its basic unit of data, the block.

#### What's a dataset?

{class}`Dataset <ray.data.Dataset>` is the main user-facing Python API. It represents a distributed data collection and defines data loading and processing operations. A typical workflow uses the API in the following steps:

1. Create a Ray Dataset from external storage or in-memory data.
1. Apply transformations to the data.
1. Write the outputs to external storage or feed the outputs to training workers.

#### What's a block?

A *block* is the basic unit of data that Ray Data stores in the object store and transfers over the network. Each block contains a disjoint subset of rows, and Ray Data loads and transforms these blocks in parallel.

The following figure visualizes a dataset with three blocks, each holding 1000 rows. Ray Data holds the {class}`~ray.data.Dataset` on the process that triggers execution, which is usually the driver, and stores the blocks as objects in Ray's shared-memory {ref}`object store <objects-in-ray>`.

```{image} images/dataset-arch.svg
```

<!--
https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit
-->

(block-formats)=

#### What formats do blocks use?

Blocks are Arrow tables or `pandas` DataFrames. Generally, blocks are Arrow tables unless Arrow can't represent your data.

The block format doesn't affect the type of data that APIs such as {meth}`~ray.data.Dataset.iter_batches` return.

(block-size-limiting)=

#### How does Ray Data limit block size?

Ray Data bounds block sizes to avoid excessive communication overhead and prevent out-of-memory errors. Small blocks improve latency and streaming, while large blocks reduce scheduler and communication overhead. The default range attempts to balance these trade-offs for most jobs.

Ray Data attempts to bound block sizes between 1 MiB and 128 MiB. To change the block size range, configure the `target_min_block_size` and `target_max_block_size` attributes of {class}`~ray.data.context.DataContext`.

```{testcode}
import ray

ctx = ray.data.DataContext.get_current()
ctx.target_min_block_size = 1 * 1024 * 1024
ctx.target_max_block_size = 128 * 1024 * 1024
```

(dynamic-block-splitting)=

#### How does Ray Data split large blocks?

If a block is larger than 192 MiB, which is 50% more than the target maximum size, Ray Data dynamically splits the block into smaller blocks.

To change the size at which Ray Data splits blocks, configure `MAX_SAFE_BLOCK_SIZE_FACTOR`. The default value is 1.5.

```{testcode}
import ray

ray.data.context.MAX_SAFE_BLOCK_SIZE_FACTOR = 1.5
```

Ray Data can't split rows, so if your dataset contains large rows, such as large images, Ray Data can't bound the block size.

(shuffle-algorithms)=

### What shuffle algorithms does Ray Data provide?

In data processing, *shuffling* is the process of redistributing a dataset's partitions. Ray Data calls these partitions {ref}`blocks <data_key_concepts>`.

Ray Data provides several shuffle backends. {ref}`Shuffle v2 <shuffle-v2>` is the default for key-based operations and is the intended replacement for the classical {ref}`hash-shuffling <hash-shuffle>` and {ref}`range-partitioning <range-partitioning-shuffle>` backends.

(shuffle-v2)=

#### How does shuffle v2 work?

:::{note}
Shuffle v2 is the default shuffle strategy for key-based operations, which are aggregations, group-by operations, key-based repartitioning, and joins. Its strategy value is `ShuffleStrategy.SHUFFLE_V2`.
:::

Shuffle v2 is the intended replacement for the other shuffle backends. It provides an updated hash-shuffle implementation but doesn't yet replace the range-partitioning shuffle. Unlike the aggregator-actor model that the previous {ref}`hash-shuffling <hash-shuffle>` implementation uses, shuffle v2 is *driver-driven* and doesn't store intermediate shuffle data in long-lived aggregator actors. Instead, that data lives in the object store, which has two effects:

- Ray can spill intermediate data to disk under memory pressure, avoiding the out-of-memory risk of holding whole partitions in aggregator memory.
- Reduce-side memory adapts to observed partition sizes, improving memory accounting on skewed datasets.

Shuffle v2 also coalesces shuffle inputs into larger batches before partitioning.

Shuffle v2 supports the following operations:

- Aggregations through {meth}`Dataset.aggregate <ray.data.Dataset.aggregate>`
- Group-by operations through {meth}`Dataset.groupby <ray.data.Dataset.groupby>`
- Key-based repartitioning through {meth}`Dataset.repartition <ray.data.Dataset.repartition>` with `keys`
- Joins through {meth}`Dataset.join <ray.data.Dataset.join>`

Shuffle v2 doesn't yet support {meth}`Dataset.sort <ray.data.Dataset.sort>` or {meth}`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>`, which use the {ref}`range-partitioning shuffle <range-partitioning-shuffle>`.

(tuning-shuffle-v2)=

##### Tune shuffle v2

Use the following two settings to tune shuffle v2:

**Input batch size**: `DataContext.shuffle_input_batch_bytes`, environment variable `RAY_DATA_SHUFFLE_INPUT_BATCH_BYTES`, default 1 GiB. This setting applies only to the `SHUFFLE_V2` strategy.

Shuffle v2 buffers input blocks from the same node until their combined size reaches this threshold, then partitions that batch as a unit. The threshold controls the trade-off between shuffle parallelism and the number of intermediate shard objects:

- A higher threshold produces fewer, larger intermediate shard objects and less object-store overhead, but reduces shuffle parallelism.
- A lower threshold increases shuffle parallelism at the cost of more, smaller intermediate objects. Lower the threshold if CPU utilization is low because the units of work are too coarse-grained.
- Set the threshold to `0` to disable batching and partition each input bundle individually.

**Inline object threshold**: `max_direct_call_object_size`, environment variable `RAY_max_direct_call_object_size`, default 100 KiB. This is a Ray Core setting, not a Ray Data setting.

When shuffle v2 partitions a block across many partitions, an individual partition's shard can be smaller than this threshold. Ray transfers objects below the threshold *inline*, storing them in the memory of the process that submitted the work instead of in the object store. That process is typically the driver on the head node. For a shuffle that produces many small shards, these inline objects accumulate on the head node and can cause an out-of-memory failure there.

To reduce the risk of head-node out-of-memory failures, lower this threshold, for example `RAY_max_direct_call_object_size=8192` for 8 KiB. With a lower threshold, only small metadata stays inline, and shard data travels through the object store, which is spillable and distributed across the cluster.

(hash-shuffle)=

(hash-shuffling)=

#### How does hash-shuffling work?

:::{note}
Hash-shuffling is available in Ray 2.46.
:::

Hash-shuffling is a classical hash-partitioning shuffle with three phases:

1. **Partition phase:** Ray Data hash-partitions the rows in every block into a specified number of partitions based on the values in the *key columns*, following the residual formula `hash(key-values) % N`.
1. **Push phase:** Ray Data pushes each partition's shards from the individual blocks to the corresponding aggregating actors, named `HashShuffleAggregator`, that handle the respective partitions.
1. **Reduce phase:** The aggregators combine the shards they receive for each partition back into blocks, optionally applying additional transformations, before producing the resulting blocks.

Hash-shuffling suits operations that require deterministic partitioning based on keys, such as joins, group-by operations, and key-based repartitioning, because it ensures that rows with the same key values land in the same partition.

:::{note}
Hash-shuffle was the default shuffle strategy for key-based operations before {ref}`shuffle v2 <shuffle-v2>` replaced it. To opt back in, set `ray.data.DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE` before creating a `Dataset`.
:::

(range-partitioning-shuffle)=

#### How does range-partitioning shuffle work?

Range-partitioning shuffle is also a classical algorithm. It splits the dataset into a target number of ranges, determined by boundaries that approximate the real ranges of the totally ordered, or sorted, dataset. It runs in three phases:

1. **Sampling phase:** Ray Data randomly samples 10 rows from every input block and combines the samples into a single dataset. It then sorts that dataset and splits it into the target number of partitions, which define approximate *range boundaries*.
1. **Partition phase:** Ray Data sorts every block and splits it into partitions based on the *range boundaries* from the previous step.
1. **Reduce phase:** Ray Data recombines the individual partitions within the same range to produce the resulting block.

(operators-plans-and-planning)=

### How does Ray Data plan execution?

The following sections describe operators, plans, the planner, plan optimization, and the types of physical operators.

#### What are operators?

Ray Data has two types of operators, *logical operators* and *physical operators*. Logical operators are stateless objects that describe "what" to do. Physical operators are stateful objects that describe "how" to do it. For example, `ReadOp` is a logical operator, and `TaskPoolMapOperator` is a physical operator.

#### What are plans?

A *logical plan* is a series of logical operators, and a *physical plan* is a series of physical operators. When you call APIs such as {func}`ray.data.read_images` and {meth}`ray.data.Dataset.map_batches`, Ray Data produces a logical plan. When execution starts, the planner generates a corresponding physical plan.

#### What does the planner do?

The Ray Data planner translates logical operators to one or more physical operators. For example, the planner translates the `ReadOp` logical operator into two physical operators, an `InputDataBuffer` and a `TaskPoolMapOperator`. The `ReadOp` logical operator only describes the input data, while the `TaskPoolMapOperator` physical operator launches tasks to read the data.

(plan-optimization)=

#### How does Ray Data optimize plans?

Ray Data applies optimizations to both logical and physical plans. For example, the `OperatorFusionRule` combines a chain of physical map operators into a single map operator, which prevents unnecessary serialization between map operators.

To add custom optimization rules, implement a class that extends `Rule` and configure `DEFAULT_LOGICAL_RULES` or `DEFAULT_PHYSICAL_RULES`.

```{testcode}
import ray
from ray.data._internal.logical.interfaces import Rule
from ray.data._internal.logical.optimizers import get_logical_ruleset

class CustomRule(Rule):
    def apply(self, plan):
        ...

logical_ruleset = get_logical_ruleset()
logical_ruleset.add(CustomRule)
```

```{testcode}
:hide:

logical_ruleset.remove(CustomRule)
```

(types-of-physical-operators)=

#### What types of physical operators does Ray Data use?

Physical operators take in a stream of block references and output another stream of block references. Some physical operators launch Ray tasks and actors to transform the blocks, and others only manipulate the references.

`MapOperator` is the most common operator. It implements all read, transform, and write operations. To process data, `MapOperator` implementations use either Ray tasks or Ray actors.

Non-map operators include `OutputSplitter` and `LimitOperator`. These two operators manipulate references to data but don't launch tasks or modify the underlying data.

### How does Ray Data execute a dataset?

The following sections describe the executor, out queues, streaming execution, and the scheduling loop.

#### What's the executor?

The *executor* schedules tasks and moves data between physical operators.

The executor and operators run on the process where dataset execution starts. For batch inference jobs, this process is usually the driver. For training jobs, the executor runs on a special actor called `SplitCoordinator`, which handles {meth}`~ray.data.Dataset.streaming_split`.

Ray schedules the tasks and actors that operators launch across the cluster and stores their outputs in Ray's distributed object store. The executor manipulates references to those objects and doesn't fetch the underlying data.

(out-queues)=

#### What's an out queue?

Each physical operator has an associated *out queue*. When a physical operator produces outputs, the executor moves the outputs to the operator's out queue.

(streaming_execution)=

#### What's streaming execution?

Unlike bulk synchronous execution, Ray Data's streaming execution doesn't wait for one operator to complete before starting the next. Each operator takes in and outputs a stream of blocks. With this approach, you can process datasets that are too large to fit in your cluster's memory.

(the-scheduling-loop)=

#### How does the scheduling loop work?

The executor runs a loop. Each iteration of the loop does the following:

1. Wait until running tasks and actors have new outputs.
1. Move new outputs into the appropriate operator out queues.
1. Choose some operators and assign new inputs to them. These operators process the new inputs by either launching new tasks or manipulating metadata.

Choosing the best operator to assign inputs to is one of the most important decisions in Ray Data, because it's critical to the performance, stability, and scalability of a Ray Data job. The executor can schedule an operator if the operator satisfies the following conditions:

* The operator has inputs.
* Adequate resources are available.
* The operator isn't backpressured.

If multiple operators are viable, the executor chooses the operator with the smallest out queue.

## How does Ray Data schedule tasks?

Ray Data uses Ray Core for execution. The following list summarizes the {ref}`scheduling strategy <ray-scheduling-strategies>` for Ray Data:

* The `SPREAD` scheduling strategy ensures that data blocks and map tasks are evenly balanced across the cluster.
* Map operations use the `SPREAD` scheduling strategy if the total argument size is less than 50 MB. Otherwise, they use the `DEFAULT` scheduling strategy.
* Read operations use the `SPREAD` scheduling strategy.
* All other operations, such as split, sort, and shuffle, use the `DEFAULT` scheduling strategy.

(datasets_tune)=

(ray-data-and-tune)=

### Use Ray Data with Ray Tune

When you use Ray Data with {ref}`Ray Tune <tune-main>`, leave enough free CPUs for Ray Data to run on. By default, Tune tries to fully use cluster CPUs, which can prevent Ray Data from scheduling tasks and reduce performance or cause workloads to hang.

To ensure CPU resources are always available for Ray Data execution, limit the number of concurrent Tune trials with the `max_concurrent_trials` Tune option.

```{literalinclude} ./doc_code/key_concepts.py
:language: python
:start-after: __resource_allocation_1_begin__
:end-before: __resource_allocation_1_end__
```

(data_memory_management)=

## How does Ray Data manage memory?

This section describes how Ray Data manages execution and object store memory.

Ray divides each node's memory into three pools. By default, it reserves 30% for the object store and 10% for system overhead, and treats the remaining memory as logical memory.

```{image} ./data-memory-model-1.svg
:width: 300
:align: center
```

Each pool serves a different purpose:

- **Logical memory** is what's available for the heap of UDFs and built-in transformations such as reads.
- **Object store** holds buffered blocks.
- **System memory** is what's left for Ray Core processes, such as the raylet, and for other processes outside your tasks.

:::{note}
Zero-copy deserializable objects are an exception. The UDF uses them, but Ray Data accounts for them only in the object store, so they serve as both the buffer and the working memory.
:::

```{image} ./data-memory-model-2.svg
:width: 360
:align: center
```

When a UDF processes data, it uses heap memory to do the work. For example, a UDF that calls a Torch preprocessor holds the tensors on the heap. As the UDF produces output rows or batches, Ray Data serializes them into PyArrow tables and stores them in the shared object store.

```{image} ./data-memory-model-3.svg
:width: 550
:align: center
```

To limit object store use, Ray Data applies backpressure and stops launching tasks once enough data is buffered. If Ray Data produces more data than fits, Ray Core *spills* those objects to disk.

:::{note}
A common misconception is that heavy queuing causes out-of-memory (OOM) errors. Heavy object store use does contribute to worker OOMs by leaving less memory for the heaps of tasks and actors. But heavy queuing doesn't cause OOMs directly, because Ray spills objects to disk. If Ray Data queues too much data, you see out-of-disk errors instead.
:::

To limit heap memory use, Ray Data relies on memory hints that you provide to estimate how much heap memory each UDF needs. It passes those hints to Ray Core so the scheduler doesn't oversubscribe the cluster. These hints don't enforce any OS-level limit. They only guide scheduling.
