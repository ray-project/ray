---
myst:
  html_meta:
    description: "Core Ray Data concepts: Datasets and blocks, the lazy streaming execution model, and how data is partitioned and processed in parallel."
---

(data_key_concepts)=

# Key concepts

## Datasets and blocks

Ray Data has two main concepts, datasets and blocks.

A {class}`Dataset <ray.data.Dataset>` represents a distributed collection of data and defines the operations that load and process it. It's the primary API you work with in Ray Data. You typically create a {class}`Dataset <ray.data.Dataset>` from external storage or in-memory data, apply transformations to the data, and then write the outputs to external storage or feed them to training workers.

The Dataset API is lazy. Ray Data doesn't execute operations until you materialize or consume the dataset with a method such as {meth}`~ray.data.Dataset.show`. Because execution waits until then, Ray Data can optimize the execution plan and execute operations in a pipelined, streaming fashion.

A *block* is a set of rows that represents a single partition of the dataset. Blocks hold their rows in columnar formats such as Arrow, and they're the basic unit of data processing in Ray Data. Ray Data processes a dataset as follows:

1. Ray Data partitions every dataset into a number of blocks.
1. Ray Data distributes and parallelizes processing of the whole dataset at the block level. It processes blocks in parallel and, for the most part, independently.

The following figure shows a dataset with three blocks, each holding 1000 rows. Ray Data holds the {class}`~ray.data.Dataset` on the process that triggers execution. That process is usually the entrypoint of the program, called the {term}`driver`. Ray Data stores the blocks as objects in Ray's shared-memory {ref}`object store <objects-in-ray>`. Internally, Ray Data can natively handle a block as either a pandas `DataFrame` or a PyArrow `Table`.

```{image} images/dataset-arch-with-blocks.svg
```
<!--
https://docs.google.com/drawings/d/1kOYQqHdMrBp2XorDIn0u0G_MvFj-uSA4qm6xf9tsFLM/edit
-->

## Operators and plans

Ray Data uses a two-phase planning process to execute operations efficiently. When you write a program with the Dataset API, Ray Data first builds a *logical plan*, which is a high-level description of what operations to perform. When execution begins, Ray Data converts the logical plan into a *physical plan* that specifies exactly how to execute those operations.

The following diagram shows the complete planning process.

<!-- https://docs.google.com/drawings/d/1WrVAg3LwjPo44vjLsn17WLgc3ta2LeQGgRfE8UHrDA0/edit -->

```{image} images/get_execution_plan.svg
:width: 600
:align: center
```

Operators are the building blocks of these plans. Ray Data uses two kinds of operators, one for each plan:

* Logical plans consist of *logical operators* that describe *what* operation to perform. For example, when you write `dataset = ray.data.read_parquet(...)`, Ray Data creates a `ReadOp` logical operator to specify what data to read.
* Physical plans consist of *physical operators* that describe *how* to execute the operation. For example, Ray Data converts the `ReadOp` logical operator into a `TaskPoolMapOperator` physical operator that launches Ray tasks to read the data.

The following example shows how Ray Data builds a logical plan. As you chain operations, Ray Data constructs the logical plan behind the scenes:

```{testcode}
import ray

dataset = ray.data.range(100)
dataset = dataset.add_column("test", lambda x: x["id"] + 1)
dataset = dataset.select_columns("test")
```

You can inspect the resulting logical plan by printing the dataset:

```
Project
+- MapBatches(add_column)
   +- Dataset(schema={...})
```

When execution begins, Ray Data optimizes the logical plan and then translates it into a physical plan, which is a series of operators that implement the data transformations. Two things happen during this translation:

* A single logical operator can become multiple physical operators. For example, `ReadOp` becomes both `InputDataBuffer` and `TaskPoolMapOperator`.
* Both logical and physical plans go through optimization passes. For example, `OperatorFusionRule` combines map operators to reduce serialization overhead.

Each physical operator does the following:

* Takes in a stream of block references.
* Performs its operation, either by transforming data with Ray tasks or actors, or by manipulating references.
* Outputs another stream of block references.

For more details on Ray tasks and actors, see {ref}`Ray Core Concepts <core-key-concepts>`.

:::{note}
A dataset's execution plan only runs when you materialize or consume the dataset through operations such as {meth}`~ray.data.Dataset.show`.
:::

(streaming-execution)=

## Streaming execution model

Ray Data can stream data through a pipeline of operators to process large datasets efficiently.

With streaming execution, different operators in an execution can scale independently while they run concurrently, which makes resource allocation more flexible and fine-grained. For example, if two map operators require different amounts or types of resources, the streaming execution model can run them concurrently and independently while maintaining high performance.

:::{note}
Streaming is primarily useful for non-shuffle operations. Shuffle operations such as {meth}`ds.sort() <ray.data.Dataset.sort>` and {meth}`ds.groupby() <ray.data.Dataset.groupby>` require materializing data, which stops streaming until the shuffle is complete.
:::

The following example shows how streaming execution works in Ray Data:

```python
import ray

# Create a dataset with 1K rows
ds = ray.data.read_parquet(...)

# Define a pipeline of operations
ds = ds.map(cpu_function, num_cpus=2)
ds = ds.map(GPUClass, num_gpus=1)
ds = ds.map(cpu_function2, num_cpus=4)
ds = ds.filter(filter_func)

# Data starts flowing when you call a method like show()
ds.show(5)
```

This code creates a logical plan like the following:

```
Filter(filter_func)
+- Map(cpu_function2)
   +- Map(GPUClass)
      +- Map(cpu_function)
            +- Dataset(schema={...})
```

The streaming topology looks like the following:

<!-- https://docs.google.com/drawings/d/10myFIVtpI_ZNdvTSxsaHlOhA_gHRdUde_aHRC9zlfOw/edit -->

```{image} images/streaming-topology.svg
:width: 1000
:align: center
```

In the streaming execution model, operators form a pipeline, and each operator's output queue feeds directly into the input queue of the next downstream operator. This design creates an efficient flow of data through the execution plan.

Because of the pipeline, multiple stages can execute concurrently, which improves overall performance and resource utilization. For example, if the map operator requires GPU resources, the streaming execution model can execute the map operator concurrently with the filter operator, which might run on CPUs. This way, the pipeline uses the GPU effectively through its entire duration.

For more about the streaming execution model, see this [Anyscale blog post on streaming execution across CPUs and GPUs](https://www.anyscale.com/blog/streaming-distributed-execution-across-cpus-and-gpus).
