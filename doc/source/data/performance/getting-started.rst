.. _performance_getting_started:

========================================
Getting Started with Ray Data Performance Optimization
========================================

This guide helps you establish a foundation for Ray Data performance optimization. You'll learn to measure performance, identify bottlenecks, and apply your first optimizations.

**What You'll Learn:**
- How to measure Ray Data performance using built-in tools
- The most common performance issues and how to spot them
- Simple optimizations that can provide significant performance improvements
- How to verify that your optimizations actually worked

**If You're New to Ray Data:** Don't worry! We'll explain Ray Data concepts as we encounter them. Ray Data is a universal distributed data processing library similar to Apache Spark or Dask. It handles traditional ETL workloads, modern ML/AI pipelines, and emerging data processing patterns. Ray Data automatically parallelizes your data processing code across multiple machines, similar to how Spark distributes DataFrames across a cluster, but with unified support for traditional batch processing, mixed CPU/GPU workloads, and streaming execution.

**Prerequisites for This Guide:** You should know basic Python and have worked with data processing libraries like pandas or NumPy. Experience with Ray Data is helpful but not required - we'll explain Ray Data concepts as needed.

Prerequisites
=============

Before optimizing Ray Data performance, ensure you have:

**Environment Setup**
- Ray Data installed: ``pip install -U 'ray[data]'``
- Access to your data source (local files, S3, GCS, etc.)
- Basic familiarity with Ray Data concepts (:ref:`data_key_concepts`)

**Baseline Measurement**
- A representative workload to optimize
- Ability to run the workload multiple times for comparison
- Access to Ray Dashboard for monitoring

Performance Measurement Fundamentals
===================================

Establish Performance Baselines
-------------------------------

Always measure before optimizing. Here's how to collect baseline metrics:

.. testcode::

    import ray
    import time
    
    # Enable detailed performance tracking
    ctx = ray.data.DataContext.get_current()
    ctx.enable_progress_bars = True
    ctx.enable_operator_progress_bars = True
    
    # Record start time
    start_time = time.time()
    
    # Your Ray Data pipeline
    ds = ray.data.read_parquet("s3://my-bucket/data/")
    result = ds.map_batches(lambda batch: batch).materialize()
    
    # Note: Ray Data splits your data into "blocks" (like Spark partitions)
    # Each block is processed by a separate task in parallel
    
    # Record end time and print stats
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print(result.stats())

**Expected output:**

.. code-block:: text

    Total execution time: 12.34 seconds
    Operator 1 ReadParquet: 4 tasks executed, 4 blocks produced in 2.1s
    * Remote wall time: 1.2s min, 2.1s max, 1.6s mean, 6.4s total
    * Peak heap memory usage (MiB): 128 min, 256 max, 192 mean
    * Output num rows: 1000 min, 1000 max, 1000 mean, 4000 total

**Key metrics to capture:**
- **Total execution time**: End-to-end pipeline duration
- **Throughput**: Rows or bytes processed per second  
- **Resource utilization**: CPU, GPU, memory usage
- **I/O performance**: Read/write speeds
- **Object store usage**: Memory consumption patterns

Understanding Ray Data Stats
---------------------------

Ray Data provides detailed execution statistics. Here's how to interpret them:

.. code-block:: text

    Execution Summary:
    * Operator 1 ReadParquet: 4 tasks executed, 4 blocks produced in 2.1s
      * Remote wall time: 1.2s min, 2.1s max, 1.6s mean, 6.4s total
      * Remote cpu time: 0.8s min, 1.1s max, 0.95s mean, 3.8s total  
      * Peak heap memory usage (MiB): 128 min, 256 max, 192 mean
      * Output num rows: 1000 min, 1000 max, 1000 mean, 4000 total
      * Output size bytes: 125000 min, 125000 max, 125000 mean, 500000 total

**What each metric means (explained for beginners):**

- **Remote wall time**: The actual clock time each task took from start to finish. This includes time spent waiting for data to load from storage or network.
- **Remote cpu time**: The time spent actually processing data on the CPU. This excludes waiting time.
- **Peak heap memory**: The maximum amount of RAM each task used. High values might indicate memory pressure.
- **Output rows/bytes**: How much data was processed - useful for calculating throughput.

**How to Interpret These Numbers:**

- **Wall time much higher than CPU time?** Your tasks are spending a lot of time waiting for I/O (reading files, network transfers). This suggests I/O bottlenecks.
- **High memory usage?** Your tasks are using a lot of RAM. If this is close to your available memory, you might need to reduce batch sizes.
- **Many small tasks?** If you see hundreds of tiny tasks, you might have too many small files or blocks.

.. tip::
   **For beginners**: Start by looking at wall time (total execution time) and memory usage. These two metrics will help you identify the most common performance issues.

Identifying Performance Bottlenecks
===================================

Common Performance Issues
------------------------

Use this checklist to identify the most common Ray Data performance issues:

**I/O Bottlenecks**
□ Reading many small files (< 1MB each)
□ Not using column pruning when reading Parquet
□ Inefficient file formats (CSV vs Parquet)
□ Network latency to data sources

**Transform Bottlenecks**  
□ Using :meth:`~ray.data.Dataset.map` instead of :meth:`~ray.data.Dataset.map_batches`
□ Using pandas batch_format unnecessarily
□ Non-vectorized operations in transforms
□ Memory-intensive operations without proper sizing

**Resource Bottlenecks**
□ Incorrect batch sizes causing OOM or underutilization
□ Wrong concurrency settings for workload
□ Not utilizing available GPUs
□ Excessive object store usage

**System Bottlenecks**
□ Insufficient cluster resources
□ Network bandwidth limitations
□ Disk I/O limitations

Using Ray Dashboard for Performance Analysis
------------------------------------------

The Ray Dashboard is your primary tool for monitoring Ray Data performance. It provides real-time insights without requiring custom monitoring code.

**Dashboard Navigation (Step-by-Step for Beginners):**

Ray Dashboard is a web interface that shows you what's happening with your Ray Data jobs in real-time. Think of it like the task manager on your computer, but for distributed data processing.

1. **Navigate to Ray Dashboard** (usually http://localhost:8265) - Open this URL in your web browser
2. **Go to Jobs tab** to see your Ray Data job - This shows all the data processing jobs you've run
3. **Check Metrics tab** for resource utilization graphs - This shows how much CPU, memory, and GPU your job is using
4. **Use Timeline view** to identify bottlenecks and task scheduling - This shows when each task ran and how long it took

**Key Dashboard Metrics to Monitor (Beginner-Friendly Explanations):**

- **CPU Utilization**: Shows how busy your processors are. High is good (70-90%) - it means your CPUs are actively working. Low CPU usage might mean Ray Data isn't using your hardware efficiently.

- **Memory Usage**: Shows how much RAM is being used. Watch for steady growth over time - this might indicate memory leaks where your program keeps using more and more memory.

- **Network I/O**: Shows data transfer rates. High network activity is normal during data loading/writing phases when Ray Data is moving data to/from storage.

- **Object Store Memory**: Ray Data's shared memory space. Should stay below 80% to avoid "spilling" (writing to disk), which is much slower than keeping data in memory.

- **Task Timeline**: Visual representation of when each task runs. Look for gaps where no tasks are running - this might indicate that Ray Data isn't keeping your cluster busy.

**Interpreting Dashboard Metrics (What Problems Look Like):**

- **Low CPU with many tasks**: Your CPUs aren't busy despite having lots of work. This usually means tasks are too small and scheduling overhead is dominating.

- **High memory with spilling warnings**: Your data chunks (blocks) are too large, or you're running too many tasks simultaneously, causing memory pressure.

- **High network I/O with low CPU**: Your tasks are spending most of their time waiting for data transfers rather than processing. This suggests I/O bottlenecks.

- **Red failed tasks in timeline**: Tasks that crashed, usually due to out-of-memory errors or data corruption.

Your First Optimizations
========================

Quick Win #1: Use map_batches Instead of map
--------------------------------------------

**Problem**: Row-by-row processing is inefficient
**Solution**: Process data in vectorized batches

.. tab-set::

    .. tab-item:: INEFFICIENT Inefficient (map)

        .. code-block:: python

            # Processes one row at a time - slow!
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map(lambda row: {"value": row["value"] * 2})

    .. tab-item:: EFFICIENT Efficient (map_batches)

        .. code-block:: python

            # Processes batches of rows - much faster!
            ds = ray.data.read_parquet("data.parquet") 
            result = ds.map_batches(lambda batch: {"value": batch["value"] * 2})

**Verify the improvement:**

.. testcode::

    # Check that batching was used
    print(result.stats())

**Expected output:**

.. code-block:: text

    Operator 1 ReadParquet->MapBatches(<lambda>): 4 tasks executed, 4 blocks produced in 0.8s
    * Batch size: 1024 (default batching)

**Potential improvement**: Significant speedup for vectorizable operations

The performance gain comes from vectorization - instead of calling your function thousands of times for individual rows, Ray Data calls it fewer times with batches of rows. This allows libraries like NumPy to use optimized CPU instructions (SIMD) and reduces function call overhead.

Quick Win #2: Use Column Pruning
--------------------------------

**Problem**: Reading unnecessary columns wastes I/O and memory
**Solution**: Specify only needed columns when reading

Column pruning is one of the most effective I/O optimizations because it reduces the amount of data transferred from storage to your cluster. When reading from columnar formats like Parquet, Ray Data can skip entire columns at the file level, dramatically reducing network traffic and memory usage.

This optimization is particularly effective for wide tables (many columns) where you only need a subset of columns for processing. The savings multiply across your entire pipeline - less data to read, transfer, store, and process.

.. tab-set::

    .. tab-item:: INEFFICIENT Reading all columns

        .. code-block:: python

            # Reads all columns - wasteful if you only need a few
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map_batches(lambda batch: batch["important_column"])

    .. tab-item:: EFFICIENT Column pruning

        .. code-block:: python

            # Only reads the column you need - much more efficient
            ds = ray.data.read_parquet("data.parquet", columns=["important_column"])
            result = ds.map_batches(lambda batch: batch["important_column"])

**Verify column pruning worked:**

.. testcode::

    # Check that only the specified column was read
    print(f"Schema: {ds.schema()}")
    print(f"Columns read: {list(ds.schema().names)}")

**Expected output:**

.. code-block:: text

    Schema: important_column: string
    Columns read: ['important_column']

**Potential improvement**: Substantial reduction in I/O and memory usage

The improvement depends on how many columns you eliminate. Reading fewer columns from wide tables can significantly reduce data transfer and memory usage, which may translate to faster execution and potentially lower costs.

Quick Win #3: Optimize Block Sizes
----------------------------------

**Problem**: Default block sizes may not match your workload
**Solution**: Tune ``override_num_blocks`` based on data size and cluster

Block size optimization balances parallelism with efficiency. Ray Data processes data in blocks (chunks of rows), and each block is handled by a separate task. Too many small blocks create task scheduling overhead, while too few large blocks limit parallelism and can cause memory issues.

The optimal block count typically ranges from 2-4x your cluster's CPU count. This ensures good parallelization without overwhelming the scheduler. For very large datasets, you might need more blocks to prevent individual blocks from becoming too large and causing out-of-memory errors.

.. code-block:: python

    import ray
    
    # For small datasets (< 1GB): Use fewer blocks
    ds = ray.data.read_parquet("small_data.parquet", override_num_blocks=4)
    
    # For large datasets (> 100GB): Use more blocks  
    ds = ray.data.read_parquet("large_data.parquet", override_num_blocks=200)
    
    # General rule: 2-4x the number of CPU cores in your cluster
    num_cpus = ray.cluster_resources()["CPU"]
    optimal_blocks = int(num_cpus * 3)
    ds = ray.data.read_parquet("data.parquet", override_num_blocks=optimal_blocks)

**Verify block optimization:**

.. testcode::

    # Check the actual number of blocks created
    materialized = ds.materialize()
    print(f"Requested blocks: {optimal_blocks}")
    print(f"Actual blocks: {materialized.num_blocks()}")
    print(f"Average block size: {materialized.size_bytes() / materialized.num_blocks() / (1024**2):.1f}MB")

**Expected output:**

.. code-block:: text

    Requested blocks: 24
    Actual blocks: 24
    Average block size: 64.2MB

**Potential improvement**: Possible throughput improvement

Performance Baseline Expectations
=================================

Understanding typical Ray Data performance helps you set realistic expectations and identify when optimizations are needed.

**Typical Performance Ranges by Workload:**

**Typical Performance Characteristics by Workload:**

Performance varies significantly based on data characteristics, cluster configuration, and operation complexity. These ranges provide general guidance:

**Traditional ETL Workloads:**
- **CSV processing**: Varies widely based on file size and parsing complexity
- **Parquet analytics**: Generally faster due to columnar format and compression
- **Data cleaning/transformation**: Depends on transformation complexity
- **Aggregations**: Performance varies with grouping cardinality and data size

**ML/AI Workloads:**
- **Feature engineering**: Depends on feature complexity and data types
- **Image preprocessing**: Varies with image size and processing operations
- **Text processing**: Depends on text length and processing complexity
- **Model inference**: Varies significantly with model size and hardware

**Real-time Processing:**
- **Streaming ingestion**: Limited by network bandwidth and data source
- **Real-time transformations**: Depends on processing complexity and latency requirements
- **Event processing**: Varies with event size and processing logic

**When to Consider Optimization:**

Consider optimization when performance doesn't meet your application requirements or when you observe resource underutilization in Ray Dashboard.

Measuring Your Improvements
===========================

Compare Before and After
------------------------

Always measure the impact of your optimizations:

.. code-block:: python

    import ray
    import time
    
    def benchmark_pipeline(name, pipeline_func):
        """Benchmark a Ray Data pipeline."""
        start_time = time.time()
        result = pipeline_func()
        end_time = time.time()
        
        execution_time = end_time - start_time
        stats = result.stats()
        
        print(f"\n{name} Results:")
        print(f"Execution time: {execution_time:.2f}s")
        print(f"Throughput: {len(result) / execution_time:.0f} rows/sec")
        return execution_time
    
    # Benchmark original implementation
    def original_pipeline():
        return ray.data.read_parquet("data.parquet").map(lambda x: x).materialize()
    
    # Benchmark optimized implementation  
    def optimized_pipeline():
        return ray.data.read_parquet("data.parquet", columns=["needed_col"]) \
                      .map_batches(lambda batch: batch, override_num_blocks=16) \
                      .materialize()
    
    original_time = benchmark_pipeline("Original", original_pipeline)
    optimized_time = benchmark_pipeline("Optimized", optimized_pipeline)
    
    improvement = original_time / optimized_time
    print(f"\nPerformance improvement: {improvement:.1f}x faster")

Performance Testing Best Practices
----------------------------------

**Performance Testing Best Practices**

- **Use Ray Dashboard** as your primary monitoring tool
- **Test with representative data sizes** that match your production workload
- **Run tests consistently** with the same cluster configuration
- **Monitor Ray Dashboard metrics** during test runs to identify bottlenecks

**Key Metrics to Track in Ray Dashboard:**

- **Execution time**: Visible in the Jobs tab timeline
- **Resource utilization**: CPU, memory, GPU usage graphs in Metrics tab  
- **Object store usage**: Memory pressure indicators
- **Task performance**: Individual task execution times and failures
- **Network I/O**: Data transfer rates during read/write operations

Next Steps
==========

Now that you have the basics, choose your optimization path:

**For Immediate Results**
→ Follow the :ref:`quickfix_path` to apply high-impact optimizations

**For Systematic Learning**  
→ Start the :ref:`beginner_path` to build comprehensive optimization skills

**For Specific Issues**
→ Jump to targeted optimization guides:
- :ref:`reading_optimization` for I/O issues
- :ref:`transform_optimization` for processing bottlenecks  
- :ref:`memory_optimization` for memory issues
- :ref:`troubleshooting` for debugging help

**For Advanced Users**
→ Explore :ref:`patterns_antipatterns` to learn expert-level techniques

.. tip::
   **Keep Learning**: Performance optimization is iterative. Start with quick wins, measure improvements, then tackle more advanced optimizations as you gain experience.
