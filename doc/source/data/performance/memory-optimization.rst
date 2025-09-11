.. _memory_optimization:

======================================
Ray Data Memory Optimization
======================================

Memory management is critical for Ray Data performance and reliability. This guide covers comprehensive strategies to optimize memory usage, prevent out-of-memory errors, and handle large datasets efficiently.

.. contents::
   :local:
   :depth: 2

Memory Optimization Quick Wins
==============================

Start with these immediate memory optimizations:

**Essential Memory Settings**
- Configure ``DataContext`` memory limits appropriately
- Tune batch sizes to prevent OOM errors
- Use streaming execution instead of materialization
- Monitor object store usage regularly

**Block Size Optimization**
- Set appropriate ``target_max_block_size`` for your workload
- Use ``override_num_blocks`` to control parallelism vs memory trade-offs
- Avoid creating too many small blocks

**Transform Memory Management**
- Set memory limits in ``ray_remote_args``
- Use in-place operations when possible
- Clean up intermediate results explicitly

Understanding Ray Data Memory Model
===================================

Before diving into optimization techniques, it's essential to understand how Ray Data manages memory across different components of the system.

**Memory Management for Data Scientists and Engineers:**

Ray Data's memory model is similar to other distributed systems but with some key differences optimized for ML workloads:

- **Object Store**: Similar to Spark's BlockManager or Dask's distributed memory - stores data blocks that can be shared across tasks
- **Task Heap Memory**: Like Spark executor memory - temporary RAM used during data processing within individual tasks
- **GPU Memory**: Specialized memory for GPU-accelerated operations, similar to CUDA memory management but integrated with the distributed system

Unlike Spark where you primarily tune executor memory, Ray Data requires managing multiple memory spaces because it supports diverse workloads with varying memory patterns - from traditional ETL (primarily CPU and object store memory) to ML pipelines (complex CPU → GPU → CPU patterns) to emerging real-time AI workloads (streaming memory management).

Ray Data automatically manages these different memory spaces, but understanding them helps you optimize performance and prevent out-of-memory errors common in large-scale ML pipelines.

Ray Data Memory Components
--------------------------

Ray Data uses multiple memory spaces that need to be managed. Understanding these components helps you identify where memory bottlenecks occur and how to address them.

**For Beginners:** Don't worry about memorizing all these components - focus on understanding that Ray Data uses different types of memory for different purposes, and each can become a bottleneck if not managed properly.

.. list-table:: Ray Data Memory Components
   :header-rows: 1
   :class: memory-components-table

   * - Component
     - Purpose
     - Default Limit
     - Optimization Strategy
   * - **Object Store**
     - Stores dataset blocks
     - 30% of system RAM
     - Control block sizes, avoid materialization
   * - **Heap Memory**
     - Task execution memory
     - No limit (dangerous!)
     - Set explicit limits, monitor usage
   * - **Shared Memory**
     - Inter-process communication
     - System dependent
     - Minimize data copying
   * - **GPU Memory**
     - GPU computations
     - GPU VRAM size
     - Batch size tuning, memory pooling

**Memory Flow Visualization**

Understanding how data flows through Ray Data's memory system helps you optimize at each stage:

.. code-block:: text

    Data Source → Object Store → Task Heap → Processing → Object Store → Output
                     ↑              ↑                        ↑
                 Block Storage   Transform Memory        Result Storage
                 (Persistent)    (Temporary)             (Persistent)

This flow shows three critical memory management points:

1. **Block Storage**: Blocks (Ray Data's equivalent of Spark partitions - logical chunks of your dataset) are stored persistently in Ray's object store (distributed shared memory)
2. **Transform Memory**: Temporary heap memory used during data processing in tasks (similar to Spark executor memory)
3. **Result Storage**: Processed results stored back in the object store for downstream operations

DataContext Memory Configuration
===============================

Core Memory Settings
--------------------

Ray Data's ``DataContext`` provides centralized control over memory settings. Think of it as the "settings menu" for Ray Data - it's where you configure how Ray Data behaves across your entire program.

**For Beginners: What is DataContext?**

DataContext is like the control panel for Ray Data. Just like you might adjust settings in a video game (graphics quality, difficulty level, etc.), DataContext lets you adjust how Ray Data processes your data. The key difference is that these settings affect performance rather than gameplay.

Once you change DataContext settings, they apply to all Ray Data operations in your program until you change them again. This is different from passing parameters to individual functions - DataContext sets global defaults.

**Key Memory Configuration Areas:**

1. **Block Size Limits**: Control how Ray Data splits your data into chunks (like choosing page size for a book)
2. **Resource Limits**: Set boundaries for CPU, GPU, and memory usage (like setting speed limits)
3. **Execution Options**: Configure how operations are scheduled and executed (like choosing traffic routing rules)

Here's how to configure the basic memory settings:

.. testcode::

    import ray
    
    # Get the current context - this affects all Ray Data operations
    ctx = ray.data.DataContext.get_current()

**Configure Block Sizes**

Block size configuration is fundamental to Ray Data memory management. Each block represents a chunk of your dataset that gets stored in Ray's object store and processed by individual tasks.

The block size settings control how Ray Data partitions your data:

- **target_max_block_size**: Prevents individual blocks from becoming too large and causing out-of-memory errors
- **target_min_block_size**: Prevents blocks from becoming too small and creating excessive task overhead

The trade-off is between memory efficiency and processing efficiency. Smaller blocks use less memory per task but create more tasks and scheduling overhead. Larger blocks are more efficient to process but consume more memory.

.. testcode::

    # Configure block sizes (affects object store usage)
    ctx.target_max_block_size = 64 * 1024 * 1024   # 64MB max blocks
    ctx.target_min_block_size = 1 * 1024 * 1024    # 1MB min blocks

These settings tell Ray Data to aim for blocks between 1MB and 64MB. Ray Data will automatically adjust the number of blocks to stay within these bounds based on your data size.

**Set Resource Limits**

Resource limits prevent Ray Data from overwhelming your system:

.. testcode::

    # Configure execution resources
    ctx.execution_options.resource_limits.cpu = 8
    ctx.execution_options.resource_limits.gpu = 2
    ctx.execution_options.resource_limits.object_store_memory = 2 * 1024**3  # 2GB

**Memory-Conscious Configuration**

For environments with limited memory (like laptops or small cloud instances), use conservative settings that prioritize memory efficiency over raw performance:

.. testcode::

    import psutil
    
    ctx = ray.data.DataContext.get_current()
    
    # Use smaller blocks to reduce memory pressure
    ctx.target_max_block_size = 16 * 1024 * 1024   # 16MB blocks
    ctx.target_min_block_size = 1 * 1024 * 1024    # 1MB minimum

This configuration trades some performance for memory safety. Smaller blocks mean more tasks and overhead, but they prevent out-of-memory errors:

.. testcode::

    # Limit object store to 10% of system memory
    available_memory = psutil.virtual_memory().total
    ctx.execution_options.resource_limits.object_store_memory = int(
        available_memory * 0.1
    )
    
    # Enable streaming optimizations for memory efficiency
    ctx.execution_options.preserve_order = False  # Allow reordering

**Verify memory configuration:**

.. testcode::

    # Check that configuration was applied
    print(f"Max block size: {ctx.target_max_block_size / (1024**2):.0f}MB")
    print(f"Object store limit: {ctx.execution_options.resource_limits.object_store_memory / (1024**3):.1f}GB")
    print(f"Preserve order: {ctx.execution_options.preserve_order}")

**Expected output:**

.. code-block:: text

    Max block size: 16MB
    Object store limit: 1.6GB
    Preserve order: False

Verify the configuration has been applied:

.. testcode::

    print(f"Configured for memory-constrained environment:")
    print(f"  Max block size: {ctx.target_max_block_size / (1024*1024):.0f}MB")
    print(f"  Object store limit: {ctx.execution_options.resource_limits.object_store_memory / (1024**3):.1f}GB")

**High-Memory Configuration**

.. testcode::

    def configure_for_high_memory_environment():
        """Configure Ray Data for high-memory environments."""
        
        ctx = ray.data.DataContext.get_current()
        
        # Larger blocks for better throughput
        ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
        ctx.target_min_block_size = 64 * 1024 * 1024   # 64MB minimum
        
        # Use more object store memory
        available_memory = psutil.virtual_memory().total
        ctx.execution_options.resource_limits.object_store_memory = int(
            available_memory * 0.4  # Use 40% of system memory
        )
        
        print(f"Configured for high-memory environment:")
        print(f"  Max block size: {ctx.target_max_block_size / (1024*1024):.0f}MB")
        print(f"  Object store limit: {ctx.execution_options.resource_limits.object_store_memory / (1024**3):.1f}GB")
    
    # Apply high-memory configuration
    configure_for_high_memory_environment()

Block Size Optimization
=======================

Understanding Block Size Impact
------------------------------

Blocks are Ray Data's fundamental unit of data processing - each block contains a subset of your dataset's rows, similar to how Spark partitions contain subsets of a DataFrame. Block size is one of the most important factors affecting both memory usage and performance.

**Why Block Size Matters:**

- **Memory Impact**: Larger blocks consume more memory but reduce overhead
- **Parallelism Impact**: More blocks enable better parallel processing
- **Network Impact**: Fewer, larger blocks reduce network transfer overhead
- **Processing Impact**: Block size affects how efficiently your transformations run

The key is finding the right balance for your specific workload and cluster characteristics:

.. testcode::

    import ray
    import numpy as np
    
    def analyze_block_size_impact(data_path, block_sizes):
        """Analyze how different block sizes affect memory usage."""
        
        results = {}
        
        for num_blocks in block_sizes:
            print(f"\nTesting with {num_blocks} blocks:")
            
            # Read with specific block count
            ds = ray.data.read_parquet(data_path, override_num_blocks=num_blocks)
            
            # Materialize to see actual memory usage
            materialized = ds.materialize()
            stats = materialized.stats()
            
            avg_block_size_mb = stats.total_bytes / stats.num_blocks / (1024**2)
            
            results[num_blocks] = {
                "num_blocks": stats.num_blocks,
                "avg_block_size_mb": avg_block_size_mb,
                "total_memory_mb": stats.total_bytes / (1024**2)
            }
            
            print(f"  Actual blocks: {stats.num_blocks}")
            print(f"  Avg block size: {avg_block_size_mb:.1f}MB")
            print(f"  Total memory: {stats.total_bytes / (1024**2):.1f}MB")
        
        return results
    
    # Test different block configurations
    block_sizes = [1, 4, 16, 64, 256]
    results = analyze_block_size_impact("s3://bucket/data.parquet", block_sizes)

**Optimal Block Size Calculator**

**Block Count Decision Framework**

Use this systematic approach to determine optimal block count:

.. list-table:: Block Count Calculation Guide
   :header-rows: 1
   :class: block-count-guide

   * - Constraint Type
     - Calculation Method
     - Example (100GB data, 16GB memory, 8 CPUs)
     - Reasoning
   * - **Memory Constraint**
     - Available Memory ÷ Target Block Size
     - 16GB ÷ 64MB = 256 blocks
     - Prevents object store overflow
   * - **CPU Constraint** 
     - CPU Count × 3
     - 8 × 3 = 24 blocks
     - Ensures good parallelization
   * - **Data Size Constraint**
     - Dataset Size ÷ Min Block Size
     - 100GB ÷ 32MB = 3,200 blocks
     - Prevents blocks too small

**Decision Process:**

1. **Calculate each constraint** using your specific values
2. **Choose the middle value** to balance all constraints
3. **Apply the result** to your Ray Data operations

**Example Calculation:**

.. testcode::

    # Your specific values
    dataset_size_gb = 100
    available_memory_gb = 16
    cpu_count = 8
    
    # Calculate constraints
    memory_based_blocks = int((available_memory_gb * 1024) / 64)  # 256 blocks
    cpu_based_blocks = cpu_count * 3  # 24 blocks
    size_based_blocks = max(1, int(dataset_size_gb * 1024 / 32))  # 3,200 blocks
    
    print(f"Block count analysis:")
    print(f"  Memory constraint: {memory_based_blocks} blocks")
    print(f"  CPU constraint: {cpu_based_blocks} blocks") 
    print(f"  Size constraint: {size_based_blocks} blocks")

**Choose the Balanced Option:**

From the example above, the middle value (256 blocks) balances all constraints. Use this approach rather than complex logic:

.. testcode::

    # Choose middle value: 256 blocks (balances memory and CPU needs)
    optimal_blocks = 256
    print(f"  Recommended: {optimal_blocks} blocks")

Transform Memory Management
==========================

Setting Memory Limits
---------------------

Prevent OOM errors by setting explicit memory limits for transformations:

.. testcode::

    def memory_limited_transform(batch):
        """Transform with explicit memory management."""
        
        # Monitor memory usage
        import psutil
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024**2)
        
        # Your transformation logic
        result = expensive_computation(batch)
        
        memory_after = process.memory_info().rss / (1024**2)
        memory_used = memory_after - memory_before
        
        if memory_used > 500:  # Alert if using > 500MB
            print(f"WARNING: High memory usage: {memory_used:.1f}MB")
        
        return result
    
    # Apply with memory limits
    ds.map_batches(
        memory_limited_transform,
        ray_remote_args={
            "memory": 2 * 1024**3,  # 2GB limit per task
            "max_retries": 2         # Retry on OOM
        }
    )

**Adaptive Memory Management**

.. testcode::

    import psutil
    import gc
    
    class MemoryAwareTransform:
        """Transform that adapts to available memory."""
        
        def __init__(self, base_transform, memory_threshold_mb=1000):
            self.base_transform = base_transform
            self.memory_threshold_mb = memory_threshold_mb
            self.memory_stats = []
        
        def __call__(self, batch):
            # Check available memory
            available_memory_mb = psutil.virtual_memory().available / (1024**2)
            
            if available_memory_mb < self.memory_threshold_mb:
                # Low memory: process in smaller chunks
                return self._process_in_chunks(batch)
            else:
                # Normal processing
                return self._process_normally(batch)
        
        def _process_normally(self, batch):
            """Normal processing for adequate memory."""
            return self.base_transform(batch)
        
        def _process_in_chunks(self, batch):
            """Process in smaller chunks when memory is low."""
            print("WARNING: Low memory detected, processing in chunks")
            
            chunk_size = len(batch) // 4  # Quarter-size chunks
            results = []
            
            for i in range(0, len(batch), chunk_size):
                chunk = {k: v[i:i+chunk_size] for k, v in batch.items()}
                chunk_result = self.base_transform(chunk)
                results.append(chunk_result)
                
                # Force garbage collection between chunks
                gc.collect()
            
            # Combine results
            combined = {}
            for key in results[0].keys():
                combined[key] = []
                for result in results:
                    combined[key].extend(result[key])
            
            return combined
    
    # Usage
    memory_aware = MemoryAwareTransform(my_transform)
    ds.map_batches(memory_aware)

Batch Size Optimization for Memory
==================================

Memory-Conscious Batch Sizing
-----------------------------

Optimize batch sizes to balance performance and memory usage:

**Memory-Safe Batch Size Calculation:**

To prevent out-of-memory errors, calculate batch size based on your available memory and estimated row size. The calculation follows these principles:

1. **Use 50% of available memory** as a safety margin (similar to leaving headroom in Spark executor memory)
2. **Account for processing overhead** - operations often temporarily double memory usage
3. **Divide by estimated row size** to get the number of rows that fit safely

**Example Calculation for Image Processing:**

For a workload processing 5MB images on a machine with 8GB available memory:

- Safe memory limit: 8GB × 50% = 4GB
- Effective memory (accounting for overhead): 4GB ÷ 2 = 2GB  
- Batch size: 2GB ÷ 5MB per image = ~400 images per batch

**Apply the Calculation:**

For the image processing example above (5MB per image, 8GB available memory):
- Safe memory: 8GB × 50% = 4GB
- Effective memory: 4GB ÷ 2 = 2GB
- Batch size: 2GB ÷ 5MB = ~400 images

**Use the Calculated Batch Size:**

.. testcode::

    ds.map_batches(image_processing_function, batch_size=400)

**Batch Size Adjustment Based on Memory Pressure**

Instead of complex dynamic adjustment, use these proven approaches:

**How Ray Data Processes Batches:**

Understanding how Ray Data handles batches helps you choose appropriate sizes:

1. **Block Loading**: Ray Data loads a block (data partition) from the object store into task memory
2. **Batch Creation**: The block is split into batches of the specified size
3. **Sequential Processing**: Batches are processed sequentially within the task
4. **Result Accumulation**: Processed batches are combined into output blocks
5. **Block Storage**: Output blocks are stored back in the object store

**Memory Implications:**

During batch processing, task memory contains:
- **Input batch**: The current batch being processed
- **Intermediate results**: Temporary data created during processing
- **Output accumulation**: Results waiting to be formed into output blocks

**Batch Size Selection Strategy:**

Instead of automated testing, choose batch sizes based on your operation characteristics:
- **Memory-intensive operations**: Start with 32-128 rows per batch
- **CPU-intensive operations**: Use 256-1024 rows per batch  
- **Simple operations**: Can handle 1024-2048 rows per batch
- **GPU operations**: Optimize for GPU memory, typically 128-512 rows

**Monitor Memory Usage:**

Use Ray Dashboard to monitor memory during batch processing:

1. **Watch Object Store Memory** in the Metrics tab
2. **Monitor for spilling alerts** in the progress bars
3. **Check individual task memory** in the Timeline view
4. **Look for OOM task failures** which indicate batch sizes are too large

**If you see memory pressure:**

- **Reduce batch size** by half and test again
- **Check Ray Dashboard** to confirm memory usage decreases
- **Use proven batch sizes** from the table above based on your operation type

Object Store Optimization
=========================

Minimizing Object Store Usage
-----------------------------

The Ray object store is shared across all Ray Data operations. Optimize its usage:

.. testcode::

    def monitor_object_store_usage():
        """Monitor and report object store memory usage."""
        
        # Get object store statistics
        try:
            from ray._private.internal_api import memory_summary
            memory_info = memory_summary(stats_only=True)
            
            object_store_used = memory_info.get("object_store_used_memory", 0)
            object_store_total = memory_info.get("object_store_total_memory", 1)
            
            usage_percent = (object_store_used / object_store_total) * 100
            
            print(f"Object Store Usage:")
            print(f"  Used: {object_store_used / (1024**3):.2f}GB")
            print(f"  Total: {object_store_total / (1024**3):.2f}GB") 
            print(f"  Usage: {usage_percent:.1f}%")
            
            if usage_percent > 80:
                print("WARNING: Object store usage is high!")
                print("Consider:")
                print("- Reducing batch sizes")
                print("- Using streaming execution")
                print("- Avoiding unnecessary materialization")
            
            return usage_percent
            
        except Exception as e:
            print(f"Could not get object store stats: {e}")
            return 0
    
    # Monitor before and after operations
    print("Before processing:")
    monitor_object_store_usage()
    
    # Your Ray Data operations
    result = ds.map_batches(my_transform).write_parquet("output/")
    
    print("\nAfter processing:")
    monitor_object_store_usage()

**Object Store Cleanup Patterns**

.. testcode::

    def cleanup_object_store_transform(transform_func):
        """Wrapper that helps clean up object store memory."""
        
        def wrapped_transform(batch):
            # Process the batch
            result = transform_func(batch)
            
            # Periodically trigger cleanup
            if hash(str(batch)) % 50 == 0:  # Every ~50 batches
                # Force garbage collection
                import gc
                gc.collect()
                
                # Check if cleanup is needed
                usage = monitor_object_store_usage()
                if usage > 70:
                    print("CLEANUP: High object store usage, consider cleanup")
            
            return result
        
        return wrapped_transform
    
    # Usage
    cleanup_transform = cleanup_object_store_transform(my_transform)
    ds.map_batches(cleanup_transform)

Streaming vs Materialization
============================

Avoiding Unnecessary Materialization
------------------------------------

*Materialization* means loading entire datasets into Ray's object store memory (distributed shared memory across the cluster). Ray Data's streaming execution model is designed to avoid this, but certain operations can accidentally trigger materialization.

**Understanding Materialization (For Data Engineers):**

Materialization in Ray Data is similar to calling `.cache()` or `.persist()` in Spark - it forces evaluation of all pending transformations and stores the results in memory. However, unlike Spark where you explicitly choose to cache, some Ray Data operations accidentally trigger materialization.

When you materialize a dataset, Ray Data executes all pending operations and stores all resulting blocks (data partitions) in memory simultaneously. This can quickly exhaust available memory for large datasets, similar to how caching a large Spark DataFrame can cause executor OOM errors.

Use streaming when possible to process data incrementally:

.. tab-set::

    .. tab-item:: ANTIPATTERN Excessive Materialization

        .. code-block:: python

            # Bad: Multiple materializations
            ds = ray.data.read_parquet("large_data.parquet")
            
            # Materialization 1: Unnecessary
            intermediate = ds.map_batches(transform1).materialize()
            
            # Materialization 2: Unnecessary  
            result = intermediate.map_batches(transform2).materialize()
            
            # Final materialization for output
            result.write_parquet("output/")

    .. tab-item:: EFFICIENT Streaming Execution

        .. code-block:: python

            # Good: Streaming execution
            ds = ray.data.read_parquet("large_data.parquet")
            
            # Chain operations without materialization
            result = ds.map_batches(transform1) \
                      .map_batches(transform2) \
                      .write_parquet("output/")  # Only materialize for output

**When Materialization is Appropriate**

.. code-block:: python

    # EFFICIENT Good: Materialize for reuse
    processed_data = ds.map_batches(expensive_transform).materialize()
    
    # Reuse materialized data multiple times
    result1 = processed_data.filter(lambda row: row["category"] == "A")
    result2 = processed_data.filter(lambda row: row["category"] == "B")
    
    # EFFICIENT Good: Materialize before shuffle operations
    ds = ray.data.read_parquet("data.parquet")
    prepared = ds.map_batches(prepare_for_shuffle).materialize()
    shuffled = prepared.random_shuffle()  # Shuffle benefits from materialization

**Streaming Configuration**

.. testcode::

    def configure_streaming_execution():
        """Configure Ray Data for optimal streaming."""
        
        ctx = ray.data.DataContext.get_current()
        
        # Enable streaming optimizations
        ctx.execution_options.preserve_order = False  # Allow reordering
        ctx.execution_options.locality_with_output = True  # Prefer local execution
        
        # Configure for streaming
        ctx.target_max_block_size = 64 * 1024 * 1024  # 64MB blocks
        
        print("Configured for streaming execution")
    
    configure_streaming_execution()

Avoiding Accidental Materialization
===================================

Hidden Materialization Triggers
-------------------------------

Several common operations accidentally trigger materialization, forcing Ray Data to load your entire dataset into memory. Being aware of these helps you avoid unexpected memory usage.

**Operations That Trigger Materialization:**

1. **count() and len()**: Computing dataset size requires processing all data
2. **schema() on transformed datasets**: Schema inference may require data scanning
3. **show() and take()**: Display operations materialize the requested rows
4. **Certain aggregations**: Operations like sum(), mean() across the entire dataset
5. **Some debugging operations**: Stats collection and dataset inspection

Avoiding count() Materialization
--------------------------------

The `count()` operation is particularly dangerous because it seems harmless but requires processing the entire dataset.

**Problem Example:**

.. testcode::

    # ANTIPATTERN This accidentally materializes the entire dataset!
    ds = ray.data.read_parquet("s3://bucket/huge-dataset/")  # 1TB dataset
    ds = ds.map_batches(expensive_transform)
    
    # This line triggers full materialization - very expensive!
    num_rows = ds.count()  # Processes all 1TB to count rows
    print(f"Processing {num_rows} rows...")

**Better Approaches:**

Instead of using `count()`, use these memory-friendly alternatives:

.. testcode::

    # EFFICIENT Estimate count from metadata (for file-based sources)
    # Sample a small portion to estimate
    sample = ds.limit(1000)  # Only process 1000 rows
    sample_count = len(sample)
    
    # Get file count/size info to extrapolate
    estimated_total = sample_count * 100  # Rough estimate based on sampling
    
    print(f"Estimated rows: ~{estimated_total:,} (based on sample)")

**For Parquet files specifically**, you can read row count from metadata without processing data:

Parquet files store rich metadata including row counts, column statistics, and schema information in their headers. This metadata can be read very quickly without processing any actual data, making it perfect for getting dataset information without triggering expensive materialization.

This approach works because Parquet metadata is stored at the beginning of each file and includes:
- Total row count across all row groups
- Column schemas and data types  
- Min/max values for each column (useful for filtering)
- Compression information and file structure

.. testcode::

    import pyarrow.parquet as pq
    
    # Read only metadata, not data - this is very fast
    parquet_file = pq.ParquetFile("s3://bucket/data.parquet")
    total_rows = parquet_file.metadata.num_rows
    
    print(f"Rows from metadata: {total_rows:,}")

This metadata read typically takes milliseconds even for very large files, compared to minutes or hours for full data processing.

Avoiding schema() Materialization
---------------------------------

Calling `schema()` on transformed datasets can trigger data processing to infer the schema. This happens because Ray Data's lazy evaluation means transformations haven't been executed yet, so the output schema is unknown.

**Why Schema Inference is Expensive:**

When you call `schema()` on a transformed dataset, Ray Data may need to:

1. **Execute transformations**: Run your map_batches functions to see what data they produce
2. **Sample data**: Process enough data to understand the output schema
3. **Infer types**: Analyze the processed data to determine column types and structure

This can be particularly expensive for complex transformations or large datasets. The schema inference might process a significant portion of your data just to determine the structure.

**Problem Example:**

.. testcode::

    # ANTIPATTERN Schema inference triggers materialization
    ds = ray.data.read_parquet("data.parquet")
    ds = ds.map_batches(complex_transform)  # Changes schema
    
    # This might process data to infer the new schema
    schema = ds.schema()  # Potentially expensive!

**Better Approaches:**

.. testcode::

    # EFFICIENT Get schema before transformations
    ds = ray.data.read_parquet("data.parquet")
    original_schema = ds.schema()  # Fast - from file metadata
    
    # Apply transformations
    ds = ds.map_batches(complex_transform)
    
    # Use the original schema info to understand your data structure
    print(f"Original schema: {original_schema}")

**For transformed datasets**, sample a small portion to infer schema:

.. testcode::

    # EFFICIENT Infer schema from small sample
    sample = ds.limit(10)  # Just 10 rows
    sample_schema = sample.schema()
    
    print(f"Transformed schema (from sample): {sample_schema}")

Safe Dataset Inspection
----------------------

When you need to inspect datasets during development, use these memory-safe approaches:

**Safe Data Inspection:**

.. testcode::

    # EFFICIENT Inspect data without full materialization
    def safely_inspect_dataset(ds, sample_size=100):
        """Inspect dataset characteristics without triggering materialization."""
        
        print("Safe Dataset Inspection:")
        
        # Get basic info that doesn't require processing
        try:
            # For file-based datasets, this is usually fast
            print(f"  Dataset: {ds}")
        except:
            print("  Could not get basic dataset info")
        
        # Sample a small portion for inspection
        sample = ds.limit(sample_size)
        sample_data = sample.take(min(5, sample_size))
        
        print(f"  Sample data ({len(sample_data)} rows):")
        for i, row in enumerate(sample_data):
            print(f"    Row {i}: {str(row)[:100]}...")  # Truncate long rows
        
        # Get schema from sample
        sample_schema = sample.schema()
        print(f"  Schema: {sample_schema}")
        
        return sample_data, sample_schema

**Safe Statistics Collection:**

Instead of computing expensive statistics on the full dataset, use sampling:

.. testcode::

    def get_safe_dataset_statistics(ds, sample_fraction=0.01):
        """Get dataset statistics using sampling to avoid materialization."""
        
        print(f"Computing statistics from {sample_fraction*100}% sample:")
        
        # Sample the dataset
        sampled = ds.random_sample(sample_fraction)
        
        # Compute statistics on sample
        sample_stats = sampled.stats()
        
        # Extrapolate to full dataset
        estimated_total_size = sample_stats.total_bytes / sample_fraction
        estimated_total_rows = len(sampled) / sample_fraction
        
        print(f"  Estimated total size: {estimated_total_size / (1024**3):.2f}GB")
        print(f"  Estimated total rows: {estimated_total_rows:,.0f}")
        print(f"  Sample processing time: {sample_stats}")
        
        return {
            "estimated_size_gb": estimated_total_size / (1024**3),
            "estimated_rows": int(estimated_total_rows),
            "sample_stats": sample_stats
        }

Monitoring for Accidental Materialization
-----------------------------------------

Set up monitoring to detect when operations accidentally trigger materialization:

.. testcode::

    import time
    import psutil
    
    def materialization_detector(operation_name):
        """Decorator to detect if an operation triggers materialization."""
        
        def decorator(func):
            def wrapper(*args, **kwargs):
                # Monitor memory and time before operation
                memory_before = psutil.virtual_memory().percent
                time_before = time.time()
                
                # Execute operation
                result = func(*args, **kwargs)
                
                # Check if operation was unexpectedly expensive
                time_after = time.time()
                memory_after = psutil.virtual_memory().percent
                
                execution_time = time_after - time_before
                memory_increase = memory_after - memory_before
                
                # Alert if operation seems to have materialized data
                if execution_time > 10:  # More than 10 seconds
                    print(f"WARNING: {operation_name} took {execution_time:.1f}s - possible materialization")
                
                if memory_increase > 10:  # More than 10% memory increase
                    print(f"WARNING: {operation_name} increased memory by {memory_increase:.1f}% - possible materialization")
                
                return result
            return wrapper
        return decorator
    
    # Usage example
    @materialization_detector("dataset_inspection")
    def inspect_my_dataset(ds):
        """Inspect dataset with materialization detection."""
        schema = ds.schema()  # This might trigger materialization
        count = ds.count()    # This definitely triggers materialization
        return schema, count

**Best Practices for Avoiding Accidental Materialization:**

1. **Use sampling** instead of full dataset operations for exploration
2. **Get schema before transformations** when possible
3. **Avoid count() in production code** unless absolutely necessary
4. **Use limit() and take()** for data inspection instead of show()
5. **Monitor execution time** of seemingly simple operations
6. **Cache expensive computations** if you need them multiple times

GPU Memory Optimization
=======================

GPU Memory Management
--------------------

GPU memory is typically more constrained than system memory:

.. testcode::

    class GPUMemoryOptimizedTransform:
        """Transform optimized for GPU memory constraints."""
        
        def __init__(self):
            import cupy as cp
            self.memory_pool = cp.get_default_memory_pool()
        
        def __call__(self, batch):
            import cupy as cp
            
            # Clear GPU memory before processing
            self.memory_pool.free_all_blocks()
            
            try:
                # Monitor GPU memory
                gpu_memory_before = self.memory_pool.used_bytes()
                
                # Move data to GPU
                gpu_data = cp.asarray(batch["data"])
                
                # Process on GPU
                result = self._gpu_computation(gpu_data)
                
                # Move result back to CPU immediately
                cpu_result = cp.asnumpy(result)
                
                # Clean up GPU memory
                del gpu_data, result
                self.memory_pool.free_all_blocks()
                
                gpu_memory_after = self.memory_pool.used_bytes()
                
                return {"result": cpu_result}
                
            except cp.cuda.memory.OutOfMemoryError as e:
                print(f"GPU OOM: {e}")
                # Fall back to CPU processing
                return self._cpu_fallback(batch)
        
        def _gpu_computation(self, data):
            import cupy as cp
            return cp.sqrt(data * 2 + 1)
        
        def _cpu_fallback(self, batch):
            import numpy as np
            return {"result": np.sqrt(np.array(batch["data"]) * 2 + 1)}
    
    # Use with appropriate GPU batch sizes
    ds.map_batches(
        GPUMemoryOptimizedTransform,
        batch_size=256,  # Smaller batches for GPU memory
        num_gpus=1
    )

**GPU Memory Pool Configuration**

.. testcode::

    def configure_gpu_memory_pool():
        """Configure GPU memory pool for optimal usage."""
        
        try:
            import cupy as cp
            
            # Configure memory pool
            memory_pool = cp.get_default_memory_pool()
            
            # Set memory pool limits (optional)
            # memory_pool.set_limit(size=2**30)  # 1GB limit
            
            # Use memory pool with growth strategy
            memory_pool.set_growth_factor(2.0)  # Double size when needed
            
            print("GPU memory pool configured")
            
            return memory_pool
            
        except ImportError:
            print("CuPy not available, skipping GPU memory configuration")
            return None
    
    # Configure at startup
    gpu_pool = configure_gpu_memory_pool()

Memory Monitoring and Debugging
==============================

Memory Monitoring with Ray Dashboard
-----------------------------------

Use Ray Dashboard to monitor memory usage during Ray Data operations. The dashboard provides comprehensive memory metrics without requiring custom monitoring code.

**Dashboard Memory Metrics:**

1. **Navigate to Ray Dashboard** → **Metrics tab**
2. **Object Store Memory**: Monitor object store usage and spilling
3. **Node Memory**: Track memory usage across cluster nodes
4. **Task Memory**: See memory usage per individual task

**Key Memory Indicators in Dashboard:**

- **Object Store Used/Total**: Should stay below 80% to avoid spilling
- **Memory by Component**: Shows breakdown of memory usage
- **Spilling Events**: Alerts when object store spills to disk
- **Task Memory Usage**: Individual task memory consumption

**Memory Issue Detection:**

- **Spilling alerts**: Dashboard shows warnings when spilling occurs
- **Memory pressure**: Gradual increase in memory usage over time
- **OOM task failures**: Failed tasks often indicate memory issues
- **Slow performance**: High memory pressure slows down processing

**Simple Memory Check:**

.. testcode::

    # Enable progress bars to see memory warnings
    ctx = ray.data.DataContext.get_current()
    ctx.enable_progress_bars = True
    
    # Run your pipeline and watch Ray Dashboard for memory alerts
    result = ds.map_batches(my_transform).write_parquet("output/")
    
    # Check final stats for memory usage summary
    print(result.stats())

Memory Leak Detection
--------------------

Use Ray Dashboard to detect memory leaks in Ray Data transformations:

**Signs of Memory Leaks in Ray Dashboard:**

- **Steadily increasing memory usage** over time in the Metrics tab
- **Object store memory growth** that doesn't level off
- **Node memory usage** that keeps climbing during processing
- **Task memory** that increases with each batch processed

**Simple Memory Leak Check:**

.. testcode::

    # Run a small test to check for obvious leaks
    test_ds = ds.limit(100)  # Small test dataset
    
    # Process multiple times to detect leaks
    for i in range(5):
        result = test_ds.map_batches(my_transform).materialize()
        print(f"Iteration {i+1}: {result.stats()}")
        # Watch Ray Dashboard memory metrics between iterations

**Common Memory Leak Causes:**

- **Stateful actors** that accumulate data without cleanup
- **Global variables** that grow with each batch
- **Unclosed file handles** in custom transforms
- **Large intermediate objects** not being garbage collected

**Prevention:**

- Use stateless functions instead of stateful actors when possible
- Avoid global state in transform functions
- Explicitly close any resources opened in transforms
- Use Ray Dashboard to monitor memory trends during development

Best Practices Summary
=====================

**Essential Memory Management**
1. **Configure DataContext appropriately** for your environment
2. **Set explicit memory limits** in ray_remote_args
3. **Use streaming execution** instead of materialization when possible
4. **Monitor memory usage continuously** in production

**Block Size Optimization**
1. **Balance block size** with available memory and CPU count
2. **Use smaller blocks** in memory-constrained environments
3. **Use larger blocks** for high-throughput workloads
4. **Monitor actual vs configured block sizes**

**Transform Memory Management**
1. **Use in-place operations** when possible
2. **Clean up intermediate results** explicitly
3. **Handle OOM errors gracefully** with retries and chunking
4. **Profile memory usage** of custom transformations

**Object Store Optimization**
1. **Avoid creating many small objects**
2. **Monitor object store usage** regularly
3. **Clean up unused objects** when possible
4. **Configure appropriate object store limits**

**GPU Memory Management**
1. **Use smaller batch sizes** for GPU operations
2. **Clear GPU memory** between batches
3. **Implement CPU fallbacks** for GPU OOM
4. **Monitor GPU memory pools**

Next Steps
==========

Continue optimizing your Ray Data memory usage:

- **Implement monitoring** in your production pipelines
- **Profile your specific workloads** to find memory bottlenecks
- **Learn advanced operations**: :ref:`advanced_operations`
- **Debug memory issues**: :ref:`troubleshooting`

**Advanced Memory Management Strategies:**

**Memory Optimization by Data Type:**

**Structured Data (Tables):**
- Use columnar formats to minimize memory footprint
- Implement schema optimization for memory efficiency
- Configure appropriate data types to reduce memory usage
- Use compression to reduce memory requirements

**Image Data:**
- Optimize image loading and preprocessing pipelines
- Use efficient image format conversions
- Implement memory-efficient augmentation strategies
- Configure GPU memory management for computer vision

**Text Data:**
- Optimize tokenization and text processing
- Use efficient string handling strategies
- Implement memory-efficient embedding generation
- Configure for large language model processing

**Time Series Data:**
- Optimize temporal data structures
- Use efficient windowing strategies
- Implement memory-efficient aggregations
- Configure for streaming time series processing

**Memory Optimization by Processing Pattern:**

**Batch Processing:**
- Configure large blocks for efficiency
- Use high memory limits for throughput
- Implement efficient checkpointing
- Optimize for sequential processing

**Streaming Processing:**
- Configure smaller blocks for low latency
- Use streaming memory management
- Implement efficient state management
- Optimize for continuous processing

**Interactive Processing:**
- Balance memory usage with responsiveness
- Use caching for frequently accessed data
- Implement efficient result materialization
- Optimize for ad-hoc queries

**Memory Optimization by Cluster Configuration:**

**Single Node:**
- Optimize for local memory limits
- Use efficient garbage collection
- Implement memory pooling strategies
- Configure for NUMA awareness

**Multi-Node Clusters:**
- Optimize object store distribution
- Use efficient inter-node communication
- Implement load balancing strategies
- Configure for network-aware memory management

**Memory Performance Monitoring Strategies:**

**Real-time Memory Monitoring:**
- Monitor object store usage across cluster nodes
- Track memory allocation patterns over time
- Implement memory pressure alerts and notifications
- Use Ray Dashboard for continuous memory monitoring

**Memory Leak Detection:**
- Monitor memory growth patterns over time
- Implement automated leak detection algorithms
- Configure alerts for unusual memory growth
- Use profiling tools for memory leak analysis

**Memory Optimization Validation:**
- Measure memory usage before and after optimizations
- Validate memory improvements across different workloads
- Test memory optimization effectiveness under load
- Monitor memory optimization impact on performance

**Capacity Planning:**
- Forecast memory requirements for growing workloads
- Plan cluster sizing based on memory optimization
- Implement memory usage trend analysis
- Configure for seasonal workload variations

**Memory Optimization for Specialized Workloads:**

**Scientific Computing:**
- Optimize for large numerical computations
- Use efficient matrix operations and linear algebra
- Implement memory-efficient scientific algorithms
- Configure for high-performance computing environments

**Financial Analytics:**
- Optimize for time series and market data processing
- Use efficient risk calculation and portfolio analysis
- Implement memory-efficient financial algorithms
- Configure for regulatory compliance requirements

**Media Processing:**
- Optimize for audio, video, and image processing
- Use efficient media format conversions
- Implement memory-efficient media algorithms
- Configure for high-bandwidth media workflows

**IoT and Sensor Data:**
- Optimize for high-volume sensor data processing
- Use efficient time series aggregation and analysis
- Implement memory-efficient IoT data pipelines
- Configure for edge computing environments

**Data Lifecycle Memory Optimization:**

**Data Ingestion Optimization:**
- Configure memory-efficient data ingestion patterns
- Use streaming ingestion for large datasets
- Implement backpressure handling for data sources
- Optimize for variable ingestion rates

**Data Processing Optimization:**
- Use memory-efficient transformation strategies
- Implement incremental processing for large datasets
- Configure for memory-aware processing pipelines
- Optimize for complex multi-stage processing

**Data Storage Optimization:**
- Use memory-efficient data storage strategies
- Implement efficient data compression and encoding
- Configure for optimal storage layout
- Optimize for data access patterns

**Data Archival Optimization:**
- Implement memory-efficient archival strategies
- Use tiered storage for different data access patterns
- Configure for long-term data retention
- Optimize for archival data retrieval

**Memory Optimization Automation:**

**Automated Memory Tuning:**
- Implement automated memory configuration optimization
- Use machine learning for memory usage prediction
- Configure adaptive memory management strategies
- Monitor and adjust memory settings automatically

**Memory Usage Analytics:**
- Implement comprehensive memory usage analytics
- Use predictive analytics for memory planning
- Configure for memory usage trend analysis
- Monitor memory optimization effectiveness

**Memory Performance Benchmarking:**
- Establish memory performance benchmarks
- Implement standardized memory testing procedures
- Use representative workloads for memory benchmarking
- Maintain memory performance baselines

**Advanced Memory Debugging:**

**Memory Profiling Techniques:**
- Use advanced memory profiling tools
- Implement memory allocation tracking
- Configure for detailed memory analysis
- Monitor memory usage patterns

**Memory Leak Analysis:**
- Implement sophisticated memory leak detection
- Use heap analysis for memory leak identification
- Configure for automated leak prevention
- Monitor memory leak prevention effectiveness

**Memory Optimization Validation:**
- Validate memory optimizations across different scenarios
- Use statistical analysis for memory improvement validation
- Implement A/B testing for memory optimizations
- Monitor long-term memory optimization effectiveness

**See also:**
- :ref:`transform_optimization` - Optimize transformations for better memory usage
- :ref:`reading_optimization` - Memory-efficient data loading strategies  
- :ref:`patterns_antipatterns` - Memory-related patterns and antipatterns
- :ref:`data-internals` - Understanding Ray Data's memory architecture
