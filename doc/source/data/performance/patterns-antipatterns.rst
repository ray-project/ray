.. _patterns_antipatterns:

========================================
Ray Data Patterns and Antipatterns
========================================

This guide presents proven patterns for high-performance Ray Data usage alongside common antipatterns that hurt performance. Learn to recognize both to write efficient, maintainable Ray Data code.

**What are Patterns and Antipatterns?**

Think of patterns and antipatterns like cooking recipes:

- **Patterns** are like proven recipes that consistently produce great results - techniques that experienced Ray Data users have found to work well
- **Antipatterns** are like common cooking mistakes that ruin the dish - coding practices that seem reasonable but actually hurt performance

**Why This Matters for Beginners:**

Learning patterns and antipatterns helps you avoid spending weeks debugging performance issues that could be prevented with simple code changes. Many Ray Data performance problems come from a few common mistakes that are easy to fix once you know what to look for.

Understanding patterns and antipatterns helps you avoid common performance pitfalls and leverage Ray Data's :ref:`streaming execution <streaming-execution>` (processing data incrementally instead of loading everything at once), :ref:`operator fusion <data-internals>` (combining operations for efficiency), and distributed processing capabilities effectively across traditional ETL, modern AI/ML, and emerging real-time workloads.

.. contents::
   :local:
   :depth: 2

Antipattern Overview
===================

Ray Data antipatterns are common coding mistakes that significantly impact performance. This guide categorizes them by severity and provides clear solutions.

.. list-table:: Antipattern Severity Levels
   :header-rows: 1
   :class: severity-table

   * - Severity
     - Performance Impact
     - Description
     - Examples
   * - **Critical**
     - 10-100x slower
     - Fundamental design issues
     - Using map() for vectorizable ops
   * - **High**
     - 2-10x slower
     - Inefficient resource usage
     - Wrong batch formats
   * - **Medium**
     - 1.5-3x slower
     - Suboptimal configurations
     - Poor batch sizing
   * - **Low**
     - 1.1-1.5x slower
     - Minor inefficiencies
     - Missing optimizations

Reading Antipatterns & Patterns
===============================

Antipattern: Reading Too Many Small Files
-----------------------------------------

**Severity**: Critical **Impact**: Can significantly degrade performance

**Problem**: Reading thousands of small files (< 1MB) creates excessive overhead and poor parallelization.

**Understanding the Issue:**

When Ray Data reads files, it typically creates one Ray task (similar to a Spark task - a unit of work that processes data) per file. With thousands of small files, this creates an enormous number of tasks that overwhelm the scheduler and create more overhead than actual work.

**The Root Cause:**

Small file proliferation often happens when data is written incrementally (like streaming data or frequent batch jobs) without consolidation. Each small write creates a separate file, and over time you accumulate thousands of tiny files that are inefficient to process.

**Performance Impact Breakdown:**

1. **Task Overhead**: Each file requires task scheduling, which has fixed overhead regardless of file size
2. **Network Latency**: Each file requires a separate network request, multiplying connection overhead
3. **Metadata Overhead**: Ray Data must track metadata for thousands of blocks instead of dozens
4. **Resource Fragmentation**: Many tiny tasks prevent efficient resource utilization

The overhead can be so significant that the scheduler spends more time managing tasks than the tasks spend processing data.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Reading 10,000 small files (100KB each)
            ds = ray.data.read_parquet("s3://bucket/small-files/")
            # Each file becomes a separate task - massive overhead!

        **Why this is bad:**
        - Creates 10,000+ :ref:`Ray tasks <core-key-concepts>` for just 1GB of data
        - Task scheduling overhead dominates actual processing time
        - Poor resource utilization due to task startup costs
        - Network latency multiplied by file count
        - :ref:`Object store <objects-in-ray>` overwhelmed with tiny blocks

    .. tab-item:: PATTERN Pattern: File Consolidation

        .. code-block:: python

            # Consolidate small files into larger ones during ETL
            ds = ray.data.read_parquet("s3://bucket/small-files/")
            
            # Repartition to create larger, fewer files
            target_blocks = 16  # Reduce from thousands to 16 blocks
            consolidated = ds.repartition(target_blocks)
            consolidated.write_parquet("s3://bucket/consolidated-files/")
            
            # Use consolidated files for much better performance
            ds = ray.data.read_parquet("s3://bucket/consolidated-files/")

        **Benefits:**
        - Substantially fewer tasks (thousands of files → manageable number of blocks)
        - Better resource utilization through reduced task scheduling overhead
        - Reduced network overhead from fewer concurrent connections
        - Improved parallelization with appropriately sized work units
        
        **Performance Impact:**
        
        File consolidation can dramatically improve performance by reducing task scheduling overhead. The improvement depends on the degree of file fragmentation and cluster characteristics.

**Detection**: Check ``ds.num_blocks()`` - if much higher than CPU count with small total data size, you likely have this issue.

**Quick Fix**: Use ``override_num_blocks`` to reduce task count:

.. code-block:: python

    # Quick fix: Force fewer blocks instead of one per file
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    ds = ray.data.read_parquet(
        "s3://bucket/small-files/",
        override_num_blocks=cluster_cpus * 2  # Much fewer than file count
    )

Antipattern: Not Using Column Pruning
-------------------------------------

**Severity**: High **Impact**: Can substantially increase processing time

**Problem**: Reading all columns when only a subset is needed wastes I/O, memory, and processing time.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Reading all 50 columns when only 3 are needed
            ds = ray.data.read_parquet("s3://bucket/wide-table/")
            result = ds.map_batches(lambda batch: {
                "user_id": batch["user_id"],
                "timestamp": batch["timestamp"], 
                "value": batch["value"]
            })

        **Problems:**
        - Reads 47 unnecessary columns
        - Wastes network bandwidth
        - Increases memory usage 15x+
        - Slows down all operations

    .. tab-item:: PATTERN Pattern: Column Pruning

        .. code-block:: python

            # Only read needed columns
            ds = ray.data.read_parquet(
                "s3://bucket/wide-table/",
                columns=["user_id", "timestamp", "value"]  # Only what you need
            )
            result = ds.map_batches(lambda batch: {
                "user_id": batch["user_id"],
                "timestamp": batch["timestamp"],
                "value": batch["value"]
            })

        **Benefits:**
        - Substantially less data transfer when reading fewer columns
        - Reduced memory usage in object store and tasks
        - Faster processing due to less data movement
        - Potentially lower cloud data transfer costs
        
        **Performance Impact:**
        
        Column pruning effectiveness depends on the ratio of needed to total columns. Reading fewer columns from wide tables can provide substantial performance improvements and cost savings.

**Advanced Column Pruning Pattern**:

.. code-block:: python

    def smart_column_selection(path, required_cols, optional_cols=None):
        """Intelligently select columns based on availability."""
        
        # Get schema to see available columns
        sample = ray.data.read_parquet(path, override_num_blocks=1)
        available_cols = list(sample.schema().names)
        
        # Select required columns
        columns = [col for col in required_cols if col in available_cols]
        
        # Add optional columns if available
        if optional_cols:
            columns.extend([col for col in optional_cols if col in available_cols])
        
        return ray.data.read_parquet(path, columns=columns)
    
    # Usage
    ds = smart_column_selection(
        "s3://bucket/data/",
        required_cols=["id", "value"],
        optional_cols=["metadata", "tags"]
    )

Antipattern: Wrong File Format Choice
------------------------------------

**Severity**: High **Impact**: Can substantially reduce performance

**Problem**: Using inefficient file formats like CSV for large analytical workloads.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Using CSV for 100GB analytical dataset
            ds = ray.data.read_csv("s3://bucket/huge-dataset.csv")
            # Problems:
            # - No column pruning possible
            # - No compression
            # - No predicate pushdown
            # - Text parsing overhead

    .. tab-item:: PATTERN Pattern: Optimal Format Selection

        .. code-block:: python

            # Convert to Parquet for analytical workloads
            def convert_to_parquet(csv_path, parquet_path):
                """Convert CSV to Parquet for better performance."""
                
                # Read CSV in chunks to avoid memory issues
                ds = ray.data.read_csv(csv_path)
                
                # Write as Parquet with compression
                ds.write_parquet(
                    parquet_path,
                    compression="snappy"  # Good balance of speed/size
                )
            
            # Use Parquet for analytics
            ds = ray.data.read_parquet(
                "s3://bucket/dataset.parquet",
                columns=["needed_col1", "needed_col2"]  # Column pruning works!
            )

**Format Selection Guide:**

Choose the optimal file format based on your use case and data characteristics:

.. list-table:: File Format Selection Guide
   :header-rows: 1
   :class: format-selection-guide

   * - Use Case
     - Data Type
     - Recommended Format
     - Key Benefits
   * - **Traditional Analytics**
     - Structured tables
     - Parquet
     - Column pruning, compression, fast aggregations
   * - **ETL Pipelines**
     - Mixed structured data
     - Parquet/CSV
     - Schema evolution, data lineage
   * - **ML Training**
     - Numerical/multi-modal
     - Arrow/Feather
     - Zero-copy reads, GPU-friendly
   * - **Real-time Processing**
     - Append-only streams
     - Delta Lake/Arrow
     - ACID transactions, low latency
   * - **Semi-structured/API Data**
     - Flexible schema
     - JSON
     - Schema flexibility, nested data
   * - **General Purpose**
     - Any workload type
     - Parquet
     - Best overall performance across use cases

**Default Recommendation**: Use Parquet for most Ray Data workloads unless you have specific requirements that favor other formats.

Transform Antipatterns & Patterns
=================================

Antipattern: Using map() for Vectorizable Operations
---------------------------------------------------

**Severity**: Critical **Impact**: Can severely degrade performance

**Problem**: Using :meth:`~ray.data.Dataset.map` for operations that can be vectorized.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Processing one row at a time - extremely inefficient!
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map(lambda row: {
                "normalized": row["value"] / 100.0,
                "squared": row["value"] ** 2
            })

        **Problems:**
        - Processes one row at a time
        - Cannot leverage vectorization
        - High per-row overhead
        - Poor CPU utilization

    .. tab-item:: PATTERN Pattern: Vectorized Processing

        .. code-block:: python

            import numpy as np
            
            # Process batches with vectorized operations
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map_batches(lambda batch: {
                "normalized": np.array(batch["value"]) / 100.0,
                "squared": np.array(batch["value"]) ** 2
            })

        **Benefits:**
        - Significantly faster execution
        - Better CPU utilization
        - Leverages SIMD instructions
        - Lower memory overhead per operation

**Advanced Vectorization Pattern**:

.. code-block:: python

    def create_vectorized_transform(operations):
        """Create a vectorized transform from operation descriptions."""
        
        def vectorized_transform(batch):
            import numpy as np
            results = {}
            
            # Convert to numpy for vectorization
            arrays = {col: np.array(batch[col]) for col in batch.keys()}
            
            # Apply vectorized operations
            for output_col, (input_col, operation, *args) in operations.items():
                if operation == "normalize":
                    results[output_col] = arrays[input_col] / args[0]
                elif operation == "log":
                    results[output_col] = np.log(arrays[input_col] + args[0])
                elif operation == "clip":
                    results[output_col] = np.clip(arrays[input_col], args[0], args[1])
                # Add more operations as needed
            
            return results
        
        return vectorized_transform
    
    # Usage
    operations = {
        "normalized_value": ("value", "normalize", 100.0),
        "log_value": ("value", "log", 1.0),
        "clipped_value": ("value", "clip", 0, 1000)
    }
    
    transform = create_vectorized_transform(operations)
    result = ds.map_batches(transform)

Antipattern: Using Pandas Batch Format Unnecessarily
----------------------------------------------------

**Severity**:  High **Impact**: 2-5x slower

**Problem**: Using ``batch_format="pandas"`` when native formats would work better.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Unnecessary pandas conversion for simple operations
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map_batches(
                lambda df: df.assign(doubled=df["value"] * 2),
                batch_format="pandas"  # Expensive conversion!
            )

        **Problems:**
        - Expensive Arrow → Pandas conversion
        - Higher memory usage
        - Slower operations
        - Unnecessary dependency

    .. tab-item:: PATTERN Pattern: Native Format Usage

        .. code-block:: python

            import numpy as np
            
            # Use native format for simple operations
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map_batches(lambda batch: {
                **batch,  # Keep existing columns
                "doubled": np.array(batch["value"]) * 2
            })

        **Benefits:**
        - No conversion overhead
        - Lower memory usage
        - Faster execution
        - Better integration with Ray Data

**When Pandas is Appropriate**:

.. code-block:: python

    # PATTERN Good use of pandas: Complex operations that benefit from pandas API
    def complex_pandas_operation(df):
        """Operations that genuinely benefit from pandas."""
        return (df.groupby("category")
                 .agg({"value": ["mean", "std", "count"]})
                 .reset_index())
    
    result = ds.map_batches(
        complex_pandas_operation,
        batch_format="pandas"  # Justified here
    )
    
    # PATTERN Alternative: Use PyArrow compute when possible
    import pyarrow.compute as pc
    
    def arrow_aggregation(batch):
        """Use PyArrow for aggregations when possible."""
        # Group by category and compute mean
        # (simplified example - actual implementation more complex)
        categories = batch["category"]
        values = batch["value"]
        
        unique_cats = pc.unique(categories)
        means = []
        
        for cat in unique_cats:
            mask = pc.equal(categories, cat)
            filtered_values = pc.filter(values, mask)
            mean_val = pc.mean(filtered_values)
            means.append(mean_val.as_py())
        
        return {"categories": unique_cats, "means": means}
    
    result = ds.map_batches(arrow_aggregation, batch_format="pyarrow")

Antipattern: Wrong Batch Size Selection
--------------------------------------

**Severity**:  Medium **Impact**: 1.5-3x slower

**Problem**: Using inappropriate batch sizes that don't match workload characteristics.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Using default batch size for memory-intensive operation
            def memory_intensive_transform(batch):
                # Creates large intermediate arrays
                data = np.array(batch["large_array"])  # 100MB per row
                processed = np.fft.fft2(data)  # Memory doubles
                return {"result": processed}
            
            # Default batch_size=1024 causes OOM!
            result = ds.map_batches(memory_intensive_transform)

        **Problems:**
        - Out of memory errors
        - Task failures and retries
        - Poor resource utilization
        - Inconsistent performance

    .. tab-item:: PATTERN Pattern: Adaptive Batch Sizing

        .. code-block:: python

            import psutil
            
            def adaptive_batch_size_transform(ds, transform_func, 
                                            estimated_memory_per_row_mb):
                """Automatically determine optimal batch size."""
                
                # Get available memory
                available_memory_gb = psutil.virtual_memory().available / (1024**3)
                
                # Target using 10% of available memory per task
                target_memory_mb = available_memory_gb * 1024 * 0.1
                
                # Calculate optimal batch size
                optimal_batch_size = max(1, int(
                    target_memory_mb / estimated_memory_per_row_mb
                ))
                
                print(f"Using batch size: {optimal_batch_size}")
                
                return ds.map_batches(
                    transform_func,
                    batch_size=optimal_batch_size
                )
            
            # Usage
            result = adaptive_batch_size_transform(
                ds,
                memory_intensive_transform,
                estimated_memory_per_row_mb=200  # 200MB per row
            )

**Batch Size Selection Guide:**

Choose batch sizes based on your operation type and data characteristics:

.. list-table:: Batch Size Recommendations by Operation Type
   :header-rows: 1
   :class: batch-size-recommendations

   * - Operation Type
     - Base Batch Size
     - Adjust for Large Rows
     - Adjust for Memory-Intensive
     - Final Range
   * - **Simple Math**
     - 2048
     - ÷ 4 = 512
     - ÷ 2 = 256
     - 256-2048
   * - **String Processing**
     - 1024
     - ÷ 4 = 256
     - ÷ 2 = 128
     - 128-1024
   * - **ML Inference**
     - 256
     - ÷ 4 = 64
     - ÷ 2 = 32
     - 32-256
   * - **Image Processing**
     - 32
     - ÷ 4 = 8
     - ÷ 2 = 4
     - 4-32
   * - **GPU Operations**
     - 512
     - ÷ 4 = 128
     - ÷ 2 = 64
     - 64-512

**Selection Process:**

1. **Start with base batch size** for your operation type
2. **Reduce by 4x** if you have large rows (>10MB per row)
3. **Reduce by 2x** if your operation is memory-intensive
4. **Test and adjust** based on actual performance

Memory Antipatterns & Patterns
==============================

Antipattern: Ignoring Object Store Limits
-----------------------------------------

**Severity**:  Critical **Impact**: OOM crashes, 10x+ slower

**Problem**: Creating too many objects in Ray's object store, causing spilling and crashes.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Creating thousands of small objects
            def create_many_objects(batch):
                results = []
                for item in batch["items"]:
                    # Each result becomes a separate object - bad!
                    result = ray.put(expensive_computation(item))
                    results.append(result)
                return {"results": results}
            
            ds.map_batches(create_many_objects)

        **Problems:**
        - Thousands of small objects in object store
        - High memory overhead
        - Spilling to disk
        - Poor performance

    .. tab-item:: PATTERN Pattern: Batched Object Management

        .. code-block:: python

            def efficient_object_management(batch):
                """Process items in batch and return consolidated results."""
                
                # Process all items in batch
                results = []
                for item in batch["items"]:
                    result = expensive_computation(item)
                    results.append(result)
                
                # Return as single batch - creates one object
                return {"results": results}
            
            # Configure memory limits
            ctx = ray.data.DataContext.get_current()
            ctx.target_max_block_size = 64 * 1024 * 1024  # 64MB blocks
            
            ds.map_batches(efficient_object_management)

**Object Store Monitoring Pattern**:

.. code-block:: python

    import ray
    
    def monitor_object_store_usage():
        """Monitor object store usage and alert on high usage."""
        
        store_stats = ray.cluster_resources()
        object_store_memory = store_stats.get("object_store_memory", 0)
        
        # Get current usage (simplified - actual implementation more complex)
        current_usage = ray._private.internal_api.memory_summary()
        
        usage_pct = (current_usage / object_store_memory) * 100
        
        if usage_pct > 80:
            print(f"WARNING:  Object store usage high: {usage_pct:.1f}%")
            print("Consider reducing batch sizes or materializing less data")
        
        return usage_pct
    
    # Use in your pipeline
    def monitored_transform(batch):
        result = my_transform(batch)
        
        # Check object store usage periodically
        if hash(str(batch)) % 100 == 0:  # Check every ~100 batches
            monitor_object_store_usage()
        
        return result

Antipattern: Not Handling Memory Pressure
-----------------------------------------

**Severity**:  High **Impact**: OOM crashes, inconsistent performance

**Problem**: Ignoring Ray Data's backpressure signals and memory management.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # Ignoring memory constraints
            def memory_hungry_transform(batch):
                # Creates multiple large copies
                data1 = np.array(batch["data"])      # Copy 1
                data2 = data1 * 2                   # Copy 2  
                data3 = np.concatenate([data1, data2]) # Copy 3
                result = expensive_ml_model(data3)   # Copy 4
                return {"result": result}
            
            # No memory management
            ds.map_batches(memory_hungry_transform)

    .. tab-item:: PATTERN Pattern: Memory-Conscious Processing

        .. code-block:: python

            def memory_efficient_transform(batch):
                """Memory-efficient version with explicit cleanup."""
                
                # Process in-place when possible
                data = np.array(batch["data"])
                
                # Use views instead of copies when possible
                doubled = data * 2  # This might reuse memory
                
                # Process and clean up intermediate results
                result = expensive_ml_model(doubled)
                
                # Explicit cleanup of large intermediate objects
                del data, doubled
                
                return {"result": result}
            
            # Configure memory limits
            ds.map_batches(
                memory_efficient_transform,
                ray_remote_args={"memory": 2 * 1024**3}  # 2GB limit per task
            )

**Memory Pressure Detection Pattern**:

.. code-block:: python

    import psutil
    import gc
    
    def memory_pressure_aware_transform(transform_func):
        """Wrapper that monitors and responds to memory pressure."""
        
        def wrapped_transform(batch):
            # Check memory before processing
            memory_before = psutil.Process().memory_info().rss / (1024**2)
            
            # Execute transform
            result = transform_func(batch)
            
            # Check memory after
            memory_after = psutil.Process().memory_info().rss / (1024**2)
            memory_increase = memory_after - memory_before
            
            # If memory increased significantly, force garbage collection
            if memory_increase > 100:  # 100MB increase
                gc.collect()
                print(f"Memory increased by {memory_increase:.1f}MB, ran GC")
            
            return result
        
        return wrapped_transform
    
    # Usage
    safe_transform = memory_pressure_aware_transform(my_transform)
    ds.map_batches(safe_transform)

Resource Management Antipatterns & Patterns
===========================================

Antipattern: Wrong Concurrency Settings
---------------------------------------

**Severity**:  Medium **Impact**: 1.5-3x slower, resource underutilization

**Problem**: Using inappropriate concurrency that doesn't match workload characteristics or cluster resources.

.. tab-set::

    .. tab-item:: ANTIPATTERN Antipattern

        .. code-block:: python

            # CPU-intensive task with too much concurrency
            def cpu_intensive_task(batch):
                # Uses 100% CPU per task
                return heavy_computation(batch)
            
            # Oversubscribes CPUs!
            ds.map_batches(
                cpu_intensive_task,
                concurrency=64  # But only have 16 CPUs
            )

        **Problems:**
        - CPU oversubscription
        - Context switching overhead
        - Poor cache locality
        - Inconsistent performance

    .. tab-item:: PATTERN Pattern: Resource-Aware Concurrency

        **Concurrency Selection Guide:**

        Choose concurrency based on your task characteristics:

        .. list-table:: Concurrency by Task Type
           :header-rows: 1
           :class: concurrency-selection-guide

           * - Task Type
             - Concurrency Formula
             - Example (8 CPUs, 2 GPUs, 16GB RAM)
             - Reasoning
           * - **CPU-intensive**
             - CPU Count
             - 8
             - Match available CPU cores
           * - **I/O-intensive**
             - CPU Count × 2
             - 16
             - CPUs wait for I/O, allow oversubscription
           * - **Memory-intensive**
             - RAM ÷ Memory per Task
             - 16GB ÷ 2GB = 8
             - Prevent out-of-memory errors
           * - **GPU-accelerated**
             - GPU Count
             - 2
             - Match available GPU devices
           * - **Default/Mixed**
             - CPU Count ÷ 2
             - 4
             - Conservative balanced approach

        **Apply the Appropriate Concurrency:**

        .. code-block:: python

            import ray
            
            # Example: CPU-intensive task
            cluster_cpus = int(ray.cluster_resources().get("CPU", 1))
            
            ds.map_batches(
                cpu_intensive_task,
                concurrency=cluster_cpus  # Match CPU count
            )

**Dynamic Concurrency Pattern**:

.. code-block:: python

    class AdaptiveConcurrencyTransform:
        """Transform that adapts concurrency based on performance."""
        
        def __init__(self, base_transform, initial_concurrency=None):
            self.base_transform = base_transform
            self.current_concurrency = initial_concurrency or ray.cluster_resources()["CPU"]
            self.performance_history = []
        
        def __call__(self, batch):
            import time
            start_time = time.time()
            
            result = self.base_transform(batch)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Track performance
            self.performance_history.append(processing_time)
            
            # Adapt concurrency based on recent performance
            if len(self.performance_history) > 10:
                recent_avg = sum(self.performance_history[-10:]) / 10
                older_avg = sum(self.performance_history[-20:-10]) / 10
                
                if recent_avg > older_avg * 1.2:  # Performance degraded
                    self.current_concurrency = max(1, self.current_concurrency - 1)
                    print(f"Reducing concurrency to {self.current_concurrency}")
                
            return result
    
    # Usage
    adaptive_transform = AdaptiveConcurrencyTransform(my_transform)
    ds.map_batches(adaptive_transform, concurrency=adaptive_transform.current_concurrency)

Advanced Patterns
================

Pattern: Streaming Execution Optimization
-----------------------------------------

Optimize Ray Data's streaming execution for maximum throughput:

**Streaming Execution Configuration Guide**

Choose your configuration based on your primary optimization goal:

.. list-table:: Streaming Execution Configuration by Workload Type
   :header-rows: 1
   :class: streaming-config-table

   * - Workload Type
     - Max Block Size
     - Min Block Size
     - Preserve Order
     - Object Store Limit
     - Use Case
   * - **High Throughput**
     - 128MB
     - 16MB
     - False
     - Default
     - Batch processing, ETL
   * - **Low Latency**
     - 16MB
     - 1MB
     - True
     - Default
     - Real-time processing
   * - **Memory Constrained**
     - 32MB
     - 1MB
     - False
     - 1GB
     - Limited memory environments

**High Throughput Configuration:**

For maximum data processing speed when you have sufficient memory and CPU resources:

.. testcode::

    ctx = ray.data.DataContext.get_current()
    
    # Optimize for maximum throughput
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
    ctx.target_min_block_size = 16 * 1024 * 1024   # 16MB minimum
    ctx.execution_options.preserve_order = False    # Allow reordering for efficiency

**Low Latency Configuration:**

For real-time processing where response time is critical:

.. testcode::

    ctx = ray.data.DataContext.get_current()
    
    # Optimize for low latency
    ctx.target_max_block_size = 16 * 1024 * 1024   # 16MB blocks
    ctx.target_min_block_size = 1 * 1024 * 1024    # 1MB minimum
    ctx.execution_options.preserve_order = True     # Maintain order

**Memory Constrained Configuration:**

For environments with limited memory availability:

.. testcode::

    ctx = ray.data.DataContext.get_current()
    
    # Optimize for limited memory
    ctx.target_max_block_size = 32 * 1024 * 1024   # 32MB blocks
    ctx.execution_options.resource_limits.object_store_memory = 1 * 1024**3  # 1GB limit

Apply your chosen configuration, then run your pipeline:

.. testcode::

    # Your pipeline will now use the optimized settings
    result = ds.map_batches(transform).write_parquet("output/")

Pattern: Fault-Tolerant Processing
----------------------------------

Build resilient Ray Data pipelines that handle failures gracefully:

.. code-block:: python

    class FaultTolerantTransform:
        """Transform that handles various types of failures."""
        
        def __init__(self, base_transform, max_retries=3):
            self.base_transform = base_transform
            self.max_retries = max_retries
            self.failure_stats = {"total": 0, "retries": 0, "permanent_failures": 0}
        
        def __call__(self, batch):
            for attempt in range(self.max_retries + 1):
                try:
                    result = self.base_transform(batch)
                    
                    # Add metadata about processing
                    if isinstance(result, dict):
                        result["_processing_attempts"] = attempt + 1
                        result["_processing_success"] = True
                    
                    return result
                    
                except MemoryError as e:
                    # Handle OOM by reducing batch size
                    if attempt < self.max_retries:
                        print(f"OOM on attempt {attempt + 1}, reducing batch size")
                        # Process in smaller chunks
                        return self._process_in_chunks(batch)
                    else:
                        self.failure_stats["permanent_failures"] += 1
                        return self._create_failure_result(batch, str(e))
                
                except Exception as e:
                    self.failure_stats["total"] += 1
                    
                    if attempt < self.max_retries:
                        self.failure_stats["retries"] += 1
                        print(f"Retry {attempt + 1} for error: {e}")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        self.failure_stats["permanent_failures"] += 1
                        return self._create_failure_result(batch, str(e))
            
        def _process_in_chunks(self, batch):
            """Process batch in smaller chunks to avoid OOM."""
            chunk_size = len(batch) // 4  # Quarter size chunks
            results = []
            
            for i in range(0, len(batch), chunk_size):
                chunk = {k: v[i:i+chunk_size] for k, v in batch.items()}
                chunk_result = self.base_transform(chunk)
                results.append(chunk_result)
            
            # Combine results
            combined = {}
            for key in results[0].keys():
                combined[key] = []
                for result in results:
                    combined[key].extend(result[key])
            
            return combined
        
        def _create_failure_result(self, batch, error_msg):
            """Create result structure for failed processing."""
            batch_size = len(next(iter(batch.values())))
            return {
                "results": [None] * batch_size,
                "_processing_success": False,
                "_error_message": error_msg,
                "_processing_attempts": self.max_retries + 1
            }
        
        def get_failure_stats(self):
            """Get statistics about processing failures."""
            return self.failure_stats
    
    # Usage
    fault_tolerant_transform = FaultTolerantTransform(my_risky_transform)
    result = ds.map_batches(fault_tolerant_transform)
    
    # Check failure statistics
    print("Failure stats:", fault_tolerant_transform.get_failure_stats())

Pattern: Performance Regression Detection
-----------------------------------------

Automatically detect performance regressions in your Ray Data pipelines:

.. code-block:: python

    import time
    import json
    from pathlib import Path
    
    class PerformanceMonitor:
        """Monitor and detect performance regressions."""
        
        def __init__(self, baseline_file="performance_baseline.json"):
            self.baseline_file = baseline_file
            self.baselines = self._load_baselines()
        
        def _load_baselines(self):
            """Load performance baselines from file."""
            if Path(self.baseline_file).exists():
                with open(self.baseline_file, 'r') as f:
                    return json.load(f)
            return {}
        
        def _save_baselines(self):
            """Save performance baselines to file."""
            with open(self.baseline_file, 'w') as f:
                json.dump(self.baselines, f, indent=2)
        
        def benchmark_pipeline(self, pipeline_name, pipeline_func, *args, **kwargs):
            """Benchmark a pipeline and check for regressions."""
            
            print(f"Benchmarking {pipeline_name}...")
            
            start_time = time.time()
            result = pipeline_func(*args, **kwargs)
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Get additional metrics
            if hasattr(result, 'stats'):
                stats = result.stats()
                throughput = len(result) / execution_time if execution_time > 0 else 0
            else:
                throughput = 0
            
            metrics = {
                "execution_time": execution_time,
                "throughput": throughput,
                "timestamp": time.time()
            }
            
            # Check for regression
            self._check_regression(pipeline_name, metrics)
            
            # Update baseline if this is better or first run
            self._update_baseline(pipeline_name, metrics)
            
            return result
        
        def _check_regression(self, pipeline_name, current_metrics):
            """Check if current performance is a regression."""
            
            if pipeline_name not in self.baselines:
                print(f"BASELINE: {pipeline_name}: First run, establishing baseline")
                return
            
            baseline = self.baselines[pipeline_name]
            current_time = current_metrics["execution_time"]
            baseline_time = baseline["execution_time"]
            
            # Check for significant regression (>20% slower)
            if current_time > baseline_time * 1.2:
                regression_pct = ((current_time - baseline_time) / baseline_time) * 100
                print(f"ALERT: PERFORMANCE REGRESSION in {pipeline_name}:")
                print(f"   Current: {current_time:.2f}s")
                print(f"   Baseline: {baseline_time:.2f}s") 
                print(f"   Regression: {regression_pct:.1f}% slower")
            
            elif current_time < baseline_time * 0.8:
                improvement_pct = ((baseline_time - current_time) / baseline_time) * 100
                print(f"IMPROVEMENT: PERFORMANCE IMPROVEMENT in {pipeline_name}:")
                print(f"   Current: {current_time:.2f}s")
                print(f"   Baseline: {baseline_time:.2f}s")
                print(f"   Improvement: {improvement_pct:.1f}% faster")
        
        def _update_baseline(self, pipeline_name, metrics):
            """Update baseline if performance is better or first run."""
            
            if (pipeline_name not in self.baselines or 
                metrics["execution_time"] < self.baselines[pipeline_name]["execution_time"]):
                
                self.baselines[pipeline_name] = metrics
                self._save_baselines()
                print(f"Updated baseline for Updated baseline for {pipeline_name}")
    
    # Usage
    monitor = PerformanceMonitor()
    
    def my_pipeline():
        ds = ray.data.read_parquet("data.parquet")
        return ds.map_batches(my_transform).write_parquet("output/")
    
    # Benchmark and monitor for regressions
    result = monitor.benchmark_pipeline("data_processing_pipeline", my_pipeline)

Best Practices Summary
=====================

**Critical Patterns to Follow (Universal Across Workloads)**

1. **Use map_batches over map** for vectorizable operations
   - **ETL**: Data cleaning, type conversions, business logic
   - **ML/AI**: Feature engineering, model inference, data preprocessing  
   - **Real-time**: Stream processing, event transformation

2. **Always use column pruning** when reading structured data
   - **ETL**: Read only needed columns for transformations
   - **ML/AI**: Load only features required for training/inference
   - **Analytics**: Select columns relevant to specific queries

3. **Choose optimal file formats** based on workload characteristics
   - **ETL**: Parquet for analytics, CSV for legacy integration
   - **ML/AI**: Arrow for training, Parquet for feature stores
   - **Real-time**: Delta Lake for streaming, Arrow for low latency

4. **Monitor and respect memory limits** across all workload types
   - **ETL**: Prevent OOM during large data transformations
   - **ML/AI**: Manage memory for GPU workloads and large models
   - **Real-time**: Control memory accumulation in streaming pipelines

5. **Configure concurrency** based on workload characteristics
   - **ETL**: Balance parallelism with resource constraints
   - **ML/AI**: Optimize for mixed CPU/GPU resource utilization
   - **Real-time**: Tune for latency requirements vs throughput

**Critical Antipatterns to Avoid**
1. **Never read thousands of small files** without consolidation
2. **Don't use pandas batch_format** unless necessary
3. **Don't ignore Ray Data's memory management** signals
4. **Don't use default settings** without understanding your workload
5. **Don't skip performance monitoring** and regression detection

**Implementation Strategy**
1. **Start with quick wins**: Apply high-impact patterns first
2. **Measure before and after**: Always validate performance improvements
3. **Monitor continuously**: Set up automated performance regression detection
4. **Learn incrementally**: Master basic patterns before advanced techniques
5. **Share knowledge**: Document patterns that work for your team

Next Steps
==========

**Advanced Pattern Categories:**

**Scalability Patterns:**

**Horizontal Scaling Pattern:**
- Design operations that scale linearly with cluster size
- Use data-parallel processing strategies
- Implement efficient load distribution
- Configure for elastic cluster scaling

**Vertical Scaling Pattern:**
- Optimize for high-memory, high-CPU nodes
- Use memory-intensive processing strategies
- Implement efficient resource utilization
- Configure for specialized hardware

**Reliability Patterns:**

**Circuit Breaker Pattern:**
- Implement failure detection and isolation
- Use graceful degradation strategies
- Configure automatic recovery mechanisms
- Monitor system health and performance

**Bulkhead Pattern:**
- Isolate different workload types
- Use resource partitioning strategies
- Implement fault isolation
- Configure for workload independence

**Performance Patterns:**

**Caching Pattern:**
- Implement multi-level caching strategies
- Use cache warming and invalidation
- Configure cache sizing and eviction policies
- Monitor cache hit rates and effectiveness

**Lazy Loading Pattern:**
- Defer expensive computations until needed
- Use streaming execution effectively
- Implement just-in-time data loading
- Configure for memory efficiency

**Monitoring Patterns:**

**Observability Pattern:**
- Implement comprehensive metrics collection
- Use distributed tracing for complex operations
- Configure alerting for performance issues
- Monitor business and technical metrics

**Performance Baseline Pattern:**
- Establish performance baselines for different workloads
- Implement automated performance regression detection
- Use statistical analysis for performance trends
- Configure alerts for significant performance changes

**Advanced Antipattern Categories:**

**Scalability Antipatterns:**

**Hot Spot Antipattern:**
- Uneven data distribution causing performance bottlenecks
- Single nodes becoming overwhelmed with work
- Resource contention from poor load balancing
- Network bottlenecks from concentrated traffic

**Resource Contention Antipattern:**
- Multiple workloads competing for same resources
- Inefficient resource allocation strategies
- Poor isolation between different workload types
- Suboptimal cluster resource utilization

**Error Handling Patterns:**

**Graceful Degradation Pattern:**
- Implement fallback strategies for performance issues
- Use circuit breaker patterns for failing operations
- Configure automatic retry with exponential backoff
- Monitor error rates and performance impact

**Data Quality Handling Pattern:**
- Implement schema validation at ingestion time
- Use data quality checks with performance optimization
- Configure error isolation to prevent pipeline failures
- Monitor data quality metrics alongside performance metrics

**Resource Exhaustion Pattern:**
- Implement memory pressure detection and response
- Use adaptive resource allocation strategies
- Configure graceful handling of resource limits
- Monitor resource utilization trends

**Network Resilience Pattern:**
- Implement network failure detection and recovery
- Use connection pooling and retry strategies
- Configure timeout handling for network operations
- Monitor network performance and reliability

**Optimization Validation Patterns:**

**A/B Testing Pattern:**
- Compare optimized vs unoptimized performance
- Use statistical significance testing
- Configure gradual rollout strategies
- Monitor business and technical metrics

**Canary Deployment Pattern:**
- Deploy optimizations to subset of traffic
- Monitor performance impact in real-time
- Configure automatic rollback on regression
- Use progressive traffic shifting

**Performance Regression Detection Pattern:**
- Establish performance baselines for all operations
- Implement automated performance monitoring
- Configure alerts for significant performance changes
- Use statistical analysis for trend detection

Continue building your Ray Data expertise:

- **Apply these patterns** to your current Ray Data pipelines
- **Set up monitoring** to detect when antipatterns creep in
- **Explore advanced optimizations**: :ref:`advanced_operations`
- **Debug performance issues**: :ref:`troubleshooting`
- **Contribute patterns**: Share your discoveries with the Ray Data community
