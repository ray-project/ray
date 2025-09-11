.. _transform_optimization:

==========================================
Ray Data Transform Performance Optimization
==========================================

Transformations are often the most compute-intensive part of Ray Data pipelines across all workload types - from traditional ETL data cleaning and aggregation, to ML feature engineering and model inference, to emerging real-time AI processing. This guide covers comprehensive strategies to optimize :meth:`~ray.data.Dataset.map`, :meth:`~ray.data.Dataset.map_batches`, and other transformation operations across these diverse use cases.

.. contents::
   :local:
   :depth: 2

Transform Optimization Quick Wins
=================================

Start with these high-impact optimizations for immediate performance improvements:

**Essential Optimizations**
- Use :meth:`~ray.data.Dataset.map_batches` instead of :meth:`~ray.data.Dataset.map`
- Avoid ``batch_format="pandas"`` unless necessary
- Optimize batch sizes for your workload
- Use actors for stateful operations
- Leverage vectorized operations (NumPy, PyArrow)

**Resource Optimizations**
- Tune concurrency based on your cluster
- Configure appropriate CPU/GPU allocation
- Set memory limits to prevent OOM
- Enable :ref:`operator fusion <data-internals>` when possible

map vs map_batches: The Critical Choice
======================================

Understanding the Difference
----------------------------

The choice between :meth:`~ray.data.Dataset.map` and :meth:`~ray.data.Dataset.map_batches` is the most important decision for Ray Data transform performance. This choice affects how your data is processed and can result in significant performance differences.

**For Data Scientists and Engineers: Understanding the Difference**

If you've used pandas or NumPy, you know that vectorized operations are much faster than loops. The map vs map_batches choice is similar:

- **map()** approach: Like using a Python loop - `[func(row) for row in dataframe.iterrows()]` - processes one row at a time
- **map_batches()** approach: Like using pandas vectorized operations - `dataframe.apply(func)` - processes chunks of rows using optimized libraries

The performance difference comes from the same principles that make pandas operations faster than pure Python loops: vectorization, reduced function call overhead, and better memory locality.

**Conceptual Difference:**

- **map()** processes one row at a time, creating a separate function call for each row
- **map_batches()** processes multiple rows together in batches, enabling vectorized operations

The performance difference comes from vectorization - modern CPUs and libraries like NumPy can process arrays of data much faster than individual elements. This is because CPUs have special instructions (called SIMD - Single Instruction, Multiple Data) that can perform the same operation on multiple data points simultaneously.

.. list-table:: map vs map_batches Comparison
   :header-rows: 1
   :class: comparison-table

   * - Aspect
     - map()
     - map_batches()
   * - **Processing Unit**
     - Single row
     - Batch of rows
   * - **Performance**
     - Fair
     - Excellent
   * - **Memory Efficiency**
     - Good
     - Good
   * - **Vectorization**
     - No
     - Yes
   * - **Use Case**
     - Simple row operations
     - Vectorized operations

**When to Use Each**

.. tab-set::

    .. tab-item:: Use map() when

        - Processing single rows independently
        - Operations cannot be vectorized
        - Working with complex nested data structures
        - Memory constraints require row-by-row processing

        .. code-block:: python

            # Good use case for map()
            ds = ray.data.range(1000)
            result = ds.map(lambda x: {"complex_id": f"user_{x}_processed"})

    .. tab-item:: Use map_batches() when

        - Operations can be vectorized (NumPy, PyArrow)
        - Working with numerical data
        - Applying ML models
        - Most data processing tasks

        .. code-block:: python

            # Good use case for map_batches()
            ds = ray.data.read_parquet("data.parquet")
            result = ds.map_batches(lambda batch: {
                "normalized": batch["value"] / batch["value"].max()
            })

Batch Format Optimization
=========================

Choosing the Right Batch Format
-------------------------------

When using :meth:`~ray.data.Dataset.map_batches`, you can specify how Ray Data should format the data before passing it to your function. This choice significantly impacts performance because different formats have different conversion costs and processing efficiencies.

**Format Conversion Process:**

Ray Data internally stores :ref:`blocks <data_key_concepts>` as Arrow tables, but converts them to your requested format before calling your function. After processing, results are converted back to Arrow format for storage in the :ref:`object store <objects-in-ray>`.

**Understanding Format Conversion Costs:**

Each format conversion involves computational overhead and temporary memory allocation:

1. **Arrow → Your Format**: Ray Data converts the internal Arrow table to your requested format
2. **Processing**: Your function operates on the converted data
3. **Your Format → Arrow**: Results are converted back to Arrow for storage

The conversion overhead varies significantly by format:

- **Default (dict)**: Minimal conversion, just exposes Arrow columns as Python objects
- **NumPy**: Efficient conversion for numerical data, leverages Arrow's zero-copy capabilities
- **Pandas**: Expensive conversion that creates a full DataFrame copy in memory
- **PyArrow**: No conversion needed since data is already in Arrow format

The conversion process affects both memory usage and processing time:

.. list-table:: Batch Format Performance Guide
   :header-rows: 1
   :class: format-guide-table

   * - Format
     - Speed
     - Memory
     - Use Case
     - Example
   * - **"default" (dict)**
     - Excellent
     - Excellent
     - Universal
     - ETL, data cleaning, simple transforms
   * - **"pyarrow"**
     - Good
     - Good
     - Columnar operations
     - Analytics, data warehousing, aggregations
   * - **"pandas"**
     - Fair
     - Fair
     - Complex data manipulation
     - ETL with complex logic, data science exploration
   * - **"numpy"**
     - Excellent
     - Excellent
     - Numerical computing
     - ML inference, scientific computing, signal processing

**Performance Comparison Example**

To understand the performance impact of different batch formats, let's compare them with a realistic numerical operation. First, create a test dataset:

.. testcode::

    import ray
    import numpy as np
    import time
    
    # Create test dataset with numerical data
    ds = ray.data.range(10000).map_batches(
        lambda batch: {"values": np.random.rand(len(batch["item"]))}
    )

Now we'll benchmark each format performing the same mathematical operation (doubling values). The operation is simple but representative of common numerical processing:

**Default Format Architecture:**

Ray Data's default format exposes Arrow data as Python dictionaries with minimal conversion overhead:

.. testcode::

    # Default format - minimal conversion
    result = ds.map_batches(
        lambda batch: {"result": np.array(batch["values"]) * 2}
    )

**NumPy Format Architecture:**

NumPy format leverages Arrow's zero-copy capabilities for efficient numerical processing:

.. testcode::

    # NumPy format - efficient for numerical data
    result = ds.map_batches(
        lambda batch: {"result": batch["values"] * 2},
        batch_format="numpy"
    )

**Pandas Format Architecture:**

Pandas format involves expensive conversions but provides familiar DataFrame operations:

.. testcode::

    # Pandas format - expensive conversion but familiar API
    result = ds.map_batches(
        lambda batch: batch.assign(result=batch["values"] * 2),
        batch_format="pandas"
    )

**Optimization Guidelines**

.. code-block:: python

    # Optimal: Use default format for simple operations
    ds.map_batches(lambda batch: {"doubled": np.array(batch["value"]) * 2})
    
    # Good: Use numpy format for numerical operations
    ds.map_batches(
        lambda batch: {"normalized": batch / batch.max()},
        batch_format="numpy"
    )
    
    # Use pandas only when necessary
    ds.map_batches(
        lambda batch: batch.groupby("category").mean(),
        batch_format="pandas"
    )
    
    # Avoid: Unnecessary format conversion
    ds.map_batches(
        lambda batch: {"simple": batch["value"]},  # Simple operation
        batch_format="pandas"  # Expensive conversion
    )

Batch Size Optimization
=======================

Understanding Batch Size Impact
------------------------------

Batch size significantly affects performance and memory usage:

.. testcode::

    import ray
    import numpy as np
    
    def test_batch_sizes(ds, batch_sizes, operation):
        """Test different batch sizes for a given operation."""
        results = {}
        
        for batch_size in batch_sizes:
            start_time = time.time()
            
            result = ds.map_batches(
                operation,
                batch_size=batch_size
            ).materialize()
            
            end_time = time.time()
            results[batch_size] = end_time - start_time
            
            print(f"Batch size {batch_size}: {end_time - start_time:.2f}s")
        
        return results
    
    # Create test dataset
    ds = ray.data.range(10000).map_batches(
        lambda batch: {"data": np.random.rand(len(batch["item"]), 100)}
    )
    
    # Test different batch sizes
    def expensive_operation(batch):
        """Simulate compute-intensive operation."""
        data = np.array(batch["data"])
        return {"result": np.mean(data, axis=1)}
    
    batch_sizes = [32, 128, 512, 1024, 2048]
    results = test_batch_sizes(ds, batch_sizes, expensive_operation)

**Batch Size Selection Guide**

Choose batch size based on your operation's resource requirements:

.. list-table:: Batch Size by Operation Type
   :header-rows: 1
   :class: batch-size-guide

   * - Operation Type
     - Recommended Batch Size
     - Memory Usage
     - Reasoning
   * - **Memory-intensive**
     - 32-128
     - High per row
     - Prevent out-of-memory errors
   * - **CPU-intensive**
     - 512-1024
     - Medium per row
     - Balance overhead and parallelism
   * - **Simple operations**
     - 1024-2048
     - Low per row
     - Minimize task overhead
   * - **GPU operations**
     - 128-512
     - GPU memory dependent
     - Fit in GPU memory constraints
   * - **I/O operations**
     - 256-512
     - Variable
     - Balance I/O wait time

**Memory-Intensive Operations:**

.. testcode::

    # Memory-intensive operations: Use smaller batches
    ds.map_batches(
        memory_intensive_function,
        batch_size=32  # Prevent OOM
    )

**CPU-Intensive Operations:**

.. testcode::

    # CPU-intensive operations: Use medium batches
    ds.map_batches(
        cpu_intensive_function,
        batch_size=512  # Balance overhead and parallelism
    )

**Simple Operations:**

.. testcode::

    # Simple operations: Use larger batches
    ds.map_batches(
        simple_function,
        batch_size=2048  # Minimize overhead
    )

**GPU Operations:**

.. testcode::

    # GPU operations: Optimize for GPU memory
    ds.map_batches(
        gpu_function,
        batch_size=256,  # Fit in GPU memory
        num_gpus=1
    )

Batch Size Testing
------------------

Instead of complex dynamic sizing, test a few proven batch sizes to find what works best for your specific workload:

**How Ray Data Determines Optimal Batch Size:**

Ray Data's batch processing works within the constraints of your cluster's memory and processing capabilities. Understanding this helps you choose appropriate batch sizes:

**Batch Processing Architecture:**

1. **Task receives a block** (data partition) from the object store
2. **Block is split into batches** of your specified size
3. **Each batch is processed sequentially** by your transform function
4. **Results accumulate in task memory** until the block is complete
5. **Completed block is stored** back in the object store

**Memory and Performance Trade-offs:**

- **Larger batches**: More efficient processing, higher memory usage per batch
- **Smaller batches**: Lower memory usage, more function call overhead
- **Optimal size**: Balances memory constraints with processing efficiency

**Selection Guidelines:**

Choose batch sizes based on your operation's memory footprint and processing characteristics rather than automated testing.

Concurrency and Resource Optimization
=====================================

Understanding Concurrency
-------------------------

Concurrency controls how many transformation tasks run simultaneously:

.. testcode::

    import ray
    
    # Default: Ray Data automatically determines concurrency
    ds.map_batches(transform_func)

For resource-intensive operations, set explicit concurrency limits:

.. testcode::

    # Fixed concurrency: Useful for resource-intensive operations
    ds.map_batches(
        transform_func,
        concurrency=8  # Exactly 8 concurrent tasks
    )

Calculate concurrency based on your cluster size:

.. testcode::

    # Resource-aware concurrency
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    optimal_concurrency = cluster_cpus // 2  # Use half the CPUs
    
    ds.map_batches(
        transform_func,
        concurrency=optimal_concurrency
    )

**Concurrency Configuration Guide**

Choose concurrency based on your operation characteristics:

.. list-table:: Concurrency by Operation Type
   :header-rows: 1
   :class: concurrency-guide

   * - Operation Type
     - Concurrency Formula
     - Example (8 CPUs, 2 GPUs)
     - Reasoning
   * - **CPU-intensive**
     - CPU Count
     - 8
     - Match available CPU cores
   * - **Memory-intensive**
     - 2-4 (fixed)
     - 4
     - Prevent out-of-memory errors
   * - **I/O-bound**
     - CPU Count × 2
     - 16
     - CPUs wait for I/O, allow oversubscription
   * - **GPU operations**
     - GPU Count
     - 2
     - Match available GPU devices

**CPU-Intensive Operations:**

For operations that fully utilize CPU cores:

.. testcode::

    # CPU-bound operations: Match CPU count
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    ds.map_batches(
        cpu_intensive_func,
        concurrency=cluster_cpus
    )

**Memory-Intensive Operations:**

For operations that use significant memory per task:

.. testcode::

    # Memory-intensive operations: Use fixed low concurrency
    ds.map_batches(
        memory_intensive_func,
        concurrency=4  # Fixed limit to prevent OOM
    )

**I/O-Bound Operations:**

For operations that spend time waiting for network or disk I/O:

.. testcode::

    # I/O-bound operations: Higher concurrency
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    ds.map_batches(
        io_bound_func,
        concurrency=cluster_cpus * 2  # Allow oversubscription
    )

**GPU Operations:**

For GPU-accelerated processing:

.. testcode::

    # GPU operations: Match GPU count
    cluster_gpus = int(ray.cluster_resources().get("GPU", 0))
    ds.map_batches(
        gpu_func,
        concurrency=cluster_gpus,
        num_gpus=1
    )

Stateful vs Stateless Transformations
====================================

Tasks vs Actors Decision
------------------------

Ray Data can execute transformations using either :ref:`Ray tasks or Ray actors <core-key-concepts>`. Understanding when to use each is crucial for performance:

- **Tasks**: Stateless functions that start fresh for each execution
- **Actors**: Stateful classes that maintain state between function calls

**The Performance Trade-off:**

Tasks and actors have fundamentally different performance characteristics:

**Tasks** are lightweight and fast to start but cannot maintain state between function calls. This means any expensive initialization (like loading a machine learning model) happens for every batch, which can be very inefficient.

**Actors** have higher startup overhead because they need to initialize and stay running, but they can maintain expensive resources like loaded models, database connections, or caches across multiple function calls. For operations that benefit from persistent state, actors can be dramatically faster despite their higher startup cost.

**Decision Framework:**

The choice depends on whether your operation benefits from persistent state across batches. Consider the initialization cost of your operation - if it's expensive and can be reused, actors are likely better.

Choose between tasks and actors based on your operation characteristics:

.. list-table:: Tasks vs Actors Guide
   :header-rows: 1
   :class: tasks-actors-table

   * - Aspect
     - Tasks (Stateless)
     - Actors (Stateful)
   * - **Startup Time**
     - Fast
     - Slow (one-time)
   * - **Memory Usage**
     - Lower
     - Higher (persistent)
   * - **State Management**
     - None
     - Persistent state
   * - **Use Case**
     - Simple functions
     - Model loading, caches

**Stateless Transformations (Tasks)**

.. testcode::

    # EFFICIENT Good for stateless operations
    def simple_transform(batch):
        """Stateless function - no persistent state."""
        return {"result": batch["value"] * 2}
    
    # Uses tasks by default
    ds.map_batches(simple_transform)

**Stateful Transformations (Actors)**

.. testcode::

    # EFFICIENT Good for operations requiring persistent state
    class ModelInference:
        def __init__(self):
            # Expensive initialization (e.g., model loading)
            self.model = load_expensive_model()
            self.cache = {}
        
        def __call__(self, batch):
            # Use persistent state
            results = []
            for item in batch["data"]:
                if item in self.cache:
                    results.append(self.cache[item])
                else:
                    result = self.model.predict(item)
                    self.cache[item] = result
                    results.append(result)
            return {"predictions": results}
    
    # Uses actors automatically for callable classes
    ds.map_batches(ModelInference, concurrency=4)

**Explicit Actor Configuration**

.. testcode::

    from ray.data import ActorPoolStrategy
    
    # Force actor usage with specific configuration
    ds.map_batches(
        ModelInference,
        concurrency=4,
        compute=ActorPoolStrategy(
            min_size=2,      # Keep at least 2 actors alive
            max_size=8,      # Don't exceed 8 actors
        )
    )

Vectorization Techniques
=======================

NumPy Vectorization
-------------------

Leverage NumPy for high-performance numerical operations:

.. tab-set::

    .. tab-item:: ANTIPATTERN Non-vectorized

        .. code-block:: python

            # Slow: Row-by-row processing
            def slow_transform(batch):
                results = []
                for value in batch["values"]:
                    result = value * 2 + 1
                    results.append(result)
                return {"results": results}

    .. tab-item:: EFFICIENT Vectorized

        .. code-block:: python

            # Fast: Vectorized operations
            def fast_transform(batch):
                values = np.array(batch["values"])
                results = values * 2 + 1  # Vectorized operation
                return {"results": results}

**Advanced Vectorization Patterns**

.. testcode::

    import numpy as np
    
    def advanced_vectorized_transform(batch):
        """Demonstrate advanced vectorization techniques."""
        
        # Convert to NumPy arrays for vectorization
        values = np.array(batch["values"])
        categories = np.array(batch["categories"])
        
        # Vectorized conditional operations
        results = np.where(
            categories == "A",
            values * 2,      # If category A
            values * 0.5     # If not category A
        )
        
        # Vectorized aggregations
        group_means = np.array([
            values[categories == cat].mean() 
            for cat in np.unique(categories)
        ])
        
        # Vectorized mathematical operations
        normalized = (values - values.mean()) / values.std()
        
        return {
            "results": results,
            "normalized": normalized,
            "group_means": group_means
        }
    
    ds.map_batches(advanced_vectorized_transform)

PyArrow Vectorization
--------------------

Use PyArrow for efficient columnar operations:

.. testcode::

    import pyarrow.compute as pc
    
    def arrow_vectorized_transform(batch):
        """Use PyArrow compute functions for vectorization."""
        
        # PyArrow vectorized operations
        values = batch["values"]
        
        # Arithmetic operations
        doubled = pc.multiply(values, 2)
        
        # String operations
        if "text" in batch:
            upper_text = pc.utf8_upper(batch["text"])
            text_length = pc.utf8_length(batch["text"])
        
        # Conditional operations
        filtered = pc.filter(values, pc.greater(values, 0))
        
        return {
            "doubled": doubled,
            "filtered": filtered
        }
    
    ds.map_batches(arrow_vectorized_transform, batch_format="pyarrow")

GPU Acceleration
===============

GPU Transform Optimization
--------------------------

Optimize transformations for GPU acceleration:

.. testcode::

    import ray
    import cupy as cp  # GPU-accelerated NumPy
    
    class GPUTransform:
        def __init__(self):
            # Initialize on GPU
            self.device_id = 0
            
        def __call__(self, batch):
            # Move data to GPU
            values = cp.asarray(batch["values"])
            
            # GPU-accelerated operations
            result = cp.sqrt(values * 2 + 1)
            
            # Move result back to CPU
            return {"result": cp.asnumpy(result)}
    
    # Use GPU resources
    ds.map_batches(
        GPUTransform,
        concurrency=2,           # Number of GPU actors
        num_gpus=1,             # GPU per actor
        batch_size=1024         # Larger batches for GPU efficiency
    )

**GPU Memory Management**

.. testcode::

    import cupy as cp
    
    class MemoryEfficientGPUTransform:
        def __init__(self):
            self.device_id = 0
            
        def __call__(self, batch):
            # Clear GPU memory before processing
            cp.get_default_memory_pool().free_all_blocks()
            
            try:
                # Process on GPU
                gpu_data = cp.asarray(batch["data"])
                result = self.gpu_intensive_operation(gpu_data)
                
                # Convert back to CPU immediately
                cpu_result = cp.asnumpy(result)
                
                # Clear GPU memory after processing
                del gpu_data, result
                cp.get_default_memory_pool().free_all_blocks()
                
                return {"result": cpu_result}
                
            except cp.cuda.memory.OutOfMemoryError:
                # Fallback to CPU if GPU OOM
                return self.cpu_fallback(batch)
        
        def gpu_intensive_operation(self, data):
            # Your GPU operation here
            return cp.sqrt(data)
        
        def cpu_fallback(self, batch):
            # CPU fallback implementation
            import numpy as np
            return {"result": np.sqrt(batch["data"])}
    
    ds.map_batches(
        MemoryEfficientGPUTransform,
        num_gpus=1,
        batch_size=512  # Smaller batches to fit in GPU memory
    )

Advanced Transform Patterns
===========================

Operator Fusion
---------------

*Operator fusion* is Ray Data's automatic optimization that combines multiple compatible operations into a single task. This reduces data movement between operations and improves performance by eliminating intermediate serialization.

**How Operator Fusion Works:**

Instead of executing three separate tasks that pass data through the :ref:`object store <objects-in-ray>`, Ray Data combines compatible operations into a single task that processes data in memory.

.. testcode::

    # These operations will be automatically fused into a single task
    ds = ray.data.read_parquet("data.parquet")
    result = ds.map_batches(transform1) \
              .map_batches(transform2) \
              .map_batches(transform3)

You can verify that fusion occurred by examining the execution plan:

.. testcode::

    # Check execution plan to see fusion
    print(result._plan)

**How to Verify Operator Fusion:**

Operator fusion is visible in the execution plan. When Ray Data fuses operations, you'll see them connected with arrows (→) in the plan output:

.. testcode::

    # Check execution plan for fusion
    print(result._plan)

**What Fusion Looks Like:**

Fused operations appear as: `TaskPoolMapOperator[ReadParquet->MapBatches(transform1)->MapBatches(transform2)]`

The arrow (→) indicates that these operations will run in a single task, eliminating data movement between operations.

**Fusion Architecture Benefits:**

When operations are fused:
- **No intermediate serialization**: Data stays in task memory between operations
- **Reduced object store pressure**: Fewer intermediate blocks created
- **Better cache locality**: Data processing happens in the same memory space
- **Lower network overhead**: No data transfer between fused operations

**Fusion-Friendly Patterns**

.. code-block:: python

    # EFFICIENT Fusion-friendly: Same compute requirements
    ds.map_batches(cpu_transform1) \
      .map_batches(cpu_transform2)
    
    # EFFICIENT Fusion-friendly: Compatible resource requirements
    ds.map_batches(simple_transform, num_cpus=1) \
      .map_batches(another_simple_transform, num_cpus=1)
    
    # ANTIPATTERN Fusion-breaking: Different resource requirements
    ds.map_batches(cpu_transform, num_cpus=2) \
      .map_batches(gpu_transform, num_gpus=1)

Conditional Transformations
--------------------------

Implement efficient conditional processing:

.. testcode::

    def conditional_transform(batch):
        """Apply different transforms based on data characteristics."""
        
        # Vectorized condition checking
        condition = np.array(batch["category"]) == "special"
        
        # Apply different operations based on condition
        results = np.where(
            condition,
            np.array(batch["value"]) * 2,      # Special processing
            np.array(batch["value"]) * 0.5     # Regular processing
        )
        
        return {"result": results}
    
    ds.map_batches(conditional_transform)

Error Handling in Transforms
============================

Robust Transform Implementation
------------------------------

Implement proper error handling to prevent pipeline failures:

.. testcode::

    import logging
    
    def robust_transform(batch):
        """Transform with comprehensive error handling."""
        try:
            # Main transformation logic
            result = expensive_operation(batch)
            return {"result": result, "status": "success"}
            
        except ValueError as e:
            # Handle expected errors gracefully
            logging.warning(f"Data validation error: {e}")
            return {
                "result": [None] * len(batch["item"]),
                "status": "validation_error",
                "error": str(e)
            }
            
        except Exception as e:
            # Handle unexpected errors
            logging.error(f"Unexpected error in transform: {e}")
            return {
                "result": [None] * len(batch["item"]),
                "status": "error",
                "error": str(e)
            }
    
    # Use with error tolerance
    result = ds.map_batches(robust_transform)

**Partial Failure Handling**

.. testcode::

    def partial_failure_transform(batch):
        """Handle partial failures within a batch."""
        results = []
        errors = []
        
        for i, item in enumerate(batch["data"]):
            try:
                result = process_item(item)
                results.append(result)
                errors.append(None)
            except Exception as e:
                results.append(None)
                errors.append(str(e))
                logging.warning(f"Failed to process item {i}: {e}")
        
        return {
            "results": results,
            "errors": errors,
            "success_rate": sum(1 for r in results if r is not None) / len(results)
        }
    
    ds.map_batches(partial_failure_transform)

Performance Monitoring and Debugging
===================================

Transform Performance Profiling
------------------------------

Profile your transformations to identify bottlenecks:

.. testcode::

    import ray
    import time
    import cProfile
    import pstats
    
    def profile_transform(transform_func):
        """Profile a transformation function."""
        
        def profiled_transform(batch):
            profiler = cProfile.Profile()
            profiler.enable()
            
            start_time = time.time()
            result = transform_func(batch)
            end_time = time.time()
            
            profiler.disable()
            
            # Print profiling results
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            stats.print_stats(10)  # Top 10 functions
            
            print(f"Transform time: {end_time - start_time:.4f}s")
            print(f"Batch size: {len(batch.get('item', batch.get(list(batch.keys())[0])))}")
            
            return result
        
        return profiled_transform
    
    # Usage
    profiled_func = profile_transform(my_transform)
    ds.map_batches(profiled_func)

**Memory Usage Monitoring**

.. testcode::

    import psutil
    import os
    
    def memory_monitoring_transform(transform_func):
        """Monitor memory usage during transformation."""
        
        def monitored_transform(batch):
            process = psutil.Process(os.getpid())
            
            # Memory before
            memory_before = process.memory_info().rss / (1024**2)  # MB
            
            # Execute transformation
            result = transform_func(batch)
            
            # Memory after
            memory_after = process.memory_info().rss / (1024**2)  # MB
            memory_diff = memory_after - memory_before
            
            if memory_diff > 100:  # Alert if using > 100MB
                print(f"High memory usage: {memory_diff:.1f}MB increase")
            
            return result
        
        return monitored_transform
    
    # Usage
    monitored_func = memory_monitoring_transform(my_transform)
    ds.map_batches(monitored_func)

Best Practices Summary
=====================

**Transformation Choice**
1. Use :meth:`~ray.data.Dataset.map_batches` for most operations
2. Avoid pandas batch_format unless necessary
3. Choose appropriate batch sizes for your workload
4. Use actors for stateful operations

**Performance Optimization**
1. Leverage vectorization (NumPy, PyArrow)
2. Tune concurrency based on resource requirements
3. Configure appropriate memory limits
4. Use GPU acceleration for suitable workloads

**Error Handling**
1. Implement robust error handling
2. Handle partial failures gracefully
3. Log errors for debugging
4. Use appropriate retry strategies

**Monitoring**
1. Profile transformation performance
2. Monitor memory usage
3. Track success rates
4. Set up alerts for performance regressions

Next Steps
==========

Continue optimizing your Ray Data pipeline:

- **Optimize memory usage**: :ref:`memory_optimization`
- **Learn advanced operations**: :ref:`advanced_operations`
- **Explore patterns and antipatterns**: :ref:`patterns_antipatterns`
- **Debug performance issues**: :ref:`troubleshooting`

**Production Transform Optimization Checklist:**

Before deploying transform optimizations to production:

- **Test with production data volumes**: Ensure optimizations work at scale
- **Validate resource requirements**: Confirm cluster sizing meets optimized workload needs
- **Monitor error rates**: Ensure optimizations don't introduce reliability issues
- **Document configuration choices**: Record optimization decisions for team knowledge
- **Set up alerting**: Monitor for performance regressions in production
- **Plan rollback procedures**: Have a plan to revert optimizations if issues arise

**Real-World Transform Examples:**

**Traditional ETL Transforms:**
- Data type conversions, null value handling, data validation
- String cleaning, date parsing, format standardization
- Business logic application, derived column creation

**ML/AI Transforms:**
- Feature scaling, encoding, dimensionality reduction
- Image resizing, normalization, augmentation
- Text tokenization, embedding generation, preprocessing

**Emerging/Future Transforms:**
- Real-time feature computation, streaming aggregations
- Multi-modal data fusion, cross-format transformations

**Data Format Migration for Performance:**

When migrating between data formats for optimization:

**CSV to Parquet Migration:**
- Significant performance improvement for analytical workloads
- Enables column pruning and predicate pushdown
- Reduces storage size through compression
- Improves query performance for wide tables

**JSON to Structured Formats:**
- Convert semi-structured JSON to Parquet for better performance
- Use schema inference to determine optimal structure
- Implement data validation during conversion
- Consider schema evolution strategies

**Legacy Format Modernization:**
- Assess current format performance characteristics
- Plan migration strategy with minimal disruption
- Test performance improvements with representative workloads
- Implement gradual migration with fallback options

**Performance Optimization by Transform Type:**

**Data Cleaning Transforms:**
- Use vectorized operations for type conversions
- Implement efficient null value handling
- Optimize string processing operations
- Use parallel validation strategies

**Feature Engineering Transforms:**
- Optimize mathematical operations with NumPy
- Use efficient encoding strategies
- Implement scalable normalization techniques
- Optimize aggregation operations

**Data Enrichment Transforms:**
- Optimize lookup operations with broadcast patterns
- Use efficient join strategies
- Implement caching for reference data
- Optimize for data freshness requirements

**Multi-Modal Processing:**
- Balance CPU and GPU operations
- Optimize data format conversions
- Implement efficient serialization
- Use appropriate batch sizes for mixed data types

**GPU Optimization Strategies:**

**GPU Memory Management:**
- Configure GPU memory pools for efficient allocation
- Use memory mapping for large datasets
- Implement GPU memory monitoring and alerting
- Optimize for GPU memory bandwidth

**GPU Processing Patterns:**
- Use appropriate batch sizes for GPU operations
- Implement efficient CPU-GPU data transfer
- Configure GPU task scheduling and load balancing
- Optimize for GPU kernel execution efficiency

**Mixed CPU/GPU Workloads:**
- Balance CPU preprocessing with GPU processing
- Use efficient data pipelines between CPU and GPU
- Implement resource allocation strategies
- Configure for heterogeneous cluster environments

**GPU Acceleration for Different Data Types:**

**Image Processing:**
- Use GPU-accelerated image transformations
- Implement efficient image loading and preprocessing
- Configure memory management for large images
- Optimize batch sizes for GPU memory constraints

**Text Processing:**
- Use GPU-accelerated tokenization and embedding
- Implement efficient text preprocessing pipelines
- Configure memory management for large language models
- Optimize batch sizes for transformer models

**Numerical Computing:**
- Use GPU-accelerated mathematical operations
- Implement efficient numerical algorithms
- Configure memory management for large matrices
- Optimize for scientific computing workloads

**Performance Optimization Across Ray Data Versions:**

**Version Compatibility:**
- Understand performance characteristics across Ray versions
- Plan optimization strategies for version upgrades
- Test performance impact of version changes
- Implement version-aware optimization strategies

**Feature Evolution:**
- Track new optimization features in Ray Data releases
- Adopt new performance optimizations as they become available
- Maintain compatibility with existing optimization strategies
- Plan migration strategies for deprecated features

**See also:**
- :ref:`reading_optimization` - Optimize data loading for better transform performance
- :ref:`data_key_concepts` - Understanding blocks, tasks, and actors
- :ref:`streaming-execution` - How Ray Data's streaming model affects transforms
- :ref:`execution-configurations` - Advanced execution configuration options
