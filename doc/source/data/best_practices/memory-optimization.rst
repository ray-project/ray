.. _memory-optimization:

Memory Optimization: Blocks, Backpressure & Configuration
=========================================================

**Keywords:** Ray Data memory optimization, block size configuration, backpressure management, object store optimization, streaming execution

Master Ray Data's memory management to process datasets of any size efficiently, prevent out-of-memory errors, and optimize resource utilization through proper block sizing and backpressure configuration.

**What you'll learn:**

* Block size configuration for optimal memory usage
* Backpressure prevention through proper resource allocation
* Object store optimization and spilling prevention
* Memory troubleshooting and optimization techniques

Understanding Ray Data Memory Management
----------------------------------------

**Ray Data uses a sophisticated memory management system with two primary components:**

**1. Worker Heap Memory**
Used for processing data within tasks and storing intermediate results during computation.

**2. Object Store Memory**  
Used for storing blocks between operations in Ray's distributed object store.

**Memory Usage Formula:**
```
Expected Heap Memory = num_execution_slots × max_block_size
```

Where:
- **num_execution_slots**: Usually equals number of CPUs (unless custom resources specified)
- **max_block_size**: `DataContext.target_max_block_size` (default 128 MiB)

.. code-block:: python

    from ray.data.context import DataContext
    import ray

    # Check current memory configuration
    ctx = DataContext.get_current()
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    
    expected_memory_mb = (cluster_cpus * ctx.target_max_block_size) / (1024 * 1024)
    print(f"Expected heap memory usage: {expected_memory_mb:.1f} MB")
    print(f"Max block size: {ctx.target_max_block_size / (1024*1024):.1f} MB")

Block Size Configuration
------------------------

**Block size is the most critical factor for Ray Data performance and memory usage.**

**Default Block Sizes and When to Change Them**

:::list-table
   :header-rows: 1

- - **Workload Type**
  - **Recommended Block Size**
  - **Configuration**
  - **Reasoning**
- - **ETL Processing**
  - 128-256 MB
  - ``target_max_block_size = 256*1024*1024``
  - Balance parallelism and memory efficiency
- - **Image Processing**
  - 64-128 MB
  - ``target_max_block_size = 128*1024*1024``
  - Optimize for GPU memory constraints
- - **Large Analytics**
  - 256-512 MB
  - ``target_max_block_size = 512*1024*1024``
  - Maximize vectorized operation efficiency
- - **Memory Constrained**
  - 32-64 MB
  - ``target_max_block_size = 64*1024*1024``
  - Prevent OOM in limited memory environments
- - **Shuffle Heavy**
  - 512 MB-1 GB
  - ``target_shuffle_max_block_size = 1024*1024*1024``
  - Optimize for shuffle performance

:::

**Configuring Block Sizes for Different Workloads:**

.. code-block:: python

    from ray.data.context import DataContext

    def configure_for_workload(workload_type):
        """Configure optimal block sizes based on workload characteristics."""
        ctx = DataContext.get_current()
        
        if workload_type == "etl_processing":
            # ETL workloads benefit from balanced block sizes
            ctx.target_max_block_size = 256 * 1024 * 1024  # 256 MB
            ctx.target_min_block_size = 64 * 1024 * 1024   # 64 MB
            return "ETL optimized: 256MB max, 64MB min"
            
        elif workload_type == "image_processing":
            # Image processing needs GPU memory optimization
            ctx.target_max_block_size = 128 * 1024 * 1024  # 128 MB
            ctx.target_min_block_size = 32 * 1024 * 1024   # 32 MB
            return "Image optimized: 128MB max, 32MB min"
            
        elif workload_type == "large_analytics":
            # Analytics benefits from larger blocks for vectorization
            ctx.target_max_block_size = 512 * 1024 * 1024  # 512 MB
            ctx.target_min_block_size = 128 * 1024 * 1024  # 128 MB
            return "Analytics optimized: 512MB max, 128MB min"
            
        elif workload_type == "memory_constrained":
            # Memory-constrained environments need smaller blocks
            ctx.target_max_block_size = 64 * 1024 * 1024   # 64 MB
            ctx.target_min_block_size = 16 * 1024 * 1024   # 16 MB
            return "Memory optimized: 64MB max, 16MB min"

Avoiding Backpressure Through Proper Configuration
--------------------------------------------------

**Backpressure occurs when downstream operators can't keep up with upstream data production, causing pipeline stalls and performance degradation.**

**Understanding Backpressure Mechanisms**

Ray Data implements two backpressure policies:

1. **ResourceBudgetBackpressurePolicy**: Prevents resource exhaustion
2. **ConcurrencyCapBackpressurePolicy**: Limits concurrent tasks

**Configuring Resources to Prevent Backpressure:**

.. code-block:: python

    # Balanced resource allocation to prevent backpressure
    def configure_balanced_pipeline():
        """Configure pipeline to prevent backpressure through balanced resources."""
        
        # Load data with appropriate parallelization
        ds = ray.data.read_parquet(
            "s3://large-data/",
            override_num_blocks=32  # Match downstream processing capacity
        )
        
        # Configure transforms with balanced resource allocation
        processed = ds.map_batches(
            cpu_intensive_transform,
            concurrency=8,    # Limit concurrency to prevent resource exhaustion
            num_cpus=2,       # Allocate sufficient CPU per task
            batch_size=1000   # Balance memory usage and throughput
        ).map_batches(
            gpu_intensive_transform,
            concurrency=4,    # Fewer GPU tasks due to resource constraints
            num_gpus=1,       # One GPU per task
            batch_size=32     # Smaller batches for GPU memory
        )
        
        return processed

**Preventing CPU Backpressure:**

.. code-block:: python

    # Problem: CPU-intensive operations causing backpressure
    def prevent_cpu_backpressure():
        """Prevent CPU backpressure through proper resource allocation."""
        
        # Check available CPU resources
        available_cpus = int(ray.cluster_resources()["CPU"])
        
        # Configure CPU-intensive operations
        cpu_optimized = ds.map_batches(
            cpu_heavy_function,
            num_cpus=2,                           # Allocate sufficient CPU
            concurrency=available_cpus // 2,      # Don't oversubscribe CPUs
            batch_size=500                        # Balance memory and CPU usage
        )
        
        return cpu_optimized

**Preventing GPU Backpressure:**

.. code-block:: python

    # Problem: GPU operations causing memory pressure and stalls
    def prevent_gpu_backpressure():
        """Prevent GPU backpressure through memory-aware configuration."""
        
        # Check available GPU resources and memory
        available_gpus = int(ray.cluster_resources().get("GPU", 0))
        
        # Configure GPU operations with memory awareness
        gpu_optimized = ds.map_batches(
            gpu_intensive_function,
            num_gpus=1,                          # One GPU per actor
            concurrency=available_gpus,          # Match available GPUs
            batch_size=32,                       # Conservative for GPU memory
            memory=4*1024*1024*1024             # Reserve 4GB heap per actor
        )
        
        return gpu_optimized

**Stage Fusion Optimization**

**Ray Data automatically fuses compatible operations for better performance. Configure your pipeline to encourage fusion:**

**Fusion-Friendly Patterns:**

.. code-block:: python

    # Fusion-friendly: Compatible operations that can be combined
    fused_pipeline = ds \
        .map(simple_transform) \
        .filter(lambda row: row["value"] > 0) \
        .map(another_simple_transform)  # These operations can be fused
    
    # Check execution plan to see fusion
    print(fused_pipeline.execution_plan())

**Preventing Fusion Breaks:**

.. code-block:: python

    # Fusion-breaking: Operations that prevent fusion
    broken_fusion = ds \
        .map_batches(transform_1) \
        .repartition(10) \    # Repartition breaks fusion
        .map_batches(transform_2)  # Separate execution stage
    
    # Fusion-optimized: Combine operations when possible
    optimized_fusion = ds.map_batches(
        lambda batch: transform_2(transform_1(batch)),  # Combined in single stage
        batch_size=1000
    )

**Encouraging Stage Fusion Through Block Count Matching:**

.. code-block:: python

    # Match block counts between operations to encourage fusion
    def fusion_optimized_pipeline():
        """Configure pipeline for optimal stage fusion."""
        
        # Read with specific block count
        ds = ray.data.read_parquet(
            "s3://data/",
            override_num_blocks=16  # Explicit block count
        )
        
        # Transform without changing block structure
        transformed = ds \
            .map(row_transform) \
            .filter(lambda row: row["valid"]) \
            .map(final_transform)  # All operations can be fused
        
        # Avoid operations that break fusion unnecessarily
        # DON'T: .repartition(), .random_shuffle(), .sort() unless required
        
        return transformed

**Advanced Backpressure Configuration**

**Fine-tune backpressure policies for specific workloads:**

.. code-block:: python

    from ray.data.context import DataContext

    def configure_backpressure_policies():
        """Configure backpressure policies for optimal performance."""
        ctx = DataContext.get_current()
        
        # Configure resource limits to prevent backpressure
        ctx.execution_options.resource_limits.cpu = 80  # Reserve 20% CPU
        ctx.execution_options.resource_limits.object_store_memory = 8e9  # 8GB limit
        
        # Configure for locality optimization
        ctx.execution_options.locality_with_output = True  # For ML training
        
        # Enable deterministic execution if needed (may reduce performance)
        ctx.execution_options.preserve_order = False  # Default: optimize for speed
        
        return "Backpressure policies configured"

**Memory Optimization Troubleshooting Guide**

**Issue: Pipeline Stalls Due to Backpressure**

.. code-block:: python

    # Diagnosis: Check for backpressure indicators
    def diagnose_backpressure():
        """Diagnose backpressure issues in your pipeline."""
        
        # Run pipeline and check execution stats
        result = ds.map_batches(your_transform).materialize()
        stats = result.stats()
        
        # Look for backpressure indicators
        if "backpressure" in str(stats).lower():
            print("Backpressure detected!")
            print("Solutions:")
            print("1. Reduce batch_size")
            print("2. Increase concurrency")
            print("3. Allocate more memory per task")
            print("4. Configure smaller block sizes")

**Memory Optimization Best Practices**

**Essential Configuration Checklist:**
- [ ] **Block sizes**: Configured appropriately for workload (64MB-512MB range)
- [ ] **Batch sizes**: Sized to prevent OOM while maintaining efficiency
- [ ] **Resource allocation**: CPU/GPU/memory allocated to prevent contention
- [ ] **Streaming execution**: Used for datasets larger than available memory
- [ ] **Object store monitoring**: Spilling prevention and capacity management

**Advanced Optimization Checklist:**
- [ ] **Backpressure policies**: Configured to prevent pipeline stalls
- [ ] **Stage fusion**: Pipeline designed to encourage automatic fusion
- [ ] **Memory monitoring**: Continuous monitoring of memory usage patterns
- [ ] **Resource limits**: Appropriate limits set to prevent resource exhaustion
- [ ] **Locality optimization**: Configured for specific use cases (ML training, etc.)

Next Steps
----------

**Master Memory Optimization:**
- **Apply techniques**: Use memory optimization in your Ray Data workloads
- **Monitor performance**: Track memory usage and optimization effectiveness
- **Advanced techniques**: Explore :ref:`Advanced Topics <advanced>` for deeper insights
- **Production deployment**: Apply memory optimization to :ref:`Production Deployment <production-deployment>`

