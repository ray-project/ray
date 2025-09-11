.. _reading_optimization:

=======================================
Ray Data Reading Performance Optimization
=======================================

Reading data efficiently is crucial for Ray Data performance. This guide covers comprehensive strategies to optimize data ingestion, from file format selection to advanced I/O tuning.

**How Ray Data Reading Works (Beginner Explanation):**

When Ray Data reads your data files, it doesn't just load everything into memory like pandas would. Instead, it:

1. **Splits your data** into chunks called "blocks" (think of cutting a large document into pages)
2. **Distributes the blocks** across multiple computers in your cluster
3. **Processes blocks in parallel** using separate "tasks" (independent workers)
4. **Stores results** in a shared memory space called the "object store"

This approach allows Ray Data to process datasets much larger than what would fit in a single computer's memory, supporting traditional ETL workloads, AI/ML data pipelines, and emerging real-time processing patterns.

**Key Concepts for Data Scientists and Engineers:**
- **Blocks**: Ray Data's equivalent of Spark partitions or Dask chunks - logical divisions of your dataset that work for any data type (structured tables, images, text, time series)
- **Tasks**: Stateless workers similar to Spark tasks - handle traditional ETL operations, ML feature engineering, and GPU-accelerated processing
- **Object Store**: Ray's distributed shared memory system, optimized for mixed workloads from traditional analytics to AI inference
- **Parallelism**: Concurrent processing across cluster nodes that scales from traditional batch jobs to real-time ML inference

Understanding this architecture helps you optimize reading performance by tuning how the data is split (block sizes), how many workers you use (parallelism), and how resources are allocated.

Quick Reading Optimization Checklist
====================================

Before diving into details, use this checklist to identify immediate optimization opportunities:

**File Organization**
- Consolidate small files (< 10MB) into larger files
- Use efficient formats (Parquet > CSV > JSON)
- Implement logical partitioning strategy
- Apply appropriate compression (snappy, gzip, lz4)

**Ray Data Configuration**
- Use column pruning with ``columns`` parameter
- Tune ``override_num_blocks`` for your cluster size
- Configure ``ray_remote_args`` for I/O-intensive reads
- Enable filter pushdown when possible

**Resource Optimization**
- Match cluster resources to data size
- Use appropriate node types for I/O workloads
- Configure network settings for cloud storage

File Format Optimization
========================

Choosing the Right Format
-------------------------

Different file formats have significant performance implications due to their underlying storage mechanisms, compression capabilities, and metadata support.

**Why File Format Matters:**

File format choice affects every aspect of your Ray Data pipeline:

- **Read Performance**: Columnar formats like Parquet can skip unnecessary data
- **Storage Efficiency**: Compression and encoding reduce storage costs and transfer time
- **Query Capabilities**: Metadata enables predicate pushdown and schema evolution
- **Compatibility**: Some formats work better with specific processing libraries

**Format Characteristics Across Workload Types:**

- **Parquet**: Columnar format excellent for traditional analytics, ML feature stores, and structured AI data
- **CSV**: Row-based format suitable for simple ETL, data migration, and legacy system integration
- **JSON**: Flexible schema format for semi-structured data, API responses, and evolving AI data schemas
- **Arrow**: In-memory format optimized for high-performance analytics, ML training pipelines, and real-time processing
- **ORC**: Hadoop-ecosystem format for traditional big data workloads and data warehouse integration

.. list-table:: File Format Performance Comparison
   :header-rows: 1
   :class: format-comparison-table

   * - Format
     - Read Speed
     - Storage Size
     - Column Access
     - Best Use Case
   * - **Parquet**
     - Excellent
     - Excellent
     - Excellent
     - Analytics, ML training
   * - **CSV**
     - Fair
     - Fair
     - Poor
     - Simple data, compatibility
   * - **JSON**
     - Poor
     - Poor
     - Fair
     - Semi-structured data
   * - **Arrow/Feather**
     - Excellent
     - Good
     - Excellent
     - In-memory caching
   * - **ORC**
     - Good
     - Excellent
     - Good
     - Hadoop ecosystem

**Recommendation**: Use Parquet for most Ray Data workloads due to its excellent compression, columnar access, and metadata support.

**Real-World Format Selection Examples:**

**Traditional ETL Scenarios:**
- **Data warehouse ETL**: Parquet for dimensional tables, CSV for legacy system exports
- **Log processing**: JSON for application logs, Parquet for processed/aggregated logs
- **Data migration**: CSV for simple transfers, Parquet for ongoing analytics

**ML/AI Scenarios:**
- **Feature stores**: Parquet for structured features, Arrow for real-time serving
- **Training data**: Parquet for tabular data, binary formats for images/audio
- **Model artifacts**: Arrow for inference data, JSON for metadata

**Future/Emerging Scenarios:**
- **Real-time AI**: Arrow for low-latency processing, Delta Lake for streaming updates
- **Multi-modal AI**: Mixed formats optimized for different data types within same pipeline

Parquet Optimization Deep Dive
------------------------------

Parquet is the recommended format for Ray Data. Here's how to optimize Parquet reading:

**Column Pruning**

.. testcode::

    import ray
    
    # INEFFICIENT Reads all columns - inefficient
    ds = ray.data.read_parquet("s3://bucket/data/")
    
    # EFFICIENT Only reads needed columns - much faster
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        columns=["user_id", "timestamp", "value"]
    )

**Schema Enforcement**

.. testcode::

    import pyarrow as pa
    
    # Define consistent schema for better performance
    schema = pa.schema([
        pa.field("user_id", pa.int64()),
        pa.field("timestamp", pa.timestamp("s")),
        pa.field("value", pa.float64()),
    ])
    
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        schema=schema  # Ensures consistent data types
    )

**How Schema Enforcement Works:**

When you specify a schema, Ray Data validates and converts incoming data to match the expected types. This provides several benefits:

- **Type consistency**: Ensures all data matches expected types across files
- **Performance optimization**: Eliminates runtime type inference overhead
- **Error detection**: Catches data quality issues early in the pipeline
- **Memory efficiency**: Optimal memory layout for specified data types

**Filter Pushdown**

.. testcode::

    import pyarrow.compute as pc
    
    # Push filters down to file level for efficiency
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        filter=pc.greater(pc.field("value"), 100)  # Only reads relevant row groups
    )

**Partition Filtering**

.. testcode::

    # For Hive-partitioned datasets
    def partition_filter(paths):
        # Only read partitions from the last 7 days
        return any("date=2024-01" in path for path in paths)
    
    ds = ray.data.read_parquet(
        "s3://bucket/partitioned-data/",
        partition_filter=partition_filter
    )

Block Size Optimization
=======================

Understanding Block Sizing
--------------------------

:ref:`Blocks <data_key_concepts>` are the fundamental units Ray Data uses to partition and process your data. Each block contains a subset of rows and is processed by a separate :ref:`Ray task <core-key-concepts>`. 

**The Block Size Trade-off:**

Block sizing involves balancing two competing factors:
- **Parallelism**: More, smaller blocks enable better parallel processing across your cluster
- **Efficiency**: Fewer, larger blocks reduce task scheduling and data transfer overhead

**Block Size Guidelines:**

- **Small blocks (1-10MB)**: Maximize parallelism but increase overhead - good for clusters with many CPUs
- **Medium blocks (10-128MB)**: Balanced approach that works well for most workloads - Ray Data's default
- **Large blocks (128MB-1GB)**: Minimize overhead but reduce parallelism - good for memory-rich, CPU-limited environments

**Calculating Optimal Block Count**

The optimal number of blocks depends on your data size, cluster resources, and workload characteristics. Block count optimization is often the single most impactful reading performance optimization because it affects parallelism, memory usage, and task scheduling efficiency.

**Why Block Count Matters:**

Block count directly determines how Ray Data distributes work across your cluster:

- **Too few blocks**: Limits parallelism, some CPUs sit idle, larger memory footprint per task
- **Too many blocks**: Creates scheduling overhead, task startup costs dominate actual work
- **Optimal count**: Fully utilizes cluster resources while minimizing overhead

Here's a systematic approach to determine the right block count based on proven heuristics from Ray Data usage patterns:

**Step 1: Start with CPU-based calculation**

The general rule is to use 2-4x your cluster's CPU count to ensure good parallelization:

.. testcode::

    import ray
    
    # Get cluster CPU count
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    base_blocks = cluster_cpus * 3  # 3x CPU count as starting point

**Step 2: Adjust for data size characteristics**

Small and large datasets need different approaches:

**Block Count Decision Guide**

Choose your approach based on dataset size characteristics:

.. list-table:: Block Count by Dataset Size
   :header-rows: 1
   :class: block-size-guide

   * - Dataset Size
     - Block Count Formula
     - Example (8 CPUs)
     - Reasoning
   * - **Small (< 1GB)**
     - CPU Count ÷ 4
     - 8 ÷ 4 = 2 blocks
     - Avoid overhead from too many tasks
   * - **Medium (1-100GB)**
     - CPU Count × 3
     - 8 × 3 = 24 blocks
     - Good balance of parallelism and efficiency
   * - **Large (> 100GB)**
     - CPU Count × 6
     - 8 × 6 = 48 blocks
     - Maximize parallelism for large data

**Apply the Appropriate Configuration:**

.. testcode::

    # Example: Medium dataset (50GB) with 8 CPUs
    data_size_gb = 50
    cluster_cpus = int(ray.cluster_resources()["CPU"])
    
    # Based on table above: Medium dataset = CPU Count × 3
    optimal_blocks = cluster_cpus * 3  # 24 blocks for 8 CPUs

**Step 3: Apply the calculation**

Use the calculated block count when reading your data:

.. testcode::

    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        override_num_blocks=optimal_blocks
    )

**Dynamic Block Sizing**

.. testcode::

    import ray
    
    # Adaptive block sizing based on file characteristics
    def adaptive_read_parquet(path, target_block_size_mb=64):
        """Dynamically determine optimal block count."""
        
        # Get basic dataset info (this is a simplified example)
        sample_ds = ray.data.read_parquet(path, override_num_blocks=1)
        sample_stats = sample_ds.stats()
        
        # Estimate total size and calculate blocks
        estimated_size_mb = sample_stats.total_bytes / (1024 * 1024)
        optimal_blocks = max(int(estimated_size_mb / target_block_size_mb), 1)
        
        # Read with optimized block count
        return ray.data.read_parquet(path, override_num_blocks=optimal_blocks)
    
    ds = adaptive_read_parquet("s3://bucket/data/", target_block_size_mb=128)

Resource Configuration
=====================

CPU and Memory Allocation
-------------------------

Configure Ray Data reading tasks for optimal resource usage:

**Basic Resource Configuration**

.. testcode::

    # For I/O-intensive reading (more tasks, less CPU per task)
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        ray_remote_args={"num_cpus": 0.5}  # Allow 2 read tasks per CPU
    )
    
    # For CPU-intensive reading (fewer tasks, more CPU per task)
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        ray_remote_args={"num_cpus": 2}  # Use 2 CPUs per read task
    )

**Memory-Optimized Reading**

.. testcode::

    # For large files that might cause OOM
    ds = ray.data.read_parquet(
        "s3://bucket/large-files/",
        ray_remote_args={
            "num_cpus": 1,
            "memory": 4 * 1024**3  # 4GB memory per task
        }
    )

**GPU Resource Allocation**

.. testcode::

    # For GPU-accelerated reading (e.g., with cuDF)
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        ray_remote_args={
            "num_cpus": 1,
            "num_gpus": 0.25  # Share GPU across 4 read tasks
        }
    )

Network and I/O Optimization
============================

Cloud Storage Optimization
--------------------------

**S3 Optimization**

.. testcode::

    import ray
    
    # Configure S3 settings for better performance
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        # Use S3 transfer acceleration if available
        filesystem=pa.fs.S3FileSystem(
            region="us-west-2",  # Same region as your cluster
            request_timeout=60,   # Longer timeout for large files
            connect_timeout=10
        )
    )

**Connection Pooling**

.. testcode::

    import pyarrow as pa
    
    # Reuse connections for better performance
    fs = pa.fs.S3FileSystem(
        region="us-west-2",
        # Connection pooling settings
        background_writes=True,
        default_metadata={"connection_pool_size": "10"}
    )
    
    ds = ray.data.read_parquet("s3://bucket/data/", filesystem=fs)

**Parallel Transfer Configuration**

.. testcode::

    # Optimize for high-bandwidth networks
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        override_num_blocks=64,  # More parallelism for network I/O
        ray_remote_args={
            "num_cpus": 0.25,    # Allow many concurrent transfers
            "resources": {"network_bandwidth": 1}  # Custom resource
        }
    )

Error Handling and Reliability
==============================

Robust Reading Configuration
----------------------------

**Retry Configuration**

.. testcode::

    import ray
    
    # Configure retries for transient failures
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        ray_remote_args={
            "retry_exceptions": True,
            "max_retries": 3
        }
    )

**Handling Corrupted Files**

.. testcode::

    # Allow some files to fail without stopping the job
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        max_errored_blocks=5  # Allow up to 5 corrupted files
    )

**Timeout Configuration**

.. testcode::

    # Set appropriate timeouts for slow data sources
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        ray_remote_args={
            "runtime_env": {
                "env_vars": {
                    "RAY_task_retry_delay_ms": "5000",  # 5 second retry delay
                    "RAY_task_max_retries": "3"
                }
            }
        }
    )

Advanced Reading Patterns
=========================

Streaming Reads
---------------

For very large datasets, use streaming patterns to avoid memory issues:

.. testcode::

    # Streaming read pattern for massive datasets
    def streaming_read_large_dataset(path, chunk_size_gb=10):
        """Read large dataset in streaming chunks."""
        
        # Get list of files
        import pyarrow.fs as fs
        filesystem = fs.S3FileSystem()
        file_info = filesystem.get_file_info(fs.FileSelector(path))
        files = [f.path for f in file_info if f.type == fs.FileType.File]
        
        # Process in chunks
        chunk_size = chunk_size_gb * 1024**3  # Convert to bytes
        current_chunk = []
        current_size = 0
        
        for file_path in files:
            file_size = filesystem.get_file_info(file_path).size
            
            if current_size + file_size > chunk_size and current_chunk:
                # Process current chunk
                chunk_ds = ray.data.read_parquet(current_chunk)
                yield chunk_ds
                current_chunk = []
                current_size = 0
            
            current_chunk.append(file_path)
            current_size += file_size
        
        # Process final chunk
        if current_chunk:
            yield ray.data.read_parquet(current_chunk)
    
    # Usage
    for chunk_ds in streaming_read_large_dataset("s3://bucket/massive-data/"):
        # Process each chunk
        result = chunk_ds.map_batches(my_processing_function)
        result.write_parquet("s3://bucket/output/")

Cached Reading
-------------

For repeatedly accessed datasets, implement caching:

.. testcode::

    import ray
    from functools import lru_cache
    
    @lru_cache(maxsize=10)
    def cached_read_parquet(path, columns=None):
        """Cache frequently accessed datasets."""
        return ray.data.read_parquet(path, columns=columns).materialize()
    
    # First call reads from storage
    ds1 = cached_read_parquet("s3://bucket/reference-data/", columns=["id", "name"])
    
    # Subsequent calls use cached version
    ds2 = cached_read_parquet("s3://bucket/reference-data/", columns=["id", "name"])

Performance Monitoring
======================

Reading Performance Monitoring
-----------------------------

Use Ray Dashboard to monitor reading performance:

**Dashboard Reading Metrics:**

1. **Navigate to Ray Dashboard** → **Jobs tab** → Select your job
2. **Check Timeline**: See read operation duration and parallelization
3. **Monitor Metrics**: View I/O throughput and resource usage
4. **Watch for Alerts**: Spilling warnings or task failures

**How Ray Data Reading Performance is Measured:**

Ray Data automatically tracks reading performance through its built-in statistics system. When you run a reading operation, Ray Data records:

- **Task execution times**: How long each read task took
- **Data throughput**: Bytes per second read from storage
- **Parallelization efficiency**: How well work was distributed across tasks
- **Memory usage**: Peak memory consumption during reading

**Simple Performance Check:**

.. testcode::

    # Read data with optimizations
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        columns=["col1", "col2"],      # Column pruning
        override_num_blocks=32         # Explicit parallelism
    )
    
    # Ray Data automatically tracks performance
    result = ds.materialize()
    result.stats()  # Shows detailed performance breakdown

**What to Look for in Stats Output:**

- **Read operation time**: Should be reasonable for your data size
- **Number of tasks**: Should match your `override_num_blocks` setting
- **Block sizes**: Should be in the 10-128MB range for efficiency
- **No error messages**: All read tasks should complete successfully

Troubleshooting Reading Issues
=============================

Common Reading Problems
----------------------

**Problem: Slow reading performance**

.. code-block:: python

    # Diagnosis
    ds = ray.data.read_parquet("s3://bucket/data/")
    stats = ds.materialize().stats()
    
    # Check for these issues:
    # 1. Too many small blocks
    if stats.num_blocks > ray.cluster_resources()["CPU"] * 4:
        print("Too many blocks - reduce override_num_blocks")
    
    # 2. Blocks too large
    avg_block_size = stats.total_bytes / stats.num_blocks / (1024**2)
    if avg_block_size > 512:  # MB
        print("Blocks too large - increase override_num_blocks")
    
    # 3. Reading unnecessary columns
    print("Consider using column pruning if you don't need all columns")

**Problem: Out of memory during reading**

.. code-block:: python

    # Solution: Reduce memory pressure
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        override_num_blocks=128,  # More, smaller blocks
        ray_remote_args={"memory": 2 * 1024**3}  # Limit memory per task
    )

**Problem: Network timeouts**

.. code-block:: python

    # Solution: Configure retries and timeouts
    import pyarrow as pa
    
    filesystem = pa.fs.S3FileSystem(
        connect_timeout=30,
        request_timeout=300  # 5 minute timeout
    )
    
    ds = ray.data.read_parquet(
        "s3://bucket/data/",
        filesystem=filesystem,
        ray_remote_args={"max_retries": 5}
    )

Best Practices Summary
=====================

**File Organization**
1. Use Parquet format for structured data
2. Consolidate small files (aim for 64-256MB files)
3. Implement logical partitioning
4. Use appropriate compression (snappy for speed, gzip for size)

**Ray Data Configuration**
1. Always use column pruning when possible
2. Tune ``override_num_blocks`` to 2-4x your CPU count
3. Configure ``ray_remote_args`` for I/O characteristics
4. Enable filter pushdown for large datasets

**Performance Monitoring**
1. Always measure before and after optimizations
2. Monitor throughput, memory usage, and resource utilization
3. Use Ray Dashboard for real-time monitoring
4. Set up alerts for performance regressions

**Error Handling**
1. Configure appropriate retries for network operations
2. Set reasonable timeouts for slow data sources
3. Use ``max_errored_blocks`` for datasets with some corrupted files
4. Implement proper logging for debugging

Next Steps
==========

**Reading Optimization by Workload Type:**

**Traditional ETL Workloads:**
- Prioritize Parquet format for structured data processing
- Use column pruning aggressively for wide tables
- Configure larger block sizes for batch efficiency
- Optimize for sequential access patterns

**ML/AI Workloads:**
- Balance file formats for mixed data types (structured, images, text)
- Optimize block sizes for GPU memory constraints
- Use streaming reads for large training datasets
- Configure for mixed CPU/GPU access patterns

**Real-time Processing:**
- Minimize file count and optimize for low latency
- Use Arrow format for zero-copy operations
- Configure smaller blocks for faster processing start
- Optimize for random access patterns

**Performance Optimization by Storage System:**

**Cloud Storage (S3, GCS, Azure Blob):**
- Configure connection pooling and retry strategies
- Optimize for network bandwidth limitations
- Use region-aware data placement
- Implement intelligent prefetching

**Distributed File Systems (HDFS, Lustre):**
- Optimize for block size alignment
- Configure for network topology awareness
- Use data locality optimization
- Implement load balancing strategies

**Local Storage (NVMe, SSD, HDD):**
- Optimize for sequential vs random access
- Configure for storage bandwidth limits
- Use parallel I/O strategies
- Implement efficient caching

**Advanced Compression Strategies:**

**Compression Algorithm Selection:**
- **Snappy**: Fast compression/decompression, moderate compression ratio
- **LZ4**: Extremely fast, good for real-time processing
- **GZIP**: High compression ratio, good for storage optimization
- **ZSTD**: Balanced compression ratio and speed, good general purpose
- **Brotli**: High compression ratio, good for network transfer

**Compression by Data Type:**

**Structured Data:**
- Use dictionary encoding for categorical columns
- Apply run-length encoding for repetitive data
- Use bit packing for boolean and small integer columns
- Implement delta encoding for time series data

**Text Data:**
- Use text-specific compression algorithms
- Implement dictionary compression for repeated terms
- Apply character encoding optimization
- Use compression for large text fields

**Binary Data:**
- Use format-specific compression (JPEG for images, etc.)
- Implement lossless vs lossy compression trade-offs
- Apply binary data optimization techniques
- Use specialized compression for different binary formats

**Network Optimization Strategies:**

**Connection Management:**
- Use connection pooling for data source connections
- Implement connection reuse strategies
- Configure connection timeout and retry policies
- Monitor connection health and performance

**Bandwidth Optimization:**
- Use compression for network data transfer
- Implement efficient data serialization
- Configure for bandwidth-limited environments
- Monitor network utilization and optimize accordingly

**Latency Optimization:**
- Use data locality optimization
- Implement intelligent data placement
- Configure for low-latency access patterns
- Monitor and optimize for latency-sensitive workloads

**Security and Performance Trade-offs:**

**Encryption Performance Impact:**
- Understand encryption overhead on data processing
- Use efficient encryption algorithms
- Implement encryption at appropriate layers
- Monitor encryption impact on performance

**Access Control Performance:**
- Optimize authentication and authorization
- Use efficient permission checking strategies
- Implement caching for access control decisions
- Monitor access control impact on performance

Now that you've optimized your reading performance:

- **Continue to transforms**: :ref:`transform_optimization`
- **Learn memory optimization**: :ref:`memory_optimization`  
- **Explore advanced patterns**: :ref:`patterns_antipatterns`
- **Debug issues**: :ref:`troubleshooting`
