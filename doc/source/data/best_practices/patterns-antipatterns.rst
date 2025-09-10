.. _patterns-antipatterns:

Patterns & Anti-Patterns
=========================

This guide provides comprehensive best practices and common mistakes when using Ray Data. Learn proven patterns that maximize performance and avoid anti-patterns that lead to poor performance, memory issues, and production problems.

**What you'll learn:**

* Proven design patterns for different Ray Data workloads
* Common anti-patterns and how to avoid them
* Performance optimization patterns based on production experience
* Debugging patterns for troubleshooting issues

Design Patterns
---------------

**Pattern 1: Streaming Processing Pipeline**

**✅ GOOD: Use streaming execution for large datasets**

.. code-block:: python

    import ray

    # Load data lazily
    large_dataset = ray.data.read_parquet("s3://huge-dataset/")
    
    # Chain operations without materialization
    result = large_dataset \
        .filter(lambda row: row["valid"]) \
        .map_batches(transform_data, batch_size=256) \
        .groupby("category") \
        .aggregate(ray.data.aggregate.Sum("amount")) \
        .write_parquet("s3://output/")  # Stream directly to output
    
    # ✅ Never call .take_all() or .materialize() on large datasets

**❌ ANTI-PATTERN: Materializing large datasets**

.. code-block:: python

    import ray

    # Load data
    large_dataset = ray.data.read_parquet("s3://huge-dataset/")
    
    # ❌ This loads everything into memory
    all_data = large_dataset.take_all()
    
    # ❌ This also materializes the entire dataset
    materialized = large_dataset.materialize()

**Pattern 2: Efficient Batch Processing**

**✅ GOOD: Vectorized batch operations**

.. code-block:: python

    import pandas as pd
    import numpy as np

    def vectorized_transform(batch):
        """Efficient vectorized transformation."""
        # ✅ Use vectorized pandas/numpy operations
        batch["amount_squared"] = batch["amount"] ** 2
        batch["log_amount"] = np.log(batch["amount"] + 1)
        batch["category_encoded"] = pd.Categorical(batch["category"]).codes
        return batch

    # Apply vectorized transformation
    result = dataset.map_batches(
        vectorized_transform,
        batch_size=512,  # Large batches for vectorization efficiency
        num_cpus=2
    )

**❌ ANTI-PATTERN: Row-by-row processing**

.. code-block:: python

    import math

    def slow_row_transform(row):
        """Inefficient row-by-row processing."""
        # ❌ This processes one row at a time
        row["amount_squared"] = row["amount"] ** 2
        row["log_amount"] = math.log(row["amount"] + 1)
        return row

    # ❌ This is 10-100x slower than map_batches for vectorized ops
    result = dataset.map(slow_row_transform)

**Pattern 3: Resource-Aware Processing**

**✅ GOOD: Appropriate resource allocation**

.. code-block:: python

    # CPU-intensive operations
    cpu_result = dataset.map_batches(
        cpu_heavy_transform,
        num_cpus=4,      # Allocate multiple CPUs
        num_gpus=0,      # No GPU needed
        batch_size=1024  # Large batches for CPU efficiency
    )
        
        # GPU-intensive operations
        gpu_result = cpu_result.map_batches(
            gpu_transform,
            num_cpus=1,      # Minimal CPU for coordination
            num_gpus=1,      # Full GPU allocation
            compute=ray.data.ActorPoolStrategy(size=4),  # Actor pool
            batch_size=64    # Smaller batches for GPU memory
        )
        
        # Memory-intensive operations
        memory_result = gpu_result.map_batches(
            memory_heavy_transform,
            num_cpus=1,      # Single CPU to control memory
            batch_size=32,   # Small batches
            compute=ray.data.ActorPoolStrategy(size=2)  # Fewer actors
        )
        
        return memory_result

**❌ ANTI-PATTERN: Inefficient resource allocation**

.. code-block:: python

    # ❌ Over-allocating CPUs for GPU work
    result = dataset.map_batches(
        gpu_function,
        num_cpus=8,   # Too many CPUs
        num_gpus=0.5  # Fractional GPU is inefficient
    )
    
    # ❌ Under-allocating for CPU-intensive work
    result = dataset.map_batches(
        cpu_intensive_function,
        num_cpus=0.5,    # Not enough CPU
        batch_size=32    # Too small for CPU vectorization
    )

**Pattern 4: Memory-Efficient Processing**

**✅ GOOD: Memory-conscious operations**

.. code-block:: python

    from ray.data.context import DataContext

    # Configure for memory efficiency
    ctx = DataContext.get_current()
    ctx.target_max_block_size = 64 * 1024 * 1024  # 64MB blocks
    ctx.eager_free = True  # Immediate cleanup
    ctx.max_errored_blocks = 5  # Allow some failures

    def memory_efficient_transform(batch):
        """Transform that minimizes memory usage."""
        # ✅ Process in chunks if batch is large
        if len(batch) > 1000:
            results = []
            for chunk in [batch[i:i+500] for i in range(0, len(batch), 500)]:
                results.append(process_chunk(chunk))
            return pd.concat(results, ignore_index=True)
        else:
            return process_chunk(batch)

    # Apply memory-efficient transformation
    result = dataset.map_batches(
        memory_efficient_transform,
        batch_size=256,  # Moderate batch size
        compute=ray.data.ActorPoolStrategy(size=4)
    )

**❌ ANTI-PATTERN: Memory-intensive operations**

.. code-block:: python

    def memory_hungry_transform(batch):
        """Inefficient memory usage."""
        # ❌ Creating large intermediate objects
        large_intermediate = batch.copy()
        another_copy = large_intermediate.copy()
        yet_another_copy = another_copy.copy()
        
        # ❌ Not cleaning up intermediate results
        result = process_data(yet_another_copy)
        # Missing: del large_intermediate, another_copy, yet_another_copy
        return result

    # ❌ Using huge batch sizes with memory-intensive operations
    result = dataset.map_batches(
        memory_hungry_transform,
        batch_size=10000  # Too large for memory-intensive ops
    )

**Pattern 5: Efficient Data Access**

**✅ GOOD: Optimized data loading**

.. code-block:: python

    def efficient_data_access_pattern():
        """Optimize data loading for performance."""
        
        # Use predicate pushdown and column pruning
        optimized_read = ray.data.read_parquet(
            "s3://data-lake/sales/",
            filter=pa.compute.and_(
                pa.compute.greater_equal(pa.compute.field("date"), pa.scalar("2024-01-01")),
                pa.compute.greater(pa.compute.field("amount"), pa.scalar(100))
            ),
            columns=["customer_id", "amount", "date", "product_id"]  # Only needed columns
        )
        
        # Configure for cloud storage optimization
        ctx = DataContext.get_current()
        ctx.streaming_read_buffer_size = 64 * 1024 * 1024  # 64MB buffer
        
        # Use appropriate parallelism
        return ray.data.read_csv(
            "s3://logs/*.csv",
            override_num_blocks=32,  # Distribute across 32 tasks
            ray_remote_args={"num_cpus": 0.5}  # Allow higher I/O parallelism
        )

**❌ ANTI-PATTERN: Inefficient data access**

.. code-block:: python

    def inefficient_data_access_antipattern():
        """DON'T DO THIS - slow and wasteful data access."""
        
        # ❌ Reading all columns when only few are needed
        all_data = ray.data.read_parquet("s3://data-lake/sales/")  # Reads all columns
        needed_columns = all_data.select_columns(["customer_id", "amount"])  # Too late
        
        # ❌ No predicate pushdown
        all_data = ray.data.read_parquet("s3://data-lake/sales/")
        filtered_data = all_data.filter(lambda row: row["amount"] > 100)  # Filters after loading
        
        # ❌ Inefficient parallelism
        return ray.data.read_csv(
            "s3://small-files/*.csv",
            override_num_blocks=1000  # Too many tasks for small files
        )

**Pattern 6: Actor Pool Optimization**

**✅ GOOD: Efficient actor pool usage**

.. code-block:: python

    def actor_pool_pattern():
        """Use actor pools efficiently for stateful operations."""
        
        class StatefulProcessor:
            def __init__(self):
                # ✅ Initialize expensive resources once per actor
                self.model = load_expensive_model()
                self.connection_pool = create_connection_pool()
                
            def __call__(self, batch):
                # ✅ Reuse initialized resources
                predictions = self.model.predict(batch["features"])
                # Save to database using connection pool
                self.save_predictions(predictions)
                return {"predictions": predictions}
        
        return dataset.map_batches(
            StatefulProcessor,
            compute=ray.data.ActorPoolStrategy(
                size=4,      # Pool of 4 actors
                min_size=2   # Maintain minimum for fault tolerance
            ),
            batch_size=128
        )

**❌ ANTI-PATTERN: Inefficient actor usage**

.. code-block:: python

    def actor_antipattern():
        """DON'T DO THIS - inefficient actor usage."""
        
        def stateless_function(batch):
            # ❌ Using actors for stateless operations
            return {"result": batch["value"] * 2}
        
        # ❌ Actor overhead without benefit
        return dataset.map_batches(
            stateless_function,
            compute=ray.data.ActorPoolStrategy(size=8)  # Unnecessary actor overhead
        )
        
        class BadActor:
            def __call__(self, batch):
                # ❌ Initializing expensive resources on every call
                model = load_expensive_model()  # Should be in __init__
                return model.predict(batch)

Data Format Patterns
--------------------

**Pattern 7: Optimal File Format Selection**

**✅ GOOD: Choose appropriate formats**

.. code-block:: python

    def file_format_pattern():
        """Choose optimal file formats for different use cases."""
        
        # For analytics: Use Parquet with compression
        analytical_data.write_parquet(
            "s3://analytics/",
            compression="snappy",  # Good balance of speed/size
            partition_cols=["year", "month"]  # Partition for query performance
        )
        
        # For ML training: Use optimized formats
        training_data.write_tfrecords("s3://ml-data/training/")
        
        # For data exchange: Use JSON for flexibility
        api_data.write_json("s3://api-exports/")
        
        # For archival: Use Parquet with high compression
        archival_data.write_parquet(
            "s3://archive/",
            compression="gzip"  # High compression for storage cost
        )

**❌ ANTI-PATTERN: Poor format choices**

.. code-block:: python

    def format_antipattern():
        """DON'T DO THIS - inefficient format choices."""
        
        # ❌ Using CSV for large analytical datasets
        large_analytical_data.write_csv("s3://analytics/")  # Slow, large, no compression
        
        # ❌ Using JSON for high-volume structured data
        structured_data.write_json("s3://warehouse/")  # Inefficient for structured data
        
        # ❌ No compression for storage-intensive data
        large_dataset.write_parquet("s3://storage/", compression=None)  # Wastes storage

**Pattern 8: Database Integration Optimization**

**✅ GOOD: Optimized database operations**

.. code-block:: python

    def database_pattern():
        """Optimize database operations for performance."""
        
        # Efficient database reads with parallelization
        def optimized_db_read():
            return ray.data.read_sql(
                """
                SELECT customer_id, amount, date 
                FROM transactions 
                WHERE date >= %s AND amount > %s
                """,
                connection_factory,
                parallelism=8,  # Parallel connections
                override_num_blocks=16,  # Distribute processing
                shard_keys=["customer_id"],  # Enable sharding
                shard_hash_fn="MD5"  # Hash function for sharding
            )
        
        # Efficient database writes with batching
        def optimized_db_write(dataset):
            return dataset.write_sql(
                "INSERT INTO results (customer_id, score) VALUES (%s, %s)",
                connection_factory,
                sql_options={
                    "method": "multi",  # Bulk insert
                    "chunksize": 5000   # Large chunks
                }
            )

**❌ ANTI-PATTERN: Inefficient database usage**

.. code-block:: python

    def database_antipattern():
        """DON'T DO THIS - inefficient database operations."""
        
        # ❌ Single-threaded database reads
        slow_read = ray.data.read_sql(
            "SELECT * FROM huge_table",  # No filtering
            connection_factory,
            parallelism=1  # Single connection
        )
        
        # ❌ Row-by-row database writes
        def slow_write(row):
            connection = create_connection()
            cursor = connection.cursor()
            cursor.execute("INSERT INTO results VALUES (%s, %s)", (row["id"], row["value"]))
            connection.commit()
            connection.close()
        
        dataset.map(slow_write)  # Extremely slow

Memory Management Patterns
--------------------------

**Pattern 9: Memory-Conscious Block Sizing**

**✅ GOOD: Adaptive block sizing**

.. code-block:: python

    def adaptive_block_sizing_pattern():
        """Adapt block sizes based on data characteristics."""
        
        from ray.data.context import DataContext
        
        ctx = DataContext.get_current()
        
        def configure_for_workload(workload_type, data_size_gb):
            """Configure block sizes based on workload and data size."""
            
            if workload_type == "large_analytics" and data_size_gb > 100:
                # Large blocks for analytical workloads
                ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB
                ctx.target_min_block_size = 128 * 1024 * 1024  # 128MB
                
            elif workload_type == "gpu_processing":
                # Medium blocks for GPU memory constraints
                ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB
                ctx.target_min_block_size = 64 * 1024 * 1024   # 64MB
                
            elif workload_type == "memory_constrained":
                # Small blocks for memory-limited environments
                ctx.target_max_block_size = 32 * 1024 * 1024   # 32MB
                ctx.target_min_block_size = 16 * 1024 * 1024   # 16MB
                
            else:
                # Default configuration
                ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB
                ctx.target_min_block_size = 1 * 1024 * 1024    # 1MB

**❌ ANTI-PATTERN: Fixed block sizing**

.. code-block:: python

    def fixed_block_antipattern():
        """DON'T DO THIS - using same block size for all workloads."""
        
        ctx = DataContext.get_current()
        
        # ❌ Using huge blocks for memory-constrained workloads
        ctx.target_max_block_size = 1024 * 1024 * 1024  # 1GB - too large
        
        # ❌ Using tiny blocks for large analytical workloads
        ctx.target_max_block_size = 1 * 1024 * 1024     # 1MB - too small

**Pattern 10: Efficient Shuffle Operations**

**✅ GOOD: Optimized shuffle patterns**

.. code-block:: python

    def efficient_shuffle_pattern():
        """Optimize shuffle operations for performance."""
        
        ctx = DataContext.get_current()
        
        # Configure for shuffle optimization
        ctx.use_push_based_shuffle = True  # More efficient shuffle
        ctx.target_shuffle_max_block_size = 512 * 1024 * 1024  # 512MB for shuffles
        
        def optimized_groupby(dataset):
            """Efficient groupby with pre-filtering."""
            
            # ✅ Filter before groupby to reduce shuffle volume
            filtered = dataset.filter(lambda row: row["amount"] > 0)
            
            # ✅ Use built-in aggregations (faster than custom)
            return filtered.groupby("category").aggregate(
                ray.data.aggregate.Sum("amount"),
                ray.data.aggregate.Count("transaction_id"),
                ray.data.aggregate.Mean("amount")
            )
        
        def optimized_sort(dataset):
            """Efficient sorting with column selection."""
            
            # ✅ Sort only necessary columns
            return dataset.select_columns(["date", "amount", "id"]) \
                          .sort(["date", "amount"])

**❌ ANTI-PATTERN: Inefficient shuffle operations**

.. code-block:: python

    def shuffle_antipattern():
        """DON'T DO THIS - inefficient shuffle operations."""
        
        # ❌ Groupby without filtering (shuffles all data)
        unfiltered_groupby = dataset.groupby("category").aggregate(
            ray.data.aggregate.Sum("amount")
        )
        
        # ❌ Sorting entire wide datasets
        inefficient_sort = dataset.sort("date")  # Sorts all columns
        
        # ❌ Multiple shuffle operations in sequence
        multiple_shuffles = dataset \
            .sort("date") \
            .groupby("category") \
            .aggregate(ray.data.aggregate.Sum("amount")) \
            .sort("sum(amount)")  # Three shuffle operations

Error Handling Patterns
-----------------------

**Pattern 11: Robust Error Handling**

**✅ GOOD: Comprehensive error handling**

.. code-block:: python

    def robust_error_handling_pattern():
        """Implement robust error handling for production."""
        
        ctx = DataContext.get_current()
        
        # Configure error tolerance
        ctx.max_errored_blocks = 10  # Allow up to 10 block failures
        ctx.actor_task_retry_on_errors = ["ConnectionError", "TimeoutError"]
        
        def fault_tolerant_transform(batch):
            """Transform with comprehensive error handling."""
            
            try:
                return process_batch(batch)
            except ValueError as e:
                # Handle data quality issues gracefully
                logger.warning(f"Data quality issue in batch: {e}")
                return clean_batch(batch)
            except Exception as e:
                # Log unexpected errors
                logger.error(f"Unexpected error in batch processing: {e}")
                raise  # Re-raise for Ray Data error handling
        
        return dataset.map_batches(
            fault_tolerant_transform,
            compute=ray.data.ActorPoolStrategy(size=4)
        )

**❌ ANTI-PATTERN: Poor error handling**

.. code-block:: python

    def poor_error_handling_antipattern():
        """DON'T DO THIS - poor error handling."""
        
        def fragile_transform(batch):
            """Transform without error handling."""
            
            # ❌ No error handling - any bad data crashes pipeline
            result = risky_operation(batch)
            return result
        
        # ❌ No error tolerance configuration
        return dataset.map_batches(fragile_transform)

Production Deployment Patterns
------------------------------

**Pattern 12: Production-Ready Configuration**

**✅ GOOD: Production deployment pattern**

.. code-block:: python

    def production_deployment_pattern():
        """Configure Ray Data for production deployment."""
        
        ctx = DataContext.get_current()
        
        # Production-optimized configuration
        ctx.enable_progress_bars = False  # Reduce overhead
        ctx.enable_auto_log_stats = True  # Enable monitoring
        ctx.verbose_stats_logs = False    # Reduce log volume
        ctx.eager_free = True             # Memory efficiency
        ctx.max_errored_blocks = 5        # Limited error tolerance
        
        # Configure retry policies
        ctx.actor_task_retry_on_errors = [
            "ConnectionError", "TimeoutError", "BrokenPipeError"
        ]
        
        # Set up monitoring
        ctx.enable_per_node_metrics = True
        ctx.memory_usage_poll_interval_s = 60  # Monitor memory usage
        
        def production_pipeline(dataset):
            """Production-ready data processing pipeline."""
            
            try:
                result = dataset \
                    .map_batches(
                        production_transform,
                        batch_size=256,
                        compute=ray.data.ActorPoolStrategy(size=8, min_size=4)
                    ) \
                    .write_parquet("s3://production-output/")
                
                # Log success metrics
                logger.info(f"Pipeline completed successfully")
                return result
                
            except Exception as e:
                # Comprehensive error logging
                logger.error(f"Pipeline failed: {e}")
                logger.error(f"Cluster resources: {ray.cluster_resources()}")
                logger.error(f"Available resources: {ray.available_resources()}")
                raise

**❌ ANTI-PATTERN: Development configuration in production**

.. code-block:: python

    def development_config_antipattern():
        """DON'T DO THIS - development settings in production."""
        
        ctx = DataContext.get_current()
        
        # ❌ Development settings that hurt production performance
        ctx.enable_progress_bars = True   # Overhead in production
        ctx.verbose_stats_logs = True     # Too much logging
        ctx.trace_allocations = True      # Significant overhead
        ctx.max_errored_blocks = -1       # No error limits
        
        # ❌ No monitoring or error handling
        def fragile_production_pipeline(dataset):
            return dataset.map_batches(transform).take_all()  # No error handling

Debugging Patterns
------------------

**Pattern 13: Systematic Debugging**

**✅ GOOD: Systematic debugging approach**

.. code-block:: python

    def debugging_pattern():
        """Systematic approach to debugging Ray Data issues."""
        
        def debug_performance_issue(dataset):
            """Debug performance issues systematically."""
            
            # Step 1: Test with small sample
            sample = dataset.limit(100)
            sample_result = sample.map_batches(transform).take_all()
            print(f"Sample processing successful: {len(sample_result)} records")
            
            # Step 2: Check resource utilization
            print(f"Cluster resources: {ray.cluster_resources()}")
            print(f"Available resources: {ray.available_resources()}")
            
            # Step 3: Profile memory usage
            import psutil
            memory_before = psutil.virtual_memory()
            
            medium_sample = dataset.limit(1000)
            medium_result = medium_sample.map_batches(transform).take_all()
            
            memory_after = psutil.virtual_memory()
            memory_delta = memory_after.used - memory_before.used
            print(f"Memory usage for 1000 records: {memory_delta / (1024*1024):.1f} MB")
            
            # Step 4: Test with production configuration
            ctx = DataContext.get_current()
            ctx.enable_auto_log_stats = True
            ctx.verbose_stats_logs = True
            
            production_result = dataset.map_batches(transform).take(10000)
            print("Production test completed")
            
            return production_result

**❌ ANTI-PATTERN: Ad-hoc debugging**

.. code-block:: python

    def debugging_antipattern():
        """DON'T DO THIS - inefficient debugging approach."""
        
        # ❌ Testing with full dataset immediately
        def bad_debugging():
            huge_dataset = ray.data.read_parquet("s3://petabyte-dataset/")
            result = huge_dataset.map_batches(untested_transform).take_all()  # Likely to fail
        
        # ❌ No incremental testing
        def no_incremental_testing():
            return dataset.map_batches(complex_transform)  # No testing with samples first

Best Practices Summary
----------------------

**Memory Management**
* Use streaming execution for large datasets
* Configure block sizes based on workload characteristics
* Enable eager memory cleanup in production
* Monitor memory usage and set appropriate limits

**Resource Allocation**
* Match resource allocation to operation requirements
* Use actor pools for stateful operations
* Avoid over-allocation of CPU/GPU resources
* Monitor resource utilization and adjust accordingly

**Data Access Optimization**
* Use predicate pushdown and column pruning
* Configure appropriate parallelism for I/O operations
* Choose optimal file formats for use cases
* Implement connection pooling for database operations

**Error Handling**
* Configure appropriate error tolerance levels
* Implement retry policies for transient failures
* Add comprehensive logging and monitoring
* Test error scenarios in development

**Production Deployment**
* Use production-optimized configurations
* Implement comprehensive monitoring and alerting
* Plan for fault tolerance and recovery
* Regular performance testing and optimization

**Pattern 14: Comprehensive Fault Tolerance**

**✅ GOOD: Multi-level fault tolerance**

.. code-block:: python

    def comprehensive_fault_tolerance_pattern():
        """Implement fault tolerance at all levels."""
        
        from ray.data.context import DataContext
        
        ctx = DataContext.get_current()
        
        # Configure block-level error tolerance
        ctx.max_errored_blocks = 5  # Allow up to 5 block failures
        
        # Configure retry policies
        ctx.actor_task_retry_on_errors = ["ConnectionError", "TimeoutError"]
        
        def fault_tolerant_transform(batch):
            """Transform with task-level error handling."""
            
            try:
                return process_batch(batch)
            except DataQualityError as e:
                # Handle data quality issues gracefully
                logger.warning(f"Data quality issue: {e}")
                return clean_batch(batch)
            except Exception as e:
                # Log and re-raise for Ray Data retry handling
                logger.error(f"Processing error: {e}")
                raise
        
        return dataset.map_batches(
            fault_tolerant_transform,
            compute=ray.data.ActorPoolStrategy(size=4),
            max_task_retries=3,     # Task-level retries
            max_restarts=2,         # Actor-level restarts
            retry_exceptions=["ConnectionError", "TimeoutError"]
        )

**❌ ANTI-PATTERN: No fault tolerance**

.. code-block:: python

    def no_fault_tolerance_antipattern():
        """DON'T DO THIS - no error handling or fault tolerance."""
        
        def fragile_transform(batch):
            """Transform without any error handling."""
            # ❌ No error handling - any issue crashes entire pipeline
            return risky_operation(batch)
        
        # ❌ No retry configuration
        return dataset.map_batches(
            fragile_transform,
            max_task_retries=0,  # No retries
            max_restarts=0       # No actor restarts
        )

Next Steps
----------

* **Fault Tolerance**: Comprehensive fault tolerance strategies → :ref:`fault-tolerance`
* **Performance Optimization**: Deep dive into optimization → :ref:`performance-optimization`
* **Troubleshooting**: Debug performance issues → :ref:`troubleshooting`
* **Architecture Deep Dive**: Understand technical details → :ref:`architecture-deep-dive`
* **Monitoring**: Set up production monitoring → :ref:`monitoring-observability`
