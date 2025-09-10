.. _troubleshooting:

===============================
Ray Data Performance Troubleshooting
===============================

This comprehensive troubleshooting guide helps you diagnose and fix Ray Data performance issues. Use the diagnostic flowcharts, common issue patterns, and systematic debugging approaches to resolve problems quickly.

**Understanding Ray Data Performance Issues (For Beginners):**

If you're new to Ray Data, performance issues might seem mysterious, but they usually fall into a few common categories that will be familiar if you've worked with other distributed systems:

- **Memory pressure** (like Spark executor OOM): Too much data trying to fit in available memory spaces
- **Resource underutilization** (like idle Spark executors): Not enough parallelism or poor task distribution
- **Data skew** (like uneven Spark partitions): Data split incorrectly across workers leading to hot spots
- **I/O bottlenecks** (like slow data source reads): Network or storage limitations affecting data loading

**Technical Translation:**

Most Ray Data performance problems stem from suboptimal interaction with its core architecture components:
- **Blocks** (data chunks) that are too large or too small for your cluster
- **Ray tasks** (workers) that are poorly configured for the workload  
- **Object store** (shared memory) pressure from too much data
- **Streaming execution** (incremental processing) being disrupted by operations that load everything at once

**The Good News:** Most of these issues have simple fixes once you know what to look for!

**Common Issue Patterns Across Workloads:**

**ETL Pipeline Issues:**
- Large file processing causing memory pressure
- Complex joins creating shuffle bottlenecks
- Data quality issues causing task failures

**ML/AI Pipeline Issues:**
- GPU underutilization during mixed CPU/GPU workloads
- Memory pressure from large models or high-resolution data
- Inefficient data loading for training pipelines

**Real-time Processing Issues:**
- Latency spikes from batch size misconfiguration
- Memory accumulation in streaming workloads
- Resource contention between concurrent streams

Quick Diagnostic Checklist
==========================

Start with this checklist to identify the most common performance issues:

**Immediate Checks**
□ Check Ray Dashboard for resource utilization
□ Look for OOM errors in logs
□ Verify cluster has sufficient resources
□ Check for data skew in operations

**Performance Symptoms**
□ Slow execution time
□ High memory usage
□ Low CPU/GPU utilization  
□ Network bottlenecks
□ Frequent task retries

**Data Characteristics**
□ Very large or very small files
□ Wide tables with many columns
□ Skewed data distribution
□ Complex nested data structures

Performance Issue Diagnostic Guide
==================================

**Step 1: Identify Your Primary Symptom**

.. list-table:: Symptom-Based Troubleshooting Guide
   :header-rows: 1
   :class: diagnostic-guide

   * - Primary Symptom
     - Likely Cause
     - Quick Check
     - Solution Section
   * - **Out of Memory Errors**
     - Memory pressure, large blocks
     - Check block sizes, memory usage
     - :ref:`memory_issues_section`
   * - **Low CPU Utilization**
     - Poor parallelization, wrong concurrency
     - Check CPU usage in dashboard
     - :ref:`resource_issues_section`
   * - **Slow I/O Performance**
     - Network latency, many small files
     - Check read/write speeds
     - :ref:`io_issues_section`
   * - **Frequent Task Failures**
     - Data corruption, resource limits
     - Check error logs
     - :ref:`reliability_issues_section`
   * - **Inconsistent Performance**
     - Resource contention, data skew
     - Check task execution times
     - :ref:`advanced_issues_section`

**Step 2: Use the Quick Check**

For each symptom, perform the corresponding quick check to confirm the diagnosis. The quick checks are designed to be fast and non-invasive - they won't disrupt your running pipeline or require extensive setup.

These checks help you avoid misdiagnosis by confirming that the symptom you're observing actually matches the underlying cause. For example, slow performance could be caused by memory pressure, poor parallelization, or network issues - the quick check helps you identify which.

**Step 3: Apply Solutions**

Navigate to the appropriate solution section based on your confirmed diagnosis. Each solution section provides:

- **Root cause analysis**: Why the problem occurs
- **Proven fixes**: Solutions that have been tested in production environments  
- **Verification steps**: How to confirm the fix worked
- **Prevention strategies**: How to avoid the problem in the future

The solutions are ordered by impact and ease of implementation, so you can start with the most effective fixes first.

**Systematic Troubleshooting Workflow:**

1. **Identify the symptom** using the diagnostic table above
2. **Confirm the diagnosis** with the quick check
3. **Apply the primary fix** from the corresponding solution section
4. **Verify the fix worked** using Ray Dashboard metrics
5. **Document the solution** for future reference
6. **Monitor for recurrence** to ensure the fix is permanent

**Escalation Path:**

If standard fixes don't resolve the issue:
1. **Gather detailed information**: Ray Dashboard screenshots, error logs, dataset characteristics
2. **Check for known issues**: Search Ray Data GitHub issues for similar problems
3. **Engage community**: Post in Ray Slack or GitHub Discussions with details
4. **Contact support**: For enterprise users, engage Ray support with comprehensive details

.. _memory_issues_section:

Section A: Memory Issues
========================

Out of Memory (OOM) Errors
--------------------------

**Symptoms:**
- Tasks failing with "OutOfMemoryError"
- Ray object store spilling to disk
- System becoming unresponsive

**Diagnostic Steps:**

.. testcode::

    def diagnose_memory_issues(ds):
        """Diagnose memory-related issues in Ray Data pipeline."""
        
        print("Memory Diagnostics:")
        
        # Check dataset characteristics
        stats = ds.stats()
        total_size_gb = stats.total_bytes / (1024**3)
        avg_block_size_mb = stats.total_bytes / stats.num_blocks / (1024**2)
        
        print(f"  Dataset size: {total_size_gb:.2f}GB")
        print(f"  Number of blocks: {stats.num_blocks}")
        print(f"  Average block size: {avg_block_size_mb:.1f}MB")
        
        # Check system memory
        import psutil
        memory_info = psutil.virtual_memory()
        available_gb = memory_info.available / (1024**3)
        
        print(f"  Available system memory: {available_gb:.1f}GB")
        
        # Check Ray object store
        try:
            import ray
            cluster_resources = ray.cluster_resources()
            object_store_memory = cluster_resources.get("object_store_memory", 0) / (1024**3)
            print(f"  Object store memory: {object_store_memory:.1f}GB")
        except:
            print("  Could not get object store info")
        
        # Identify issues
        issues = []
        
        if avg_block_size_mb > 512:
            issues.append("Blocks too large (>512MB)")
        
        if total_size_gb > available_gb * 0.5:
            issues.append("Dataset larger than 50% of available memory")
        
        if stats.num_blocks < ray.cluster_resources().get("CPU", 1):
            issues.append("Too few blocks for parallelization")
        
        if issues:
            print("  Issues found:")
            for issue in issues:
                print(f"    {issue}")
        else:
            print("  No obvious memory issues detected")
        
        return issues

**Common Memory Fixes:**

.. testcode::

    def fix_memory_issues(ds, issues):
        """Apply fixes for common memory issues."""
        
        fixes_applied = []
        
        # Fix 1: Reduce block sizes
        if any("Blocks too large" in issue for issue in issues):
            print("Applying Fix 1: Reducing block sizes")
            
            # Calculate better block count
            stats = ds.stats()
            target_block_size_mb = 64  # 64MB target
            optimal_blocks = max(1, int(stats.total_bytes / (target_block_size_mb * 1024**2)))
            
            ds = ds.repartition(optimal_blocks)
            fixes_applied.append("Reduced block sizes")
        
        # Fix 2: Configure memory limits
        if any("Dataset larger than" in issue for issue in issues):
            print("Applying Fix 2: Configuring memory limits")
            
            ctx = ray.data.DataContext.get_current()
            ctx.target_max_block_size = 32 * 1024 * 1024  # 32MB max
            
            # Configure execution limits
            available_memory = psutil.virtual_memory().total
            ctx.execution_options.resource_limits.object_store_memory = int(available_memory * 0.2)
            
            fixes_applied.append("Configured memory limits")
        
        # Fix 3: Increase parallelization
        if any("Too few blocks" in issue for issue in issues):
            print("Applying Fix 3: Increasing parallelization")
            
            cpu_count = int(ray.cluster_resources().get("CPU", 4))
            optimal_blocks = cpu_count * 3
            ds = ds.repartition(optimal_blocks)
            
            fixes_applied.append("Increased parallelization")
        
        print(f"Applied fixes: {', '.join(fixes_applied)}")
        return ds

**Memory Pressure Monitoring:**

.. testcode::

    import threading
    import time
    
    class MemoryPressureMonitor:
        """Monitor memory pressure during Ray Data operations."""
        
        def __init__(self, alert_threshold=80):
            self.alert_threshold = alert_threshold
            self.monitoring = False
            self.alerts = []
        
        def start_monitoring(self):
            """Start memory pressure monitoring."""
            self.monitoring = True
            self.thread = threading.Thread(target=self._monitor_loop)
            self.thread.daemon = True
            self.thread.start()
            print(f"ANALYSIS: Memory monitoring started (alert at {self.alert_threshold}%)")
        
        def stop_monitoring(self):
            """Stop monitoring and return alerts."""
            self.monitoring = False
            if hasattr(self, 'thread'):
                self.thread.join()
            return self.alerts
        
        def _monitor_loop(self):
            """Main monitoring loop."""
            while self.monitoring:
                memory_percent = psutil.virtual_memory().percent
                
                if memory_percent > self.alert_threshold:
                    alert = {
                        "timestamp": time.time(),
                        "memory_percent": memory_percent,
                        "message": f"High memory usage: {memory_percent:.1f}%"
                    }
                    self.alerts.append(alert)
                    print(f"ALERT: {alert['message']}")
                
                time.sleep(2)  # Check every 2 seconds
    
    # Usage
    monitor = MemoryPressureMonitor(alert_threshold=75)
    monitor.start_monitoring()
    
    try:
        # Your Ray Data operations
        result = ds.map_batches(my_transform).write_parquet("output/")
    finally:
        alerts = monitor.stop_monitoring()
        print(f"Memory alerts during execution: {len(alerts)}")

.. _resource_issues_section:

Section B: Resource Utilization Issues
======================================

Low CPU/GPU Utilization
-----------------------

**Symptoms:**
- CPUs/GPUs sitting idle during processing
- Slower than expected execution
- Tasks not running in parallel

**Diagnostic Steps:**

.. testcode::

    def diagnose_resource_utilization():
        """Diagnose resource utilization issues."""
        
        print("ANALYSIS: Resource Utilization Diagnostics:")
        
        # Check cluster resources
        cluster_resources = ray.cluster_resources()
        cpu_total = cluster_resources.get("CPU", 0)
        gpu_total = cluster_resources.get("GPU", 0)
        
        print(f"  Total CPUs: {cpu_total}")
        print(f"  Total GPUs: {gpu_total}")
        
        # Check current utilization
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"  Current CPU usage: {cpu_percent:.1f}%")
        
        # Check Ray task utilization
        try:
            from ray._private.internal_api import cluster_resources_usage
            usage = cluster_resources_usage()
            cpu_used = usage.get("CPU", 0)
            gpu_used = usage.get("GPU", 0)
            
            cpu_utilization = (cpu_used / cpu_total * 100) if cpu_total > 0 else 0
            gpu_utilization = (gpu_used / gpu_total * 100) if gpu_total > 0 else 0
            
            print(f"  Ray CPU utilization: {cpu_utilization:.1f}%")
            print(f"  Ray GPU utilization: {gpu_utilization:.1f}%")
            
            # Identify issues
            if cpu_utilization < 50:
                print("  ERROR: Low CPU utilization - check concurrency settings")
            if gpu_total > 0 and gpu_utilization < 50:
                print("  ERROR: Low GPU utilization - check GPU task configuration")
            
        except Exception as e:
            print(f"  Could not get Ray resource usage: {e}")

**Resource Utilization Fixes:**

.. testcode::

    def optimize_resource_utilization(ds, operation_type="cpu_bound"):
        """Optimize resource utilization based on operation type."""
        
        cluster_cpus = int(ray.cluster_resources().get("CPU", 4))
        cluster_gpus = int(ray.cluster_resources().get("GPU", 0))
        
        print(f"OPTIMIZING: Optimizing for {operation_type} operations")
        
        if operation_type == "cpu_bound":
            # CPU-intensive operations: use all CPUs
            optimal_concurrency = cluster_cpus
            
            def optimized_transform(batch):
                # Your CPU-intensive transformation
                return my_cpu_transform(batch)
            
            result = ds.map_batches(
                optimized_transform,
                concurrency=optimal_concurrency,
                ray_remote_args={"num_cpus": 1}
            )
            
        elif operation_type == "io_bound":
            # I/O-bound operations: higher concurrency
            optimal_concurrency = cluster_cpus * 2
            
            def optimized_io_transform(batch):
                # Your I/O-intensive transformation
                return my_io_transform(batch)
            
            result = ds.map_batches(
                optimized_io_transform,
                concurrency=optimal_concurrency,
                ray_remote_args={"num_cpus": 0.5}  # Less CPU per task
            )
            
        elif operation_type == "gpu_accelerated" and cluster_gpus > 0:
            # GPU operations: match GPU count
            optimal_concurrency = cluster_gpus
            
            def optimized_gpu_transform(batch):
                # Your GPU transformation
                return my_gpu_transform(batch)
            
            result = ds.map_batches(
                optimized_gpu_transform,
                concurrency=optimal_concurrency,
                ray_remote_args={"num_gpus": 1, "num_cpus": 2}
            )
            
        else:
            # Default: balanced approach
            optimal_concurrency = max(1, cluster_cpus // 2)
            result = ds.map_batches(
                my_transform,
                concurrency=optimal_concurrency
            )
        
        print(f"SUCCESS: Configured concurrency: {optimal_concurrency}")
        return result

.. _io_issues_section:

Section C: I/O Performance Issues
=================================

Slow Data Loading/Writing
-------------------------

**Symptoms:**
- Long delays during read/write operations
- Network timeouts
- High latency to data sources

**I/O Performance Diagnostic:**

.. testcode::

    def diagnose_io_performance(data_path):
        """Diagnose I/O performance issues."""
        
        print("ANALYSIS: I/O Performance Diagnostics:")
        
        # Test read performance
        start_time = time.time()
        
        # Small sample read
        sample_ds = ray.data.read_parquet(data_path, override_num_blocks=1)
        sample_stats = sample_ds.stats()
        
        sample_time = time.time() - start_time
        sample_size_mb = sample_stats.total_bytes / (1024**2)
        
        if sample_time > 0:
            read_speed_mbps = sample_size_mb / sample_time
            print(f"  Sample read speed: {read_speed_mbps:.1f} MB/s")
            
            if read_speed_mbps < 50:  # Less than 50 MB/s
                print("  ERROR: Slow read performance detected")
                print("    Possible causes:")
                print("    - Network latency to data source")
                print("    - Too many small files")
                print("    - Inefficient file format")
                print("    - Insufficient read parallelism")
        
        # Check file characteristics
        print(f"  Sample size: {sample_size_mb:.1f}MB")
        
        # Estimate total dataset characteristics
        try:
            # This is a simplified estimation
            full_ds = ray.data.read_parquet(data_path)
            full_stats = full_ds.stats()
            
            total_size_gb = full_stats.total_bytes / (1024**3)
            num_files = full_stats.num_blocks  # Approximation
            avg_file_size_mb = (full_stats.total_bytes / num_files) / (1024**2)
            
            print(f"  Estimated total size: {total_size_gb:.2f}GB")
            print(f"  Estimated files: {num_files}")
            print(f"  Average file size: {avg_file_size_mb:.1f}MB")
            
            if avg_file_size_mb < 1:
                print("  ERROR: Many small files detected - consider consolidation")
            
        except Exception as e:
            print(f"  Could not analyze full dataset: {e}")

**I/O Performance Fixes:**

.. testcode::

    def optimize_io_performance(data_path, optimization_type="read"):
        """Optimize I/O performance."""
        
        print(f"OPTIMIZING: Optimizing {optimization_type} performance")
        
        if optimization_type == "read":
            # Optimize reading
            cluster_cpus = int(ray.cluster_resources().get("CPU", 4))
            
            # Strategy 1: Increase read parallelism
            ds = ray.data.read_parquet(
                data_path,
                override_num_blocks=cluster_cpus * 4,  # More parallelism
                ray_remote_args={"num_cpus": 0.25}     # Allow more concurrent reads
            )
            
            print("  SUCCESS: Increased read parallelism")
            
        elif optimization_type == "write":
            # Optimize writing
            def optimized_write(ds, output_path):
                # Configure for optimal write performance
                ctx = ray.data.DataContext.get_current()
                original_block_size = ctx.target_max_block_size
                
                try:
                    # Larger blocks for writing
                    ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB
                    
                    # Write with optimized settings
                    ds.write_parquet(
                        output_path,
                        ray_remote_args={"num_cpus": 1}  # Full CPU per write task
                    )
                    
                    print("  SUCCESS: Optimized write configuration")
                    
                finally:
                    ctx.target_max_block_size = original_block_size
            
            return optimized_write
        
        return ds

**Network Optimization:**

.. testcode::

    def optimize_network_io(data_source_type="s3"):
        """Optimize network I/O for different data sources."""
        
        print(f"OPTIMIZING: Optimizing network I/O for {data_source_type}")
        
        if data_source_type == "s3":
            # S3 optimization
            import pyarrow as pa
            
            # Configure S3 filesystem for better performance
            s3_fs = pa.fs.S3FileSystem(
                request_timeout=60,      # Longer timeout
                connect_timeout=10,      # Connection timeout
                region="us-west-2"       # Specify region
            )
            
            def optimized_s3_read(path):
                return ray.data.read_parquet(
                    path,
                    filesystem=s3_fs,
                    override_num_blocks=32,  # More parallelism for network I/O
                    ray_remote_args={"num_cpus": 0.5}
                )
            
            print("  SUCCESS: Configured S3 optimization")
            return optimized_s3_read
            
        elif data_source_type == "gcs":
            # GCS optimization
            def optimized_gcs_read(path):
                return ray.data.read_parquet(
                    path,
                    override_num_blocks=64,  # High parallelism for GCS
                    ray_remote_args={
                        "num_cpus": 0.25,
                        "resources": {"network_bandwidth": 1}
                    }
                )
            
            print("  SUCCESS: Configured GCS optimization")
            return optimized_gcs_read
        
        else:
            print(f"  No specific optimization for {data_source_type}")
            return None

.. _reliability_issues_section:

Section D: Reliability Issues
============================

Task Failures and Retries
-------------------------

**Symptoms:**
- Tasks failing and retrying repeatedly
- Inconsistent execution times
- Error messages in logs

**Reliability Diagnostic:**

.. testcode::

    def diagnose_reliability_issues(ds):
        """Diagnose reliability issues in Ray Data pipeline."""
        
        print("ANALYSIS: Reliability Diagnostics:")
        
        # Create a test transform to check for common failure modes
        def test_transform(batch):
            """Test transform to identify failure patterns."""
            
            # Check for data quality issues
            if "value" in batch:
                values = batch["value"]
                
                # Check for None/NaN values
                none_count = sum(1 for v in values if v is None)
                if none_count > 0:
                    print(f"  ERROR: Found {none_count} None values")
                
                # Check for data type consistency
                types = set(type(v).__name__ for v in values if v is not None)
                if len(types) > 1:
                    print(f"  ERROR: Inconsistent data types: {types}")
            
            # Check for memory issues
            import psutil
            memory_mb = psutil.Process().memory_info().rss / (1024**2)
            if memory_mb > 1000:  # > 1GB
                print(f"  WARNING: High memory usage in transform: {memory_mb:.0f}MB")
            
            return batch
        
        # Test with a small sample
        try:
            sample = ds.limit(100)
            sample.map_batches(test_transform).materialize()
            print("  SUCCESS: Basic transform test passed")
        except Exception as e:
            print(f"  ERROR: Transform test failed: {e}")
            return ["Transform failures"]
        
        return []

**Reliability Fixes:**

Instead of complex retry logic, use these proven approaches to make transforms more reliable:

**Approach 1: Basic Error Handling**

Add simple error handling to your transform functions:

.. testcode::

    def reliable_transform(batch):
        try:
            return my_transform(batch)
        except Exception as e:
            print(f"Transform error: {e}")
            return batch  # Return original data on error

**Approach 2: Data Validation**

Check for common data issues before processing:

.. testcode::

    def validated_transform(batch):
        # Check for empty batches
        if not batch or not any(batch.values()):
            return {k: [] for k in batch.keys()}
        
        # Your transform logic here
        return my_transform(batch)

**Approach 3: Memory-Safe Processing**

For memory-intensive operations, add memory checks:

.. testcode::

    import psutil
    
    def memory_safe_transform(batch):
        # Check available memory before processing
        if psutil.virtual_memory().percent > 90:
            print("WARNING: High memory pressure")
            # Process smaller chunks or skip
        
        return my_transform(batch)

**Apply Reliable Transforms:**

Use these patterns in your Ray Data pipelines:

.. testcode::

    # Example: Using error handling in your pipeline
    result = ds.map_batches(reliable_transform)

These approaches provide robust error handling without complex retry logic.

.. _advanced_issues_section:

Section E: Advanced Troubleshooting
===================================

Performance Analysis with Ray Dashboard
---------------------------------------

Use Ray Dashboard's built-in profiling capabilities instead of custom profiling code:

**Ray Dashboard Profiling Features:**

1. **Timeline View**: Shows task execution patterns and identifies bottlenecks
2. **Flame Graph**: CPU profiling for individual tasks (click on any task)
3. **Task Profiling**: Memory and CPU usage per task
4. **Operator Metrics**: Performance breakdown by Ray Data operation

**Performance Analysis Process:**

1. **Run your pipeline** with Ray Dashboard open
2. **Navigate to Timeline tab** to see task execution patterns
3. **Look for patterns** like long-running tasks or frequent failures
4. **Click on slow tasks** to see detailed profiling information
5. **Check Metrics tab** for resource utilization trends

**Common Performance Issues Visible in Dashboard:**

- **Task scheduling gaps**: Indicates resource constraints or poor parallelization
- **Memory spilling warnings**: Object store exceeding capacity
- **Long task execution times**: Suggests need for batch size optimization
- **High task failure rates**: Points to memory or resource issues

**Dashboard-Guided Optimization:**

.. testcode::

    # Simple approach: Enable detailed stats and use dashboard
    ctx = ray.data.DataContext.get_current()
    ctx.enable_progress_bars = True
    ctx.enable_operator_progress_bars = True
    
    # Run pipeline and monitor dashboard
    result = ds.map_batches(my_transform).write_parquet("output/")

Ray Dashboard will show real-time performance metrics and help you identify bottlenecks without custom monitoring code.

**Simple Performance Analysis:**

Instead of complex custom metrics, use Ray Data's built-in stats and Ray Dashboard:

**How Ray Data Provides Performance Insights:**

Ray Data automatically collects detailed performance statistics during execution. These stats include:

- **Operator execution times**: How long each operation (read, transform, write) took
- **Task distribution**: How work was distributed across your cluster
- **Memory usage patterns**: Peak and average memory consumption
- **Block processing metrics**: How efficiently data was processed in chunks

.. testcode::

    # Enable stats collection and run pipeline
    result = ds.map_batches(my_transform).write_parquet("output/")
    
    # Ray Data automatically tracks performance
    print(result.stats())

**Use Ray Dashboard for Detailed Analysis:**

1. **Open Ray Dashboard** during pipeline execution
2. **Monitor the Metrics tab** for resource utilization trends
3. **Check Timeline tab** for task execution patterns
4. **Look for alerts** about memory spilling or task failures
5. **Use built-in profiling** by clicking on individual tasks

**Key Dashboard Indicators:**

- **High CPU utilization**: Good sign for CPU-bound workloads
- **Steady memory usage**: Indicates good memory management
- **No spilling alerts**: Memory usage is under control
- **Even task distribution**: Good parallelization across cluster

Section E: Advanced Troubleshooting
===================================

Performance Analysis with Ray Dashboard
---------------------------------------

Use Ray Dashboard's built-in profiling capabilities instead of custom profiling code:

**Ray Dashboard Profiling Features:**

1. **Timeline View**: Shows task execution patterns and identifies bottlenecks
2. **Flame Graph**: CPU profiling for individual tasks (click on any task)
3. **Task Profiling**: Memory and CPU usage per task
4. **Operator Metrics**: Performance breakdown by Ray Data operation

**Performance Analysis Process:**

1. **Run your pipeline** with Ray Dashboard open
2. **Navigate to Timeline tab** to see task execution patterns
3. **Look for patterns** like long-running tasks or frequent failures
4. **Click on slow tasks** to see detailed profiling information
5. **Check Metrics tab** for resource utilization trends

**Common Performance Issues Visible in Dashboard:**

- **Task scheduling gaps**: Indicates resource constraints or poor parallelization
- **Memory spilling warnings**: Object store exceeding capacity
- **Long task execution times**: Suggests need for batch size optimization
- **High task failure rates**: Points to memory or resource issues

**Dashboard-Guided Optimization:**

.. testcode::

    # Simple approach: Enable detailed stats and use dashboard
    ctx = ray.data.DataContext.get_current()
    ctx.enable_progress_bars = True
    ctx.enable_operator_progress_bars = True
    
    # Run pipeline and monitor dashboard
    result = ds.map_batches(my_transform).write_parquet("output/")
    
    # Dashboard will show real-time performance metrics

**Simple Performance Analysis:**

Instead of complex custom metrics, use Ray Data's built-in stats and Ray Dashboard:

.. testcode::

    import time
    
    # Simple timing and stats collection
    print("Starting performance analysis")
    
    start_time = time.time()
    result = ds.map_batches(my_transform).write_parquet("output/")
    end_time = time.time()
    
    execution_time = end_time - start_time
    stats = result.stats()
    
    print(f"Performance Results:")
    print(f"  Execution time: {execution_time:.2f}s")
    print(f"  Ray Data stats:")
    print(stats)

**Use Ray Dashboard for Detailed Analysis:**

1. **Open Ray Dashboard** during pipeline execution
2. **Monitor the Metrics tab** for resource utilization trends
3. **Check Timeline tab** for task execution patterns
4. **Look for alerts** about memory spilling or task failures
5. **Use built-in profiling** by clicking on individual tasks

**Key Dashboard Indicators:**

- **High CPU utilization**: Good sign for CPU-bound workloads
- **Steady memory usage**: Indicates good memory management
- **No spilling alerts**: Memory usage is under control
- **Even task distribution**: Good parallelization across cluster

Comprehensive Troubleshooting Toolkit
=====================================

All-in-One Diagnostic Tool
--------------------------

When facing performance issues, use this systematic diagnostic approach to identify the root cause. The diagnostic covers all major components of Ray Data's architecture.

**System Resource Analysis**

Use Ray Dashboard to check if your system has adequate resources:

**Dashboard Resource Monitoring:**

1. **Navigate to Ray Dashboard** → **Cluster tab**
2. **Check Node Resources**: See total and available CPU, memory, GPU per node
3. **Monitor Resource Usage**: Real-time utilization across all cluster nodes
4. **Identify Resource Constraints**: Nodes approaching resource limits

**Key Resource Indicators:**

- **Node Status**: All nodes should show as "Alive" and healthy
- **CPU Usage**: Should be balanced across nodes during processing
- **Memory Usage**: No nodes should consistently exceed 80% memory usage
- **GPU Usage**: GPUs should show high utilization during GPU operations

**Ray Cluster Analysis**

Use Ray Dashboard's Cluster tab to verify your cluster configuration:

**Cluster Configuration Check:**

1. **Total Resources**: Verify expected number of CPUs, GPUs, memory
2. **Node Health**: All nodes should show as "Alive"
3. **Resource Allocation**: Check that Ray Data can access cluster resources
4. **Object Store**: Verify object store memory allocation

**Simple Cluster Resource Check:**

.. testcode::

    # Basic cluster resource verification
    cluster_resources = ray.cluster_resources()
    print(f"Ray CPUs: {cluster_resources.get('CPU', 0)}")
    print(f"Ray GPUs: {cluster_resources.get('GPU', 0)}")
    print(f"Object Store: {cluster_resources.get('object_store_memory', 0) / (1024**3):.1f}GB")

**Dataset Characteristics Analysis**

Analyze your dataset's structure to identify potential issues:

.. testcode::

    def analyze_dataset_characteristics(ds):
        """Analyze dataset structure for performance issues."""
        
        print("Dataset Analysis:")
        
        try:
            stats = ds.stats()
            total_size_gb = stats.total_bytes / (1024**3)
            avg_block_size_mb = stats.total_bytes / stats.num_blocks / (1024**2)
            
            print(f"   Dataset size: {total_size_gb:.2f}GB")
            print(f"   Number of blocks: {stats.num_blocks}")
            print(f"   Average block size: {avg_block_size_mb:.1f}MB")
            
            # Identify potential issues
            if avg_block_size_mb > 512:
                print("   WARNING: Large blocks detected - may cause memory issues")
            elif avg_block_size_mb < 1:
                print("   WARNING: Very small blocks detected - high overhead")
            
            return stats
        except Exception as e:
            print(f"   Could not analyze dataset: {e}")
            return None

**Performance Testing**

Test your operations on a small sample to identify bottlenecks safely:

.. testcode::

    def test_operation_performance(ds, operation_func):
        """Test operation performance on a small sample."""
        
        if not operation_func:
            print("   No operation provided for testing")
            return
        
        print("Performance Test:")
        
        try:
            # Test with small sample to avoid expensive operations
            start_time = time.time()
            test_result = operation_func(ds.limit(1000))
            end_time = time.time()
            
            test_time = end_time - start_time
            print(f"   Test execution time: {test_time:.2f}s")
            
            if hasattr(test_result, 'stats'):
                test_stats = test_result.stats()
                throughput = (test_stats.total_bytes / (1024**2)) / test_time
                print(f"   Test throughput: {throughput:.1f} MB/s")
                
        except Exception as e:
            print(f"   Performance test failed: {e}")

**Complete Diagnostic Function**

Combine all diagnostic steps into a comprehensive analysis:

.. testcode::

    def comprehensive_ray_data_diagnostics(ds, operation_func=None):
        """Run complete diagnostic analysis."""
        
        print("ANALYSIS: Comprehensive Ray Data Diagnostics")
        print("=" * 50)
        
        # Run each diagnostic step
        check_system_resources()
        check_ray_cluster() 
        dataset_stats = analyze_dataset_characteristics(ds)
        test_operation_performance(ds, operation_func)
        
        # Generate actionable recommendations
        print("\nRECOMMENDATIONS: Recommendations:")
        recommendations = generate_recommendations(ds, dataset_stats)
        for rec in recommendations:
            print(f"   • {rec}")
        
        print("\n" + "=" * 50)
        print("Diagnostics complete!")
    
    def generate_recommendations(ds):
        """Generate performance recommendations based on dataset characteristics."""
        recommendations = []
        
        try:
            stats = ds.stats()
            total_size_gb = stats.total_bytes / (1024**3)
            avg_block_size_mb = stats.total_bytes / stats.num_blocks / (1024**2)
            
            # Block size recommendations
            if avg_block_size_mb > 256:
                recommendations.append("Consider using more blocks (reduce block size)")
            elif avg_block_size_mb < 16:
                recommendations.append("Consider using fewer blocks (increase block size)")
            
            # Memory recommendations
            available_memory_gb = psutil.virtual_memory().available / (1024**3)
            if total_size_gb > available_memory_gb * 0.8:
                recommendations.append("Dataset is large relative to memory - use streaming execution")
            
            # Parallelism recommendations
            cpu_count = int(ray.cluster_resources().get("CPU", 4))
            if stats.num_blocks < cpu_count:
                recommendations.append("Consider increasing parallelism (more blocks)")
            elif stats.num_blocks > cpu_count * 8:
                recommendations.append("Consider reducing parallelism (fewer blocks)")
            
        except Exception:
            recommendations.append("Could not analyze dataset for recommendations")
        
        if not recommendations:
            recommendations.append("Configuration looks reasonable!")
        
        return recommendations

Best Practices for Troubleshooting
==================================

**Systematic Approach**
1. **Start with the diagnostic checklist** to identify issue category
2. **Use the flowchart** to navigate to specific solutions
3. **Apply targeted fixes** rather than random changes
4. **Measure before and after** to validate improvements
5. **Document solutions** for future reference

**Prevention Strategies**
1. **Monitor performance continuously** in production
2. **Set up alerts** for common issues (OOM, low utilization)
3. **Use performance regression detection**
4. **Regular performance reviews** of Ray Data pipelines
5. **Keep Ray Data updated** for latest optimizations

**When to Seek Help**
- Issues persist after applying standard fixes
- Unusual error messages not covered in documentation
- Performance degradation without obvious cause
- Complex distributed system interactions

**Resources for Additional Help**
- Ray Data GitHub Issues: https://github.com/ray-project/ray/issues
- Ray Slack Community: https://ray-distributed.slack.com
- Ray Data Documentation: https://docs.ray.io/en/latest/data/
- Performance Benchmarking: https://github.com/ray-project/ray/tree/master/release/benchmarks

Next Steps
==========

After troubleshooting your Ray Data performance issues:

- **Apply the fixes** identified through diagnostics
- **Set up monitoring** to prevent future issues
- **Review optimization guides**: :ref:`reading_optimization`, :ref:`transform_optimization`, :ref:`memory_optimization`
- **Learn advanced patterns**: :ref:`patterns_antipatterns`
- **Share your solutions** with the Ray Data community
