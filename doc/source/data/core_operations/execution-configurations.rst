.. _configuration-essentials:

Configuration Essentials
========================

**Keywords:** Ray Data configuration, DataContext, ExecutionOptions, performance tuning, resource allocation

Configure Ray Data for optimal performance and resource utilization. This essential guide covers the most important configuration options for production workloads.

**What you'll learn:**

* Essential DataContext configuration for different workload types
* ExecutionOptions for resource and performance optimization  
* Common configuration patterns for production deployment
* Troubleshooting configuration issues

Essential DataContext Settings
------------------------------

**The DataContext controls Ray Data's core behavior. Configure these key settings for optimal performance:**

.. code-block:: python

    from ray.data.context import DataContext
    
    # Get current configuration
    ctx = DataContext.get_current()
    
    # Configure for your workload type
    ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
    ctx.enable_progress_bars = True  # Show progress
    ctx.verbose_stats_logs = True    # Detailed logging

**Key Configuration Options:**

**Block Size Configuration:**
- **target_max_block_size**: Default 128MB, increase for large analytics workloads
- **target_min_block_size**: Default 1MB, prevents overly small blocks
- **target_shuffle_max_block_size**: Default 1GB for shuffle operations

**Performance Options:**
- **enable_progress_bars**: Show execution progress (default: True)
- **verbose_stats_logs**: Detailed execution statistics (default: False)
- **eager_free**: Immediate memory cleanup (default: False)

**I/O Optimization:**
- **streaming_read_buffer_size**: Default 32MB, adjust for network characteristics
- **enable_pandas_block**: Enable pandas optimization (default: True)

ExecutionOptions for Resource Control
------------------------------------

**Control resource usage and execution behavior with ExecutionOptions:**

.. code-block:: python

    # Configure resource limits
    ctx.execution_options.resource_limits.cpu = 80  # Reserve 20% CPU
    ctx.execution_options.resource_limits.gpu = 7   # Reserve 1 GPU
    
    # Configure locality optimization
    ctx.execution_options.locality_with_output = True  # For ML training

**Resource Management:**
- **resource_limits**: Set soft limits on CPU, GPU, memory usage
- **exclude_resources**: Exclude resources for other workloads
- **locality_with_output**: Optimize for local data consumption

**Execution Control:**
- **preserve_order**: Maintain block ordering (default: False)
- **verbose_progress**: Individual operator progress reporting (default: True)
- **actor_locality_enabled**: Locality-aware actor task dispatch

Common Configuration Patterns
-----------------------------

**ETL Workload Configuration:**

.. code-block:: python

    # Optimize for ETL processing
    ctx.target_max_block_size = 256 * 1024 * 1024  # Larger blocks
    ctx.enable_progress_bars = True                 # Monitor progress
    ctx.execution_options.resource_limits.cpu = 90 # High CPU utilization

**AI/ML Workload Configuration:**

.. code-block:: python

    # Optimize for AI/ML processing
    ctx.target_max_block_size = 128 * 1024 * 1024  # GPU-friendly blocks
    ctx.execution_options.locality_with_output = True  # Training optimization
    ctx.enable_pandas_block = True                     # ML framework integration

**Memory-Constrained Configuration:**

.. code-block:: python

    # Optimize for limited memory
    ctx.target_max_block_size = 64 * 1024 * 1024   # Smaller blocks
    ctx.eager_free = True                           # Immediate cleanup
    ctx.execution_options.resource_limits.object_store_memory = 4e9  # 4GB limit

Configuration Troubleshooting
-----------------------------

**Common Configuration Issues:**

**Out of Memory Errors:**
- **Reduce block size**: `ctx.target_max_block_size = 64 * 1024 * 1024`
- **Enable eager free**: `ctx.eager_free = True`
- **Limit object store**: `ctx.execution_options.resource_limits.object_store_memory = 4e9`

**Slow Performance:**
- **Increase block size**: `ctx.target_max_block_size = 512 * 1024 * 1024`
- **Disable progress bars**: `ctx.enable_progress_bars = False` (production)
- **Optimize resources**: Increase CPU/GPU limits appropriately

**Resource Contention:**
- **Set resource limits**: Reserve resources for other workloads
- **Configure locality**: Use `locality_with_output` for training workloads
- **Monitor utilization**: Enable detailed logging for resource tracking

Next Steps
----------

**Apply Configuration Knowledge:**

**For Performance Optimization:**
→ :ref:`Performance Optimization <performance-optimization>` - Advanced tuning techniques

**For Production Deployment:**
→ :ref:`Production Deployment <production-deployment>` - Enterprise configuration patterns

**For Monitoring:**
→ :ref:`Monitoring & Observability <monitoring-observability>` - Track configuration effectiveness

**For Troubleshooting:**
→ :ref:`Troubleshooting <troubleshooting>` - Resolve configuration issues
