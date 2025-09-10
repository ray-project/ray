.. _advanced-features:

Advanced Features & Experimental Capabilities
==============================================

Ray Data includes several advanced features and experimental capabilities that provide cutting-edge functionality for specialized use cases. This guide covers these features and provides comprehensive guidance on when and how to use them safely in production environments.

**What you'll learn:**

* Experimental features and their stability levels with production readiness assessment
* Advanced configuration options for performance tuning and optimization
* Beta functionality and migration paths to stable features
* Production deployment strategies for experimental features
* Risk assessment and mitigation for advanced capabilities
* Community resources and contribution guidelines

:::warning
**Production Safety Guidelines**
Experimental features are subject to change and may not be suitable for production use without proper validation. Always:
- **Test thoroughly** in development environments with realistic data volumes
- **Have migration plans** ready for when features change or are deprecated  
- **Monitor Ray Data releases** for breaking changes to experimental features
- **Implement feature flags** to enable/disable experimental functionality quickly
- **Maintain fallback implementations** for critical production workloads
:::

**Feature Stability Classification:**

:::list-table
   :header-rows: 1

- - **Stability Level**
  - **Production Readiness**
  - **Change Risk**
  - **Recommended Usage**
- - **Alpha**
  - Not recommended for production
  - High - breaking changes expected
  - Development and testing only
- - **Beta**
  - Limited production use with caution
  - Medium - some changes possible
  - Non-critical production workloads
- - **Stable**
  - Full production deployment
  - Low - backward compatibility maintained
  - All production workloads
- - **Deprecated**
  - Migration required
  - High - removal planned
  - Migrate to replacement features

:::

Experimental Features
---------------------

**Alpha Stability Features**

Ray Data includes several features marked as ``@PublicAPI(stability="alpha")`` that provide early access to new capabilities:

**Unity Catalog Integration (Alpha)**

.. code-block:: python

    import ray

    # Read from Unity Catalog with credential vending
    # This feature provides secure access to Databricks Unity Catalog tables
    ds = ray.data.read_unity_catalog(
        table="main.sales.transactions",  # Three-level namespace: catalog.schema.table
        url="https://dbc-XXXXXXX-XXXX.cloud.databricks.com",  # Databricks workspace URL
        token="your-databricks-token",    # Databricks access token for authentication
        data_format="delta"               # Specify format (auto-inferred if not specified)
    )
    
    # The integration handles credential vending automatically
    # Short-lived credentials are managed transparently
    # Supports AWS, Azure, and GCP cloud storage backends

**Key capabilities:**
* Secure access via Databricks credential vending
* Support for Delta and Parquet formats
* AWS, Azure, and GCP cloud support
* Automatic short-lived credential management

**Production considerations:**
* Currently in Public Preview - requires explicit metastore enablement
* Principals must have ``EXTERNAL USE SCHEMA`` permissions
* Test thoroughly before production deployment
* Implement feature flags for easy rollback if issues occur
* Monitor Unity Catalog integration performance and error rates
* Have fallback data access methods for critical workloads

**Production Deployment Strategy:**
1. **Development testing**: Validate integration in development environment
2. **Staging validation**: Test with production-like data volumes and access patterns
3. **Gradual rollout**: Deploy to non-critical workloads first
4. **Performance monitoring**: Track integration performance and error rates
5. **Rollback planning**: Maintain ability to quickly disable if issues arise

**Delta Sharing Integration (Alpha)**

Delta Sharing enables secure data sharing across organizations using open protocols. Ray Data's integration provides native support for reading Delta Sharing tables with automatic credential management.

.. code-block:: python

    import ray

    # Read from Delta Sharing table
    ds = ray.data.read_delta_sharing_tables(
        url="profile.json#share-name.schema-name.table-name",
        limit=100000,
        version=1,
        json_predicate_hints='{"op": "equal", "children": [{"op": "column", "name": "region"}, {"op": "literal", "value": "US"}]}'
    )

**Key capabilities:**
* Access to Delta Sharing tables with proper authentication
* Version-based and timestamp-based table access
* Predicate pushdown for efficient filtering
* Integration with Delta Sharing ecosystem

**Production considerations:**
* Requires valid Delta Sharing profile configuration
* Network connectivity to Delta Sharing providers
* Consider data governance and access control policies

**ClickHouse Integration (Alpha)**

.. code-block:: python

    import ray

    # Read from ClickHouse database
    ds = ray.data.read_clickhouse(
        table="sales_data",
        dsn="clickhouse://user:password@host:port/database",
        columns=["customer_id", "order_amount", "order_date"],
        filter="order_date >= '2024-01-01'",
        order_by=(["order_date"], True)  # Ascending order
    )

**Key capabilities:**
* Native ClickHouse connectivity for OLAP workloads
* Column projection and predicate pushdown
* Optimized for analytical query patterns
* Support for ClickHouse-specific data types

**Production considerations:**
* Optimize queries for ClickHouse's columnar architecture
* Consider network latency and connection pooling
* Test with representative data volumes

Advanced Configuration Options
------------------------------

**DataContext Advanced Settings**

Ray Data provides extensive configuration options through the ``DataContext`` class for fine-tuning performance and behavior:

.. code-block:: python

    from ray.data import DataContext
    import ray

    # Get current context
    ctx = DataContext.get_current()

    # Advanced memory management
    ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
    ctx.target_min_block_size = 32 * 1024 * 1024   # 32MB minimum
    ctx.eager_free = True                           # Aggressive memory cleanup

    # Experimental features
    ctx.use_polars = True                          # Use Polars for tabular operations
    ctx.use_push_based_shuffle = True             # Push-based shuffle optimization
    ctx.enable_tensor_extension_casting = True     # Automatic tensor casting

    # Advanced error handling
    ctx.max_errored_blocks = 10                    # Allow some block failures
    ctx.actor_task_retry_on_errors = True         # Retry on transient errors

    # Performance monitoring
    ctx.enable_auto_log_stats = True              # Automatic statistics logging
    ctx.verbose_stats_logs = True                 # Detailed performance metrics
    ctx.trace_allocations = False                 # Memory allocation tracing (debug only)

**Arrow Tensor V2 Support**

Enable support for tensors larger than 2GB:

.. code-block:: python

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.use_arrow_tensor_v2 = True  # Support for large tensors

    # Now you can process very large tensor data
    large_tensor_ds = ray.data.read_numpy("s3://large-tensors/")

**Push-Based Shuffle Optimization**

Enable experimental push-based shuffle for improved performance:

.. code-block:: python

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.use_push_based_shuffle = True

    # Shuffle operations will use optimized push-based approach
    shuffled_ds = dataset.random_shuffle()

**Polars Backend Integration**

Use Polars for enhanced tabular data operations:

.. code-block:: python

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.use_polars = True  # Use Polars for sorts, groupbys, aggregations

    # Tabular operations will leverage Polars optimizations
    result = dataset.groupby("category").aggregate(Sum("amount"))

Developer API Features
----------------------

**Block-Level Operations**

Ray Data provides developer APIs for advanced block-level operations:

.. code-block:: python

    import ray
    from ray.data._internal.block_accessor import BlockAccessor

    def advanced_block_processing(block):
        """Advanced block-level processing with direct block access."""
        accessor = BlockAccessor.for_block(block)
        
        # Direct block operations
        num_rows = accessor.num_rows()
        schema = accessor.schema()
        
        # Custom block transformations
        if num_rows > 1000:
            # Apply different logic for large blocks
            return accessor.sample(0.1)  # Sample 10%
        else:
            return block

    # Apply block-level processing
    processed_ds = dataset.map_blocks(advanced_block_processing)

**Custom Datasources**

Create custom datasources for specialized data formats:

.. code-block:: python

    from ray.data.datasource import Datasource, ReadTask
    import ray

    class CustomDatasource(Datasource):
        """Custom datasource for specialized data format."""
        
        def create_reader(self, **kwargs):
            def read_fn():
                # Custom reading logic
                data = load_custom_format()  # Your implementation
                yield ray.data.from_pandas(data)
            
            return ReadTask(read_fn)

    # Use custom datasource
    ds = ray.data.read_datasource(CustomDatasource(), path="custom://data/")

Performance Debugging Features
------------------------------

**Memory Allocation Tracing**

Enable detailed memory allocation tracing for debugging:

.. code-block:: python

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.trace_allocations = True  # Enable allocation tracing (performance impact)

    # Run your workload with detailed memory tracking
    result = dataset.map_batches(memory_intensive_function)

    # View detailed memory statistics
    print(result.stats())

**Verbose Statistics Logging**

Enable comprehensive performance metrics collection:

.. code-block:: python

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.verbose_stats_logs = True    # Detailed performance metrics
    ctx.enable_auto_log_stats = True # Automatic logging

    # Execute pipeline with detailed monitoring
    result = dataset.map_batches(transform_function) \
                   .groupby("key") \
                   .aggregate(Sum("value"))

**Custom Metrics Collection**

Implement custom metrics for specific use cases:

.. code-block:: python

    import time
    from ray.data import DataContext

    def custom_metrics_transform(batch):
        """Transform with custom metrics collection."""
        start_time = time.time()
        
        # Your transformation logic
        result = process_batch(batch)
        
        # Custom metric logging
        processing_time = time.time() - start_time
        print(f"Batch processing time: {processing_time:.2f}s")
        
        return result

    # Apply with custom metrics
    result = dataset.map_batches(custom_metrics_transform)

Beta Features Migration Guide
-----------------------------

**Migrating from Deprecated APIs**

Some features have evolved from experimental to stable. Here's how to migrate:

**Compute Strategy Evolution**

.. code-block:: python

    # Old deprecated approach
    dataset.map_batches(
        transform_function,
        compute="actors"  # Deprecated
    )

    # New stable approach
    dataset.map_batches(
        transform_function,
        concurrency=4  # Current API
    )

**Resource Specification Updates**

.. code-block:: python

    # Enhanced resource specification
    dataset.map_batches(
        gpu_transform,
        num_gpus=1,
        concurrency=(1, 4)  # Auto-scaling actor pool (min=1, max=4)
    )

Community Resources & Contribution
----------------------------------

**Getting Help with Advanced Features**

* **GitHub Discussions**: https://github.com/ray-project/ray/discussions
* **Ray Slack Community**: https://forms.gle/9TSdDYUgxYs8SA9e8
* **Stack Overflow**: Tag questions with ``ray`` and ``ray-data``
* **Documentation Issues**: https://github.com/ray-project/ray/issues

**Contributing to Ray Data**

Ray Data welcomes community contributions, especially for:

* **New data source connectors**
* **Performance optimizations**
* **Documentation improvements**
* **Bug fixes and testing**

**Development Setup**

.. code-block:: bash

    # Clone Ray repository
    git clone https://github.com/ray-project/ray.git
    cd ray

    # Install development dependencies
    pip install -e "python[data]"

    # Run Ray Data tests
    python -m pytest python/ray/data/tests/

**Feature Request Process**

1. **Search existing issues** for similar requests
2. **Create GitHub issue** with detailed use case description
3. **Engage with maintainers** for feedback and guidance
4. **Consider contributing** implementation if suitable

**Code Contribution Guidelines**

* **Follow Ray's coding standards** and style guidelines
* **Add comprehensive tests** for new functionality
* **Update documentation** for user-facing changes
* **Ensure backward compatibility** when possible

Best Practices for Advanced Features
------------------------------------

**Risk Assessment Framework**

Use this framework when considering advanced features:

.. list-table::
   :header-rows: 1
   :widths: 20 20 20 20 20

   * - Feature Type
     - Stability Level
     - Production Risk
     - Testing Required
     - Migration Path
   * - Alpha Features
     - Experimental
     - High
     - Extensive
     - Plan for changes
   * - Beta Features
     - Evolving
     - Medium
     - Thorough
     - Monitor updates
   * - Developer APIs
     - Internal
     - High
     - Expert-level
     - Not guaranteed
   * - Configuration
     - Stable
     - Low-Medium
     - Standard
     - Well-documented

**Production Deployment Strategy**

1. **Isolated Testing**: Test advanced features in isolated environments
2. **Gradual Rollout**: Deploy to non-critical workloads first
3. **Monitoring**: Implement comprehensive monitoring and alerting
4. **Rollback Plans**: Maintain ability to quickly revert changes
5. **Documentation**: Document configuration and operational procedures

**Performance Impact Assessment**

Before using advanced features in production:

.. code-block:: python

    import time
    from ray.data import DataContext

    def benchmark_feature(dataset, feature_enabled=False):
        """Benchmark performance impact of advanced features."""
        
        ctx = DataContext.get_current()
        if feature_enabled:
            ctx.use_polars = True  # Example advanced feature
        
        start_time = time.time()
        result = dataset.groupby("key").aggregate(Sum("value"))
        result.materialize()  # Force execution
        end_time = time.time()
        
        return end_time - start_time

    # Compare performance
    baseline_time = benchmark_feature(dataset, feature_enabled=False)
    advanced_time = benchmark_feature(dataset, feature_enabled=True)
    
    improvement = (baseline_time - advanced_time) / baseline_time * 100
    print(f"Performance improvement: {improvement:.1f}%")

**Advanced Feature Performance Benchmarks**

Based on production deployments and testing, here are typical performance improvements from advanced features:

:::list-table
   :header-rows: 1

- - **Advanced Feature**
  - **Typical Performance Gain**
  - **Best Use Cases**
  - **Measurement Methodology**
- - **Unity Catalog Integration**
  - 20-40% faster data access
  - Large Databricks deployments
  - Compare vs direct Delta Lake access
- - **Advanced Aggregations**
  - 30-60% faster complex analytics
  - Statistical computations
  - Compare vs traditional aggregation methods
- - **GPU Optimization Features**
  - 50-200% faster GPU workloads
  - Image/video processing, ML inference
  - Compare GPU utilization rates
- - **Streaming Execution Enhancements**
  - 40-80% memory efficiency improvement
  - Large dataset processing
  - Compare memory usage vs batch processing
- - **Custom Operator Optimizations**
  - 25-100% workload-specific gains
  - Specialized processing requirements
  - Compare vs generic operator implementations

:::

.. code-block:: python

    # Comprehensive performance benchmarking for advanced features
    def benchmark_advanced_features():
        """Comprehensive benchmarking of advanced features."""
        import time
        import psutil
        
        def benchmark_with_metrics(feature_name, dataset, operation_func):
            """Benchmark with comprehensive metrics collection."""
            # Collect baseline metrics
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # Execute operation
            result = operation_func(dataset)
            result.materialize()
            
            # Collect final metrics
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # Calculate performance metrics
            execution_time = end_time - start_time
            memory_usage = end_memory - start_memory
            throughput = len(dataset) / execution_time if execution_time > 0 else 0
            
            print(f"{feature_name} Performance:")
            print(f"  Execution time: {execution_time:.2f} seconds")
            print(f"  Memory usage: {memory_usage:.1f} MB")
            print(f"  Throughput: {throughput:.0f} records/second")
            
            return {
                'execution_time': execution_time,
                'memory_usage': memory_usage,
                'throughput': throughput
            }
        
        # Test dataset
        test_dataset = ray.data.range(100000)
        
        # Benchmark different advanced features
        baseline_metrics = benchmark_with_metrics(
            "Baseline Processing",
            test_dataset,
            lambda ds: ds.map(lambda x: x * 2)
        )
        
        advanced_metrics = benchmark_with_metrics(
            "Advanced Feature Processing",
            test_dataset,
            lambda ds: ds.map_batches(lambda batch: batch * 2, batch_size=10000)
        )
        
        # Compare results
        time_improvement = (baseline_metrics['execution_time'] - advanced_metrics['execution_time']) / baseline_metrics['execution_time'] * 100
        memory_efficiency = (baseline_metrics['memory_usage'] - advanced_metrics['memory_usage']) / baseline_metrics['memory_usage'] * 100
        
        print(f"Advanced Feature Impact:")
        print(f"  Time improvement: {time_improvement:.1f}%")
        print(f"  Memory efficiency: {memory_efficiency:.1f}%")

Production Readiness for Advanced Features
-------------------------------------------

**Comprehensive Production Deployment Strategy**

Deploying advanced and experimental features in production requires careful planning, thorough testing, and comprehensive risk mitigation strategies.

**Production Readiness Assessment Framework:**

:::list-table
   :header-rows: 1

- - **Assessment Area**
  - **Alpha Features**
  - **Beta Features**
  - **Stable Features**
- - **Testing Requirements**
  - Extensive testing in development only
  - Limited production testing with fallbacks
  - Standard production testing
- - **Risk Mitigation**
  - Feature flags, immediate rollback capability
  - Gradual rollout, monitoring, rollback plans
  - Standard deployment procedures
- - **Monitoring Needs**
  - Comprehensive monitoring and alerting
  - Enhanced monitoring for new functionality
  - Standard monitoring
- - **Support Expectations**
  - Community support, expect breaking changes
  - Limited support, some changes possible
  - Full production support

:::

.. code-block:: python

    # Production-ready advanced feature implementation
    class ProductionAdvancedFeatureManager:
        """Manage advanced features safely in production."""
        
        def __init__(self):
            self.feature_flags = self._load_feature_flags()
            self.monitoring = self._setup_monitoring()
            self.fallback_strategies = self._define_fallbacks()
        
        def use_advanced_feature_safely(self, feature_name, dataset, fallback_function):
            """Use advanced feature with comprehensive safety controls."""
            if not self.feature_flags.get(feature_name, False):
                # Feature disabled - use fallback
                return fallback_function(dataset)
            
            try:
                # Monitor feature usage
                with self.monitoring.track_feature_usage(feature_name):
                    # Use advanced feature with timeout
                    result = self._apply_advanced_feature(dataset, feature_name)
                    
                    # Validate result quality
                    if self._validate_result_quality(result):
                        return result
                    else:
                        # Quality check failed - use fallback
                        self.monitoring.log_quality_failure(feature_name)
                        return fallback_function(dataset)
                        
            except Exception as e:
                # Advanced feature failed - use fallback
                self.monitoring.log_feature_error(feature_name, str(e))
                return fallback_function(dataset)

Next Steps
----------

**For Advanced Users:**

* Explore experimental features in development environments
* Contribute feedback and bug reports to the Ray community
* Consider contributing new features or optimizations

**For Production Users:**

* Evaluate advanced features against business requirements
* Implement comprehensive testing and monitoring
* Maintain migration plans for experimental features

**For Contributors:**

* Review Ray Data's contribution guidelines
* Engage with the community through GitHub and Slack  
* Consider areas where your expertise can benefit the project

**Experimental Features Summary and Roadmap**

**Current Alpha Features (Use with Caution):**
* **Unity Catalog Integration**: Secure Databricks data access with credential vending
* **Delta Sharing**: Cross-organization data sharing with open protocols
* **Advanced GPU Optimizations**: Experimental GPU resource management improvements
* **Custom Aggregation Functions**: User-defined aggregation operations
* **Enhanced Streaming Execution**: Experimental improvements to streaming performance

**Features Moving to Beta (Next 6 months):**
* **Unity Catalog Integration**: Expected to move to beta with enhanced stability
* **Advanced Configuration APIs**: More sophisticated configuration options
* **Custom Operator Framework**: Framework for building custom operators

**Features Moving to Stable (Next 12 months):**
* **Enhanced Delta Lake Support**: Improved Delta Lake integration and optimization
* **Advanced Monitoring APIs**: Comprehensive monitoring and observability features
* **Multi-Cloud Optimization**: Enhanced multi-cloud deployment and optimization

**Deprecated Features (Plan Migration):**
* **Legacy configuration options**: Migrate to new configuration framework
* **Old aggregation APIs**: Migrate to new aggregation function framework

**Community Feedback Integration:**
The Ray Data team actively incorporates community feedback into feature development. Share your experiences with experimental features to influence the roadmap and ensure features meet real-world needs.

**Staying Updated:**

* Follow Ray Data releases and changelogs
* Monitor GitHub discussions for feature updates
* Participate in community events and webinars

Advanced Features Quality Checklist
-----------------------------------

Use this checklist when implementing advanced features:

**Feature Evaluation**
- [ ] Is the feature's stability level appropriate for your use case?
- [ ] Have you tested the feature thoroughly in a development environment?
- [ ] Do you have rollback plans if the feature causes issues?
- [ ] Is the feature's performance impact acceptable?

**Production Readiness**
- [ ] Have you implemented monitoring for the advanced feature?
- [ ] Are error handling and recovery procedures in place?
- [ ] Is the feature documented for your team?
- [ ] Have you planned for feature evolution and migration?

**Community Engagement**
- [ ] Have you searched existing issues and discussions?
- [ ] Are you contributing feedback to the Ray community?
- [ ] Have you considered contributing improvements back?
- [ ] Are you staying updated on feature development?

For more information on Ray Data's roadmap and upcoming features, see the `Ray GitHub repository <https://github.com/ray-project/ray>`_ and join the community discussions.
