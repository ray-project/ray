.. _data_performance_guide:

==========================================
Ray Data Performance Optimization Guide
==========================================

.. meta::
   :description: Comprehensive guide to optimizing Ray Data performance with patterns, antipatterns, and step-by-step improvements.

Welcome to the comprehensive Ray Data performance optimization guide. This guide helps you systematically improve Ray Data performance through proven patterns, common antipattern identification, and targeted optimizations.

**New to Ray Data?** This guide is designed to be accessible even if you're just getting started. We'll explain Ray Data concepts as we go, but you'll get the most value if you first complete the :ref:`Ray Data Quickstart <data_quickstart>` to understand the basics.

**What is Ray Data?** Ray Data is a universal distributed data processing library that handles traditional ETL workloads, modern AI/ML pipelines, and emerging data processing patterns. Similar to Spark or Dask, it automatically distributes your data processing work across available CPUs and GPUs, making it possible to handle datasets that are too large for a single machine. 

Ray Data's architecture supports:
- **Traditional workloads**: ETL pipelines, data warehousing, batch analytics (proven patterns)
- **AI/ML workloads**: Feature engineering, model training data prep, batch inference (current focus)
- **Future workloads**: Real-time AI, streaming ML, multi-modal processing (emerging patterns)

Unlike systems designed for only one workload type, Ray Data provides a unified platform that scales from traditional data processing to cutting-edge AI applications.

**What is Performance Optimization?** Performance optimization means making your data processing faster, more memory-efficient, and more cost-effective. Ray Data provides many configuration options you can tune to potentially improve performance significantly with simple configuration changes.

.. toctree::
   :maxdepth: 2
   :hidden:

   getting-started
   reading-optimization
   transform-optimization
   memory-optimization
   advanced-operations
   patterns-antipatterns
   troubleshooting

Quick Performance Assessment
============================

.. raw:: html

   <div class="performance-assessment">
   <h3>Find Your Optimization Path</h3>
   <p>Answer a few questions to get personalized optimization recommendations:</p>
   </div>

**What's your primary Ray Data use case?**

.. grid:: 2 2 2 3
    :gutter: 2
    :class-container: assessment-grid

    .. grid-item-card:: ETL and Batch Processing
        :class-card: assessment-card
        
        Traditional ETL pipelines and large batch analytics jobs
        
        +++
        .. button-ref:: etl_batch_processing_path
            :color: primary
            :outline:
            :expand:
            
            Optimize ETL/Batch Processing

    .. grid-item-card:: Streaming and Real-time
        :class-card: assessment-card
        
        Real-time data processing and streaming analytics
        
        +++
        .. button-ref:: streaming_realtime_path
            :color: primary
            :outline:
            :expand:
            
            Optimize Streaming

    .. grid-item-card:: ML Training Data
        :class-card: assessment-card
        
        Preprocessing data for model training
        
        +++
        .. button-ref:: ml_training_path
            :color: primary
            :outline:
            :expand:
            
            Optimize ML Training

    .. grid-item-card:: Batch Inference
        :class-card: assessment-card
        
        Running inference on large datasets
        
        +++
        .. button-ref:: batch_inference_path
            :color: primary
            :outline:
            :expand:
            
            Optimize Batch Inference

    .. grid-item-card:: Transform Heavy
        :class-card: assessment-card
        
        Complex data transformations
        
        +++
        .. button-ref:: transform_path
            :color: primary
            :outline:
            :expand:
            
            Optimize Transforms

    .. grid-item-card:: Troubleshooting
        :class-card: assessment-card
        
        Fixing performance issues
        
        +++
        .. button-ref:: troubleshooting_path
            :color: primary
            :outline:
            :expand:
            
            Debug Performance

Performance Impact Quick Wins
=============================

Start with these high-impact optimizations that typically provide immediate benefits:

.. grid:: 1 2 2 2
    :gutter: 2
    :class-container: quick-wins-grid

    .. grid-item-card:: Use map_batches over map
        :class-card: quick-win-card
        
        **Impact**: Significant speedup for vectorizable operations
        **Time**: 5 minutes
        **Difficulty**: Easy
        
        +++
        Replace row-by-row operations with vectorized batch operations
        
        **Verification**: Check execution plan shows "MapBatches" instead of "Map"
        
        .. button-ref:: map_batches_optimization
            :color: success
            :outline:
            :size: sm
            
            Learn More

    .. grid-item-card:: Optimize block sizes
        :class-card: quick-win-card
        
        **Impact**: Potential throughput improvement
        **Time**: 10 minutes  
        **Difficulty**: Easy
        
        +++
        Tune `override_num_blocks` for your data size and cluster
        
        **Verification**: Check `ds.num_blocks()` matches your target
        
        .. button-ref:: block_size_optimization
            :color: success
            :outline:
            :size: sm
            
            Learn More

    .. grid-item-card:: Use column pruning
        :class-card: quick-win-card
        
        **Impact**: Substantial I/O reduction when reading wide tables
        **Time**: 2 minutes
        **Difficulty**: Easy
        
        +++
        Only read the columns you actually need
        
        **Verification**: Check `ds.schema().names` shows only selected columns
        
        .. button-ref:: column_pruning_optimization
            :color: success
            :outline:
            :size: sm
            
            Learn More

    .. grid-item-card:: Enable streaming execution
        :class-card: quick-win-card
        
        **Impact**: Significant memory reduction for large datasets
        **Time**: 1 minute
        **Difficulty**: Easy
        
        +++
        Process data without full materialization using Ray Data's :ref:`streaming execution model <streaming-execution>`
        
        **Verification**: Check memory usage stays constant during processing
        
        .. button-ref:: streaming_optimization
            :color: success
            :outline:
            :size: sm
            
            Learn More

Learning Paths
==============

Choose a structured learning path based on your experience level:

.. grid:: 1 3 3 3
    :gutter: 2
    :class-container: learning-paths-grid

    .. grid-item-card:: Beginner Path
        :class-card: path-card beginner-path
        
        **For**: New to Ray Data
        **Duration**: 2-4 hours
        **Outcome**: Solid foundation + first optimizations
        
        +++
        1. Ray Data basics
        2. Common patterns
        3. Simple optimizations
        4. Basic troubleshooting
        
        .. button-ref:: beginner_path
            :color: primary
            :expand:
            
            Start Beginner Path

    .. grid-item-card:: Quick Fixes Path  
        :class-card: path-card quickfix-path
        
        **For**: Immediate performance gains
        **Duration**: 30-60 minutes
        **Outcome**: 2-5x performance improvement
        
        +++
        1. Performance assessment
        2. High-impact optimizations
        3. Validation & measurement
        4. Next steps
        
        .. button-ref:: quickfix_path
            :color: warning
            :expand:
            
            Start Quick Fixes

    .. grid-item-card:: Expert Path
        :class-card: path-card expert-path
        
        **For**: Advanced optimization mastery
        **Duration**: 8-12 hours
        **Outcome**: Expert-level optimization skills
        
        +++
        1. Advanced patterns
        2. Complex optimizations
        3. Custom solutions
        4. Contributing back
        
        .. button-ref:: expert_path
            :color: info
            :expand:
            
            Start Expert Path

Common Antipatterns to Avoid
=============================

Learn to recognize and fix these common Ray Data performance killers:

.. list-table:: Top Ray Data Antipatterns
   :header-rows: 1
   :class: antipattern-table

   * - Antipattern
     - Impact
     - Quick Fix
     - Learn More
   * - Using pandas batch_format unnecessarily
     - 2-5x slowdown
     - Use native formats
     - :ref:`pandas_antipattern`
   * - Reading too many small files
     - 10-50x slowdown  
     - Consolidate files
     - :ref:`small_files_antipattern`
   * - Not using vectorized operations
     - 5-20x slowdown
     - Use map_batches with vectorization
     - :ref:`vectorization_antipattern`
   * - Ignoring memory pressure
     - OOM crashes
     - Tune batch sizes and memory limits
     - :ref:`memory_antipattern`
   * - Wrong concurrency settings
     - Resource underutilization
     - Profile and tune concurrency
     - :ref:`concurrency_antipattern`

.. note::
   **Pro Tip**: Use the :ref:`antipattern_detector` to automatically scan your Ray Data code for common performance issues.

Security and Compliance Considerations
======================================

When optimizing Ray Data performance, consider these security and compliance implications:

**Data Security:**
- Column pruning reduces data exposure by limiting columns read into memory
- Memory optimization prevents sensitive data from being written to disk during spilling
- Streaming execution minimizes data persistence in the object store

**Compliance Considerations:**
- Monitor data lineage when applying optimizations that change processing order
- Ensure audit logs capture performance configuration changes
- Verify that optimizations don't affect data retention policies

**Production Safety:**
- Test optimizations in non-production environments first
- Monitor performance changes to detect regressions
- Implement gradual rollout of optimization changes

Performance Measurement & Monitoring
====================================

Track your optimization progress with these essential metrics:

.. code-block:: python

   import ray
   
   # Enable detailed stats collection
   ctx = ray.data.DataContext.get_current()
   ctx.enable_progress_bars = True
   ctx.enable_operator_progress_bars = True
   
   # Run your pipeline
   ds = ray.data.read_parquet("s3://my-bucket/data/")
   result = ds.map_batches(my_transform).write_parquet("s3://output/")
   
   # Analyze performance
   print(result.stats())

**Key metrics to monitor:**

- **Throughput**: Rows/second processed
- **Memory usage**: Peak and average object store usage  
- **Resource utilization**: CPU/GPU utilization percentages
- **I/O efficiency**: Data read/write speeds
- **Task execution time**: Time spent in different operations
- **Cost efficiency**: Processing cost per GB or per row (for cloud deployments)

**Cost Optimization Considerations:**

Performance optimizations may help reduce cloud computing costs through:
- **Reducing runtime**: More efficient processing can lower compute costs
- **Improving resource utilization**: Better CPU/GPU usage may reduce idle time
- **Minimizing data transfer**: Column pruning and compression can reduce network costs
- **Preventing failures**: More reliable processing may avoid retry costs

**Production Deployment Considerations:**

When deploying Ray Data optimizations in production environments:

- **Gradual rollout**: Test optimizations on subsets of data before full deployment
- **Monitoring integration**: Ensure Ray Dashboard metrics are captured in your monitoring systems
- **Resource planning**: Size clusters based on optimized performance characteristics
- **Failure handling**: Design pipelines to handle node failures and data corruption gracefully
- **Performance regression detection**: Monitor key metrics to catch performance degradation
- **Documentation**: Document optimization choices for team knowledge sharing

Community & Support
===================

.. grid:: 1 2 2 2
    :gutter: 2

    .. grid-item-card:: Ray Data Community
        
        Get help from the community and Ray Data experts
        
        +++
        `Join Ray Slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_ • `GitHub Discussions <https://github.com/ray-project/ray/discussions>`_

    .. grid-item-card:: Additional Resources
        
        More learning materials and advanced topics
        
        +++
        `Ray Data Blog <https://www.anyscale.com/blog/tag/ray-data>`_ • `Performance Benchmarks <https://github.com/ray-project/ray/tree/master/release/benchmarks>`_

    .. grid-item-card:: Report Issues
        
        Found a performance issue or have suggestions?
        
        +++
        `File an Issue <https://github.com/ray-project/ray/issues/new?template=bug-report.yml&labels=bug%2Ctriage%2Cdata>`_ • `Feature Request <https://github.com/ray-project/ray/issues/new?template=feature_request.yml&labels=enhancement%2Ctriage%2Cdata>`_

    .. grid-item-card:: Contribute
        
        Help improve Ray Data performance for everyone
        
        +++
        `Contributing Guide <https://docs.ray.io/en/latest/ray-contribute/getting-involved.html>`_ • `Performance Optimization PRs Welcome <https://github.com/ray-project/ray/labels/data>`_

Next Steps
==========

Ready to optimize your Ray Data performance? Here's what to do next:

1. **Take the assessment** above to find your optimization path
2. **Start with quick wins** for immediate improvements  
3. **Follow a learning path** for systematic skill building
4. **Join the community** for ongoing support and learning

.. tip::
   **New to Ray Data?** Start with the :ref:`data_quickstart` to learn the basics, then return here for performance optimization.

   **Experienced user?** Jump straight to the :ref:`expert_path` or browse specific optimization topics in the navigation.

**Performance Optimization by Data Size:**

Different dataset sizes require different optimization strategies:

- **Small datasets (< 1GB)**: Focus on reducing task overhead, fewer blocks, simple optimizations
- **Medium datasets (1-100GB)**: Balance parallelism and efficiency, standard block sizes
- **Large datasets (100GB-1TB)**: Maximize parallelism, optimize memory usage, streaming execution
- **Very large datasets (> 1TB)**: Advanced techniques, cluster scaling, specialized optimizations

**Performance Optimization by Cluster Size:**

Optimization strategies vary with cluster configuration:

- **Single node**: Optimize for local processing, minimize overhead
- **Small clusters (2-10 nodes)**: Balance network and compute, moderate parallelism
- **Medium clusters (10-50 nodes)**: Optimize data distribution, network efficiency
- **Large clusters (50+ nodes)**: Advanced coordination, fault tolerance, load balancing

**See also:**
- :ref:`data_key_concepts` - Understand Ray Data fundamentals including blocks and streaming execution
- :ref:`data_user_guide` - General Ray Data usage patterns and best practices
- :ref:`data-internals` - Deep dive into Ray Data architecture and internals
- :ref:`data-api` - Complete API reference for all Ray Data operations
