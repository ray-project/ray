.. _loading-overview:

Loading Data: Essential Guide
=============================

**Keywords:** Ray Data loading, data sources, file formats, database connectivity, data ingestion, distributed loading

Data loading is the foundation of any data processing workflow. This essential guide covers the most important data loading patterns and best practices for Ray Data.

**What you'll learn:**

* Load data from the most common sources (files, databases, cloud storage)
* Choose the right loading approach for your data type and use case
* Optimize loading performance for production workloads
* Handle common loading challenges and errors

Quick Start: Load Any Data Source
----------------------------------

Ray Data provides unified loading for 26+ data sources with consistent APIs:

.. code-block:: python

    import ray

    # Load files (most common)
    csv_data = ray.data.read_csv("s3://bucket/data.csv")
    parquet_data = ray.data.read_parquet("s3://bucket/data.parquet")
    
    # Load from databases
    sql_data = ray.data.read_sql("SELECT * FROM table", connection_factory)
    
    # Load unstructured data
    images = ray.data.read_images("s3://bucket/images/")
    text = ray.data.read_text("s3://bucket/documents/")

**Why Ray Data for loading:**
- **Universal API**: Single interface for all data types
- **Distributed loading**: Parallel loading across cluster nodes
- **Streaming execution**: Handle datasets larger than memory
- **Format optimization**: Native optimization for each data format

Common Loading Patterns
-----------------------

**Pattern 1: File-Based Loading**

.. code-block:: python

    # Load structured files
    sales_data = ray.data.read_parquet("s3://data-lake/sales/")
    
    # Load with column selection for performance
    essential_cols = ray.data.read_parquet(
        "s3://data-lake/sales/",
        columns=["customer_id", "amount", "date"]
    )
    
    # Load with filters for efficiency
    recent_sales = ray.data.read_parquet(
        "s3://data-lake/sales/",
        filter="date >= '2024-01-01'"
    )

**Pattern 2: Database Loading**

.. code-block:: python

    # Load from SQL databases
    customer_data = ray.data.read_sql(
        "SELECT * FROM customers WHERE active = true",
        connection_factory
    )
    
    # Load from data warehouses
    snowflake_data = ray.data.read_snowflake(
        "SELECT * FROM analytics.customer_metrics",
        connection_parameters={
            "user": "analyst",
            "account": "company",
            "database": "ANALYTICS",
            "schema": "PUBLIC"
        }
    )

**Pattern 3: Unstructured Data Loading**

.. code-block:: python

    # Load images for computer vision
    product_images = ray.data.read_images("s3://products/images/")
    
    # Load text for NLP
    documents = ray.data.read_text("s3://documents/")
    
    # Load audio for analysis
    audio_files = ray.data.read_audio("s3://audio/")

Performance Optimization
------------------------

**Loading Performance Best Practices:**

**Optimize File Reading:**
- **Use column selection**: Only load required columns
- **Apply filters early**: Push filters to data source when possible
- **Optimize block sizes**: Configure for your workload type
- **Parallel loading**: Use appropriate parallelization

**Database Optimization:**
- **Use connection pooling**: Efficient database connections
- **Optimize queries**: Use selective queries with proper indexing
- **Batch loading**: Load data in appropriate batch sizes
- **Connection management**: Handle connection failures gracefully

**Cloud Storage Optimization:**
- **Use native formats**: Parquet for analytics, optimized formats for AI
- **Optimize partitioning**: Use proper partitioning schemes
- **Configure credentials**: Use appropriate authentication methods
- **Monitor costs**: Track storage and transfer costs

Common Issues and Solutions
---------------------------

**Issue 1: Out of Memory Errors**
- **Solution**: Use streaming execution and optimize block sizes
- **Configuration**: Reduce `target_max_block_size` for memory-constrained environments
- **Pattern**: Load data incrementally rather than all at once

**Issue 2: Slow Loading Performance**
- **Solution**: Increase parallelization and optimize data source
- **Configuration**: Use `override_num_blocks` for more parallel tasks
- **Pattern**: Use column selection and filtering to reduce data volume

**Issue 3: Connection Failures**
- **Solution**: Implement retry logic and connection validation
- **Configuration**: Use connection pooling and timeout settings
- **Pattern**: Validate connections before large loading operations

Next Steps
----------

**Master Data Loading:**

**For Specific Data Types:**
→ :ref:`Data Type Guides <data_types>` - Specialized loading patterns for images, text, etc.

**For Database Integration:**
→ :ref:`Data Warehouses <data-warehouses>` - Enterprise database connectivity

**For Performance Optimization:**
→ :ref:`Performance Optimization <performance-optimization>` - Advanced loading optimization

**For Production Deployment:**
→ :ref:`Best Practices <best_practices>` - Production loading patterns and monitoring
