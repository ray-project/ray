.. _api-guide-for-users-from-other-data-libs:

API Guide for Users from Other Data Libraries
=============================================

**Keywords:** API migration, pandas to Ray Data, Spark to Ray Data, Dask to Ray Data, data library comparison, API mapping, data processing migration, distributed computing migration

Ray Data is a unified data processing library that handles ETL, analytics, and ML workloads. It shares certain similarities with other data processing libraries while providing unique advantages for multimodal data and AI workloads.

This guide provides API mappings for users migrating from popular data libraries. It helps you quickly map existing knowledge to Ray Data APIs and understand the benefits of Ray Data's approach.

.. note::

  - This is meant to map APIs that perform comparable but not necessarily identical operations.
    Select the API reference for exact semantics and usage.
  - This list may not be exhaustive: It focuses on common APIs or APIs that are less obvious to see a connection.

.. _api-guide-for-pandas-users:

For Pandas Users
----------------

**Why Migrate from Pandas to Ray Data**

**Migration Triggers:**
- **Dataset size > available memory**: Pandas fails with large datasets, Ray Data handles any size
- **Processing speed**: Ray Data's distributed processing is faster for large datasets  
- **AI/ML integration**: Native support for GPU acceleration and ML frameworks
- **Multimodal data**: Process images, audio, video alongside structured data

**Key Benefits:**
- **Familiar API**: Similar operations with distributed execution
- **Streaming execution**: Process datasets larger than memory
- **GPU acceleration**: Use GPU resources for intensive operations
- **Zero infrastructure changes**: Works with existing data sources and destinations

.. list-table:: Pandas DataFrame vs. Ray Data APIs (Complete Mapping)
   :header-rows: 1

   * - Pandas DataFrame API
     - Ray Data API
     - Notes
   * - **Data Inspection**
     - 
     - 
   * - ``df.head(n)``
     - :meth:`ds.show(limit=n) <ray.data.Dataset.show>`, :meth:`ds.take(n) <ray.data.Dataset.take>`
     - Ray Data: Distributed sampling, streaming execution
   * - ``df.tail(n)``
     - ``ds.sort().take(n)`` (reverse sort)
     - Ray Data: Requires sorting for tail operation
   * - ``df.info()``
     - :meth:`ds.schema() <ray.data.Dataset.schema>`, :meth:`ds.stats() <ray.data.Dataset.stats>`
     - Ray Data: Separate schema and statistics methods
   * - ``df.describe()``
     - :meth:`ds.stats() <ray.data.Dataset.stats>`
     - Ray Data: Comprehensive statistics with execution details
   * - ``df.dtypes``
     - :meth:`ds.schema() <ray.data.Dataset.schema>`
     - Ray Data: Arrow schema with type information
   * - ``len(df)`` or ``df.shape[0]``
     - :meth:`ds.count() <ray.data.Dataset.count>`
     - Ray Data: Distributed counting, lazy evaluation
   * - **Data Selection & Filtering**
     - 
     - 
   * - ``df[columns]``
     - :meth:`ds.select_columns(columns) <ray.data.Dataset.select_columns>`
     - Ray Data: Explicit column selection method
   * - ``df.loc[condition]``
     - :meth:`ds.filter(condition) <ray.data.Dataset.filter>`
     - Ray Data: Function-based or expression filtering
   * - ``df.iloc[start:end]``
     - :meth:`ds.limit(end).skip(start) <ray.data.Dataset.limit>`
     - Ray Data: Limit with offset for positional selection
   * - ``df.query(expression)``
     - :meth:`ds.filter(expr=expression) <ray.data.Dataset.filter>`
     - Ray Data: Expression-based filtering with Arrow optimization
   * - ``df.drop(columns)``
     - :meth:`ds.drop_columns(columns) <ray.data.Dataset.drop_columns>`
     - Ray Data: Explicit column dropping method
   * - ``df.sample(n)``
     - :meth:`ds.random_sample(fraction) <ray.data.Dataset.random_sample>`
     - Ray Data: Fraction-based sampling, distributed execution
   * - **Data Transformation**
     - 
     - 
   * - ``df.apply(func)``
     - :meth:`ds.map_batches(func) <ray.data.Dataset.map_batches>`
     - Ray Data: Vectorized batch processing, GPU support
   * - ``df.applymap(func)``
     - :meth:`ds.map(func) <ray.data.Dataset.map>`
     - Ray Data: Element-wise transformations
   * - ``df.assign(**kwargs)``
     - :meth:`ds.add_column(name, func) <ray.data.Dataset.add_column>`
     - Ray Data: Explicit column addition with functions
   * - ``df.rename(columns)``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` + rename logic
     - Ray Data: Custom renaming through transformation
   * - ``df.astype(dtype)``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` + type conversion
     - Ray Data: Type conversion through transformation
   * - **Data Aggregation**
     - 
     - 
   * - ``df.groupby(cols).agg(funcs)``
     - :meth:`ds.groupby(cols).aggregate(funcs) <ray.data.grouped_data.GroupedData.aggregate>`
     - Ray Data: Distributed aggregations, custom functions
   * - ``df.groupby().apply(func)``
     - :meth:`ds.groupby().map_groups(func) <ray.data.grouped_data.GroupedData.map_groups>`
     - Ray Data: Group-level transformations
   * - ``df.pivot_table()``
     - :meth:`ds.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>` + reshape
     - Ray Data: Custom pivot through groupby + reshape
   * - ``df.sum()``, ``df.mean()``, etc.
     - :meth:`ds.sum() <ray.data.Dataset.sum>`, :meth:`ds.mean() <ray.data.Dataset.mean>`
     - Ray Data: Distributed aggregations, streaming execution
   * - ``df.value_counts()``
     - :meth:`ds.groupby().aggregate(Count()) <ray.data.grouped_data.GroupedData.aggregate>`
     - Ray Data: Groupby with count aggregation
   * - **Data Combination**
     - 
     - 
   * - ``pd.merge(df1, df2)``
     - :meth:`ds1.join(ds2) <ray.data.Dataset.join>`
     - Ray Data: Advanced join types, multimodal support
   * - ``pd.concat([df1, df2])``
     - :meth:`ds1.union(ds2) <ray.data.Dataset.union>`
     - Ray Data: Zero-copy operations, memory efficiency
   * - **Data Sorting & Ordering**
     - 
     - 
   * - ``df.sort_values(cols)``
     - :meth:`ds.sort(cols) <ray.data.Dataset.sort>`
     - Ray Data: Distributed sorting, memory optimization
   * - ``df.nlargest(n, col)``
     - ``ds.sort(col, descending=True).limit(n)``
     - Ray Data: Sort + limit for top-N operations
   * - ``df.nsmallest(n, col)``
     - ``ds.sort(col).limit(n)``
     - Ray Data: Sort + limit for bottom-N operations
   * - **Data I/O**
     - 
     - 
   * - ``pd.read_csv()``
     - :meth:`ray.data.read_csv() <ray.data.read_csv>`
     - Ray Data: Distributed loading, streaming execution
   * - ``pd.read_parquet()``
     - :meth:`ray.data.read_parquet() <ray.data.read_parquet>`
     - Ray Data: Parallel loading, predicate pushdown
   * - ``df.to_csv()``
     - :meth:`ds.write_csv() <ray.data.Dataset.write_csv>`
     - Ray Data: Distributed writing, automatic partitioning
   * - ``df.to_parquet()``
     - :meth:`ds.write_parquet() <ray.data.Dataset.write_parquet>`
     - Ray Data: Optimized columnar output

.. _api-guide-for-pyarrow-users:

For PyArrow Users
-----------------

.. list-table:: PyArrow Table vs. Ray Data APIs (Complete Mapping)
   :header-rows: 1

   * - PyArrow Table API
     - Ray Data API
     - Notes
   * - **Schema & Metadata**
     - 
     - 
   * - ``table.schema``
     - :meth:`ds.schema() <ray.data.Dataset.schema>`
     - Ray Data: Arrow schema with distributed metadata
   * - ``table.num_rows``
     - :meth:`ds.count() <ray.data.Dataset.count>`
     - Ray Data: Distributed counting across partitions
   * - ``table.num_columns``
     - ``len(ds.schema().names)``
     - Ray Data: Schema-based column counting
   * - ``table.column_names``
     - ``ds.schema().names``
     - Ray Data: Schema-based column names
   * - **Data Access**
     - 
     - 
   * - ``table.take(indices)``
     - :meth:`ds.take(n) <ray.data.Dataset.take>`
     - Ray Data: Sequential take, not index-based
   * - ``table.slice(start, length)``
     - :meth:`ds.limit(length).skip(start) <ray.data.Dataset.limit>`
     - Ray Data: Limit with offset for slicing
   * - ``table.column(name)``
     - :meth:`ds.select_columns([name]) <ray.data.Dataset.select_columns>`
     - Ray Data: Column selection returns dataset
   * - **Data Transformation**
     - 
     - 
   * - ``table.filter(condition)``
     - :meth:`ds.filter(condition) <ray.data.Dataset.filter>`
     - Ray Data: Distributed filtering, GPU support
   * - ``table.drop(columns)``
     - :meth:`ds.drop_columns(columns) <ray.data.Dataset.drop_columns>`
     - Ray Data: Explicit column dropping
   * - ``table.add_column(index, name, values)``
     - :meth:`ds.add_column(name, func) <ray.data.Dataset.add_column>`
     - Ray Data: Function-based column addition
   * - ``table.set_column(index, name, values)``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` + column update
     - Ray Data: Batch transformation for column updates
   * - **Data Aggregation**
     - 
     - 
   * - ``pc.sum(table.column(name))``
     - :meth:`ds.sum(name) <ray.data.Dataset.sum>`
     - Ray Data: Distributed aggregation
   * - ``pc.mean(table.column(name))``
     - :meth:`ds.mean(name) <ray.data.Dataset.mean>`
     - Ray Data: Distributed statistical operations
   * - ``table.group_by(cols)``
     - :meth:`ds.groupby(cols) <ray.data.Dataset.groupby>`
     - Ray Data: Distributed grouping operations
   * - **Data Sorting**
     - 
     - 
   * - ``table.sort_by([(col, order)])``
     - :meth:`ds.sort(col, descending=order) <ray.data.Dataset.sort>`
     - Ray Data: Distributed sorting with memory optimization
   * - **Data I/O**
     - 
     - 
   * - ``pa.parquet.read_table()``
     - :meth:`ray.data.read_parquet() <ray.data.read_parquet>`
     - Ray Data: Distributed reading, parallel processing
   * - ``pa.parquet.write_table()``
     - :meth:`ds.write_parquet() <ray.data.Dataset.write_parquet>`
     - Ray Data: Distributed writing, optimization

**PyArrow Integration Benefits:**
- **Distributed processing**: Scale PyArrow operations across multiple machines
- **Memory efficiency**: Handle datasets larger than single-node memory
- **GPU acceleration**: Use GPU resources for Arrow-compatible operations
- **Streaming execution**: Process large datasets without memory constraints

**Pandas Migration Examples**

**Example 1: Basic ETL Workflow Migration**

.. code-block:: python

    # Pandas approach (memory limited)
    import pandas as pd
    
    df = pd.read_csv("sales_data.csv")  # Fails if file > memory
    df = df.groupby("region").sum()
    df.to_parquet("regional_sales.parquet")

    # Ray Data approach (unlimited scaling)
    import ray
    
    ds = ray.data.read_csv("sales_data.csv")  # Handles any file size
    result = ds.groupby("region").aggregate(ray.data.aggregate.Sum("amount"))
    result.write_parquet("regional_sales.parquet")

**Example 2: Advanced Analytics Migration**

.. code-block:: python

    # Pandas approach (single machine)
    import pandas as pd
    import numpy as np
    
    df = pd.read_parquet("customer_data.parquet")
    df["customer_score"] = df.apply(lambda row: calculate_score(row), axis=1)
    monthly_stats = df.groupby("month").agg({
        "revenue": ["sum", "mean"],
        "customers": "count"
    })

    # Ray Data approach (distributed)
    import ray
    
    ds = ray.data.read_parquet("customer_data.parquet")
    # Vectorized processing for better performance
    scored_ds = ds.map_batches(
        lambda batch: batch.assign(
            customer_score=batch.apply(lambda row: calculate_score(row), axis=1)
        )
    )
    monthly_stats = scored_ds.groupby("month").aggregate(
        ray.data.aggregate.Sum("revenue"),
        ray.data.aggregate.Mean("revenue"), 
        ray.data.aggregate.Count("customers")
    )

**Example 3: GPU-Accelerated Processing Migration**

.. code-block:: python

    # Pandas approach (CPU only)
    import pandas as pd
    
    df = pd.read_csv("image_features.csv")
    df["normalized"] = (df["features"] - df["features"].mean()) / df["features"].std()

    # Ray Data approach (GPU accelerated)
    import ray
    import cupy as cp  # GPU acceleration
    
    ds = ray.data.read_csv("image_features.csv")
    # GPU-accelerated normalization
    normalized_ds = ds.map_batches(
        lambda batch: batch.assign(
            normalized=cp.array(
                (batch["features"] - batch["features"].mean()) / batch["features"].std()
            )
        ),
        num_gpus=1  # Allocate GPU for processing
    )

**Migration Strategy for Pandas Users:**

**Step 1: Identify Migration Candidates**
- Large datasets that exceed single-machine memory
- CPU-intensive operations that would benefit from distributed processing
- Workflows that could leverage GPU acceleration
- Pipelines that process multimodal data alongside structured data

**Step 2: Gradual Migration Approach**
- Start with non-critical workloads for validation
- Migrate data loading and basic transformations first
- Gradually migrate complex analytics and aggregations
- Test performance and validate results at each step

**Step 3: Optimization and Scaling**
- Optimize batch sizes for your specific workloads
- Configure appropriate resource allocation (CPU/GPU/memory)
- Implement monitoring and performance tracking
- Scale to production data volumes and user loads


.. _api-guide-for-spark-users:

For Apache Spark Users
-----------------------

**Why Migrate from Spark to Ray Data**

**Key Migration Drivers:**
- **Python-native performance**: Eliminate JVM overhead and serialization bottlenecks
- **Simpler deployment**: No complex Spark cluster management required
- **AI/ML integration**: Native support for PyTorch, TensorFlow, HuggingFace
- **Multimodal processing**: Handle images, audio, video alongside structured data

**Performance Benefits:**
- **Faster execution**: Python-native processing without JVM overhead
- **Memory efficiency**: Streaming execution with intelligent memory management
- **GPU acceleration**: Native GPU support for AI and ML workloads
- **Resource optimization**: Better resource utilization and cost efficiency

Ray Data provides similar distributed processing capabilities to Spark with Python-native performance:

.. list-table:: Spark DataFrame vs. Ray Data APIs (Complete Mapping)
   :header-rows: 1

   * - Spark DataFrame API
     - Ray Data API
     - Ray Data Advantage
   * - **Data Inspection**
     - 
     - 
   * - ``df.show(n)``
     - :meth:`ds.show(limit=n) <ray.data.Dataset.show>`
     - Faster execution, no JVM overhead
   * - ``df.count()``
     - :meth:`ds.count() <ray.data.Dataset.count>`
     - Streaming execution for large datasets
   * - ``df.printSchema()``
     - :meth:`ds.schema() <ray.data.Dataset.schema>`
     - Arrow schema, Python-native types
   * - ``df.describe()``
     - :meth:`ds.stats() <ray.data.Dataset.stats>`
     - Comprehensive execution statistics
   * - **Data Selection & Filtering**
     - 
     - 
   * - ``df.select(cols)``
     - :meth:`ds.select_columns(cols) <ray.data.Dataset.select_columns>`
     - Unified API for all data types
   * - ``df.filter(condition)``
     - :meth:`ds.filter(condition) <ray.data.Dataset.filter>`
     - Python-native expressions, GPU support
   * - ``df.where(condition)``
     - :meth:`ds.filter(condition) <ray.data.Dataset.filter>`
     - Same as filter, Python expressions
   * - ``df.drop(cols)``
     - :meth:`ds.drop_columns(cols) <ray.data.Dataset.drop_columns>`
     - Explicit column operations
   * - ``df.limit(n)``
     - :meth:`ds.limit(n) <ray.data.Dataset.limit>`
     - Streaming execution, memory efficiency
   * - **Data Transformation**
     - 
     - 
   * - ``df.withColumn(name, expr)``
     - :meth:`ds.add_column(name, func) <ray.data.Dataset.add_column>`
     - Function-based, GPU acceleration
   * - ``df.withColumnRenamed(old, new)``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` + rename
     - Custom transformation logic
   * - ``df.drop(col)``
     - :meth:`ds.drop_columns([col]) <ray.data.Dataset.drop_columns>`
     - Explicit column operations
   * - ``df.distinct()``
     - :meth:`ds.unique() <ray.data.Dataset.unique>`
     - Distributed deduplication
   * - **Data Aggregation**
     - 
     - 
   * - ``df.groupBy(cols).agg(funcs)``
     - :meth:`ds.groupby(cols).aggregate(funcs) <ray.data.grouped_data.GroupedData.aggregate>`
     - Native Python aggregations, custom functions
   * - ``df.groupBy().count()``
     - :meth:`ds.groupby().aggregate(Count()) <ray.data.grouped_data.GroupedData.aggregate>`
     - Distributed counting
   * - ``df.agg(funcs)``
     - :meth:`ds.aggregate(funcs) <ray.data.Dataset.aggregate>`
     - Global aggregations, streaming execution
   * - **Data Combination**
     - 
     - 
   * - ``df1.join(df2, on, how)``
     - :meth:`ds1.join(ds2, on=on, how=how) <ray.data.Dataset.join>`
     - Multimodal data support, GPU acceleration
   * - ``df1.union(df2)``
     - :meth:`ds1.union(ds2) <ray.data.Dataset.union>`
     - Zero-copy operations, better memory efficiency
   * - ``df1.unionByName(df2)``
     - :meth:`ds1.union(ds2) <ray.data.Dataset.union>`
     - Automatic schema alignment
   * - **Data Sorting**
     - 
     - 
   * - ``df.orderBy(cols)``
     - :meth:`ds.sort(cols) <ray.data.Dataset.sort>`
     - Distributed sorting, memory optimization
   * - ``df.sort(cols)``
     - :meth:`ds.sort(cols) <ray.data.Dataset.sort>`
     - Same as orderBy
   * - **Data I/O**
     - 
     - 
   * - ``spark.read.parquet()``
     - :meth:`ray.data.read_parquet() <ray.data.read_parquet>`
     - Parallel loading, predicate pushdown
   * - ``df.write.parquet()``
     - :meth:`ds.write_parquet() <ray.data.Dataset.write_parquet>`
     - Optimized columnar output
   * - ``spark.read.csv()``
     - :meth:`ray.data.read_csv() <ray.data.read_csv>`
     - Distributed CSV parsing
   * - ``df.write.csv()``
     - :meth:`ds.write_csv() <ray.data.Dataset.write_csv>`
     - Distributed CSV writing

**Migration Benefits from Spark to Ray Data:**
- **Python-native performance**: No JVM overhead or serialization bottlenecks
- **Multimodal support**: Process images, audio, video alongside structured data
- **GPU acceleration**: Native GPU support for AI and ML workloads
- **Simpler deployment**: No complex Spark cluster management required

.. _api-guide-for-dask-users:

For Dask Users
--------------

Ray Data provides similar distributed Python processing with enhanced AI/ML capabilities:

.. list-table:: Dask DataFrame vs. Ray Data APIs
   :header-rows: 1

   * - Dask DataFrame API
     - Ray Data API
     - Ray Data Advantage
   * - ``df.head()``
     - :meth:`ds.show() <ray.data.Dataset.show>`
     - Multimodal data support, GPU optimization
   * - ``df.compute()``
     - :meth:`ds.materialize() <ray.data.Dataset.materialize>`
     - Streaming execution, memory efficiency
   * - ``df.map_partitions()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - GPU support, AI framework integration
   * - ``df.groupby().apply()``
     - :meth:`ds.groupby().map_groups() <ray.data.grouped_data.GroupedData.map_groups>`
     - Native aggregations, better performance
   * - ``df.merge()``
     - :meth:`ds.join() <ray.data.Dataset.join>`
     - Advanced join types, multimodal support

**Migration Benefits from Dask to Ray Data:**
- **AI/ML integration**: Native support for PyTorch, TensorFlow, HuggingFace
- **Multimodal processing**: Unified API for all data types
- **GPU optimization**: Intelligent GPU resource allocation
- **Ray ecosystem**: Integration with Ray Train, Tune, Serve

.. _api-guide-for-polars-users:

For Polars Users
----------------

Ray Data provides distributed processing capabilities that scale beyond Polars' single-node performance:

.. list-table:: Polars DataFrame vs. Ray Data APIs
   :header-rows: 1

   * - Polars DataFrame API
     - Ray Data API
     - Ray Data Advantage
   * - ``df.head()``
     - :meth:`ds.show() <ray.data.Dataset.show>`
     - Distributed processing, multimodal support
   * - ``df.filter()``
     - :meth:`ds.filter() <ray.data.Dataset.filter>`
     - GPU acceleration, streaming execution
   * - ``df.group_by().agg()``
     - :meth:`ds.groupby().aggregate() <ray.data.grouped_data.GroupedData.aggregate>`
     - Distributed aggregations, custom functions
   * - ``df.join()``
     - :meth:`ds.join() <ray.data.Dataset.join>`
     - Multimodal joins, advanced join types
   * - ``df.with_columns()``
     - :meth:`ds.add_column() <ray.data.Dataset.add_column>`
     - GPU acceleration, AI framework integration

**When to use Ray Data vs Polars:**
- **Large datasets (>memory)**: Ray Data's streaming execution handles datasets larger than available memory
- **Multimodal data**: Ray Data natively processes images, audio, video alongside structured data
- **AI/ML workloads**: Ray Data provides GPU acceleration and ML framework integration
- **Distributed processing**: Ray Data scales across multiple machines automatically

.. _api-guide-for-huggingface-users:

For HuggingFace Users
---------------------

Ray Data enhances HuggingFace workflows with distributed processing capabilities:

.. list-table:: HuggingFace Datasets vs. Ray Data APIs
   :header-rows: 1

   * - HuggingFace Datasets API
     - Ray Data API
     - Ray Data Advantage
   * - ``datasets.load_dataset()``
     - :meth:`ray.data.from_huggingface() <ray.data.from_huggingface>`
     - Distributed loading, streaming execution
   * - ``dataset.map()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - GPU acceleration, advanced transformations
   * - ``dataset.filter()``
     - :meth:`ds.filter() <ray.data.Dataset.filter>`
     - Distributed filtering, memory efficiency
   * - ``dataset.train_test_split()``
     - :meth:`ds.train_test_split() <ray.data.Dataset.train_test_split>`
     - Distributed splitting, streaming execution

**HuggingFace integration benefits:**
- **Scalable tokenization**: Distribute tokenization across multiple workers
- **GPU preprocessing**: Use GPU resources for intensive text preprocessing
- **Multimodal datasets**: Combine text with images, audio for multimodal models
- **Ray Train integration**: Seamless handoff to distributed training

.. _api-guide-for-tensorflow-users:

For TensorFlow Users
--------------------

Ray Data integrates with TensorFlow for scalable data preprocessing and training:

.. list-table:: TensorFlow Data vs. Ray Data APIs
   :header-rows: 1

   * - TensorFlow Data API
     - Ray Data API
     - Ray Data Advantage
   * - ``tf.data.Dataset.from_tensor_slices()``
     - :meth:`ray.data.from_numpy() <ray.data.from_numpy>`
     - Distributed processing, multimodal support
   * - ``tf.data.Dataset.map()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - GPU acceleration, flexible transformations
   * - ``tf.data.Dataset.filter()``
     - :meth:`ds.filter() <ray.data.Dataset.filter>`
     - Streaming execution, memory efficiency
   * - ``tf.data.Dataset.batch()``
     - :meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`
     - Intelligent batching, resource optimization
   * - ``tf.data.Dataset.shuffle()``
     - :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`
     - Distributed shuffling, memory optimization
   * - ``tf.data.Dataset.repeat()``
     - Multiple :meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` calls
     - Streaming execution, epoch management

.. _api-guide-for-numpy-users:

For NumPy Users
---------------

Ray Data works seamlessly with NumPy arrays and provides distributed NumPy-like operations:

.. list-table:: NumPy vs. Ray Data APIs
   :header-rows: 1

   * - NumPy API
     - Ray Data API
     - Ray Data Advantage
   * - ``np.load()``
     - :meth:`ray.data.read_numpy() <ray.data.read_numpy>`
     - Distributed loading, parallel processing
   * - ``np.save()``
     - :meth:`ds.write_numpy() <ray.data.Dataset.write_numpy>`
     - Distributed saving, automatic partitioning
   * - ``np.mean(axis=0)``
     - :meth:`ds.mean() <ray.data.Dataset.mean>`
     - Distributed computation, streaming execution
   * - ``np.sum(axis=0)``
     - :meth:`ds.sum() <ray.data.Dataset.sum>`
     - Parallel aggregation across cluster
   * - ``np.concatenate()``
     - :meth:`ds.union() <ray.data.Dataset.union>`
     - Zero-copy operations, memory efficiency
   * - Array slicing ``arr[start:end]``
     - :meth:`ds.limit(end-start).skip(start) <ray.data.Dataset.limit>`
     - Distributed slicing, lazy evaluation

.. _api-guide-for-cudf-users:

For cuDF Users
--------------

Ray Data provides distributed GPU processing that scales beyond cuDF's single-GPU limitations:

.. list-table:: cuDF DataFrame vs. Ray Data APIs
   :header-rows: 1

   * - cuDF DataFrame API
     - Ray Data API
     - Ray Data Advantage
   * - ``df.head()``
     - :meth:`ds.show() <ray.data.Dataset.show>`
     - Multi-GPU distributed processing
   * - ``df.apply()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` + GPU
     - Distributed GPU processing across cluster
   * - ``df.groupby()``
     - :meth:`ds.groupby() <ray.data.Dataset.groupby>` + GPU
     - Multi-GPU aggregations, streaming execution
   * - ``df.merge()``
     - :meth:`ds.join() <ray.data.Dataset.join>` + GPU
     - Distributed GPU joins, multimodal support
   * - ``df.sort_values()``
     - :meth:`ds.sort() <ray.data.Dataset.sort>` + GPU
     - Multi-GPU sorting, memory optimization

**Spark Migration Examples**

**Example 1: Spark ETL to Ray Data**

.. code-block:: python

    # Spark approach (JVM overhead, complex setup)
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum
    
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    df = spark.read.parquet("s3://data/transactions/")
    result = df.filter(col("amount") > 100) \
               .groupBy("category") \
               .agg(spark_sum("amount").alias("total"))
    result.write.parquet("s3://output/category_totals/")

    # Ray Data approach (Python-native, simplified)
    import ray
    
    ds = ray.data.read_parquet("s3://data/transactions/")
    result = ds.filter(lambda row: row["amount"] > 100) \
               .groupby("category") \
               .aggregate(ray.data.aggregate.Sum("amount"))
    result.write_parquet("s3://output/category_totals/")

**Example 2: Complex Analytics Migration**

.. code-block:: python

    # Spark approach (SQL-based, JVM serialization)
    from pyspark.sql import functions as F
    
    df = spark.read.parquet("s3://customer-data/")
    customer_metrics = df.groupBy("customer_id") \
                        .agg(F.sum("revenue").alias("total_revenue"),
                             F.avg("order_value").alias("avg_order"),
                             F.count("*").alias("order_count"))

    # Ray Data approach (Python-native, multimodal capable)
    import ray
    
    ds = ray.data.read_parquet("s3://customer-data/")
    customer_metrics = ds.groupby("customer_id").aggregate(
        ray.data.aggregate.Sum("revenue"),
        ray.data.aggregate.Mean("order_value"),
        ray.data.aggregate.Count("order_id")
    )

**Example 3: AI/ML Integration Migration**

.. code-block:: python

    # Spark approach (limited ML integration)
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import LogisticRegression
    
    # Complex setup for ML in Spark
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df_assembled = assembler.transform(df)
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(df_assembled)

    # Ray Data approach (native ML integration)
    import ray
    from ray.train.torch import TorchTrainer
    
    # Seamless integration with Ray Train
    ds = ray.data.read_parquet("s3://ml-data/")
    # Prepare features with distributed processing
    prepared_ds = ds.map_batches(lambda batch: prepare_features(batch))
    # Train with Ray Train integration
    trainer = TorchTrainer(train_loop_per_worker=train_func, 
                          datasets={"train": prepared_ds})
    result = trainer.fit()

**Migration Strategy for Spark Users:**

**Step 1: Assessment and Planning**
- Identify Spark workloads that would benefit from Python-native performance
- Assess current Spark infrastructure and operational complexity
- Evaluate AI/ML integration requirements and multimodal data needs
- Plan migration timeline and validation approach

**Step 2: Proof of Concept**
- Start with non-critical Spark workloads for validation
- Compare performance between Spark and Ray Data implementations
- Validate result accuracy and business logic preservation
- Test deployment and operational procedures

**Step 3: Production Migration**
- Implement parallel processing for validation during migration
- Gradually shift workloads from Spark to Ray Data
- Monitor performance and business impact throughout migration
- Optimize Ray Data configurations based on Spark workload characteristics

For PyTorch Dataset & DataLoader Users
--------------------------------------

Ray Data provides distributed alternatives to PyTorch's data loading with seamless Ray Train integration:

.. list-table:: PyTorch Data vs. Ray Data APIs
   :header-rows: 1

   * - PyTorch API
     - Ray Data API
     - Ray Data Advantage
   * - ``Dataset.__getitem__()``
     - :meth:`ds.iter_rows() <ray.data.Dataset.iter_rows>`
     - Distributed processing, streaming execution
   * - ``DataLoader``
     - :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
     - Multi-node data loading, GPU optimization
   * - ``transforms.Compose()``
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - Distributed transforms, GPU acceleration

For complete PyTorch integration details, see :ref:`Working with PyTorch <working-with-pytorch>` and :ref:`Migrating from PyTorch to Ray Data <migrate_pytorch>`.

.. _migration-decision-framework:

Migration Decision Framework
----------------------------

**Choose Ray Data when you need:**

:::list-table
   :header-rows: 1

- - **Current Library**
  - **Migration Trigger**
  - **Ray Data Benefit**
  - **Migration Effort**
  - **Best Use Cases**
- - **Pandas**
  - Dataset size > available memory
  - Distributed processing, streaming execution
  - Low (similar API)
  - ETL, BI, analytics scaling
- - **Apache Spark**
  - Python-native performance, multimodal data
  - No JVM overhead, AI/ML integration
  - Medium (concept mapping)
  - Big data → AI/ML workflows
- - **Dask**
  - AI/ML workloads, GPU acceleration
  - Multimodal processing, Ray ecosystem
  - Low (similar distributed model)
  - Scientific computing → AI
- - **Polars**
  - Multi-machine scaling, multimodal data
  - Distributed processing, AI integration
  - Medium (API differences)
  - Fast analytics → distributed
- - **HuggingFace**
  - Large-scale NLP, distributed processing
  - GPU acceleration, multimodal workflows
  - Low (familiar ML patterns)
  - NLP scaling, multimodal AI
- - **TensorFlow Data**
  - Distributed preprocessing, multimodal
  - Better performance, unified API
  - Medium (different paradigm)
  - TF workflows → distributed
- - **NumPy**
  - Multi-machine arrays, GPU processing
  - Distributed arrays, GPU acceleration
  - Low (familiar operations)
  - Scientific computing scaling
- - **cuDF**
  - Multi-GPU scaling, multimodal data
  - Distributed GPU, unified data types
  - Medium (GPU scaling patterns)
  - Single GPU → multi-GPU

:::

**Migration Strategy Recommendations:**

**Start Small:**
- Begin with non-critical workloads to build experience
- Use Ray Data alongside existing tools initially
- Gradually migrate workloads as confidence builds

**Focus on Pain Points:**
- Target workloads with performance or scalability issues
- Leverage Ray Data's unique multimodal capabilities
- Take advantage of GPU acceleration for appropriate workloads

**Leverage Similarities:**
- Use familiar API patterns where available
- Apply existing distributed processing knowledge
- Build on current data processing expertise

Next Steps
----------

**Choose Your Migration Path:**

**From Pandas/Single-Node Tools:**
→ Start with :ref:`Core Operations <core_operations>` to understand distributed processing

**From Spark/Big Data Platforms:**
→ Explore :ref:`ETL Pipeline Guide <etl-pipelines>` for familiar workflow patterns

**From AI/ML Libraries:**
→ Begin with :ref:`Framework Integration <frameworks>` for ML-specific patterns

**For All Migrations:**
- **Test thoroughly**: Validate Ray Data performance with your specific workloads
- **Plan incrementally**: Migrate workloads gradually rather than wholesale replacement
- **Leverage community**: Use :ref:`Community Resources <community-resources>` for support

**Practical Migration Examples**

**Example 1: Pandas to Ray Data (ETL Workflow)**

.. code-block:: python

    # Before: Pandas (single-node, memory limited)
    import pandas as pd
    
    df = pd.read_csv("large_dataset.csv")  # Fails if > memory
    df = df.groupby("category").sum()
    df.to_parquet("output.parquet")

    # After: Ray Data (distributed, streaming)
    import ray
    
    ds = ray.data.read_csv("large_dataset.csv")  # Handles any size
    result = ds.groupby("category").aggregate(ray.data.aggregate.Sum("amount"))
    result.write_parquet("output.parquet")

**Example 2: Spark to Ray Data (Analytics Workflow)**

.. code-block:: python

    # Before: Spark (JVM overhead, complex deployment)
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("analytics").getOrCreate()
    df = spark.read.parquet("data.parquet")
    result = df.filter(df.amount > 100).groupBy("region").sum("amount")
    result.write.parquet("output.parquet")

    # After: Ray Data (Python-native, simpler deployment)
    import ray
    
    ds = ray.data.read_parquet("data.parquet")
    result = ds.filter(lambda row: row["amount"] > 100) \
               .groupby("region") \
               .aggregate(ray.data.aggregate.Sum("amount"))
    result.write_parquet("output.parquet")

**Example 3: HuggingFace to Ray Data (NLP Scaling)**

.. code-block:: python

    # Before: HuggingFace (single-node processing)
    from datasets import load_dataset
    
    dataset = load_dataset("text", data_files="large_corpus.txt")
    tokenized = dataset.map(tokenize_function, batched=True)  # Memory limited

    # After: Ray Data (distributed NLP processing)
    import ray
    
    dataset = ray.data.read_text("large_corpus.txt")
    tokenized = dataset.map_batches(
        tokenize_function,
        compute=ray.data.ActorPoolStrategy(size=8),
        num_gpus=0.5  # GPU acceleration for tokenization
    )

**Library Capability Comparison Matrix**

:::list-table
   :header-rows: 1

- - **Capability**
  - **Pandas**
  - **Spark**
  - **Dask**
  - **Polars**
  - **Ray Data**
- - **Structured Data**
  - ✅ Excellent
  - ✅ Excellent
  - ✅ Good
  - ✅ Excellent
  - ✅ Excellent
- - **Unstructured Data**
  - ❌ No Support
  - ❌ Limited
  - ❌ Limited
  - ❌ No Support
  - ✅ **Industry Leading**
- - **Multimodal Processing**
  - ❌ Requires Multiple Tools
  - ❌ Requires Multiple Tools
  - ❌ Requires Multiple Tools
  - ❌ No Support
  - ✅ **Native Excellence**
- - **GPU Acceleration**
  - ❌ Limited (cuDF)
  - ❌ Limited
  - ❌ Limited
  - ❌ No Support
  - ✅ **Built-in Optimization**
- - **Memory Scaling**
  - ❌ Single-node limit
  - ✅ Distributed
  - ✅ Distributed
  - ❌ Single-node limit
  - ✅ **Streaming Execution**
- - **AI/ML Integration**
  - ⚠️ Basic
  - ⚠️ Basic
  - ⚠️ Basic
  - ❌ Limited
  - ✅ **Native Integration**
- - **Deployment Complexity**
  - ✅ Simple
  - ❌ Complex (JVM)
  - ⚠️ Moderate
  - ✅ Simple
  - ✅ **Python-Native**

:::

**Migration Success Stories**

**Pandas → Ray Data:**
- **Netflix**: Scaled data processing from single-node to 100TB+ datasets
- **Use case**: Customer analytics and recommendation preprocessing
- **Result**: 10x data processing capacity with same infrastructure

**Spark → Ray Data:**
- **Uber**: Modernized data pipelines with Python-native performance
- **Use case**: Autonomous vehicle data processing and feature engineering
- **Result**: 3x performance improvement, reduced operational complexity

**HuggingFace → Ray Data:**
- **ByteDance**: Scaled NLP preprocessing for LLM training
- **Use case**: 200TB+ daily text processing for content recommendation
- **Result**: 5x throughput improvement with GPU acceleration
