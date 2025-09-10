.. _data_quickstart:

Ray Data Quickstart: Learn Distributed Data Processing in 15 Minutes
====================================================================

**Keywords:** Ray Data tutorial, distributed data processing, Python data pipeline, ETL tutorial, business intelligence, machine learning data, quickstart guide, data transformation, analytics

Get started with Ray Data's :class:`Dataset <ray.data.Dataset>` abstraction for distributed data processing. This hands-on tutorial teaches you the fundamentals in 15 minutes.

**What you'll learn:**

* Load data from various sources (databases, files, cloud storage)
* Transform data for AI/ML, analytics, or data processing workflows
* Process data at scale with automatic parallelization
* Save results to data warehouses, files, or ML frameworks

**Choose your path based on your role:**

- **Executive/Decision Maker**: See business value and ROI in :ref:`Integration Examples <integration-examples>`
- **Data Engineer**: Explore comprehensive ETL capabilities in :ref:`ETL Examples <etl-examples>`
- **Data Scientist**: Begin with analytics and exploration in :ref:`BI Examples <bi-examples>`
- **AI Engineer**: Start with AI/ML examples in :ref:`Working with AI <working-with-ai>`
- **New to Ray Data**: Follow the complete tutorial below

This guide introduces you to the core capabilities of Ray Data:

* :ref:`Loading data <loading_key_concept>`
* :ref:`Transforming data <transforming_key_concept>`
* :ref:`Consuming data <consuming_key_concept>`
* :ref:`Saving data <saving_key_concept>`

Dataset operations
--------

Ray Data's main abstraction is a :class:`Dataset <ray.data.Dataset>`, which
represents a distributed collection of data. Datasets are designed for any data processing workload
and can efficiently handle data collections that exceed a single machine's memory.

.. _loading_key_concept:

Loading data
------------

Create datasets from various sources including local files, Python objects, databases, and cloud storage services like S3 or GCS.
Ray Data seamlessly integrates with any `filesystem supported by Arrow
<http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__ and enterprise data sources.

.. testcode::

    import ray

    # Load a CSV dataset directly from S3
    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    
    # Preview the first record
    ds.show(limit=1)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}

**Business Data Processing Example**

Ray Data excels at business data processing and ETL workloads. Here's a complete example processing customer transaction data:

.. testcode::

    import ray
    from ray.data.aggregate import Sum, Count, Mean

    # Create sample business data (in production, load from databases/data warehouses)
    transactions = ray.data.from_items([
        {"customer_id": 1, "product": "laptop", "amount": 1200, "region": "north"},
        {"customer_id": 2, "product": "phone", "amount": 800, "region": "south"},
        {"customer_id": 1, "product": "mouse", "amount": 25, "region": "north"},
        {"customer_id": 3, "product": "laptop", "amount": 1100, "region": "west"},
        {"customer_id": 2, "product": "keyboard", "amount": 75, "region": "south"},
    ])
    
    # Business analytics: Calculate customer metrics
    customer_metrics = transactions.groupby("customer_id").aggregate(
        Sum("amount"),      # Total spent per customer
        Count("product"),   # Number of purchases
        Mean("amount")      # Average order value
    )
    
    # View results
    customer_metrics.show()

.. testoutput::

    {'customer_id': 1, 'sum(amount)': 1225, 'count(product)': 2, 'mean(amount)': 612.5}
    {'customer_id': 2, 'sum(amount)': 875, 'count(product)': 2, 'mean(amount)': 437.5}
    {'customer_id': 3, 'sum(amount)': 1100, 'count(product)': 1, 'mean(amount)': 1100.0}

To learn more about creating datasets from different sources, read :ref:`Loading data <loading-data>`.

.. _transforming_key_concept:

Transforming data
-----------------

Apply user-defined functions (UDFs) to transform datasets for ETL, data cleaning, enrichment, and business logic. Ray automatically parallelizes these transformations
across your cluster for better performance.

.. testcode::

    from typing import Dict
    import numpy as np

    # Define a transformation to compute a "petal area" attribute
    def transform_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        vec_a = batch["petal length (cm)"]
        vec_b = batch["petal width (cm)"]
        batch["petal area (cm^2)"] = vec_a * vec_b
        return batch

    # Apply the transformation to our dataset
    transformed_ds = ds.map_batches(transform_batch)
    
    # View the updated schema with the new column
    # .materialize() will execute all the lazy transformations and
    # materialize the dataset into object store memory
    print(transformed_ds.materialize())

.. testoutput::

    MaterializedDataset(
       num_blocks=...,
       num_rows=150,
       schema={
          sepal length (cm): double,
          sepal width (cm): double,
          petal length (cm): double,
          petal width (cm): double,
          target: int64,
          petal area (cm^2): double
       }
    )

**Business Data Transformation Example**

Ray Data transformations support complex business logic and data enrichment:

.. testcode::

    from typing import Dict
    import numpy as np

    def enrich_customer_data(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """Add business metrics and customer segmentation."""
        # Calculate customer lifetime value
        batch["customer_ltv"] = batch["sum(amount)"] * 2.5  # Estimated 2.5x multiplier
        
        # Segment customers based on spending
        def segment_customer(total_spent):
            if total_spent >= 1000:
                return "premium"
            elif total_spent >= 500:
                return "standard"
            else:
                return "basic"
        
        # Apply segmentation logic
        batch["customer_segment"] = np.array([
            segment_customer(amount) for amount in batch["sum(amount)"]
        ])
        
        return batch

    # Apply business transformation
    enriched_customers = customer_metrics.map_batches(enrich_customer_data)
    
    # View enriched data
    enriched_customers.show()

.. testoutput::

    {'customer_id': 1, 'sum(amount)': 1225, 'count(product)': 2, 'mean(amount)': 612.5, 'customer_ltv': 3062.5, 'customer_segment': 'premium'}
    {'customer_id': 2, 'sum(amount)': 875, 'count(product)': 2, 'mean(amount)': 437.5, 'customer_ltv': 2187.5, 'customer_segment': 'standard'}
    {'customer_id': 3, 'sum(amount)': 1100, 'count(product)': 1, 'mean(amount)': 1100.0, 'customer_ltv': 2750.0, 'customer_segment': 'premium'}

To explore more transformation capabilities, read :ref:`Transforming data <transforming-data>`.

.. _consuming_key_concept:

Consuming data
--------------

Access dataset contents through convenient methods like :meth:`~ray.data.Dataset.take_batch` and 
:meth:`~ray.data.Dataset.iter_batches`. You can also pass datasets directly to Ray Tasks or Actors
for distributed processing.

.. testcode::

    # Extract the first 3 rows as a batch for processing
    print(transformed_ds.take_batch(batch_size=3))

.. testoutput::
    :options: +NORMALIZE_WHITESPACE

    {'sepal length (cm)': array([5.1, 4.9, 4.7]),
        'sepal width (cm)': array([3.5, 3. , 3.2]),
        'petal length (cm)': array([1.4, 1.4, 1.3]),
        'petal width (cm)': array([0.2, 0.2, 0.2]),
        'target': array([0, 0, 0]),
        'petal area (cm^2)': array([0.28, 0.28, 0.26])}

For more details on working with dataset contents, see
:ref:`Iterating over Data <iterating-over-data>` and :ref:`Saving Data <saving-data>`.

.. _saving_key_concept:

Saving data
-----------

Export processed datasets to a variety of formats and storage locations using methods
like :meth:`~ray.data.Dataset.write_parquet`, :meth:`~ray.data.Dataset.write_csv`, and more.

.. testcode::
    :hide:

    # The number of blocks can be non-deterministic. Repartition the dataset beforehand
    # so that the number of written files is consistent.
    transformed_ds = transformed_ds.repartition(2)

.. testcode::

    import os

    # Save the transformed dataset as Parquet files
    transformed_ds.write_parquet("/tmp/iris")

    # Verify the files were created
    print(os.listdir("/tmp/iris"))

.. testoutput::
    :options: +MOCK

    ['..._000000.parquet', '..._000001.parquet']

**Business Data Export Examples**

Ray Data supports exporting to various business systems and data warehouses:

.. code-block:: python

    # Export to data warehouse for BI tools
    enriched_customers.write_parquet("s3://data-warehouse/customer-metrics/")
    
    # Export to CSV for business reporting
    enriched_customers.write_csv("s3://reports/customer-segments.csv")
    
    # Export to database for application use
    # enriched_customers.write_sql(
    #     "postgresql://user:pass@host/db",
    #     "customer_analytics"
    # )

For more information on saving datasets, see :ref:`Saving data <saving-data>`.

**Quickstart Success Validation**

Congratulations! You've completed the Ray Data quickstart. Validate your understanding:

**What you've accomplished:**
- ✅ **Data loading**: Successfully loaded data using Ray Data's unified API
- ✅ **Data transformation**: Applied business logic with distributed processing  
- ✅ **Data aggregation**: Created business metrics using native aggregation functions
- ✅ **Data export**: Saved results in multiple formats for business use

**Validation checkpoint:**
Can you explain when to use `map` vs `map_batches`? If yes, you're ready for the next level!

**Your Ray Data journey progress: 15% complete**
- **Foundation**: ✅ Completed (you are here)
- **Specialization**: 🎯 Next step (choose your path below)
- **Application**: ⏳ Coming next (real-world examples)
- **Mastery**: ⏳ Advanced (production deployment)

Next Steps: Choose Your Learning Path
-------------------------------------

Now that you understand Ray Data basics, choose your path based on your role and use case:

**Data Engineers**
Ready to build production ETL pipelines? Continue with:

1. :ref:`ETL Pipeline Guide <etl-pipelines>` - Learn enterprise ETL patterns
2. :ref:`Data Quality & Governance <data-quality-governance>` - Implement validation and monitoring
3. :ref:`Performance Optimization <performance-optimization>` - Optimize for production workloads
4. :ref:`Production Deployment <production-deployment>` - Deploy to production safely

**Business Analysts** 
Want to create analytics and reports? Continue with:

1. :ref:`Business Intelligence Guide <business-intelligence>` - Learn BI and analytics patterns
2. :ref:`BI Examples <bi-examples>` - See real-world analytics implementations
3. :ref:`BI Tools Integration <bi-tools>` - Connect with Tableau, Power BI, etc.
4. :ref:`Advanced Analytics <advanced-analytics>` - Statistical analysis and custom metrics

**AI/ML Engineers**
Building AI and ML workflows? Continue with:

1. :ref:`Working with AI <working-with-ai>` - AI/ML workloads and multimodal processing
2. :ref:`Working with PyTorch <working-with-pytorch>` - PyTorch integration patterns
3. :ref:`Working with Images <working-with-images>` - Computer vision workflows
4. :ref:`Working with LLMs <working-with-llms>` - Large language model processing

**New Users**
Want to explore all capabilities? Continue with:

1. :ref:`Key Concepts <data_key_concepts>` - Understand Ray Data architecture
2. :ref:`User Guides <data_user_guide>` - Browse technical operation guides
3. :ref:`Types of Data Guide <types-of-data>` - Learn about different data types
4. :ref:`Use Cases <use_cases>` - Explore real-world examples

**Enterprise Users**
Planning enterprise deployment? Continue with:

1. :ref:`Enterprise Integration <enterprise-integration>` - Legacy systems and security
2. :ref:`Data Warehousing <data-warehousing>` - Modern data stack integration
3. :ref:`Best Practices <best_practices>` - Production deployment guidance
4. :ref:`Migration & Testing <migration-testing>` - Migration strategies and testing
