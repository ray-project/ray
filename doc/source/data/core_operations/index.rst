.. _core_operations:

Core Operations: Master Ray Data Fundamentals
==============================================

**Keywords:** Ray Data operations, data loading, data transformation, data aggregation, distributed data processing, ETL operations, data pipeline fundamentals, data processing API

Master the fundamental operations to process **any data type** at **any scale** with **any compute infrastructure**. These core concepts enable you to build efficient pipelines for any workload - from traditional ETL to cutting-edge AI.

.. toctree::
   :maxdepth: 2

   loading-overview
   inspecting-data
   transforming-data
   aggregations
   joining-data
   shuffling-data
   iterating-over-data
   saving-data
   monitoring-your-workload
   execution-configurations

Overview
--------

Ray Data's core operations provide the building blocks for any data processing workflow. These operations are optimized for distributed processing and can handle datasets larger than available memory.

**Data Input/Output Operations**
Load data from diverse sources and save results to various destinations with automatic optimization.

**Data Transformation Operations**
Transform your data using row-level or batch-level operations with intelligent resource allocation.

**Data Analysis Operations**
Analyze your data with aggregations, joins, and statistical operations using distributed computing.

**Learning Progression**

**Beginner Path (1-2 hours)**
1. :ref:`Loading Data <loading-data>` - Get data into Ray Data
2. :ref:`Inspecting Data <inspecting-data>` - Understand your dataset
3. :ref:`Transforming Data <transforming-data>` - Apply basic transformations
4. :ref:`Saving Data <saving-data>` - Export your results

**Intermediate Path (2-3 hours)**
1. :ref:`Aggregations <aggregations>` - Calculate statistics and metrics
2. :ref:`Joining Data <joining-data>` - Combine datasets for analysis
3. :ref:`Shuffling Data <shuffling-data>` - Optimize data layout
4. :ref:`Iterating Over Data <iterating-over-data>` - Access data efficiently

**Advanced Integration**
After mastering core operations, explore:
* **Workload Integration**: Apply operations with AI/ML and data processing → :ref:`Workloads <workloads>`
* **Business Applications**: Use operations for business workflows → :ref:`Business Guides <business_guides>`
* **Production Deployment**: Scale operations for production → :ref:`Best Practices <best_practices>`

**Operation Selection Guide**

**For Simple Row-Level Changes**
Use :ref:`Transforming Data <transforming-data>` with `map()` for individual record modifications.

**For Batch Processing**
Use :ref:`Transforming Data <transforming-data>` with `map_batches()` for vectorized operations and GPU acceleration.

**For Data Analysis**
Use :ref:`Aggregations <aggregations>` and :ref:`Joining Data <joining-data>` for analytical workflows.

**For Large Datasets**
Use :ref:`Shuffling Data <shuffling-data>` and :ref:`Iterating Over Data <iterating-over-data>` for memory-efficient processing.

Next Steps
----------

Apply core operations to specific scenarios:

* **Business Workflows**: Use operations for ETL and BI → :ref:`Business Guides <business_guides>`
* **Workload Types**: Apply operations to specific data types → :ref:`Workloads <workloads>`
* **AI/ML Integration**: Combine with frameworks → :ref:`Workloads <workloads>`
* **Real-World Examples**: See operations in action → :ref:`Use Cases <use_cases>`
