.. _integrations:

Platform Integrations: Connect Ray Data to Your Data Stack
==========================================================

**Keywords:** Ray Data integrations, data warehouse integration, BI tools, ETL tools, cloud platforms, Snowflake, BigQuery, Tableau, Airflow, AWS, GCP, Azure, data stack integration

Ray Data integrates seamlessly with the modern data stack, providing native connectivity with business intelligence tools, data warehouses, ETL platforms, and cloud services.

.. toctree::
   :maxdepth: 2

   data-warehouses
   bi-tools
   etl-tools
   cloud-platforms
   data-formats
   comparisons

Overview
--------

Ray Data is designed to work alongside your existing data infrastructure, not replace it. These integration guides show you how to connect Ray Data with popular tools and platforms in your data stack.

**Data Warehouses**

Connect with Snowflake, BigQuery, Redshift, Databricks, and other data warehouse platforms for seamless data movement and processing.

**Business Intelligence Data Preparation**

Prepare and export data in optimized formats for consumption by BI platforms, with performance-tuned structures for dashboard and analytics tools.

**ETL & Orchestration Tools**

Integrate with Airflow, Prefect, Dagster, and other orchestration platforms to build comprehensive data pipelines.

**Cloud Platforms**

Leverage native cloud services and optimizations on AWS, GCP, and Azure for cost-effective, scalable data processing.

**Integration Selection Guide**

Choose the right integration approach based on your current data stack:

:::list-table
   :header-rows: 1

- - **Current Platform**
  - **Ray Data Integration**
  - **Key Benefits**
  - **Implementation Guide**
- - **Snowflake Data Warehouse**
  - Native read/write connectivity
  - Direct data pipeline integration
  - :ref:`Data Warehouses <data-warehouses>`
- - **Tableau/Power BI**
  - Optimized data preparation
  - Enhanced dashboard performance
  - :ref:`BI Tools <bi-tools>`
- - **Airflow/Prefect**
  - Workflow orchestration
  - Scalable pipeline automation
  - :ref:`ETL Tools <etl-tools>`
- - **AWS/GCP/Azure**
  - Native cloud optimization
  - Cost-effective scaling
  - :ref:`Cloud Platforms <cloud-platforms>`
- - **Multiple Platforms**
  - Unified data processing
  - Simplified architecture
  - :ref:`Platform Comparisons <comparisons>`

:::

**Quick Integration Assessment:**
- **Single platform**: Choose specific integration guide for deep optimization
- **Multi-platform**: Use :ref:`Platform Comparisons <comparisons>` for evaluation framework
- **Migration planning**: Start with :ref:`Migration & Testing <migration-testing>` strategies
- **Enterprise deployment**: Focus on :ref:`Best Practices <best_practices>` for production readiness