.. _data-warehouses:

Data Warehouse Integration: Snowflake, BigQuery, Redshift & Databricks
======================================================================

**Keywords:** data warehouse integration, Snowflake, BigQuery, Redshift, Databricks, cloud data warehouse, SQL integration, data lake, lakehouse, analytics, ETL, data pipeline, cloud analytics

**Navigation:** :ref:`Ray Data <data>` → :ref:`Integrations <integrations>` → Data Warehouse Integration

Ray Data provides native integration with major data warehouse platforms, enabling seamless data movement, processing, and analytics workflows. This guide covers integration patterns, optimization strategies, and best practices for cloud data warehouses.

**Data Warehouse Platform Support**

:::list-table
   :header-rows: 1

- - **Platform**
  - **Read Support**
  - **Write Support**
  - **Key Features**
  - **Best Use Cases**
- - Snowflake
  - ✅ Native API
  - ✅ Native API
  - Multi-cluster, auto-scaling
  - Analytics, reporting, data sharing
- - Google BigQuery
  - ✅ Native API
  - ✅ Native API
  - Serverless, SQL interface
  - Large-scale analytics, ML
- - Amazon Redshift
  - ✅ SQL Interface
  - ✅ SQL Interface
  - Columnar storage, compression
  - Traditional BI, reporting
- - Databricks
  - ✅ Unity Catalog
  - ✅ Unity Catalog
  - Lakehouse, ML integration
  - Unified analytics, AI/ML
- - Azure Synapse
  - ✅ SQL Interface
  - ✅ SQL Interface
  - Hybrid analytics, integration
  - Enterprise analytics, BI

:::

**What you'll learn:**

* Native connectivity with Snowflake, BigQuery, Redshift, and Databricks
* Optimized data loading and export patterns
* Performance tuning for data warehouse workloads
* Cost optimization strategies for cloud data warehouses

Snowflake Integration
---------------------

Ray Data provides first-class Snowflake integration for both reading and writing data.

**Reading from Snowflake**

.. code-block:: python

    import ray

    # Basic Snowflake read
    snowflake_data = ray.data.read_snowflake(
        connection_uri="snowflake://user:password@account/DATABASE/SCHEMA",
        query="SELECT * FROM customer_data WHERE created_date >= '2024-01-01'"
    )

    # Read with connection parameters
    snowflake_data = ray.data.read_snowflake(
        connection_uri="snowflake://account",
        query="SELECT customer_id, total_spent FROM customer_summary",
        snowflake_options={
            "user": "your_username",
            "password": "your_password", 
            "database": "ANALYTICS_DB",
            "schema": "CUSTOMER_SCHEMA",
            "warehouse": "COMPUTE_WH",
            "role": "DATA_ANALYST_ROLE"
        }
    )

    # Read large tables with parallelization
    large_table = ray.data.read_snowflake(
        connection_uri="snowflake://account",
        query="""
        SELECT * FROM large_transactions_table 
        WHERE transaction_date BETWEEN '2024-01-01' AND '2024-12-31'
        """,
        parallelism=10  # Create 10 parallel connections
    )

**Writing to Snowflake**

.. code-block:: python

    # Process data with Ray Data
    processed_data = ray.data.read_parquet("s3://processed-data/")

    # Write to Snowflake table
    processed_data.write_snowflake(
        table="processed_customer_data",
        connection_parameters={
            "user": "etl_user",
            "password": "secure_password",
            "account": "company_account",
            "database": "ANALYTICS_DB",
            "schema": "PUBLIC"
        }
    )

    # Write with custom options
    processed_data.write_snowflake(
        table="analytics_results",
        connection_parameters={
            "user": "etl_user", 
            "password": "secure_password",
            "account": "company_account",
            "database": "ANALYTICS_DB",
            "schema": "PROCESSED_SCHEMA",
            "warehouse": "ETL_WAREHOUSE"
        }
    )

**Snowflake Performance Optimization**

.. code-block:: python

    def optimize_snowflake_integration():
        """Optimize Ray Data integration with Snowflake."""
        
        # Use appropriate warehouse size for workload
        large_dataset = ray.data.read_snowflake(
            connection_uri="snowflake://account",
            query="SELECT * FROM very_large_table",
            snowflake_options={
                "warehouse": "LARGE_WH",  # Use larger warehouse for big queries
                "user": "etl_user",
                "password": "password"
            }
        )
        
        # Optimize data types for transfer
        def optimize_for_transfer(batch):
            # Use smaller data types to reduce transfer time
            if 'id' in batch.columns:
                batch['id'] = batch['id'].astype('int32')
            if 'amount' in batch.columns:
                batch['amount'] = batch['amount'].astype('float32')
            return batch
        
        optimized_data = large_dataset.map_batches(optimize_for_transfer)
        
        # Write with staging optimization
        optimized_data.write_snowflake(
            connection_uri="snowflake://account",
            table="optimized_table",
            snowflake_options={
                "warehouse": "LARGE_WH",
                "stage": "my_stage",  # Use internal stage for better performance
                "compression": "gzip"  # Compress data during transfer
            }
        )

BigQuery Integration
--------------------

Google BigQuery integration enables processing of massive datasets with serverless scaling.

**Reading from BigQuery**

.. code-block:: python

    # Read BigQuery table
    bq_data = ray.data.read_bigquery(
        table="project.dataset.table_name",
        project_id="your-gcp-project"
    )

    # Read with SQL query
    query_result = ray.data.read_bigquery(
        query="""
        SELECT 
            customer_id,
            SUM(order_amount) as total_spent,
            COUNT(*) as order_count
        FROM `project.dataset.orders`
        WHERE order_date >= '2024-01-01'
        GROUP BY customer_id
        """,
        project_id="your-gcp-project"
    )

    # Read partitioned table efficiently
    partitioned_data = ray.data.read_bigquery(
        table="project.dataset.partitioned_table",
        project_id="your-gcp-project",
        filter="date_partition >= '2024-01-01'"  # Partition pruning
    )

**Writing to BigQuery**

.. code-block:: python

    # Write processed data to BigQuery
    processed_data = ray.data.read_parquet("gs://bucket/processed-data/")

    processed_data.write_bigquery(
        project_id="your-gcp-project",
        dataset="processed_results",
        overwrite_table=True
    )

    # Write with schema specification
    import pyarrow as pa

    schema = pa.schema([
        pa.field("customer_id", pa.int64()),
        pa.field("total_spent", pa.float64()),
        pa.field("processed_date", pa.timestamp('s'))
    ])

    processed_data.write_bigquery(
        table="project.dataset.customer_analytics",
        project_id="your-gcp-project",
        schema=schema,
        mode="append"
    )

**BigQuery Cost Optimization**

.. code-block:: python

    def optimize_bigquery_costs():
        """Optimize BigQuery integration for cost efficiency."""
        
        # Use query optimization to reduce data scanned
        optimized_query = """
        SELECT 
            customer_id,
            order_amount,
            order_date
        FROM `project.dataset.orders`
        WHERE 
            _PARTITIONTIME >= TIMESTAMP('2024-01-01')  -- Partition pruning
            AND order_amount > 100  -- Early filtering
        """
        
        cost_optimized_data = ray.data.read_bigquery(
            query=optimized_query,
            project_id="your-gcp-project"
        )
        
        # Process efficiently to minimize BigQuery slot usage
        def efficient_processing(batch):
            # Minimize processing time to reduce BigQuery costs
            result = batch.groupby('customer_id').agg({
                'order_amount': 'sum',
                'order_date': 'max'
            }).reset_index()
            return result
        
        processed = cost_optimized_data.map_batches(efficient_processing)
        
        # Write to cost-effective storage tier
        processed.write_bigquery(
            table="project.dataset.summary_results",
            project_id="your-gcp-project",
            bigquery_options={
                "time_partitioning": {"type": "DAY", "field": "processed_date"},
                "clustering_fields": ["customer_id"]  # Optimize for common queries
            }
        )

Redshift Integration
--------------------

Amazon Redshift integration for enterprise data warehouse workloads.

**Reading from Redshift**

.. code-block:: python

    # Read from Redshift
    redshift_data = ray.data.read_sql(
        connection_uri="postgresql://user:password@redshift-cluster.region.redshift.amazonaws.com:5439/database",
        query="""
        SELECT 
            customer_id,
            product_category,
            SUM(sales_amount) as total_sales
        FROM sales_fact s
        JOIN product_dim p ON s.product_id = p.product_id
        WHERE sales_date >= '2024-01-01'
        GROUP BY customer_id, product_category
        """
    )

    # Read with connection pooling for better performance
    redshift_data = ray.data.read_sql(
        connection_uri="postgresql://user:password@cluster.region.redshift.amazonaws.com:5439/db",
        query="SELECT * FROM large_table",
        parallelism=8,  # Use multiple connections
        sql_options={
            "pool_size": 10,
            "max_overflow": 20
        }
    )

**Writing to Redshift**

.. code-block:: python

    # Write to Redshift with COPY optimization
    processed_data = ray.data.read_parquet("s3://processed-data/")

    processed_data.write_sql(
        connection_uri="postgresql://user:password@cluster.region.redshift.amazonaws.com:5439/db",
        table="processed_analytics",
        mode="append",
        sql_options={
            "method": "multi",  # Use bulk insert
            "chunksize": 10000  # Optimize batch size
        }
    )

**Redshift Performance Optimization**

.. code-block:: python

    def optimize_redshift_performance():
        """Optimize Redshift integration for performance."""
        
        # Use staging tables for large writes
        def write_via_staging(dataset, final_table):
            # Write to staging table first
            staging_table = f"{final_table}_staging"
            
            dataset.write_sql(
                connection_uri="postgresql://user:pass@cluster/db",
                table=staging_table,
                mode="overwrite"
            )
            
            # Use Redshift SQL to move data efficiently
            import sqlalchemy
            engine = sqlalchemy.create_engine("postgresql://user:pass@cluster/db")
            
            with engine.connect() as conn:
                # Use Redshift's efficient INSERT INTO ... SELECT
                conn.execute(sqlalchemy.text(f"""
                    INSERT INTO {final_table}
                    SELECT * FROM {staging_table}
                """))
                
                # Clean up staging table
                conn.execute(sqlalchemy.text(f"DROP TABLE {staging_table}"))
        
        # Optimize data distribution
        def prepare_for_redshift(batch):
            # Sort by distribution key for better performance
            if 'customer_id' in batch.columns:
                batch = batch.sort_values('customer_id')
            
            # Optimize data types for Redshift
            for col in batch.columns:
                if batch[col].dtype == 'object':
                    # Limit string length for VARCHAR efficiency
                    max_length = batch[col].str.len().max()
                    if max_length < 256:
                        batch[col] = batch[col].astype(f'string[{max_length}]')
            
            return batch
        
        return prepare_for_redshift

Databricks Integration
----------------------

Databricks Unity Catalog and Delta Lake integration.

**Unity Catalog Integration**

.. code-block:: python

    # Read from Unity Catalog
    catalog_data = ray.data.read_unity_catalog(
        table="catalog.schema.customer_data",
        endpoint="https://your-workspace.cloud.databricks.com",
        token="your-databricks-token"
    )

    # Write to Unity Catalog with governance
    processed_data.write_unity_catalog(
        table="catalog.analytics.processed_customers",
        endpoint="https://your-workspace.cloud.databricks.com",
        token="your-databricks-token",
        mode="append"
    )

**Delta Lake Integration**

.. code-block:: python

    # Read Delta table
    delta_data = ray.data.read_delta("s3://bucket/delta-table/")

    # Write to Parquet format (Delta Lake write via parquet)
    processed_data.write_parquet(
        "s3://bucket/processed-delta-table/",
        partition_cols=["year", "month"]
    )

    # Time travel with Delta Lake
    historical_data = ray.data.read_delta(
        "s3://bucket/delta-table/",
        version=5  # Read specific version
    )

Multi-Warehouse Architecture
----------------------------

**Cross-Platform Data Movement**

.. code-block:: python

    def cross_platform_pipeline():
        """Move data efficiently between different data warehouses."""
        
        # Read from Snowflake
        snowflake_data = ray.data.read_snowflake(
            connection_uri="snowflake://account",
            query="SELECT * FROM customer_master"
        )
        
        # Read from BigQuery
        bigquery_data = ray.data.read_bigquery(
            table="project.dataset.transactions",
            project_id="gcp-project"
        )
        
        # Join data from different sources
        joined_data = snowflake_data.join(
            bigquery_data,
            on="customer_id",
            how="inner"
        )
        
        # Process combined data
        def calculate_customer_metrics(batch):
            # Business logic combining data from multiple sources
            batch['customer_value'] = batch['total_spent'] * batch['loyalty_multiplier']
            batch['risk_score'] = calculate_risk_score(batch)
            return batch
        
        processed_data = joined_data.map_batches(calculate_customer_metrics)
        
        # Write results to multiple destinations
        # To Redshift for operational reporting
        processed_data.write_sql(
            connection_uri="postgresql://redshift-cluster/db",
            table="customer_analytics",
            mode="overwrite"
        )
        
        # To BigQuery for advanced analytics
        processed_data.write_bigquery(
            table="project.analytics.customer_insights",
            project_id="gcp-project",
            mode="overwrite"
        )

**Data Warehouse Selection Strategy**

.. code-block:: python

    class DataWarehouseRouter:
        """Route data to appropriate warehouse based on use case."""
        
        def __init__(self):
            self.warehouse_configs = {
                'snowflake': {
                    'connection': 'snowflake://account',
                    'best_for': ['structured_analytics', 'bi_reporting'],
                    'cost_tier': 'medium'
                },
                'bigquery': {
                    'project': 'gcp-project',
                    'best_for': ['large_scale_analytics', 'ml_training'],
                    'cost_tier': 'low'
                },
                'redshift': {
                    'connection': 'postgresql://cluster/db',
                    'best_for': ['operational_reporting', 'dashboard_data'],
                    'cost_tier': 'high'
                }
            }
        
        def route_data(self, dataset, use_case, data_size_gb):
            """Route data to optimal warehouse based on use case and size."""
            
            if use_case == 'large_scale_analytics' and data_size_gb > 100:
                # BigQuery for large-scale analytics
                return dataset.write_bigquery(
                    table="project.analytics.large_dataset",
                    project_id=self.warehouse_configs['bigquery']['project']
                )
            
            elif use_case == 'operational_reporting':
                # Redshift for operational dashboards
                return dataset.write_sql(
                    connection_uri=self.warehouse_configs['redshift']['connection'],
                    table="operational_data"
                )
            
            else:
                # Snowflake for general analytics
                return dataset.write_snowflake(
                    connection_uri=self.warehouse_configs['snowflake']['connection'],
                    table="general_analytics"
                )

Performance Best Practices
--------------------------

**1. Optimize Data Transfer**

* Use appropriate compression (gzip, snappy) for network transfer
* Implement parallel connections for large datasets
* Use staging areas for bulk operations

**2. Leverage Warehouse Features**

* Use partitioning and clustering for query performance
* Implement proper indexing strategies
* Take advantage of warehouse-specific optimizations

**3. Cost Management**

* Monitor warehouse usage and costs
* Use appropriate warehouse sizes for workloads
* Implement data lifecycle management

**4. Security and Compliance**

* Use secure connection strings and credential management
* Implement proper access controls and auditing
* Ensure data encryption in transit and at rest

Next Steps
----------

* **BI Tools Integration**: Connect with visualization platforms → :ref:`bi-tools`
* **ETL Tools Integration**: Orchestrate with workflow tools → :ref:`etl-tools`
* **Cloud Platforms**: Leverage cloud-native optimizations → :ref:`cloud-platforms`