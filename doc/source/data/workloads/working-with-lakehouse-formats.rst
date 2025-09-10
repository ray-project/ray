.. _working-with-lakehouse-formats:

Working with Lakehouse Formats
===============================

Ray Data provides native support for modern lakehouse formats including Delta Lake, Apache Iceberg, Apache Hudi, and Unity Catalog. This guide shows you how to work with these formats for ACID transactions, time travel, and schema evolution.

**What you'll learn:**

* Working with Delta Lake tables and time travel
* Reading and writing Apache Iceberg tables
* Integration with Databricks Unity Catalog
* Schema evolution and data versioning patterns
* Performance optimization for lakehouse workloads

Why Use Lakehouse Formats
--------------------------

Traditional data lakes suffer from consistency issues, lack of ACID transactions, and poor performance for analytical queries. Lakehouse formats solve these problems by providing:

**ACID Transactions**
Ensure data consistency with atomic, consistent, isolated, and durable operations.

**Time Travel**
Query historical versions of data for auditing, debugging, and analysis.

**Schema Evolution**
Handle changing data schemas without breaking existing queries.

**Performance Optimization**
Advanced indexing, partitioning, and file organization for fast queries.

**Unified Batch and Streaming**
Single format that works for both batch and streaming workloads.

Working with Delta Lake
-----------------------

Delta Lake is an open-source storage framework that brings ACID transactions to Apache Spark and big data workloads.

**Reading Delta Tables**

.. code-block:: python

    import ray

    # Read current version of Delta table
    delta_table = ray.data.read_delta("s3://bucket/delta-table/")

    # Read specific version (time travel)
    historical_data = ray.data.read_delta(
        "s3://bucket/delta-table/",
        version=5  # Read version 5
    )

    # Read data as of specific timestamp
    snapshot_data = ray.data.read_delta(
        "s3://bucket/delta-table/",
        timestamp="2024-01-15 10:30:00"
    )

    # Read with filters for better performance
    filtered_data = ray.data.read_delta(
        "s3://bucket/delta-table/",
        filter="date >= '2024-01-01' AND region = 'US'"
    )

**Writing Delta Tables**

.. code-block:: python

    # Write new Delta table
    sales_data = ray.data.read_parquet("s3://raw-data/sales/")
    
    sales_data.write_delta(
        "s3://bucket/sales-delta/",
        mode="overwrite",
        partition_cols=["year", "month"]
    )

    # Append to existing Delta table
    new_sales = ray.data.read_parquet("s3://raw-data/new-sales/")
    
    new_sales.write_delta(
        "s3://bucket/sales-delta/",
        mode="append"
    )

    # Merge/upsert operations
    # Load existing Delta table
    existing_sales = ray.data.read_delta("s3://bucket/sales-delta/")
    
    # Load new/updated records
    updates = ray.data.read_parquet("s3://raw-data/sales-updates/")
    
    # Perform merge logic
    def merge_records(batch):
        """Merge new records with existing data."""
        # Implementation depends on your merge logic
        # This is a simplified example
        merged = batch.drop_duplicates(subset=['transaction_id'], keep='last')
        return merged
    
    # Combine and deduplicate
    combined_data = existing_sales.union(updates)
    merged_data = combined_data.map_batches(merge_records)
    
    # Write merged data
    merged_data.write_delta(
        "s3://bucket/sales-delta/",
        mode="overwrite"
    )

**Delta Lake Time Travel**

.. code-block:: python

    # Read current data
    current_data = ray.data.read_delta("s3://bucket/customer-data/")
    
    # Read data from 30 days ago
    historical_data = ray.data.read_delta(
        "s3://bucket/customer-data/",
        timestamp="2024-01-01 00:00:00"
    )
    
    # Compare current vs historical
    def compare_datasets(current_batch, historical_batch):
        """Compare current and historical customer data."""
        
        # Join datasets on customer_id
            current_df = current_batch.set_index('customer_id')
            historical_df = historical_batch.set_index('customer_id')
            
            # Find changes
            changes = []
            for customer_id in current_df.index:
                if customer_id in historical_df.index:
                    current_record = current_df.loc[customer_id]
                    historical_record = historical_df.loc[customer_id]
                    
                    # Check for changes in key fields
                    if current_record['total_spent'] != historical_record['total_spent']:
                        changes.append({
                            'customer_id': customer_id,
                            'field': 'total_spent',
                            'old_value': historical_record['total_spent'],
                            'new_value': current_record['total_spent'],
                            'change_amount': current_record['total_spent'] - historical_record['total_spent']
                        })
            
            return pd.DataFrame(changes)
        
        # Analyze changes
        current_pandas = current_data.to_pandas()
        historical_pandas = historical_data.to_pandas()
        
        changes_analysis = compare_datasets(current_pandas, historical_pandas)
        
        return ray.data.from_pandas(changes_analysis)

Working with Apache Iceberg
----------------------------

Apache Iceberg is a high-performance format for huge analytic tables with features like schema evolution and partition evolution.

**Reading Iceberg Tables**

.. code-block:: python

    # Read Iceberg table
    iceberg_table = ray.data.read_iceberg("s3://bucket/iceberg-table/")

    # Read specific snapshot
    snapshot_data = ray.data.read_iceberg(
        "s3://bucket/iceberg-table/",
        snapshot_id="12345678901234567890"
    )

    # Read with schema evolution
    evolved_table = ray.data.read_iceberg(
        "s3://bucket/iceberg-table/",
        schema_evolution=True  # Handle schema changes automatically
    )

**Writing Iceberg Tables**

.. code-block:: python

    # Write new Iceberg table
    data = ray.data.read_parquet("s3://raw-data/")
    
    data.write_iceberg(
        "s3://bucket/iceberg-table/",
        mode="overwrite",
        partition_spec=[("year", "identity"), ("month", "identity")]
    )

    # Append with schema evolution
    new_data_with_extra_column = ray.data.read_parquet("s3://raw-data-v2/")
    
    new_data_with_extra_column.write_iceberg(
        "s3://bucket/iceberg-table/",
        mode="append",
        allow_schema_evolution=True
    )

Unity Catalog Integration
-------------------------

Databricks Unity Catalog provides centralized metadata management for lakehouse data access.

**Reading from Unity Catalog**

.. code-block:: python

    # Read table from Unity Catalog
    catalog_table = ray.data.read_unity_catalog(
        "catalog.schema.table_name",
        endpoint="https://your-workspace.cloud.databricks.com",
        token="your-access-token"
    )

    # Read with SQL query
    query_result = ray.data.read_unity_catalog(
        sql_query="""
        SELECT customer_id, total_spent, last_purchase_date
        FROM catalog.sales.customer_summary
        WHERE total_spent > 1000
        """,
        endpoint="https://your-workspace.cloud.databricks.com",
        token="your-access-token"
    )

**Writing to Unity Catalog**

.. code-block:: python

    # Write processed data to Unity Catalog
    processed_data = ray.data.read_parquet("s3://processed-data/")
    
    processed_data.write_unity_catalog(
        "catalog.analytics.processed_sales",
        endpoint="https://your-workspace.cloud.databricks.com",
        token="your-access-token",
        mode="overwrite"
    )

**Unity Catalog Governance**

.. code-block:: python

    def governed_data_pipeline():
        """Data pipeline with Unity Catalog governance."""
        
        # Read source data with governance
        source_data = ray.data.read_unity_catalog(
            "catalog.raw.customer_transactions",
            endpoint="https://workspace.databricks.com",
            token="token"
        )
        
        # Apply data transformations
        def apply_privacy_rules(batch):
            """Apply privacy and governance rules."""
            
            # Mask PII data
            batch['email'] = batch['email'].apply(lambda x: mask_email(x))
            batch['phone'] = batch['phone'].apply(lambda x: mask_phone(x))
            
            # Apply business rules
            batch = batch[batch['age'] >= 18]  # Only adults
            batch = batch[batch['consent_given'] == True]  # Only consented users
            
            # Add audit columns
            batch['processed_at'] = pd.Timestamp.now()
            batch['processed_by'] = 'ray_data_pipeline'
            
            return batch
        
        governed_data = source_data.map_batches(apply_privacy_rules)
        
        # Write to governed analytics table
        governed_data.write_unity_catalog(
            "catalog.analytics.customer_insights",
            endpoint="https://workspace.databricks.com",
            token="token",
            mode="append"
        )
        
        return governed_data

Schema Evolution Patterns
-------------------------

Lakehouse formats support schema evolution, allowing you to modify table schemas over time without breaking existing queries.

**Adding New Columns**

.. code-block:: python

    def evolve_schema_add_column():
        """Add new column to existing lakehouse table."""
        
        # Read existing data
        existing_data = ray.data.read_delta("s3://bucket/customer-data/")
        
        # Add new column with transformation
        def add_customer_tier(batch):
            """Add customer tier based on total spent."""
            
            def calculate_tier(total_spent):
                if total_spent >= 10000:
                    return 'platinum'
                elif total_spent >= 5000:
                    return 'gold'
                elif total_spent >= 1000:
                    return 'silver'
                else:
                    return 'bronze'
            
            batch['customer_tier'] = batch['total_spent'].apply(calculate_tier)
            return batch
        
        evolved_data = existing_data.map_batches(add_customer_tier)
        
        # Write with schema evolution
        evolved_data.write_delta(
            "s3://bucket/customer-data/",
            mode="overwrite",
            schema_mode="merge"  # Merge new schema with existing
        )
        
        return evolved_data

**Handling Data Type Changes**

.. code-block:: python

    def handle_data_type_evolution():
        """Handle data type changes in lakehouse tables."""
        
        # Read data that may have evolved data types
        data = ray.data.read_iceberg("s3://bucket/evolving-table/")
        
        def standardize_data_types(batch):
            """Standardize data types across schema versions."""
            
            # Handle string to numeric conversions
            if 'amount' in batch.columns:
                # Convert string amounts to numeric
                batch['amount'] = pd.to_numeric(batch['amount'], errors='coerce')
            
            # Handle date format changes
            if 'date' in batch.columns:
                # Standardize date formats
                batch['date'] = pd.to_datetime(batch['date'], infer_datetime_format=True)
            
            # Handle new nullable columns
            if 'new_column' not in batch.columns:
                batch['new_column'] = None
            
            return batch
        
        standardized_data = data.map_batches(standardize_data_types)
        
        return standardized_data

Performance Optimization
------------------------

**Partitioning Strategies**

.. code-block:: python

    # Optimize partitioning for query patterns
    def optimize_table_partitioning():
        """Optimize table partitioning for performance."""
        
        # Read source data
        sales_data = ray.data.read_parquet("s3://raw-data/sales/")
        
        # Add partitioning columns
        def add_partition_columns(batch):
            batch['year'] = batch['sale_date'].dt.year
            batch['month'] = batch['sale_date'].dt.month
            batch['region_code'] = batch['region'].str[:2].str.upper()
            return batch
        
        partitioned_data = sales_data.map_batches(add_partition_columns)
        
        # Write with optimal partitioning
        partitioned_data.write_delta(
            "s3://bucket/optimized-sales/",
            mode="overwrite",
            partition_cols=["year", "month", "region_code"]
        )
        
        return partitioned_data

**File Size Optimization**

.. code-block:: python

    def optimize_file_sizes():
        """Optimize file sizes for better query performance."""
        
        # Read data with small files
        small_files_data = ray.data.read_delta("s3://bucket/small-files-table/")
        
        # Repartition for optimal file sizes (aim for 128MB-1GB files)
        optimized_data = small_files_data.repartition(num_blocks=50)  # Adjust based on data size
        
        # Write with optimized file sizes
        optimized_data.write_delta(
            "s3://bucket/optimized-table/",
            mode="overwrite"
        )
        
        return optimized_data

**Z-Order Optimization**

.. code-block:: python

    def apply_zorder_optimization():
        """Apply Z-order optimization for better query performance."""
        
        # This is typically done at the storage layer
        # Ray Data can prepare data for Z-order optimization
        
        data = ray.data.read_delta("s3://bucket/table/")
        
        # Sort data by columns that will be Z-ordered
        def prepare_for_zorder(batch):
            """Sort data to optimize for Z-order clustering."""
            # Sort by frequently queried columns
            return batch.sort_values(['customer_id', 'date', 'product_category'])
        
        zorder_prepared = data.map_batches(prepare_for_zorder)
        
        # Write data (Z-order clustering happens at Delta Lake level)
        zorder_prepared.write_delta(
            "s3://bucket/zorder-optimized-table/",
            mode="overwrite"
        )
        
        return zorder_prepared

Best Practices for Lakehouse Formats
------------------------------------

**1. Choose the Right Format**

* **Delta Lake**: Best for Spark ecosystems, mature tooling
* **Apache Iceberg**: Best for multi-engine support, advanced features
* **Apache Hudi**: Best for streaming workloads, CDC patterns

**2. Optimize for Query Patterns**

* Partition by frequently filtered columns
* Use appropriate file sizes (128MB-1GB)
* Consider Z-order or similar optimizations for multi-dimensional queries

**3. Schema Evolution Strategy**

* Plan for schema changes from the beginning
* Use compatible data types when possible
* Document schema evolution patterns for your team

**4. Data Governance**

* Implement proper access controls and governance
* Use Unity Catalog or similar tools for metadata management
* Maintain data lineage and audit trails

**5. Performance Monitoring**

* Monitor query performance and file statistics
* Regularly optimize table layout and partitioning
* Use time travel and versioning for debugging and analysis

Next Steps
----------

Now that you understand lakehouse formats with Ray Data, explore related topics:

* **Working with Tabular Data**: General structured data processing → :ref:`working-with-tabular-data`
* **Data Warehousing**: Integration with data warehouse platforms → :ref:`data-warehousing`
* **ETL Pipelines**: Build ETL workflows with lakehouse formats → :ref:`etl-pipelines`
* **Performance Optimization**: Optimize lakehouse workloads → :ref:`performance-optimization`

For practical examples:

* **ETL Examples**: ETL pipelines with lakehouse formats → :ref:`etl-examples`
* **Integration Examples**: Integration patterns with lakehouse platforms → :ref:`integration-examples`