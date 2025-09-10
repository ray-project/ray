.. _etl-pipelines:

ETL Pipeline Guide: Extract Transform Load with Ray Data
========================================================

**Keywords:** ETL pipeline, data engineering, extract transform load, data warehouse, data lake, data integration, batch processing, data quality, incremental processing, change data capture, data validation

**Navigation:** :ref:`Ray Data <data>` → :ref:`Business Guides <business_guides>` → ETL Pipeline Guide

**Learning Path:** This guide is part of the **Data Engineer Path**. After completing this guide, continue with :ref:`Data Quality & Governance <data-quality-governance>` or explore :ref:`ETL Examples <etl-examples>`.

Ray Data provides comprehensive capabilities for building modern ETL (Extract, Transform, Load) pipelines that handle any data type at any scale. This guide covers design patterns, best practices, and implementation strategies for production ETL workloads.

**What you'll learn:**

* ETL pipeline design patterns and architectures
* Data quality management and validation strategies  
* Incremental processing and change data capture patterns
* Error handling, monitoring, and recovery strategies
* Integration with orchestration tools and data warehouses

Why Choose Ray Data for ETL
----------------------------

Traditional ETL tools were designed for structured data and batch processing. Modern data pipelines need to handle diverse data types, provide timely insights through frequent batch processing, and scale elastically. Ray Data addresses these requirements with:

**Universal Data Support**
Process structured, semi-structured, and unstructured data in unified pipelines with native support for 20+ data formats and sources.

**Scalable Performance** 
Streaming execution for datasets larger than available memory with automatic scaling from single machines to hundreds of nodes.

**Modern Architecture**
Python-native with zero JVM overhead, cloud-native design with multi-cloud support, and integration with modern data stack tools.

**Enterprise Features**
Comprehensive monitoring and observability, security and governance capabilities, and production-ready error handling.

ETL Pipeline Architecture Patterns
-----------------------------------

**1. Batch ETL Pipeline**

The most common ETL pattern processes data in scheduled batches. This example demonstrates the complete Extract-Transform-Load workflow using Ray Data's native operations.

**Step 1: Extract Data from Multiple Sources**

Ray Data's unified API simplifies data extraction from different source systems.

.. code-block:: python

    import ray
    from datetime import datetime

    def extract_daily_data(execution_date: str):
        """Extract data from multiple sources for daily processing."""
        # Load customer data from database using native SQL reader
        def create_connection():
            import psycopg2
            return psycopg2.connect("postgresql://user:pass@host/db")
        
        customers = ray.data.read_sql(
            f"SELECT * FROM customers WHERE updated_date = '{execution_date}'",
            create_connection
        )
        
        # Load order data from data lake using native Parquet reader
        orders = ray.data.read_parquet(f"s3://raw-data/orders/date={execution_date}/")
        
        return customers, orders

**Why this approach:** Ray Data's native readers handle connection pooling, parallel loading, and format optimization automatically, eliminating the need for custom extraction logic.

**Step 2: Transform and Clean Data**

Use `map_batches` for data cleaning operations that benefit from pandas vectorization.

.. code-block:: python

    def clean_customer_data(batch):
        """Clean and standardize customer data using pandas operations."""
        # Remove duplicate customers within batch
        batch = batch.drop_duplicates(subset=["customer_id"])
        
        # Standardize phone numbers using vectorized operations
        batch["phone"] = batch["phone"].str.replace(r"[^\d]", "", regex=True)
        
        # Normalize email addresses
        batch["email"] = batch["email"].str.lower().str.strip()
        
        return batch

    # Apply cleaning transformations
    customers, orders = extract_daily_data("2024-01-01")
    clean_customers = customers.map_batches(clean_customer_data)

**Why `map_batches` for cleaning:** Data cleaning operations like deduplication and string normalization are vectorized pandas operations that process multiple rows efficiently in batches.

**Step 3: Join and Aggregate**

Use Ray Data's native join and aggregation operations for optimal performance.

.. code-block:: python

    # Join customer and order data using Ray Data native join
    customer_orders = clean_customers.join(orders, on="customer_id", how="inner")
    
    # Create daily metrics using native aggregations
    daily_metrics = customer_orders.groupby(["customer_id", "customer_segment"]).aggregate(
        ray.data.aggregate.Sum("order_amount"),    # Total spending
        ray.data.aggregate.Count("order_id"),      # Number of orders
        ray.data.aggregate.Mean("order_value")     # Average order value
    )

**Step 4: Load to Data Warehouse**

.. code-block:: python

    # Save to data warehouse using native Snowflake writer
    daily_metrics.write_snowflake(
        table="daily_customer_metrics",
        connection_parameters={
            "user": "etl_user",
            "password": "secure_password",
            "account": "company_account",
            "database": "ANALYTICS_DB",
            "schema": "CUSTOMER_METRICS"
        }
    )
    
    # Verify processing completed
    record_count = daily_metrics.count()
    print(f"Successfully processed {record_count} customer records")

**Expected Output:** Daily customer metrics loaded to Snowflake data warehouse, ready for business intelligence and reporting.

**2. Incremental ETL Pipeline**

For processing data in incremental batches with frequent updates:

.. code-block:: python

    import ray
    from ray.data.aggregate import Sum, Count, Mean

    def incremental_etl_pipeline():
        """Process incremental data with micro-batches."""
        
        def process_micro_batch(batch_path: str):
            # Load new data files as they arrive
            new_data = ray.data.read_json(batch_path)
            
            # Transform: Incremental enrichment
            def enrich_events(batch):
                """Add derived fields and lookups."""
                batch["hour"] = batch["timestamp"].dt.hour
                batch["day_of_week"] = batch["timestamp"].dt.dayofweek
                batch["is_weekend"] = batch["day_of_week"].isin([5, 6])
                return batch
            
            enriched_data = new_data.map_batches(enrich_events)
            
            # Transform: Aggregate metrics
            metrics = enriched_data.groupby(["user_id", "hour"]) \
                .aggregate(Count("event_id"), Mean("session_duration"))
            
            # Load: Save to dashboard data for frequent refresh
            metrics.write_bigquery(
                project_id="analytics_project",
                dataset="hourly_user_metrics",
                overwrite_table=False  # Append mode
            )
            
            return metrics.count()
        
        # Process each micro-batch
        batch_files = ["s3://streaming/batch_001/", "s3://streaming/batch_002/"]
        for batch_path in batch_files:
            records_processed = process_micro_batch(batch_path)
            print(f"Processed {records_processed} records from {batch_path}")

Data Quality Management
-----------------------

Data quality is critical for reliable ETL pipelines. Ray Data provides flexible patterns for implementing comprehensive data quality checks.

**Schema Validation**

.. code-block:: python

    import ray
    import pandas as pd
    from typing import Dict, Any

    def validate_schema(batch: pd.DataFrame, expected_schema: Dict[str, Any]) -> pd.DataFrame:
        """Validate data against expected schema."""
        
        validation_results = []
        
        for _, row in batch.iterrows():
            record_valid = True
            validation_errors = []
            
            # Check required fields
            for field, field_type in expected_schema.items():
                if field not in row or pd.isna(row[field]):
                    record_valid = False
                    validation_errors.append(f"Missing required field: {field}")
                elif not isinstance(row[field], field_type):
                    record_valid = False
                    validation_errors.append(f"Invalid type for {field}")
            
            validation_results.append({
                "record_id": row.get("id", "unknown"),
                "is_valid": record_valid,
                "validation_errors": validation_errors
            })
        
        batch["validation_result"] = validation_results
        return batch

    # Apply schema validation in pipeline
    expected_schema = {"customer_id": int, "email": str, "created_date": pd.Timestamp}
    
    validated_data = raw_data.map_batches(
        lambda batch: validate_schema(batch, expected_schema)
    )
    
    # Filter valid records and log invalid ones
    valid_data = validated_data.filter(lambda row: row["validation_result"]["is_valid"])
    invalid_data = validated_data.filter(lambda row: not row["validation_result"]["is_valid"])

Incremental Processing Patterns
-------------------------------

For large datasets, incremental processing is essential for performance and cost efficiency.

**Timestamp-Based Incremental Processing**

.. code-block:: python

    import ray
    from datetime import datetime

    def incremental_etl_by_timestamp(last_processed_timestamp: str):
        """Process only records updated since last run."""
        
        # Extract: Load only new/updated records
        incremental_data = ray.data.read_sql(
            "postgresql://user:pass@host/db",
            f"""
            SELECT * FROM transactions 
            WHERE updated_at > '{last_processed_timestamp}'
            ORDER BY updated_at
            """
        )
        
        # Transform: Process incremental data
        def process_incremental_batch(batch):
            """Process incremental data with upsert logic."""
            batch["transaction_month"] = batch["transaction_date"].dt.to_period('M')
            batch["is_refund"] = batch["amount"] < 0
            batch["processing_timestamp"] = pd.Timestamp.now()
            return batch
        
        processed_incremental = incremental_data.map_batches(process_incremental_batch)
        
        # Load: Upsert to target table
        processed_incremental.write_snowflake(
            "snowflake://user:pass@account/database/schema",
            "transactions_processed",
            mode="upsert",
            upsert_keys=["transaction_id"]
        )
        
        # Update checkpoint with latest timestamp
        max_timestamp = incremental_data.max("updated_at")
        save_checkpoint("last_processed_timestamp", max_timestamp)
        
        return processed_incremental.count()

Integration with Orchestration Tools
------------------------------------

Ray Data integrates seamlessly with popular orchestration tools for production ETL workflows.

**Apache Airflow Integration**

.. code-block:: python

    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime, timedelta
    import ray

    def ray_data_etl_task(**context):
        """Ray Data ETL task for Airflow."""
        
        execution_date = context["execution_date"].strftime("%Y-%m-%d")
        
        # Initialize Ray
        ray.init(address="ray://ray-cluster:10001")
        
        try:
            # Run Ray Data pipeline
            result = daily_etl_pipeline(execution_date)
            return {"records_processed": result, "status": "success"}
        finally:
            ray.shutdown()

    # Define Airflow DAG
    dag = DAG(
        "ray_data_etl_pipeline",
        default_args={
            "owner": "data-engineering",
            "start_date": datetime(2024, 1, 1),
            "retries": 2,
            "retry_delay": timedelta(minutes=5)
        },
        description="Daily ETL pipeline using Ray Data",
        schedule_interval="@daily"
    )

    etl_task = PythonOperator(
        task_id="ray_data_etl",
        python_callable=ray_data_etl_task,
        dag=dag
    )

Best Practices for Production ETL
----------------------------------

**1. Design for Scalability**

* Use appropriate data partitioning (date-based or hash-based)
* Optimize block sizes to balance memory usage with parallelization
* Leverage streaming execution for large datasets
* Plan resource allocation to right-size clusters

**2. Implement Robust Error Handling**

* Set error thresholds for different pipeline stages
* Log comprehensive error information with context
* Implement retry logic for transient failures
* Create dead letter queues for failed records

**3. Ensure Data Quality**

* Validate data quality at ingestion time
* Implement business rules with domain knowledge
* Monitor data quality metrics continuously
* Set up alerting when quality thresholds are breached

**4. Plan for Recovery**

* Create checkpoints to save pipeline state
* Implement idempotent operations for safe reruns
* Version your pipelines to enable rollbacks
* Test recovery procedures regularly

**5. Monitor and Optimize Performance**

* Collect comprehensive metrics on throughput and resource usage
* Set up alerting for performance degradation
* Profile pipelines to identify bottlenecks
* Optimize resource utilization to balance cost and performance

ETL Pipeline Quality Checklist
------------------------------

Use this checklist to ensure your Ray Data ETL pipelines follow best practices:

**Pipeline Design**
- [ ] Does the pipeline handle incremental processing for efficiency?
- [ ] Are data quality checks integrated throughout the pipeline?
- [ ] Is error handling comprehensive with proper recovery strategies?
- [ ] Are transformations idempotent for safe reruns?
- [ ] Is the pipeline designed for monitoring and observability?

**Performance & Scalability**
- [ ] Are Ray Data native operations used instead of external frameworks where possible?
- [ ] Is memory usage optimized with appropriate block sizes?
- [ ] Are CPU/GPU resources allocated efficiently for mixed workloads?
- [ ] Is the pipeline tested at production scale?
- [ ] Are performance bottlenecks identified and optimized?

**Data Quality & Governance**
- [ ] Are schema validation checks implemented?
- [ ] Is data lineage tracked throughout the pipeline?
- [ ] Are business rules validated at appropriate stages?
- [ ] Is sensitive data properly handled and secured?
- [ ] Are audit logs captured for compliance requirements?

**Production Readiness**
- [ ] Is comprehensive monitoring and alerting configured?
- [ ] Are backup and recovery procedures documented and tested?
- [ ] Is the pipeline integrated with orchestration tools?
- [ ] Are security and access controls properly implemented?
- [ ] Is documentation complete for operations teams?

Next Steps
----------

Now that you understand ETL pipeline patterns with Ray Data, explore related topics:

* **Data Quality & Governance**: Learn comprehensive data governance strategies → :ref:`data-quality-governance`
* **Performance Optimization**: Optimize ETL pipelines for cost and performance → :ref:`performance-optimization`
* **Enterprise Integration**: Integrate with enterprise systems and tools → :ref:`enterprise-integration`

For practical implementations, check out:

* **ETL Examples**: Complete ETL pipeline implementations → :ref:`etl-examples`
* **Integration Examples**: Integration patterns with orchestration tools → :ref:`integration-examples`