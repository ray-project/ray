.. _etl-tools:

ETL Tools Integration
=====================

ETL tools orchestrate data pipelines and workflow management. This guide shows you how to integrate Ray Data with popular ETL and workflow orchestration platforms.

What is ETL tool integration?
-----------------------------

ETL tool integration involves:

* **Workflow orchestration**: Schedule and manage Ray Data pipelines
* **Dependency management**: Handle task dependencies and execution order
* **Error handling**: Implement retry logic and failure recovery
* **Monitoring integration**: Track pipeline execution and performance
* **Resource management**: Coordinate cluster resources across workflows

Why integrate Ray Data with ETL tools?
--------------------------------------

* **Workflow orchestration**: Leverage proven scheduling and dependency management
* **Operational excellence**: Use established monitoring and alerting systems
* **Team collaboration**: Enable data teams to use familiar workflow tools
* **Enterprise integration**: Connect with existing data infrastructure
* **Reliability**: Benefit from mature error handling and recovery mechanisms

How to integrate with ETL tools
-------------------------------

Follow this approach for ETL tool integration:

1. **Choose orchestration platform**: Select the ETL tool that fits your environment
2. **Design workflow structure**: Plan task dependencies and execution flow
3. **Configure Ray Data tasks**: Set up Ray Data operations within workflows
4. **Implement error handling**: Add retry logic and failure notifications
5. **Set up monitoring**: Integrate with existing monitoring systems
6. **Test and deploy**: Validate workflow execution and deploy to production

Apache Airflow Integration
--------------------------

**What is Apache Airflow?**

Apache Airflow is a platform for developing, scheduling, and monitoring workflows. Ray Data integrates with Airflow through custom operators and task definitions.

**Basic Airflow Integration**

.. code-block:: python

    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    import ray

    # Define default arguments
    default_args = {
        'owner': 'data-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }

    # Create DAG
    dag = DAG(
        'ray_data_etl_pipeline',
        default_args=default_args,
        description='ETL pipeline using Ray Data',
        schedule_interval=timedelta(days=1),
        catchup=False
    )

    def extract_data_task(**context):
        """Airflow task to extract data using Ray Data"""
        
        # Initialize Ray if not already running
        if not ray.is_initialized():
            ray.init()
        
        try:
            # Extract data
            customers = ray.data.read_csv("s3://source/customers.csv")
            transactions = ray.data.read_parquet("s3://source/transactions/")
            
            # Save intermediate results
            customers.write_parquet("s3://staging/customers/")
            transactions.write_parquet("s3://staging/transactions/")
            
            return f"Extracted {customers.count()} customers, {transactions.count()} transactions"
            
        except Exception as e:
            ray.shutdown()
            raise e

    def transform_data_task(**context):
        """Airflow task to transform data using Ray Data"""
        
        if not ray.is_initialized():
            ray.init()
        
        try:
            # Load intermediate data
            customers = ray.data.read_parquet("s3://staging/customers/")
            transactions = ray.data.read_parquet("s3://staging/transactions/")
            
            # Join and aggregate
            customer_metrics = customers.join(transactions, on="customer_id") \
                .groupby("customer_id") \
                .aggregate(
                    ray.data.aggregate.Sum("amount"),
                    ray.data.aggregate.Count("transaction_id")
                )
            
            # Save processed results
            customer_metrics.write_parquet("s3://processed/customer_metrics/")
            
            return f"Processed {customer_metrics.count()} customer records"
            
        except Exception as e:
            ray.shutdown()
            raise e

    def load_data_task(**context):
        """Airflow task to load data to destination"""
        
        if not ray.is_initialized():
            ray.init()
        
        try:
            # Load processed data
            processed_data = ray.data.read_parquet("s3://processed/customer_metrics/")
            
            # Load to data warehouse
            processed_data.write_snowflake(
                table="ANALYTICS.CUSTOMER_METRICS",
                connection_parameters=snowflake_connection_params
            )
            
            return f"Loaded {processed_data.count()} records to warehouse"
            
        finally:
            ray.shutdown()

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_task,
        dag=dag
    )

    transform_task = PythonOperator(
        task_id='transform_data', 
        python_callable=transform_data_task,
        dag=dag
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_task,
        dag=dag
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task

**Advanced Airflow Integration**

.. code-block:: python

    from airflow.sensors.s3_key_sensor import S3KeySensor
    from airflow.operators.email import EmailOperator

    # Add data availability sensor
    wait_for_data = S3KeySensor(
        task_id='wait_for_source_data',
        bucket_name='source-data-bucket',
        bucket_key='daily-data/{{ ds }}/data.parquet',
        dag=dag
    )

    # Add success notification
    notify_success = EmailOperator(
        task_id='notify_pipeline_success',
        to=['data-team@company.com'],
        subject='Ray Data Pipeline Completed Successfully',
        html_content='<p>The daily ETL pipeline completed successfully.</p>',
        dag=dag
    )

    # Update task dependencies
    wait_for_data >> extract_task >> transform_task >> load_task >> notify_success

Prefect Integration
-------------------

**What is Prefect?**

Prefect is a modern workflow orchestration platform that provides dynamic, Python-native workflow management. Ray Data integrates naturally with Prefect's Python-first approach.

**Basic Prefect Integration**

.. code-block:: python

    from prefect import flow, task
    import ray

    @task
    def extract_data():
        """Extract data using Ray Data"""
        
        if not ray.is_initialized():
            ray.init()
        
        customers = ray.data.read_csv("s3://source/customers.csv")
        orders = ray.data.read_parquet("s3://source/orders/")
        
        # Save to staging
        customers.write_parquet("s3://staging/customers/")
        orders.write_parquet("s3://staging/orders/")
        
        return {"customers": customers.count(), "orders": orders.count()}

    @task
    def transform_data(extraction_results):
        """Transform data using Ray Data"""
        
        if not ray.is_initialized():
            ray.init()
        
        # Load staged data
        customers = ray.data.read_parquet("s3://staging/customers/")
        orders = ray.data.read_parquet("s3://staging/orders/")
        
        # Transform and aggregate
        customer_summary = customers.join(orders, on="customer_id") \
            .groupby("customer_id") \
            .aggregate(
                ray.data.aggregate.Sum("order_amount"),
                ray.data.aggregate.Count("order_id")
            )
        
        # Save transformed data
        customer_summary.write_parquet("s3://transformed/customer_summary/")
        
        return customer_summary.count()

    @task
    def load_data(transform_count):
        """Load data to destination"""
        
        if not ray.is_initialized():
            ray.init()
        
        try:
            # Load transformed data
            summary_data = ray.data.read_parquet("s3://transformed/customer_summary/")
            
            # Load to warehouse
            summary_data.write_snowflake(
                table="ANALYTICS.CUSTOMER_SUMMARY",
                connection_parameters=connection_params
            )
            
            return f"Loaded {summary_data.count()} records"
            
        finally:
            ray.shutdown()

    @flow(name="ray-data-etl-pipeline")
    def ray_data_etl_flow():
        """Prefect flow using Ray Data"""
        
        # Execute tasks
        extraction_results = extract_data()
        transform_count = transform_data(extraction_results)
        load_result = load_data(transform_count)
        
        return load_result

    # Execute flow
    if __name__ == "__main__":
        ray_data_etl_flow()

Dagster Integration
-------------------

**What is Dagster?**

Dagster is a data orchestrator that focuses on asset-based workflows and data quality. Ray Data integrates with Dagster through asset definitions and ops.

**Basic Dagster Integration**

.. code-block:: python

    from dagster import asset, op, job, Config
    import ray

    class RayDataConfig(Config):
        source_path: str
        destination_path: str
        batch_size: int = 1000

    @asset
    def raw_customer_data():
        """Raw customer data asset"""
        
        if not ray.is_initialized():
            ray.init()
        
        customers = ray.data.read_csv("s3://source/customers.csv")
        customers.write_parquet("s3://assets/raw_customers/")
        
        return customers.count()

    @asset
    def processed_customer_metrics(raw_customer_data):
        """Processed customer metrics asset"""
        
        if not ray.is_initialized():
            ray.init()
        
        # Load raw data
        customers = ray.data.read_parquet("s3://assets/raw_customers/")
        transactions = ray.data.read_parquet("s3://source/transactions/")
        
        # Process data
        metrics = customers.join(transactions, on="customer_id") \
            .groupby("customer_id") \
            .aggregate(
                ray.data.aggregate.Sum("amount"),
                ray.data.aggregate.Count("transaction_id")
            )
        
        # Save processed asset
        metrics.write_parquet("s3://assets/customer_metrics/")
        
        return metrics.count()

    @op
    def load_to_warehouse(context, config: RayDataConfig):
        """Load processed data to warehouse"""
        
        if not ray.is_initialized():
            ray.init()
        
        try:
            # Load processed asset
            metrics = ray.data.read_parquet("s3://assets/customer_metrics/")
            
            # Load to destination
            metrics.write_parquet(config.destination_path)
            
            context.log.info(f"Loaded {metrics.count()} records to {config.destination_path}")
            
        finally:
            ray.shutdown()

    @job(config=RayDataConfig)
    def ray_data_dagster_job():
        """Dagster job using Ray Data"""
        load_to_warehouse()

Ray Workflows Integration
-------------------------

**What are Ray Workflows?**

Ray Workflows provide native workflow orchestration within the Ray ecosystem, offering seamless integration with Ray Data.

**Basic Ray Workflows Integration**

.. code-block:: python

    import ray
    from ray import workflow

    @workflow.step
    def extract_step():
        """Extract data step"""
        
        customers = ray.data.read_csv("s3://source/customers.csv")
        transactions = ray.data.read_parquet("s3://source/transactions/")
        
        # Save to workflow storage
        customers.write_parquet("s3://workflow/customers/")
        transactions.write_parquet("s3://workflow/transactions/")
        
        return {"customers": customers.count(), "transactions": transactions.count()}

    @workflow.step
    def transform_step(extraction_results):
        """Transform data step"""
        
        # Load data from workflow storage
        customers = ray.data.read_parquet("s3://workflow/customers/")
        transactions = ray.data.read_parquet("s3://workflow/transactions/")
        
        # Apply transformations
        enriched_data = customers.join(transactions, on="customer_id")
        
        summary = enriched_data.groupby("customer_id").aggregate(
            ray.data.aggregate.Sum("amount"),
            ray.data.aggregate.Count("transaction_id")
        )
        
        # Save transformed data
        summary.write_parquet("s3://workflow/summary/")
        
        return summary.count()

    @workflow.step
    def load_step(transform_count):
        """Load data step"""
        
        summary = ray.data.read_parquet("s3://workflow/summary/")
        
        # Load to final destination
        summary.write_snowflake(
            table="ANALYTICS.WORKFLOW_SUMMARY",
            connection_parameters=connection_params
        )
        
        return f"Loaded {summary.count()} records"

    @workflow.step
    def cleanup_step(load_result):
        """Cleanup temporary data"""
        
        # Cleanup workflow storage
        # (Implementation depends on your cleanup strategy)
        
        return "Cleanup completed"

    def ray_data_workflow():
        """Complete Ray Data workflow"""
        
        extraction_results = extract_step.step()
        transform_count = transform_step.step(extraction_results)
        load_result = load_step.step(transform_count)
        cleanup_result = cleanup_step.step(load_result)
        
        return cleanup_result

    # Execute workflow
    if __name__ == "__main__":
        ray.init()
        workflow.init()
        
        result = workflow.run(ray_data_workflow, workflow_id="ray-data-etl")
        print(f"Workflow completed: {result}")

Best Practices
--------------

**Resource Management**

Coordinate Ray cluster resources with ETL tools:

.. code-block:: python

    def configure_ray_for_etl_tools():
        """Configure Ray Data for ETL tool integration"""
        
        # Initialize Ray with appropriate resources
        ray.init(
            num_cpus=16,
            num_gpus=2,
            object_store_memory=8_000_000_000,
            include_dashboard=True
        )
        
        # Configure Ray Data for workflow execution
        ctx = ray.data.DataContext.get_current()
        ctx.execution_options.resource_limits.cpu = 0.8  # Leave resources for orchestrator
        ctx.execution_options.resource_limits.object_store_memory = 6_000_000_000

**Error Handling and Recovery**

Implement robust error handling in ETL workflows:

.. code-block:: python

    def resilient_ray_data_task():
        """Ray Data task with comprehensive error handling"""
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Execute Ray Data operations
                result = ray.data.read_parquet("s3://source/data/") \
                    .map_batches(transform_function) \
                    .write_parquet("s3://output/processed/")
                
                return "Success"
                
            except Exception as e:
                retry_count += 1
                
                if retry_count >= max_retries:
                    # Send alert and fail
                    send_failure_alert(f"Ray Data task failed after {max_retries} retries: {e}")
                    raise e
                else:
                    # Log retry and wait
                    print(f"Retry {retry_count}/{max_retries} after error: {e}")
                    time.sleep(60 * retry_count)  # Exponential backoff

    def send_failure_alert(message):
        """Send alert for task failure"""
        # Implement alerting logic (email, Slack, etc.)
        print(f"ALERT: {message}")

    def transform_function(batch):
        """Your transformation logic with error handling"""
        try:
            # Apply transformations
            return batch
        except Exception as e:
            # Log transformation errors but continue processing
            print(f"Transformation error: {e}")
            return {}

**Monitoring Integration**

Integrate Ray Data metrics with ETL tool monitoring:

.. code-block:: python

    def create_etl_monitoring_task():
        """Create monitoring for Ray Data within ETL workflows"""
        
        def monitor_ray_data_execution(ds):
            """Monitor Ray Data execution and report metrics"""
            
            import time
            
            start_time = time.time()
            
            # Execute with monitoring
            result = ds.map_batches(monitored_transform)
            
            # Calculate metrics
            execution_time = time.time() - start_time
            record_count = result.count()
            records_per_second = record_count / execution_time if execution_time > 0 else 0
            
            # Report metrics to ETL tool
            metrics = {
                "execution_time": execution_time,
                "record_count": record_count,
                "records_per_second": records_per_second,
                "memory_usage": result.size_bytes()
            }
            
            # Log metrics for ETL tool consumption
            print(f"Ray Data metrics: {metrics}")
            
            return result
        
        def monitored_transform(batch):
            """Transform with execution monitoring"""
            
            batch_start = time.time()
            
            # Apply transformation
            result = your_transform_logic(batch)
            
            batch_time = time.time() - batch_start
            
            # Log batch metrics
            print(f"Processed batch of {len(batch)} records in {batch_time:.2f}s")
            
            return result
        
        return monitor_ray_data_execution

    def your_transform_logic(batch):
        """Placeholder for transformation logic"""
        return batch

Pipeline Patterns
-----------------

**Incremental Processing Pattern**

.. code-block:: python

    def incremental_etl_pattern():
        """Pattern for incremental data processing in ETL workflows"""
        
        from datetime import datetime, timedelta
        
        # Get last processed timestamp from workflow state
        last_processed = get_last_processed_timestamp()
        current_time = datetime.now()
        
        # Process only incremental data
        incremental_data = ray.data.read_parquet("s3://source/data/") \
            .filter(lambda row: datetime.fromisoformat(row["updated_at"]) > last_processed)
        
        if incremental_data.count() > 0:
            # Process incremental data
            processed_data = incremental_data.map_batches(transform_incremental_data)
            
            # Load to destination
            processed_data.write_parquet("s3://destination/incremental/")
            
            # Update checkpoint
            update_last_processed_timestamp(current_time)
            
            return f"Processed {incremental_data.count()} incremental records"
        else:
            return "No new data to process"

    def get_last_processed_timestamp():
        """Get last processed timestamp from ETL tool state"""
        # Implementation depends on your ETL tool's state management
        return datetime.now() - timedelta(days=1)

    def update_last_processed_timestamp(timestamp):
        """Update last processed timestamp in ETL tool state"""
        # Implementation depends on your ETL tool's state management
        pass

    def transform_incremental_data(batch):
        """Transform incremental data"""
        return batch

**Parallel Processing Pattern**

.. code-block:: python

    def parallel_processing_pattern():
        """Pattern for parallel processing in ETL workflows"""
        
        # Define parallel processing tasks
        def process_region_data(region):
            """Process data for specific region"""
            
            regional_data = ray.data.read_parquet(f"s3://source/region={region}/")
            
            processed_data = regional_data.map_batches(
                lambda batch: transform_regional_data(batch, region)
            )
            
            processed_data.write_parquet(f"s3://processed/region={region}/")
            
            return processed_data.count()
        
        # Process multiple regions in parallel
        regions = ["us-east", "us-west", "eu", "asia"]
        
        # This would be implemented as parallel tasks in your ETL tool
        results = {}
        for region in regions:
            results[region] = process_region_data(region)
        
        return results

    def transform_regional_data(batch, region):
        """Transform data with region-specific logic"""
        
        # Apply region-specific transformations
        if region.startswith("us"):
            # US-specific processing
            batch["currency"] = "USD"
        elif region == "eu":
            # EU-specific processing
            batch["currency"] = "EUR"
        
        return batch

Next Steps
----------

* Learn about :ref:`Cloud Platform Integration <cloud-platforms>` for cloud-native optimization
* Explore :ref:`Data Warehouse Integration <data-warehouses>` for data storage strategies
* See :ref:`Production Deployment <production-deployment>` for operational best practices
* Review :ref:`Monitoring & Observability <monitoring-observability>` for comprehensive system monitoring
