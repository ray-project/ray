.. _etl-examples:

ETL Pipeline Examples
=====================

Complete, working examples of ETL pipelines built with Ray Data for real-world business scenarios.

Customer Data Pipeline
----------------------

Combine customer data from CRM, orders, and support logs to create a unified customer view.

.. code-block:: python

    import ray
    from ray.data.aggregate import Sum, Count, Mean
    import pandas as pd

    def customer_360_pipeline():
        """Complete customer 360 ETL pipeline."""
        
        # Extract: Load data from multiple sources
        customers = ray.data.read_sql(
            "postgresql://crm-db:5432/salesforce",
            "SELECT customer_id, first_name, last_name, email FROM customers"
        )
        
        orders = ray.data.read_parquet("s3://data-lake/orders/")
        
        # Transform: Clean customer data
        def clean_customer_data(batch):
            batch = batch.drop_duplicates(subset=['customer_id'])
            batch['full_name'] = batch['first_name'] + ' ' + batch['last_name']
            batch['email_clean'] = batch['email'].str.lower().str.strip()
            return batch
        
        clean_customers = customers.map_batches(clean_customer_data)
        
        # Transform: Aggregate order metrics
        customer_metrics = orders.groupby('customer_id').aggregate(
            Sum('order_value').alias('total_spent'),
            Count('order_id').alias('total_orders'),
            Mean('order_value').alias('avg_order_value')
        )
        
        # Transform: Join customer data
        customer_360 = clean_customers.join(customer_metrics, on='customer_id', how='left')
        
        # Load: Save to data warehouse
        customer_360.write_parquet("s3://warehouse-staging/customer_360_view/")
        
        return customer_360

Financial Risk Analysis
-----------------------

Process financial transactions for risk scoring and regulatory reporting.

.. code-block:: python

    import ray
    import pandas as pd
    import numpy as np

    def financial_risk_pipeline(processing_date: str):
        """Process transactions for risk analysis."""
        
        # Extract: Load transaction data
        transactions = ray.data.read_parquet(f"s3://transactions/date={processing_date}/")
        
        # Transform: Calculate risk scores
        def calculate_risk_metrics(batch):
            # Amount-based risk
            batch['amount_z_score'] = np.abs(
                (batch['amount'] - batch['amount'].mean()) / batch['amount'].std()
            )
            
            # Time-based risk
            batch['hour'] = pd.to_datetime(batch['timestamp']).dt.hour
            batch['is_unusual_time'] = (batch['hour'] < 6) | (batch['hour'] > 22)
            
            # Combined risk score
            batch['risk_score'] = (
                np.clip(batch['amount_z_score'] * 10, 0, 50) +
                batch['is_unusual_time'].astype(int) * 25
            )
            
            batch['risk_category'] = pd.cut(
                batch['risk_score'],
                bins=[0, 25, 50, 75, 100],
                labels=['low', 'medium', 'high', 'critical']
            )
            
            return batch
        
        risk_scored = transactions.map_batches(calculate_risk_metrics)
        
        # Load: Save results
        risk_scored.write_parquet(
            f"s3://processed-transactions/date={processing_date}/",
            partition_cols=['risk_category']
        )
        
        return risk_scored

Log Analysis Pipeline
---------------------

Process application logs for monitoring and alerting.

.. code-block:: python

    import ray
    import pandas as pd
    import json
    import re

    def log_analysis_pipeline():
        """Process application logs for monitoring."""
        
        # Extract: Load log files
        logs = ray.data.read_text("s3://app-logs/*/*.log")
        
        # Transform: Parse log entries
        def parse_logs(batch):
            parsed = []
            for log_text in batch["text"]:
                for line in log_text.split('\n'):
                    if not line.strip():
                        continue
                    
                    # Parse log format: "2024-01-15 10:30:45 [ERROR] Service: Message"
                    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] (\w+): (.+)'
                    match = re.match(pattern, line)
                    
                    if match:
                        parsed.append({
                            'timestamp': pd.to_datetime(match.group(1)),
                            'level': match.group(2),
                            'service': match.group(3),
                            'message': match.group(4)
                        })
            
            return pd.DataFrame(parsed)
        
        parsed_logs = logs.map_batches(parse_logs)
        
        # Transform: Categorize and analyze
        def analyze_logs(batch):
            batch['is_error'] = batch['level'] == 'ERROR'
            batch['hour'] = batch['timestamp'].dt.hour
            return batch
        
        analyzed_logs = parsed_logs.map_batches(analyze_logs)
        
        # Transform: Generate metrics
        service_metrics = analyzed_logs.groupby(['service', 'level']).aggregate(
            ray.data.aggregate.Count('timestamp').alias('log_count')
        )
        
        # Load: Save results
        analyzed_logs.write_parquet("s3://processed-logs/", partition_cols=['service'])
        service_metrics.write_json("s3://monitoring-data/metrics.json")
        
        return analyzed_logs, service_metrics

Best Practices
--------------

**Design for Reliability**
* Implement error handling and retry logic
* Create checkpoints for long-running pipelines
* Use idempotent operations

**Optimize Performance**
* Use appropriate data partitioning
* Leverage columnar formats (Parquet)
* Monitor resource utilization

**Ensure Data Quality**
* Validate schemas and business rules
* Implement monitoring and alerting
* Handle schema evolution

Next Steps
----------

* **ETL Pipeline Guide**: Detailed patterns → :ref:`etl-pipelines`
* **Data Quality**: Governance strategies → :ref:`data-quality-governance`
* **Performance**: Optimization techniques → :ref:`performance-optimization`