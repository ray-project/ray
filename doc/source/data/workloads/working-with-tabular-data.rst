.. _working-with-tabular-data:

Working with Tabular Data
=========================

Ray Data provides comprehensive support for tabular data processing with performance that rivals specialized analytics engines. This guide shows you how to efficiently work with structured data formats like CSV, Parquet, and database tables.

**What you'll learn:**

* Loading and saving tabular data from various sources
* Performing SQL-style operations (joins, aggregations, filtering)
* Optimizing performance for analytical workloads
* Best practices for structured data processing

Loading Tabular Data
--------------------

Ray Data supports loading tabular data from multiple sources and formats:

**CSV Files**

.. code-block:: python

    import ray

    # Load CSV from local filesystem
    dataset = ray.data.read_csv("data/sales.csv")

    # Load CSV from cloud storage
    dataset = ray.data.read_csv("s3://bucket/data/sales.csv")

    # Load multiple CSV files with schema inference
    dataset = ray.data.read_csv("s3://bucket/data/*.csv")

    # Load with custom schema
    dataset = ray.data.read_csv(
        "s3://bucket/data/sales.csv",
        schema=pa.schema([
            ("customer_id", pa.int64()),
            ("order_date", pa.timestamp('s')),
            ("amount", pa.float64())
        ])
    )

**Parquet Files**

.. code-block:: python

    # Load Parquet files (recommended for analytics)
    dataset = ray.data.read_parquet("s3://bucket/data/sales.parquet")

    # Load partitioned Parquet data
    dataset = ray.data.read_parquet("s3://bucket/partitioned-data/")

    # Load with column selection for performance
    dataset = ray.data.read_parquet(
        "s3://bucket/data/sales.parquet",
        columns=["customer_id", "order_date", "amount"]
    )

    # Load with filtering (predicate pushdown)
    dataset = ray.data.read_parquet(
        "s3://bucket/data/sales.parquet",
        filter=pa.compute.greater(pa.compute.field("amount"), pa.scalar(100))
    )

**Database Tables**

.. code-block:: python

    # Load from SQL databases
    dataset = ray.data.read_sql(
        "postgresql://user:password@host:5432/database",
        "SELECT * FROM customers WHERE created_date >= '2024-01-01'"
    )

    # Load from data warehouses
    dataset = ray.data.read_snowflake(
        "snowflake://user:password@account/database/schema",
        "SELECT customer_id, total_spent FROM customer_summary"
    )

    dataset = ray.data.read_bigquery(
        "project.dataset.table",
        project_id="my-project"
    )

Basic Tabular Operations
------------------------

**Filtering Data**

.. code-block:: python

    import ray

    # Load sample data
    sales_data = ray.data.read_parquet("s3://bucket/sales.parquet")

    # Filter by single condition
    high_value_sales = sales_data.filter(lambda row: row["amount"] > 1000)

    # Filter by multiple conditions
    recent_high_value = sales_data.filter(
        lambda row: row["amount"] > 1000 and row["order_date"] >= "2024-01-01"
    )

    # Filter using map_batches for complex logic
    def complex_filter(batch):
        """Complex filtering logic using Ray Data native operations."""
        # Use numpy operations for better performance
        import numpy as np
        
        # Create boolean masks using numpy
        amount_mask = batch["amount"] > 1000
        segment_mask = batch["customer_segment"] == "enterprise"
        # Extract quarter from date strings using numpy operations
        quarter_mask = np.array([
            int(date.split("-")[1]) >= 10  # Q4 (Oct-Dec)
            for date in batch["order_date"]
        ])
        
        # Combine masks
        combined_mask = amount_mask & segment_mask & quarter_mask
        
        # Apply filter to all columns using Ray Data native operations
        filtered_batch = {}
        for col_name, col_data in batch.items():
            filtered_batch[col_name] = col_data[combined_mask]
        
        return filtered_batch

    filtered_data = sales_data.map_batches(complex_filter)

**Selecting and Transforming Columns**

.. code-block:: python

    # Select specific columns
    customer_data = sales_data.select_columns(["customer_id", "amount", "order_date"])

    # Add new columns
    enriched_data = sales_data.add_column("tax_amount", lambda row: row["amount"] * 0.08)

    # Transform columns using map_batches with Ray Data native operations
    def add_derived_columns(batch):
        """Add derived columns using Ray Data native operations."""
        import numpy as np
        from datetime import datetime
        
        # Extract year and quarter from date strings using numpy operations
        batch["order_year"] = np.array([
            int(date.split("-")[0]) for date in batch["order_date"]
        ])
        
        batch["order_quarter"] = np.array([
            (int(date.split("-")[1]) - 1) // 3 + 1 for date in batch["order_date"]
        ])
        
        # Add order size category using numpy conditions
        amounts = batch["amount"]
        order_sizes = np.where(
            amounts <= 100, "small",
            np.where(amounts <= 500, "medium",
            np.where(amounts <= 1000, "large", "enterprise"))
        )
        batch["order_size"] = order_sizes
        
        # Add profit margin (example business logic)
        batch["profit_margin"] = (batch["amount"] - batch["cost"]) / batch["amount"]
        
        return batch

    transformed_data = sales_data.map_batches(add_derived_columns)

**Sorting Data**

.. code-block:: python

    # Sort by single column
    sorted_by_amount = sales_data.sort("amount", descending=True)

    # Sort by multiple columns
    sorted_data = sales_data.sort([("order_date", "descending"), ("amount", "descending")])

Aggregations and Grouping
-------------------------

Ray Data provides powerful aggregation capabilities for analytical workloads:

**Basic Aggregations**

.. code-block:: python

    from ray.data.aggregate import Sum, Count, Mean, Min, Max, Std

    # Simple aggregations
    total_sales = sales_data.sum("amount")
    order_count = sales_data.count()
    avg_order_value = sales_data.mean("amount")

    # Multiple aggregations
    summary_stats = sales_data.aggregate(
        Sum("amount"),
        Count("order_id"),
        Mean("amount"),
        Min("amount"),
        Max("amount"),
        Std("amount")
    )

**Group By Operations**

.. code-block:: python

    # Group by single column
    sales_by_region = sales_data.groupby("region").aggregate(
        Sum("amount").alias("total_sales"),
        Count("order_id").alias("order_count"),
        Mean("amount").alias("avg_order_value")
    )

    # Group by multiple columns
    monthly_sales = sales_data.groupby(["region", "order_month"]).aggregate(
        Sum("amount").alias("monthly_sales"),
        Count("order_id").alias("monthly_orders")
    )

**Custom Aggregations**

.. code-block:: python

    # Custom aggregation function
    class Percentile:
        def __init__(self, column: str, percentile: float):
            self.column = column
            self.percentile = percentile
            
        def __call__(self, batch):
            return batch[self.column].quantile(self.percentile)

    # Use custom aggregation
    sales_percentiles = sales_data.groupby("region").aggregate(
        Mean("amount").alias("avg_sales"),
        Percentile("amount", 0.5).alias("median_sales"),
        Percentile("amount", 0.95).alias("p95_sales")
    )

Joining Data
------------

Ray Data supports SQL-style joins for combining multiple datasets:

**Basic Joins**

.. code-block:: python

    # Load datasets
    customers = ray.data.read_parquet("s3://bucket/customers.parquet")
    orders = ray.data.read_parquet("s3://bucket/orders.parquet")

    # Inner join
    customer_orders = customers.join(orders, on="customer_id", how="inner")

    # Left join (keep all customers)
    customer_orders = customers.join(orders, on="customer_id", how="left")

    # Right join (keep all orders)
    customer_orders = customers.join(orders, on="customer_id", how="right")

    # Full outer join
    customer_orders = customers.join(orders, on="customer_id", how="outer")

**Multi-Column Joins**

.. code-block:: python

    # Join on multiple columns
    detailed_orders = orders.join(
        product_details, 
        on=["product_id", "variant_id"], 
        how="inner"
    )

**Complex Join Scenarios**

.. code-block:: python

    # Join with different column names
    customer_orders = customers.join(
        orders.rename_columns({"cust_id": "customer_id"}),
        on="customer_id",
        how="inner"
    )

    # Join with aggregated data
    customer_summary = orders.groupby("customer_id").aggregate(
        Sum("amount").alias("total_spent"),
        Count("order_id").alias("order_count")
    )

    enriched_customers = customers.join(customer_summary, on="customer_id", how="left")

Business Intelligence Patterns
------------------------------

**Time Series Analysis**

.. code-block:: python

    def time_series_analysis(sales_data):
        """Perform time series analysis on sales data."""
        
        def calculate_time_metrics(batch):
            # Sort by date for time series operations
            batch = batch.sort_values("order_date")
            
            # Calculate rolling averages
            batch["sales_7day_avg"] = batch["amount"].rolling(window=7).mean()
            batch["sales_30day_avg"] = batch["amount"].rolling(window=30).mean()
            
            # Calculate growth rates
            batch["daily_growth"] = batch["amount"].pct_change()
            batch["weekly_growth"] = batch["sales_7day_avg"].pct_change()
            
            # Seasonal indicators
            batch["day_of_year"] = batch["order_date"].dt.dayofyear
            batch["is_holiday_season"] = batch["day_of_year"].between(330, 365)
        
        return batch

        return sales_data.map_batches(calculate_time_metrics)

    time_series_data = time_series_analysis(sales_data)

**Customer Analytics**

.. code-block:: python

    def customer_analytics(customer_orders):
        """Calculate customer analytics metrics."""
        
        # RFM Analysis (Recency, Frequency, Monetary)
        def calculate_rfm(batch):
            reference_date = batch["order_date"].max()
            
            rfm_metrics = batch.groupby("customer_id").agg({
                "order_date": lambda x: (reference_date - x.max()).days,  # Recency
                "order_id": "count",  # Frequency
                "amount": "sum"  # Monetary
            }).reset_index()
            
            rfm_metrics.columns = ["customer_id", "recency", "frequency", "monetary"]
            
            # Score each dimension (1-5 scale)
            rfm_metrics["recency_score"] = pd.qcut(rfm_metrics["recency"], 5, labels=[5,4,3,2,1])
            rfm_metrics["frequency_score"] = pd.qcut(rfm_metrics["frequency"].rank(method="first"), 5, labels=[1,2,3,4,5])
            rfm_metrics["monetary_score"] = pd.qcut(rfm_metrics["monetary"], 5, labels=[1,2,3,4,5])
            
            # Combined RFM score
            rfm_metrics["rfm_score"] = (
                rfm_metrics["recency_score"].astype(str) +
                rfm_metrics["frequency_score"].astype(str) +
                rfm_metrics["monetary_score"].astype(str)
            )
            
            return rfm_metrics
        
        return customer_orders.map_batches(calculate_rfm)

    customer_rfm = customer_analytics(customer_orders)

Performance Optimization
------------------------

**Columnar Storage Optimization**

.. code-block:: python

    # Optimize data types for storage efficiency
    def optimize_data_types(batch):
        # Use appropriate data types
        batch["customer_id"] = batch["customer_id"].astype("int32")
        batch["amount"] = batch["amount"].astype("float32")
        batch["region"] = batch["region"].astype("category")
        batch["order_date"] = pd.to_datetime(batch["order_date"])
        
        return batch

    optimized_data = sales_data.map_batches(optimize_data_types)

    # Save with optimal compression
    optimized_data.write_parquet(
        "s3://bucket/optimized-sales/",
        compression="snappy",  # Good balance of speed and compression
        partition_cols=["region", "order_year"]  # Partition for query performance
    )

**Query Optimization**

.. code-block:: python

    # Use predicate pushdown for filtering
    recent_sales = ray.data.read_parquet(
        "s3://bucket/sales.parquet",
        filter=pa.compute.greater_equal(
            pa.compute.field("order_date"), 
            pa.scalar(pd.Timestamp("2024-01-01"))
        )
    )

    # Column pruning - read only needed columns
    essential_columns = ray.data.read_parquet(
        "s3://bucket/sales.parquet",
        columns=["customer_id", "amount", "order_date"]
    )

**Memory Management**

.. code-block:: python

    # Configure block size for optimal performance
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks

    # Use streaming for large datasets
    large_dataset = ray.data.read_parquet("s3://bucket/very-large-data/")
    
    # Process in streaming fashion
    result = large_dataset.map_batches(process_batch).write_parquet("s3://output/")

Data Warehouse Integration
-------------------------

**Snowflake Integration**

.. code-block:: python

    # Read from Snowflake
    warehouse_data = ray.data.read_snowflake(
        "snowflake://user:pass@account/DATABASE/SCHEMA",
        "SELECT * FROM SALES_FACT WHERE ORDER_DATE >= '2024-01-01'"
    )

    # Process data
    processed_data = warehouse_data.map_batches(business_logic)

    # Write back to Snowflake
    processed_data.write_snowflake(
        "snowflake://user:pass@account/DATABASE/SCHEMA",
        "PROCESSED_SALES",
        mode="overwrite"
    )

**BigQuery Integration**

.. code-block:: python

    # Read from BigQuery
    bq_data = ray.data.read_bigquery(
        "project.dataset.sales_table",
        project_id="my-project"
    )

    # Write to BigQuery
    processed_data.write_bigquery(
        "project.dataset.processed_sales",
        project_id="my-project"
    )

Best Practices for Tabular Data
-------------------------------

**1. Choose the Right Format**

* **Parquet**: Best for analytical workloads, columnar storage, compression
* **CSV**: Good for simple data exchange, human-readable
* **Delta/Iceberg**: Best for data lakes with ACID transactions

**2. Optimize Data Layout**

* Partition by commonly filtered columns (date, region)
* Use appropriate data types to reduce storage
* Consider compression vs. query speed trade-offs

**3. Leverage Predicate Pushdown**

* Apply filters at the source when reading data
* Use column selection to read only needed data
* Take advantage of partition pruning

**4. Design for Scale**

* Use streaming execution for large datasets
* Configure appropriate block sizes
* Monitor memory usage and resource allocation

**5. SQL-Style Operations**

* Use Ray Data's SQL-style operations for familiar patterns
* Leverage joins, aggregations, and window functions
* Consider data distribution for optimal join performance

Advanced Tabular Data Patterns
-------------------------------

**Complex Analytical Queries**

.. code-block:: python

    def advanced_analytical_queries():
        """Implement complex analytical patterns."""
        
        # Load sales data
        sales = ray.data.read_parquet("s3://warehouse/sales/")
        
        # Multi-level aggregations
        def hierarchical_analysis(batch):
            """Perform hierarchical analysis with subtotals."""
            
            # Calculate totals at different levels
            total_sales = batch['amount'].sum()
            
            # By region
            regional_totals = batch.groupby('region')['amount'].sum()
            
            # By region and category
            detailed_totals = batch.groupby(['region', 'category'])['amount'].sum()
            
            # Create hierarchical summary
            summary = []
            
            # Grand total
            summary.append({
                'level': 'total',
                'region': 'ALL',
                'category': 'ALL',
                'sales': total_sales
            })
            
            # Regional totals
            for region, amount in regional_totals.items():
                summary.append({
                    'level': 'region',
                    'region': region,
                    'category': 'ALL',
                    'sales': amount
                })
            
            # Detailed totals
            for (region, category), amount in detailed_totals.items():
                summary.append({
                    'level': 'detail',
                    'region': region,
                    'category': category,
                    'sales': amount
                })
            
            return pd.DataFrame(summary)
        
        hierarchical_results = sales.map_batches(hierarchical_analysis)
        
        return hierarchical_results

**Advanced Window Functions**

.. code-block:: python

    def implement_window_functions():
        """Implement SQL-style window functions."""
        
        sales_data = ray.data.read_parquet("s3://sales/")
        
        def calculate_window_functions(batch):
            """Calculate window functions for analytics."""
            
            # Sort for window calculations
            batch = batch.sort_values(['customer_id', 'order_date'])
            
            # Running totals
            batch['running_total'] = batch.groupby('customer_id')['amount'].cumsum()
            
            # Rank functions
            batch['amount_rank'] = batch.groupby('customer_id')['amount'].rank(ascending=False)
            batch['date_rank'] = batch.groupby('customer_id')['order_date'].rank()
            
            # Lead and lag functions
            batch['previous_amount'] = batch.groupby('customer_id')['amount'].shift(1)
            batch['next_amount'] = batch.groupby('customer_id')['amount'].shift(-1)
            
            # Moving averages
            batch['amount_3_period_avg'] = batch.groupby('customer_id')['amount'].rolling(3).mean().reset_index(0, drop=True)
            
            # First and last values
            batch['first_purchase_amount'] = batch.groupby('customer_id')['amount'].transform('first')
            batch['last_purchase_amount'] = batch.groupby('customer_id')['amount'].transform('last')
            
            return batch
        
        windowed_data = sales_data.map_batches(calculate_window_functions)
        
        return windowed_data

Next Steps
----------

Now that you understand tabular data processing with Ray Data, explore related topics:

* **Business Intelligence**: Advanced analytics and BI patterns → :ref:`business-intelligence`
* **ETL Pipelines**: Complete ETL workflow patterns → :ref:`etl-pipelines`
* **Data Warehousing**: Integration with data warehouse platforms → :ref:`data-warehousing`
* **Performance Optimization**: Optimize tabular data processing → :ref:`performance-optimization`

For practical examples:

* **ETL Examples**: Real-world ETL pipeline implementations → :ref:`etl-examples`
* **BI Examples**: Business intelligence use cases → :ref:`bi-examples`