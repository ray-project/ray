# TPC-H ETL benchmark with Ray Data

**Time to complete**: 25 min | **Difficulty**: Intermediate | **Prerequisites**: Python, basic SQL knowledge

This template provides a comprehensive introduction to Ray Data for Extract, Transform, Load (ETL) workflows using the industry-standard TPC-H benchmark dataset. We'll cover both practical ETL pipeline construction and the underlying architecture that makes Ray Data powerful for distributed data processing.

## Learning Objectives

By completing this template, you will master:

**Ray Data ETL fundamentals**: Distributed data processing patterns using real enterprise benchmark data  
**TPC-H benchmark operations**: Industry-standard queries and transformations used for database performance testing  
**Advanced filtering techniques**: Complex data filtering patterns for business intelligence applications  
**Production ETL patterns**: Scalable data transformation pipelines from single-node to distributed clusters  

## Overview

**The TPC-H Challenge**: The TPC-H benchmark simulates a complex business environment with customers, orders, suppliers, and products. Traditional ETL tools struggle with TPC-H's complex relationships and high data volumes that mirror real enterprise scenarios.

**Ray Data Solution**: Ray Data provides distributed processing capabilities that handle TPC-H benchmark queries efficiently while demonstrating patterns applicable to real business data processing.

**Business Impact**: TPC-H represents realistic business scenarios - the techniques you learn here apply directly to customer analytics, order processing, and supply chain optimization in enterprise environments.

## Setup and Imports

Let's start by importing the necessary libraries and setting up our environment for TPC-H processing.

```python
import ray
import pandas as pd
import numpy as np
import pyarrow as pa
from typing import Dict, Any
import time

from ray.data import DataContext
from ray.data.aggregate import Count, Mean, Sum, Max

# Configure Ray Data for cleaner outputs
DataContext.get_current().enable_progress_bars = False

# Initialize Ray
ray.init(ignore_reinit_error=True)

print(f"Ray version: {ray.__version__}")
print(f"Ray cluster resources: {ray.cluster_resources()}")
```

## Part 1: Understanding TPC-H and Ray Data

### What is TPC-H?

The **TPC-H benchmark** is the industry standard for testing database and data processing performance. It simulates a complex business environment with realistic data relationships that mirror enterprise scenarios.

**TPC-H Business Context**: The benchmark models a wholesale supplier managing customer orders, inventory, and supplier relationships - representing real-world complexity found in enterprise data systems.

### TPC-H Schema Overview

The TPC-H benchmark consists of 8 interconnected tables representing a complete business ecosystem:

```python
# TPC-H Schema Overview for ETL Processing
tpch_tables = {
    "customer": "Customer master data with demographics and market segments",
    "orders": "Order header information with dates, priorities, and status",
    "lineitem": "Detailed line items for each order (largest table)",
    "part": "Parts catalog with specifications and retail prices", 
    "supplier": "Supplier information including contact details",
    "partsupp": "Part-supplier relationships with costs",
    "nation": "Nation reference data with geographic regions",
    "region": "Regional groupings for geographic analysis"
}

print("TPC-H Schema (8 Tables):")
for table, description in tpch_tables.items():
    print(f"  {table.upper()}: {description}")
```

## Part 2: Extract - Reading TPC-H Data

The **Extract** phase involves reading TPC-H benchmark data from S3. Ray Data's distributed reading capabilities handle the complex table relationships efficiently.

### Loading TPC-H Benchmark Data

```python
# TPC-H benchmark data location
TPCH_S3_PATH = "s3://ray-benchmark-data/tpch/parquet/sf10"

print("Loading TPC-H benchmark data for distributed processing...")
start_time = time.time()

# Read TPC-H Customer Master Data
customers_ds = ray.data.read_parquet(f"{TPCH_S3_PATH}/customer")

customers_ds = customers_ds.drop_columns(["column8"])
customers_ds = customers_ds.rename_columns([
    "c_custkey",
    "c_name", 
    "c_address",
    "c_nationkey",
    "c_phone",
    "c_acctbal",
    "c_mktsegment",
    "c_comment",
])

load_time = time.time() - start_time

print(f"TPC-H customer data loaded in {load_time:.2f} seconds")
print("Sample customer records:")
customers_ds.limit(3).to_pandas()
```

### Advanced Filtering Techniques

Ray Data provides powerful filtering capabilities for complex business logic:

```python
# Business-focused filtering for customer segmentation
print("Advanced Customer Filtering Techniques:")

# Filter high-value customers using business criteria
high_value_customers = customers_ds.filter(
    lambda x: x["c_acctbal"] > 5000 and x["c_mktsegment"] == "AUTOMOBILE"
)

print("High-value automotive customers:")
high_value_customers.limit(5).to_pandas()

# Multi-criteria filtering for market analysis
premium_segments = customers_ds.filter(
    lambda x: x["c_mktsegment"] in ["AUTOMOBILE", "MACHINERY"] and x["c_acctbal"] > 0
)

print("Premium market segments:")
premium_segments.limit(5).to_pandas()

# Geographic filtering for regional analysis
def filter_by_region(batch):
    """Filter customers by geographic criteria."""
    df = pd.DataFrame(batch)
    
    # Complex business logic filtering
    regional_customers = df[
        (df["c_nationkey"].isin([1, 2, 3, 4, 5])) &  # Specific nations
        (df["c_acctbal"] > 1000) &  # Minimum balance
        (df["c_mktsegment"] != "HOUSEHOLD")  # Exclude household segment
    ]
    
    return regional_customers.to_dict('records')

# Apply geographic filtering
regional_analysis = customers_ds.map_batches(
    filter_by_region,
    batch_format="pandas"
)

print("Regional customer analysis:")
regional_analysis.limit(5).to_pandas()
```

## Part 3: Transform - Processing TPC-H Data

The **Transform** phase applies business logic transformations to TPC-H data, demonstrating enterprise-grade data processing patterns.

### Customer Market Segment Analysis

```python
# Analyze customer distribution across market segments
from ray.data.aggregate import Count, Mean

segment_analysis = customers_ds.groupby("c_mktsegment").aggregate(
    Count(),
    Mean("c_acctbal"),
).rename_columns(["c_mktsegment", "customer_count", "avg_account_balance"])

print("Customer Market Segment Distribution:")
segment_analysis.limit(10).to_pandas()
```

### Geographic Reference Data Integration

```python
# Load and process geographic reference data
print("Loading TPC-H Nations Reference Data:")

nation_ds = ray.data.read_parquet(f"{TPCH_S3_PATH}/nation")

nation_ds = (
    nation_ds
    .select_columns(["column0", "column1", "column2", "column3"])
    .rename_columns(["n_nationkey", "n_name", "n_regionkey", "n_comment"])
)

print("Sample nation records:")
nation_ds.limit(5).to_pandas()
```

### Customer Demographics by Nation - Advanced Joins

```python
# Join customer and nation data for geographic analysis
print("Customer Demographics by Nation:")
print("Joining customer and nation data for geographic analysis...")

customer_nation_analysis = (customers_ds
    .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
    .groupby("n_name")
    .aggregate(
        Count(),
        Mean("c_acctbal"),
        Sum("c_acctbal"),
    )
    .rename_columns(["n_name", "customer_count", "avg_balance", "total_balance"])
)

print("Customer distribution by nation:")
customer_nation_analysis.sort("customer_count", descending=True).limit(10).to_pandas()
```

## Part 4: High-Volume Transaction Processing

### Orders Data Processing

```python
# Load TPC-H Orders table (transaction headers)
orders_ds = ray.data.read_parquet(f"{TPCH_S3_PATH}/orders")

orders_ds = (orders_ds
    .select_columns([f"column{i}" for i in range(9)])
    .rename_columns([
        "o_orderkey",
        "o_custkey", 
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority", 
        "o_comment",
    ])
)

print("TPC-H Orders Data:")
print("Sample order records:")
orders_ds.limit(5).to_pandas()
```

### Business Logic Transformations

:::tip GPU Acceleration Option
For even faster performance on large datasets, you can replace `pandas` with **NVIDIA RAPIDS cuDF** in the `map_batches` function below. cuDF provides GPU-accelerated DataFrame operations that can significantly speed up complex transformations, especially beneficial for high-volume TPC-H processing.

Simply replace `import pandas as pd` with `import cudf as pd` to leverage GPU acceleration for the pandas operations.
:::

```python
# Apply business transformations to order data
def enrich_order_data(batch):
    """Apply business logic transformations to TPC-H orders."""
    df = pd.DataFrame(batch)
    
    # Parse order date and create time dimensions
    df['o_orderdate'] = pd.to_datetime(df['o_orderdate'])
    df['order_year'] = df['o_orderdate'].dt.year
    df['order_quarter'] = df['o_orderdate'].dt.quarter
    df['order_month'] = df['o_orderdate'].dt.month
    
    # Business classifications
    df['is_large_order'] = df['o_totalprice'] > 200000
    df['is_urgent'] = df['o_orderpriority'].isin(['1-URGENT', '2-HIGH'])
    df['revenue_tier'] = pd.cut(
        df['o_totalprice'],
        bins=[0, 50000, 150000, 300000, float('inf')],
        labels=['Small', 'Medium', 'Large', 'Enterprise']
    )
    
    return df.to_dict('records')

# Apply transformations to orders
enriched_orders = orders_ds.map_batches(
    enrich_order_data,
    batch_format="pandas"
)

print("Enriched order data with business logic:")
enriched_orders.limit(5).to_pandas()
```

### Advanced Filtering for Business Intelligence

```python
# Advanced filtering techniques for business analysis
print("Business Intelligence Filtering:")

# Filter urgent large orders requiring special handling
urgent_large_orders = enriched_orders.filter(
    lambda x: x["is_urgent"] and x["is_large_order"]
)

print("Urgent large orders requiring expedited processing:")
urgent_large_orders.limit(10).to_pandas()

# Filter orders by time period for trend analysis
recent_orders = enriched_orders.filter(
    lambda x: x["order_year"] >= 1995
)

print("Recent orders for trend analysis:")
recent_orders.limit(5).to_pandas()

# Complex business filtering for operational insights
def complex_business_filter(batch):
    """Apply complex business filtering logic.
    
    Note: For GPU acceleration, replace 'import pandas as pd' with 'import cudf as pd'
    to leverage NVIDIA RAPIDS for faster DataFrame operations on large datasets.
    """
    df = pd.DataFrame(batch)
    
    # Multi-criteria business filtering
    filtered_df = df[
        (df["revenue_tier"].isin(["Large", "Enterprise"])) &
        (df["order_quarter"].isin([3, 4])) &  # Peak seasons
        (df["o_orderstatus"] == "F")  # Completed orders only
    ]
    
    return filtered_df.to_dict('records')

# Apply complex filtering
peak_performance_orders = enriched_orders.map_batches(
    complex_business_filter,
    batch_format="pandas"
)

print("Peak season high-value completed orders:")
peak_performance_orders.limit(10).to_pandas()
```

## Part 5: Aggregations and Analytics

### Executive Dashboard Metrics

```python
# Create executive summary dashboard using TPC-H data
print("Executive Dashboard (Business Intelligence Metrics):")

executive_summary = (enriched_orders
    .groupby("order_quarter")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("o_totalprice"),
    )
    .rename_columns([
        "order_quarter",
        "total_orders",
        "total_revenue", 
        "avg_order_value",
    ])
)

print("Quarterly Business Performance:")
executive_summary.limit(10).to_pandas()
```

### Operational Analytics

```python
# Operational metrics for business process optimization
print("Operational Analytics:")

operational_metrics = (enriched_orders
    .groupby("revenue_tier")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("is_urgent"),
    )
    .rename_columns([
        "revenue_tier",
        "order_volume",
        "total_revenue",
        "urgent_order_percentage",
    ])
)

print("Performance by Revenue Tier:")
operational_metrics.limit(10).to_pandas()

# Priority-based analysis for order management
priority_performance = (enriched_orders
    .groupby("o_orderpriority")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("o_totalprice"),
    )
    .rename_columns([
        "o_orderpriority",
        "priority_orders",
        "priority_revenue", 
        "avg_order_value",
    ])
)

print("Performance by Order Priority:")
priority_performance.sort("priority_revenue", descending=True).limit(10).to_pandas()
```

## Part 6: Advanced ETL Patterns

### Line Items Processing (High-Volume Data)

```python
# Process TPC-H line items (largest table in benchmark)
print("Processing TPC-H Line Items (High-Volume Transaction Data):")

lineitem_ds = ray.data.read_parquet(f"{TPCH_S3_PATH}/lineitem")

lineitem_cols = [f"column{str(i).zfill(2)}" for i in range(16)]
lineitem_ds = (lineitem_ds
    .select_columns(lineitem_cols)
    .rename_columns([
        "l_orderkey",
        "l_partkey", 
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate", 
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ])
)

print("Sample line item records:")
lineitem_ds.limit(3).to_pandas()
```

### Revenue Analysis with Complex Filtering

```python
# TPC-H Query 1: Revenue Analysis with advanced filtering
print("TPC-H Query 1 - Revenue Analysis:")

# Filter line items by shipping date (business date range filtering)
revenue_eligible_items = lineitem_ds.filter(
    lambda x: x["l_shipdate"] <= "1998-09-01"
)

# Revenue analysis by return flag and line status
revenue_analysis = revenue_eligible_items.groupby(["l_returnflag", "l_linestatus"]).aggregate(
    Count(),
    Sum("l_quantity"),
    Sum("l_extendedprice"),
    Mean("l_quantity"),
    Mean("l_extendedprice"),
).rename_columns([
    "l_returnflag",
    "l_linestatus", 
    "order_count",
    "sum_quantity",
    "sum_base_price",
    "avg_quantity",
    "avg_price"
])

print("Revenue analysis results:")
revenue_analysis.limit(10).to_pandas()
```

### Customer Order Analysis with Joins

```python
# Advanced customer analysis combining multiple tables
print("Customer Order Analysis with Multi-Table Joins:")

# Join customers with their orders for comprehensive analysis
customer_orders = customers_ds.join(
    orders_ds,
    left_on="c_custkey",
    right_on="o_custkey"
)

# Filter for specific business scenarios
building_segment_orders = customer_orders.filter(
    lambda x: (
        x["c_mktsegment"] == "BUILDING" and
        x["o_orderdate"] >= "1995-01-01" and
        x["o_totalprice"] > 100000
    )
)

print("Building segment high-value orders:")
building_segment_orders.limit(10).to_pandas()

# Customer spending analysis with aggregations
customer_spending = customer_orders.groupby("c_custkey").aggregate(
    Count(),
    Sum("o_totalprice"),
    Mean("o_totalprice"),
    Max("o_totalprice"),
).rename_columns([
    "c_custkey",
    "order_count", 
    "total_spent",
    "avg_order_value",
    "largest_order"
])

# Find top spending customers
top_customers = customer_spending.sort("total_spent", descending=True)
print("Top customers by spending:")
top_customers.limit(10).to_pandas()
```

## Part 7: Load - Writing Processed Data

The **Load** phase writes the processed TPC-H data to various destinations for business intelligence and analytics.

```python
# Create output directory for processed TPC-H data
import os
OUTPUT_PATH = "/tmp/tpch_etl_output"
os.makedirs(OUTPUT_PATH, exist_ok=True)

print("Writing TPC-H processed data to various formats...")

# Write business intelligence datasets
print("Writing customer segment analysis...")
segment_analysis.write_parquet(f"{OUTPUT_PATH}/customer_segments")

print("Writing executive summary metrics...")
executive_summary.write_parquet(f"{OUTPUT_PATH}/executive_summary")

print("Writing customer geographic analysis...")
customer_nation_analysis.write_parquet(f"{OUTPUT_PATH}/customer_demographics")

print("TPC-H ETL pipeline completed successfully!")
print(f"Processed data available in: {OUTPUT_PATH}")
```

## Key Takeaways

**TPC-H Benchmark Mastery**: Industry-standard data processing patterns applicable to real enterprise scenarios  
**Advanced Filtering**: Complex business logic filtering for sophisticated data analysis requirements  
**Distributed ETL**: Scalable patterns that work from single-node development to distributed production clusters  
**Ray Data Fundamentals**: Core concepts for building production-ready data processing pipelines  

## Troubleshooting Common Issues

### **Problem: "Memory pressure during large joins"**
**Solution**:
```python
# Use smaller batch sizes for memory-intensive operations
large_join = customers_ds.join(orders_ds, 
    left_on="c_custkey", right_on="o_custkey"
).map_batches(process_function, batch_size=1000, concurrency=2)
```

### **Problem: "Slow aggregation performance"**
**Solution**:
```python
# Use Ray Data native aggregations instead of pandas
from ray.data.aggregate import Count, Sum, Mean
result = dataset.groupby("category").aggregate(
    Count(), Sum("amount"), Mean("price")
)
```

### **Problem: "Column name errors"**
**Solution**:
```python
# Always check schema before processing
print(f"Available columns: {dataset.schema().names}")
# Use consistent column naming
dataset = dataset.rename_columns(["col1", "col2", "col3"])
```

## Action Items

### **Immediate Implementation (This Week)**
- [ ] Run TPC-H queries on your cluster to understand performance characteristics
- [ ] Implement basic joins and aggregations using Ray Data native operations
- [ ] Monitor resource utilization with Ray Dashboard
- [ ] Test with different batch sizes to find optimal configuration

### **Production Scaling (Next Month)**
- [ ] Implement additional TPC-H benchmark queries using Ray Data patterns
- [ ] Adapt these patterns to your organization's data processing needs
- [ ] Scale to production clusters for processing larger TPC-H scale factors
- [ ] Set up automated performance monitoring and optimization

## Next Steps

**Advanced Ray Data features**: Explore Ray Data's ML integration and GPU acceleration capabilities  
**Production deployment**: Scale this template to enterprise TPC-H implementations  
**Performance optimization**: Use Ray Dashboard for cluster tuning and resource optimization  

---

*The TPC-H benchmark provides realistic business scenarios - the techniques demonstrated here apply directly to enterprise customer analytics, order processing, and business intelligence applications.*