---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Ray Data for ETL: A Comprehensive Beginner's Guide

This notebook provides a complete introduction to Ray Data for Extract, Transform, Load (ETL) workflows. It also covers both the practical aspects of building ETL pipelines and the underlying architecture that makes Ray Data powerful for distributed data processing.

<div class="alert alert-block alert-info">
<b> Learning Roadmap:</b>
<ul>
    <li><b>Part 1:</b> What is Ray Data and ETL?</li>
    <li><b>Part 2:</b> Ray Data Architecture & Concepts</li>
    <li><b>Part 3:</b> Extract - Reading Data</li>
    <li><b>Part 4:</b> Transform - Processing Data</li>
    <li><b>Part 5:</b> Load - Writing Data</li>
    <li><b>Part 6:</b> Advanced ETL Patterns</li>
    <li><b>Part 7:</b> Performance & Best Practices</li>
    <li><b>Part 8:</b> Troubleshooting Common Issues</li>
</ul>
</div>


## Setup and Imports

Start by importing the necessary libraries and setting up your environment.



```python
import ray
import pandas as pd
import numpy as np
import pyarrow as pa
from typing import Dict, Any
import time

from ray.data import DataContext

# Access the current DataContext and set enable_progress_bars to False to make the outputs cleaner
DataContext.get_current().enable_progress_bars = False

# Initialize Ray
ray.init(ignore_reinit_error=True)

print(f"Ray version: {ray.__version__}")
print(f"Ray cluster resources: {ray.cluster_resources()}")

```

## Part 1: What is Ray Data and ETL?

### Understanding ETL

**ETL** stands for Extract, Transform, Load - a fundamental pattern in data engineering:

- **Extract**: Reading data from various sources (databases, files, APIs, etc.)
- **Transform**: Processing, cleaning, and enriching the data
- **Load**: Writing the processed data to destination systems

### What is Ray Data?

Ray Data is a distributed data processing library built on top of Ray that has recently reached **General Availability (GA)**. As the fastest-growing use case for Ray, it's designed to handle **both traditional ETL/ML workloads and next-generation AI applications**, providing a unified platform that scales from CPU clusters to heterogeneous GPU environments.

<div class="alert alert-block alert-success">
<b> Ray Data: One Platform for All Data Workloads</b><br>
Ray Data is part of Ray, the AI Compute Engine that now orchestrates <b>over 1 million clusters per month</b>. Whether you're running traditional ETL on CPU clusters or cutting-edge multimodal AI pipelines, Ray Data provides a unified solution that evolves with your needs.
</div>

<div class="alert alert-block alert-info">
<b> Ray Data: From Traditional to Transformational:</b>
<ul>
    <li><b>Traditional ETL:</b> Excellent for structured data processing, business intelligence, and reporting</li>
    <li><b>ML Workflows:</b> Perfect for feature engineering, model training pipelines, and batch scoring</li>
    <li><b>Scalable Processing:</b> Automatically scales from single machines to thousands of CPU cores</li>
    <li><b>Future-Ready:</b> Seamlessly extends to GPU workloads and multimodal data when needed</li>
    <li><b>Python-Native:</b> No JVM overhead - pure Python performance at scale</li>
    <li><b>Streaming Architecture:</b> Handle datasets larger than memory with ease</li>
</ul>
</div>

### Today's Workloads, Tomorrow's Possibilities

Ray Data excels across the entire spectrum of data processing needs:

**Traditional & Current Workloads:**
- **Business ETL**: Customer analytics, financial reporting, operational dashboards
- **Classical ML**: Recommendation systems, fraud detection, predictive analytics
- **Data Engineering**: Large-scale data cleaning, transformation, and aggregation

**Next-Generation Workloads:**
- **Multimodal AI**: Processing text, images, video, and audio together
- **LLM Pipelines**: Fine-tuning, embedding generation, and batch inference
- **Computer Vision**: Image preprocessing and model inference at scale
- **Compound AI Systems**: Orchestrating multiple models and traditional ML

### Ray Data vs Traditional Tools

See the table below to understand how Ray Data compares to other data processing tools across traditional and modern workloads:

| Feature | Ray Data | Pandas | Spark | Dask |
|---------|----------|--------|-------|------|
| **Scale** | Multi-machine | Single-machine | Multi-machine | Multi-machine |
| **Memory Strategy** | Streaming | In-memory | Mixed | In-Memory |
| **Python Performance** | Native (no JVM) | Native | JVM overhead | Native |
| **CPU Clusters** | Excellent | Single-node | Excellent | Good |
| **GPU Support** | Native | None | Limited | Limited |
| **Classical ML** | Excellent | Limited | Limited | Good |
| **Multimodal Data** | Optimized | Limited | Limited | Limited |
| **Fault Tolerance** | Built-in | None | Built-in | Limited |

### Real-World Impact Across All Workloads

Organizations worldwide are seeing dramatic results with Ray Data for both traditional and advanced workloads:

**Traditional ETL & Analytics:**
- **Amazon**: Migrated an exabyte-scale workload from Spark to Ray Data, cutting costs by **82%** and saving **$120 million annually**
- **Instacart**: Processing **100x more data** for recommendation systems and business analytics
- **Financial Services**: Major banks using Ray Data for fraud detection and risk analytics at scale

**Modern AI & ML:**
- **Niantic**: Reduced code complexity by **85%** while scaling AR/VR data pipelines
- **Canva**: Cut cloud costs in **half** while processing design assets and user data
- **Pinterest**: Boosted GPU utilization to **90%+** for image processing and recommendations

Ray Data provides a unified platform that excels at traditional ETL, classical ML, and next-generation AI workloads - eliminating the need for multiple specialized systems.


## Part 2: Ray Data Architecture & Concepts

### Ray Data Architecture

Ray Data's architecture addresses the core challenges of modern AI infrastructure:

<div class="alert alert-block alert-success">
<b> Ray: Built for the Modern Era of Computing</b><br>
Unlike traditional distributed systems, Ray was purpose-built for:<br>
<ul>
    <li><b>Python-Native:</b> No JVM overhead or serialization bottlenecks</li>
    <li><b>Heterogeneous Compute:</b> Seamlessly orchestrates CPUs, GPUs, and other accelerators</li>
    <li><b>Dynamic Workloads:</b> Adapts to varying compute needs in real-time</li>
    <li><b>Fault Tolerance:</b> Handles failures gracefully at massive scale</li>
</ul>
</div>

![Ray Data Architecture](https://docs.ray.io/en/latest/_images/dataset-arch.svg)

### Core Concepts

This section explains fundamental concepts that power Ray Data.

#### 1. Datasets and Blocks

A **Dataset** in Ray Data is a distributed collection of data that's divided into **blocks**. Think of blocks as chunks of your data that can be processed independently.

<div class="alert alert-block alert-info">
<b> Understanding Blocks:</b>
<ul>
    <li>Each block contains a subset of your data (typically 1-128 MB)</li>
    <li>Blocks are stored in Ray's distributed object store</li>
    <li>Operations are applied to blocks in parallel across the cluster</li>
    <li>Block size affects performance - too small causes overhead, too large causes memory issues</li>
</ul>
</div>


```python
# Let's create a simple dataset to understand blocks
# Create sample data
import numpy as np

data = [{"id": i, "name": f"name_{i}", "value": float(np.random.rand())} for i in range(1000)]
ds = ray.data.from_items(data)

print(f"Dataset: {ds}")
print(f"Number of blocks: {ds.num_blocks()}")
print(f"Schema: {ds.schema()}")

print("\nFirst 3 rows:")
for i, row in enumerate(ds.take(3)):
    print(f"Row {i}: {row}")

```

#### 2. Execution Model

Ray Data uses **lazy execution** by default, meaning operations aren't executed immediately but are planned and optimized before execution.

**Lazy Execution Benefits:**
- **Optimization**: Ray Data can optimize the entire pipeline before execution
- **Memory efficiency**: Only necessary data is loaded into memory
- **Fault tolerance**: Can restart from intermediate points if failures occur

<div class="alert alert-block alert-warning">
<b> Understanding Execution:</b><br>
<b>Lazy:</b> Build a plan first, then execute (default)<br>
<b>Eager:</b> Execute operations immediately as they're called<br><br>
Lazy execution allows Ray Data to optimize your entire pipeline for better performance.
</div>

Ray Data also utilizes **streaming execution**, which helps combine the best of structured streaming and batch inference. Streaming execution doesn’t wait for one operator to complete to start the next. Each operator takes in and outputs a stream of blocks. This approach allows you to process datasets that are too large to fit in your cluster’s memory and can be 

![Streaming Execution](https://images.ctfassets.net/xjan103pcp94/6EpeVQ743xNDZZfZyEfKFr/16069060673a8667b08be34aa828cc60/image8.jpg)




```python
import pandas as pd
import numpy as np

def transform(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    batch["value"] = batch["value"] * 2
    batch = batch[batch["value"] > 1.0]
    batch["category"] = np.where(batch["value"] > 1.5, "high", "medium")
    return batch

ds_lazy = ds.map_batches(transform, batch_format="pandas")  # still lazy

# Calling take() to actually execute 
result = ds_lazy.take(5)

```

## Part 3: Extract - Reading Data

The **Extract** phase involves reading data from various sources. Ray Data provides built-in connectors for many common data sources and makes it easy to scale data reading across a distributed cluster, especially for the **multimodal data** that powers modern AI applications.

### The Multimodal Data Revolution

Today's AI applications process vastly more complex data than traditional ETL pipelines:

<div class="alert alert-block alert-success">
<b> The Scale of Modern Data:</b>
<ul>
    <li><b>Unstructured Data Growth:</b> Now outpaces structured data by 10x+ in most organizations</li>
    <li><b>Video Processing:</b> Companies like OpenAI (Sora), Pinterest, and Apple process petabytes of multimodal data daily</li>
    <li><b>Foundation Models:</b> Require processing millions of images, videos, and documents</li>
    <li><b>AI-Powered Processing:</b> Every aspect of data processing is becoming AI-enhanced</li>
</ul>
</div>

### How Ray Data Reads Data Under the Hood

When you read data with Ray Data, here's what happens:

1. **File Discovery**: Ray Data discovers all files matching your path pattern
2. **Task Creation**: Files are distributed across Ray tasks (typically one file per task)
3. **Parallel Reading**: Multiple tasks read files simultaneously across the cluster
4. **Block Creation**: Each task creates data blocks stored in Ray's object store
5. **Lazy Planning**: The dataset is created but data isn't loaded until needed

This architecture enables Ray Data to efficiently handle both traditional structured data and modern unstructured formats that power AI applications.

<div class="alert alert-block alert-info">
<b> Built-in Data Sources:</b>
<ul>
    <li><b>Structured:</b> Parquet, CSV, JSON, Arrow</li>
    <li><b>Unstructured:</b> Images, Videos, Audio, Binary files</li>
    <li><b>Databases:</b> MongoDB, MySQL, PostgreSQL, Snowflake</li>
    <li><b>Cloud Storage:</b> S3, GCS, Azure Blob Storage</li>
    <li><b>Data Lakes:</b> Delta Lake, Iceberg (with RayTurbo)</li>
    <li><b>ML Formats:</b> TensorFlow Records, PyTorch datasets</li>
    <li><b>Memory:</b> Python lists, NumPy arrays, Pandas DataFrames</li>
</ul>
</div>

### Enterprise-Grade Data Connectivity

For enterprise environments, **Anyscale** provides additional connectors and optimizations:
- **Enhanced Security**: Integration with enterprise identity systems
- **Governance Controls**: Data lineage and access controls
- **Performance Optimization**: RayTurbo's streaming metadata fetching provides up to **4.5x faster** data loading
- **Hybrid Deployment**: Support for Kubernetes, on-premises, and multi-cloud environments


### Accessing TPC-H Benchmark Data

This example uses the industry-standard TPC-H benchmark dataset. This provides realistic enterprise-scale data that's used by companies worldwide to evaluate data processing systems and represents real business scenarios with complex relationships between customers, orders, suppliers, and products.



```python
# Using TPC-H Benchmark Dataset - Industry Standard for Data Processing
# TPC-H is the gold standard benchmark for decision support systems and analytics

# TPC-H S3 data location
TPCH_S3_PATH = "s3://ray-benchmark-data/tpch/parquet/sf10"

# TPC-H Schema Overview
tpch_tables = {
    "customer": "Customer master data with demographics and market segments",
    "orders": "\tOrder header information with dates, priorities, and status",
    "lineitem": "Detailed line items for each order (largest table ~6B rows)",
    "part": "\tParts catalog with specifications and retail prices", 
    "supplier": "Supplier information including contact details and geography",
    "partsupp": "Part-supplier relationships with costs and availability",
    "nation": "\tNation reference data with geographic regions",
    "region": "\tRegional groupings for geographic analysis"
}

print(f"\n TPC-H Schema (8 Tables):")
for table, description in tpch_tables.items():
    print(f"\t{table.upper()}:\t{description}")

```


```python
# Read TPC-H Customer Master Data (Traditional Structured Data Processing)
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

print(" TPC-H Customer Master Data (Traditional ETL):")
print(f"Schema: {customers_ds.schema()}")
print(f"Total customers: {customers_ds.count():,}")
print("Sample customer records:")
customers_ds.limit(25).to_pandas()

```


```python
from ray.data.aggregate import Count, Mean

segment_analysis = customers_ds.groupby("c_mktsegment").aggregate(
    Count(),             # counts rows per segment
    Mean("c_acctbal"),   # average account balance
    ).rename_columns(["c_mktsegment", "customer_count", "avg_account_balance"]
)

print("Customer Market Segment Distribution:")
segment_analysis.show(5)
```


```python
# Geographic Reference Data - Nations Table
print("TPC-H Nations Reference Data:")
print("Loading geographic data for customer demographics...")

nation_ds = ray.data.read_parquet(f"{TPCH_S3_PATH}/nation")

nation_ds = (
    nation_ds
    .select_columns(["column0", "column1", "column2", "column3"])
    .rename_columns(["n_nationkey", "n_name", "n_regionkey", "n_comment"])
)

print(f"Total nations: {nation_ds.count():,}")

print("\n Sample nation records:")
nation_ds.limit(25).to_pandas()

```


```python
# Customer Demographics by Nation - Join Analysis
print(" Customer Demographics by Nation:")
print("   Joining customer and nation data for geographic analysis...")

from ray.data.aggregate import Count, Mean, Sum

customer_nation_analysis = (customers_ds
    .join(nation_ds, on=("c_nationkey",), right_on=("n_nationkey",), join_type="inner", num_partitions=100)
    .groupby("n_name")
    .aggregate(
        Count(),
        Mean("c_acctbal"),
        Sum("c_acctbal"),
)
.rename_columns(["n_name", "customer_count", "avg_balance", "total_balance"])
)

customer_nation_analysis.sort("customer_count", descending=True).limit(10).to_pandas()

```


```python
# Read TPC-H High-Volume Transactional Data (Orders + Line Items)

# Read Orders table (header information)
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

print("TPC-H Orders Data (Enterprise Transaction Processing):")
print("Sample order records:")
orders_ds.limit(10).to_pandas()
```


```python
# Read Line Items table (detailed transaction data - largest table in TPC-H)
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
#lineitem_ds = lineitem_ds.limit(10000)

print(f"TPC-H Line Items Data (Detailed Transaction Processing):")
print(f"Schema: {lineitem_ds.schema()}")
print(f"Total line items: {lineitem_ds.count():,}")

print("\n Sample line item records:")
lineitem_ds.limit(25).to_pandas()
```


```python
from ray.data.aggregate import Count, Mean, Sum

# Order priority analysis - Ray Dataset API
order_priority_analysis = (orders_ds
    .groupby("o_orderpriority")
    .aggregate(
        Count(),
        Mean("o_totalprice"),
        Sum("o_totalprice"),
    )
    .rename_columns(["o_orderpriority", "order_count", "avg_total_price", "total_value"])
)

# Time-based order analysis
orders_with_year = orders_ds.map(lambda r: {**r, "order_year": r["o_orderdate"].year})

yearly_revenue = (orders_with_year
    .groupby("order_year")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("o_totalprice"),
    )
    .rename_columns(["order_year", "yearly_orders", "yearly_revenue", "avg_order_value"])
)


```

## Part 4: Transform - Processing Data

The **Transform** phase is where the real data processing happens. Ray Data provides several transformation operations that can be applied to datasets, and understanding how they work under the hood is key to building efficient ETL pipelines that power modern AI applications.

### Transformations for the AI Era

Modern AI workloads require more than traditional data transformations. Ray Data is designed for the era of **compound AI systems** and **agentic workflows** where:

<div class="alert alert-block alert-success">
<b> AI-Powered Transformations:</b>
<ul>
    <li><b>Multimodal Processing:</b> Simultaneously process text, images, video, and audio</li>
    <li><b>Model Inference:</b> Embed ML models directly into transformation pipelines</li>
    <li><b>GPU Acceleration:</b> Seamlessly utilize both CPU and GPU resources</li>
    <li><b>Compound AI:</b> Orchestrate multiple models and traditional ML within single workflows</li>
    <li><b>AI-Enhanced ETL:</b> Use AI to optimize every aspect of data processing</li>
</ul>
</div>

### How Ray Data Processes Transformations

When you apply transformations with Ray Data:

1. **Task Distribution**: Transformations are distributed across Ray tasks/actors
2. **Block-level Processing**: Each task processes one or more blocks independently  
3. **Streaming Execution**: Blocks flow through the pipeline without waiting for all data
4. **Operator Fusion**: Compatible operations are automatically combined for efficiency
5. **Heterogeneous Compute**: Intelligently schedules CPU and GPU work
6. **Fault Tolerance**: Failed tasks are automatically retried

This architecture enables Ray Data to handle everything from traditional business logic to cutting-edge AI inference within the same pipeline.

<div class="alert alert-block alert-info">
<b> Transformation Categories:</b>
<ul>
    <li><b>Row-wise operations:</b> <code>map()</code> - Transform individual rows</li>
    <li><b>Batch operations:</b> <code>map_batches()</code> - Transform groups of rows (ideal for ML inference)</li>
    <li><b>Filtering:</b> <code>filter()</code> - Remove rows based on conditions</li>
    <li><b>Aggregations:</b> <code>groupby()</code> - Group and aggregate data</li>
    <li><b>Joins:</b> <code>join()</code> - Combine datasets</li>
    <li><b>AI Operations:</b> Embed models for inference, embeddings, and feature extraction</li>
    <li><b>Shuffling:</b> <code>random_shuffle()</code>, <code>sort()</code> - Reorder data</li>
</ul>
</div>

While most other data frameworks support similar map operations through UDFs, with Ray Data, these are treated as first-class supported features. Instead of just arbitrary operations sent to partitions, Ray Data has several key advantages:
- Each task can have task-level concurrency and hardware allocation set instead of just global settings for all UDFs.
- These tasks support PyArrow, pandas, and NumPy format, which provides easy integrations to the rest of the Python ecosystem.
- These tasks support stateful actors, which supports initializing expensive steps like downloading an AI model, only once per replica instead of per-invocation.
- For more advanced use cases, Ray Core can be run inside of the task, supporting nested parallelism algorithms. This is useful for HPC-style applications with complicated compute tasks on top of big data.


<div class="alert alert-block alert-success">
<b> GPU Optimizations:</b>
<ul>
    <li><b>Nvidia RAPIDS:</b> <code>map_batches()</code> - Pandas ETL operations can be sped up using the Nvidia cuDF library to run the slower sections of ETL logic onto GPUs.</li>
    <li><b>Batch Inference:</b> <code>map_batches()</code> - GPU AI batch inference can be used for unstructured data ingestion or LLM processing, amongst many other use cases</li>
    <li><b>AI Training:</b> - Many data pipelines, such as time series analysis, train many small models over sections of the data. These smaller ML models, such as XGBoost models, can be trained using GPUs for faster performance</li>
</ul>
</div>

This tutorial focuses on traditional CPU-based ETL workloads, but there are other templates available for batch inference using GPUs if you are interested in learning further.

### Practical ETL Transformations

This section implements common ETL transformations using the e-commerce data:

#### 1. Data Enrichment with Business Logic



```python
def traditional_etl_enrichment_tpch(batch):
    """
    Traditional ETL transformations for TPC-H business intelligence and reporting
    This demonstrates classic data warehouse-style transformations on enterprise data
    """
    #df = batch.to_pandas() if hasattr(batch, 'to_pandas') else pd.DataFrame(batch)
    df = batch
    
    # Parse order date and create time dimensions (standard BI practice)
    df['o_orderdate'] = pd.to_datetime(df['o_orderdate'])
    df['order_year'] = df['o_orderdate'].dt.year
    df['order_quarter'] = df['o_orderdate'].dt.quarter
    df['order_month'] = df['o_orderdate'].dt.month
    df['order_day_of_week'] = df['o_orderdate'].dt.dayofweek
    
    # Business day classifications (common in traditional ETL)
    df['is_weekend'] = df['order_day_of_week'].isin([5, 6])
    df['quarter_name'] = 'Q' + df['order_quarter'].astype(str)
    df['month_name'] = df['o_orderdate'].dt.month_name()
    
    # Revenue and profit calculations (standard BI metrics)
    df['revenue_tier'] = pd.cut(
        df['o_totalprice'],
        bins=[0, 50000, 150000, 300000, float('inf')],
        labels=['Small', 'Medium', 'Large', 'Enterprise']
    )
    
    # Order priority business rules (TPC-H specific)
    priority_weights = {
        '1-URGENT': 1.0,
        '2-HIGH': 0.8,
        '3-MEDIUM': 0.6,
        '4-NOT SPECIFIED': 0.4,
        '5-LOW': 0.2
    }
    df['priority_weight'] = df['o_orderpriority'].map(priority_weights).fillna(0.4)
    df['weighted_revenue'] = df['o_totalprice'] * df['priority_weight']
    
    # Order status analysis
    df['is_urgent'] = df['o_orderpriority'].isin(['1-URGENT', '2-HIGH'])
    df['is_large_order'] = df['o_totalprice'] > 200000
    df['requires_expedited_processing'] = df['is_urgent'] | df['is_large_order']
    
    # Date-based business logic
    df['days_to_process'] = (pd.to_datetime(df['o_orderdate']) - pd.Timestamp('1992-01-01')).dt.days
    df['is_peak_season'] = df['order_month'].isin([11, 12])  # Nov-Dec peak
    
    return df

def ml_ready_feature_engineering_tpch(batch):
    """
    Modern ML feature engineering for TPC-H data
    This prepares enterprise data for machine learning models
    """
    #df = batch.to_pandas() if hasattr(batch, 'to_pandas') else pd.DataFrame(batch)
    df = batch
    
    # Temporal features for ML models
    df['days_since_epoch'] = (df['o_orderdate'] - pd.Timestamp('1992-01-01')).dt.days
    df['month_sin'] = np.sin(2 * np.pi * df['order_month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['order_month'] / 12)
    df['quarter_sin'] = np.sin(2 * np.pi * df['order_quarter'] / 4)
    df['quarter_cos'] = np.cos(2 * np.pi * df['order_quarter'] / 4)
    
    # Priority encoding for ML (one-hot style features)
    for priority in ['1-URGENT', '2-HIGH', '3-MEDIUM']:
        df[f'is_priority_{priority.split("-")[0]}'] = (df['o_orderpriority'] == priority).astype(int)
    
    # Revenue-based features (common in ML)
    df['log_total_price'] = np.log1p(df['o_totalprice'])  # Log transformation for ML
    df['revenue_per_priority'] = df['o_totalprice'] * df['priority_weight']
    df['weekend_large_order'] = (df['is_weekend'] & df['is_large_order']).astype(int)
    
    # Time-series features for predictive modeling
    df['year_normalized'] = (df['order_year'] - df['order_year'].min()) / (df['order_year'].max() - df['order_year'].min())
    df['seasonal_revenue_multiplier'] = np.where(df['is_peak_season'], 1.2, 1.0)
    
    # Customer key features (for customer analytics)
    df['customer_id_mod_100'] = df['o_custkey'] % 100  # Simple customer segmentation feature
    
    return df

traditional_enriched = orders_ds.map_batches(
    traditional_etl_enrichment_tpch,
    batch_format="pandas",
    batch_size=1024  # Larger batches for efficiency 
)

print("Traditional ETL Results:")
traditional_enriched.limit(25).to_pandas()

```


```python
# ML-Ready Feature Engineering (Preparing enterprise data for model training/inference)
print("ML-Ready Feature Engineering (Next-Generation Capabilities):")
print("Adding ML features for predictive analytics on enterprise transaction data...")

enriched_orders = traditional_enriched.map_batches(
    ml_ready_feature_engineering_tpch,
    batch_format="pandas",
    batch_size=10000
)
enriched_orders.limit(25).to_pandas()
```

#### 2. Aggregations and Analytics

Aggregations are essential for creating summary statistics and business metrics. Ray Data's `groupby()` operations distribute the computation across the cluster.

<div class="alert alert-block alert-warning">
<b> Under the Hood - GroupBy Operations:</b><br>
When you perform a GroupBy operation, Ray Data:<br>
<ol>
    <li><b>Shuffle Phase:</b> Data is redistributed so all records with the same key end up on the same node</li>
    <li><b>Local Aggregation:</b> Each node performs aggregation on its subset of data</li>
    <li><b>Result Collection:</b> Final aggregated results are collected</li>
</ol>
</div>

You can make several new datasets by aggregating data together.



```python
from ray.data.aggregate import Count, Mean, Sum, Max

#Executive Summary Dashboard - typical BI metrics on enterprise data
print("Executive Dashboard (Traditional BI on TPC-H):")
executive_summary = (enriched_orders
    .groupby("order_quarter")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("o_totalprice"),
        Sum("weighted_revenue"),
        Mean("is_urgent"),
    )
    .rename_columns([
        "order_quarter",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "weighted_revenue",
        "urgent_order_percentage",
    ])
)

print("Quarterly Business Performance:")
executive_summary.limit(25).to_pandas()
```


```python

#Operational Analytics - business process optimization
print("Operational Analytics (Enterprise Process Optimization):")
operational_metrics = (enriched_orders
    .groupby("revenue_tier")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("priority_weight"),
        Mean("requires_expedited_processing"),
        Sum("is_peak_season"),
    )
    .rename_columns([
        "revenue_tier",
        "order_volume",
        "total_revenue",
        "avg_priority_weight",
        "expedited_processing_rate",
        "peak_season_orders",
    ])
)

# Uncomment for viewing dataset output
print("Performance by Revenue Tier:")
operational_metrics.limit(25).to_pandas()
```


```python

#Priority-Based Analysis - enterprise order management
print("\n Priority-Based Analysis (Order Management Insights):")
priority_performance = (enriched_orders
    .groupby("o_orderpriority")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("o_totalprice"),
        Mean("is_large_order"),
        Mean("is_weekend"),
    )
    .rename_columns([
        "o_orderpriority",
        "priority_orders",
        "priority_revenue",
        "avg_order_value",
        "large_order_rate",
        "weekend_order_rate",
    ])
)

# Uncomment for viewing dataset output
# print("Performance by Order Priority:")
priority_performance.sort("priority_revenue", descending=True).limit(25).to_pandas()
```


```python
#Temporal Business Analysis - time-series insights
print("Temporal Analysis (Time-Series Business Intelligence):")
temporal_intelligence = (enriched_orders
    .groupby("order_year")
    .aggregate(
        Count(),
        Sum("o_totalprice"),
        Mean("o_totalprice"),
        Mean("is_peak_season"),
        Mean("is_large_order"),
    )
    .rename_columns([
        "order_year",
        "yearly_orders",
        "yearly_revenue",
        "avg_order_value",
        "peak_season_rate",
        "large_order_percentage",
    ])
)

# Uncomment for viewing dataset output
print("Year-over-Year Performance:")
temporal_intelligence.sort("order_year").limit(25).to_pandas()
```

## Part 5: Load - Writing Data

The **Load** phase involves writing the processed data to destination systems. Ray Data supports writing to various formats and destinations, and understanding how this works helps you optimize for your use case.

### How Ray Data Writes Data

When you write data with Ray Data:

1. **Parallel Writing**: Multiple tasks write data simultaneously across the cluster
2. **Partitioned Output**: Data is written as multiple files (one per block typically)
3. **Format Optimization**: Ray Data optimizes the writing process for each format
4. **Streaming Writes**: Large datasets can be written without loading everything into memory

<div class="alert alert-block alert-info">
<b> Supported Output Formats:</b>
<ul>
    <li><b>Files:</b> Parquet, CSV, JSON</li>
    <li><b>Databases:</b> MongoDB, MySQL, PostgreSQL</li>
    <li><b>Cloud Storage:</b> S3, GCS, Azure Blob Storage</li>
    <li><b>Lakehouse Formats:</b> Delta Lake (coming soon), Iceberg, Hudi</li>
    <li><b>Custom:</b> Implement your own writers using <code>FileBasedDatasource</code></li>
</ul>
</div>



```python
# Create output directories for TPC-H processed data
import os
OUTPUT_PATH = "/mnt/cluster_storage/tpch_etl_output"

os.makedirs(OUTPUT_PATH, exist_ok=True)

print(" Writing TPC-H processed data to various formats...")

# Write enriched TPC-H orders to Parquet (best for large enterprise datasets)
print(" Writing enriched TPC-H orders to Parquet...")
enriched_orders.write_parquet(f"{OUTPUT_PATH}/enriched_orders")
executive_summary.write_parquet(f"{OUTPUT_PATH}/executive_summary")
operational_metrics.write_parquet(f"{OUTPUT_PATH}/operational_metrics")
priority_performance.write_parquet(f"{OUTPUT_PATH}/priority_performance")
temporal_intelligence.write_parquet(f"{OUTPUT_PATH}/temporal_intelligence")
```

## Summary: Your Journey with Ray Data ETL

You've completed a comprehensive journey through Ray Data for ETL. This section summarizes what you've learned and explore how to take your AI data pipelines to production.

<div class="alert alert-block alert-success">
<b> What You've Mastered:</b>
<ul>
    <li><b>Ray Data Fundamentals:</b> Blocks, lazy execution, streaming processing</li>
    <li><b>Extract Phase:</b> Reading from multiple data sources efficiently, including multimodal data</li>
    <li><b>Transform Phase:</b> Distributed data processing and feature engineering</li>
    <li><b>Load Phase:</b> Writing to various destinations with optimization</li>
    <li><b>Production Patterns:</b> Error handling, monitoring, and data quality</li>
    <li><b>Performance Optimization:</b> Understanding bottlenecks and solutions</li>
</ul>
</div>

### When to Use Ray Data

**Ray Data excels across the full spectrum of data workloads:**

**Traditional ETL & Business Intelligence:**
- **High-volume transaction processing** for e-commerce, finance, and operations
- **Business intelligence** and executive reporting at scale
- **Data warehouse** loading and transformation pipelines
- **CPU cluster optimization** with pure Python performance (no JVM overhead)
- **Traditional analytics** that need to scale beyond single-node tools

**Modern ML & AI Workloads:**
- **Feature engineering** for machine learning at scale
- **Batch inference** on foundation models and LLMs
- **Multimodal data processing** (text, images, video, audio)
- **GPU-accelerated pipelines** for AI applications
- **Real-time model serving** and inference workloads

**Ray Data's Unified Platform Advantage:**
- **One system** for both traditional ETL and cutting-edge AI
- **Seamless evolution** from CPU-based analytics to GPU-powered AI
- **No migration** required as your data needs grow and change
- **Consistent APIs** whether processing structured business data or unstructured AI content

**Ray is proven at scale:**
- Processing **exabyte-scale** workloads
- **1M+ clusters** orchestrated monthly across the Ray ecosystem
- **$120M annual savings** achieved by leading enterprises
- **Traditional workloads** running alongside **next-generation AI** on the same platform

### From Open Source to Enterprise: Anyscale Platform

While Ray Data open source provides powerful capabilities, **Anyscale** offers a unified AI platform for production deployments:

<div class="alert alert-block alert-info">
<b> Anyscale: The Unified AI Platform</b>
<ul>
    <li><b>RayTurbo Runtime:</b> Up to 5.1x performance improvements over open source</li>
    <li><b>Enterprise Governance:</b> Resource quotas, usage tracking, and advanced observability</li>
    <li><b>AI Anywhere:</b> Deploy on Kubernetes, hybrid cloud, or any infrastructure</li>
    <li><b>LLM Suite:</b> Complete capabilities for embeddings, fine-tuning, and serving</li>
    <li><b>Marketplace Ready:</b> Available on AWS and GCP Marketplaces</li>
</ul>
</div>

### Production Deployment Options

**Getting Started:**
1. **Ray Open Source**: Perfect for development and smaller workloads
2. **Anyscale Platform**: Enterprise features with RayTurbo optimizations
3. **Marketplace Deployment**: One-click setup with AWS or GCP Marketplace

### Key Architectural Insights

Understanding how Ray Data works under the hood helps you build better pipelines:

1. **AI-Native Architecture**: Purpose-built for Python, GPUs, and multimodal data
2. **Streaming Execution**: Process datasets larger than cluster memory
3. **Heterogeneous Compute**: Seamlessly orchestrate CPUs, GPUs, and other accelerators
4. **Operator Fusion**: Combines compatible operations for efficiency
5. **Enterprise Scalability**: Proven to scale to 8,000+ nodes

### Production Readiness Checklist

Before deploying Ray Data pipelines to production:

-  **Architecture**: Choose between Ray OSS and Anyscale based on your needs
-  **Performance**: Consider RayTurbo for production workloads requiring maximum efficiency
-  **Governance**: Implement enterprise controls for AI sprawl and cost management
-  **Security**: Leverage enterprise identity integration and access controls
-  **Monitoring**: Use advanced observability tools for optimization insights
-  **Scalability**: Test with realistic data volumes and cluster sizes

### Join the Ray Ecosystem

The Ray community is thriving with **1,000+ contributors** and growing:

1. **Community**: Join the Ray Slack community for support and discussions
2. **Learning**: Access Ray Summit sessions and technical deep-dives
3. **Contributing**: Contribute to the fastest-growing AI infrastructure project
4. **Enterprise Support**: Explore Anyscale for production deployments

<div class="alert alert-block alert-success">
<b> One Platform for All Your Data Workloads</b><br>
You now have the knowledge to build production-ready, scalable data pipelines that handle everything from traditional business ETL to cutting-edge AI applications. Whether you're processing millions of e-commerce transactions for business intelligence or preparing multimodal data for foundation models, Ray Data provides a unified platform that scales with your needs.<br><br>
<b>Start with traditional ETL today, evolve to AI tomorrow - all on the same platform.</b> Ray Data and Anyscale eliminate the complexity of managing multiple systems as your data requirements grow.
</div>




```python

```
