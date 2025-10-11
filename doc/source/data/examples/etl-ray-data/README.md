---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Ray Data for ETL: a comprehensive beginner's guide

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/etl-ray-data"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/data/examples/etl-ray-data" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This notebook provides a complete introduction to Ray Data for Extract, Transform, Load (ETL) workflows. It covers both the practical aspects of building ETL pipelines and the underlying architecture that makes Ray Data powerful for distributed data processing.

Learning roadmap:

- **Part 1**: What is Ray Data and ETL?
- **Part 2**: Ray Data architecture and concepts
- **Part 3**: Extract - reading data
- **Part 4**: Transform - processing data
- **Part 5**: Load - writing data
- **Part 6**: Advanced ETL patterns
- **Part 7**: Performance and best practices
- **Part 8**: Troubleshooting common issues

## Setup and imports

Import the necessary libraries and set up your environment.



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

Ray Data is a distributed data processing library built on top of Ray and is a **General Availability (GA)** component. As the fastest-growing use case for Ray, it's designed to handle **both traditional ETL/ML workloads and next-generation AI applications**, providing a unified platform that scales from CPU clusters to heterogeneous GPU environments.

**Tip: Ray Data - One platform for all data workloads**

Ray Data is part of Ray, the AI Compute Engine that orchestrates **over 1 million clusters per month**. Whether you're running traditional ETL on CPU clusters or cutting-edge multimodal AI pipelines, Ray Data provides a unified solution that evolves with your needs.

**Note: Ray Data - From traditional to transformational**

- **Traditional ETL**: Excellent for structured data processing, business intelligence, and reporting
- **ML workflows**: Well-suited for feature engineering, model training pipelines, and batch scoring
- **Scalable processing**: Automatically scales from single machines to thousands of CPU cores
- **Future-ready**: Seamlessly extends to GPU workloads and multimodal data when needed
- **Python-native**: No JVM overhead - pure Python performance at scale
- **Streaming architecture**: Handle datasets larger than memory with ease

### Today's workloads, tomorrow's possibilities

Ray Data excels across the entire spectrum of data processing needs:

**Traditional and current workloads:**
- **Business ETL**: Customer analytics, financial reporting, operational dashboards
- **Classical ML**: Recommendation systems, fraud detection, predictive analytics
- **Data engineering**: Large-scale data cleaning, transformation, and aggregation

**Next-generation workloads:**
- **Multimodal AI**: Processing text, images, video, and audio together
- **LLM pipelines**: Fine-tuning, embedding generation, and batch inference
- **Computer vision**: Image preprocessing and model inference at scale
- **Compound AI systems**: Orchestrating multiple models and traditional ML

### Ray Data versus traditional tools

See the table below to understand how Ray Data compares to other data processing tools across traditional and modern workloads:

| Feature | Ray Data | Pandas | Spark | Dask |
|---------|----------|--------|-------|------|
| **Scale** | Multi-machine | Single-machine | Multi-machine | Multi-machine |
| **Memory strategy** | Streaming | In-memory | Mixed | In-Memory |
| **Python performance** | Native (no JVM) | Native | JVM overhead | Native |
| **CPU clusters** | Excellent | Single-node | Excellent | Good |
| **GPU support** | Native | None | Limited | Limited |
| **Classical ML** | Excellent | Limited | Limited | Good |
| **Multimodal data** | Optimized | Limited | Limited | Limited |
| **Fault tolerance** | Built-in | None | Built-in | Limited |

### Real-world impact across all workloads

Organizations worldwide are seeing dramatic results with Ray Data for both traditional and advanced workloads:

**Traditional ETL and analytics:**
- **Amazon**: Migrated an exabyte-scale workload from Spark to Ray Data, cutting costs by **82%** and saving **$120 million annually**
- **Instacart**: Processing **100x more data** for recommendation systems and business analytics
- **Financial services**: Major banks using Ray Data for fraud detection and risk analytics at scale

**Modern AI and ML:**
- **Niantic**: Reduced code complexity by **85%** while scaling AR/VR data pipelines
- **Canva**: Cut cloud costs in **half** while processing design assets and user data
- **Pinterest**: Boosted GPU utilization to **90%+** for image processing and recommendations

Ray Data provides a unified platform that excels at traditional ETL, classical ML, and next-generation AI workloads - eliminating the need for multiple specialized systems.


## Part 2: Ray Data architecture and concepts

### Ray Data architecture

Ray Data's architecture addresses the core challenges of modern AI infrastructure:

** Tip: Ray is built for the modern era of computing**

Unlike traditional distributed systems, Ray was purpose-built for:

- **Python-native**: No JVM overhead or serialization bottlenecks
- **Heterogeneous compute**: Seamlessly orchestrates CPUs, GPUs, and other accelerators
- **Dynamic workloads**: Adapts to varying compute needs in real-time
- **Fault tolerance**: Handles failures gracefully at massive scale

![Ray Data Architecture](https://docs.ray.io/en/latest/_images/dataset-arch.svg)

### Core concepts

This section explains fundamental concepts that power Ray Data.

#### 1. Datasets and blocks

A **Dataset** in Ray Data is a distributed collection of data that's divided into **blocks**. Think of blocks as chunks of your data that can be processed independently.

**Note: Understanding blocks**

- Each block contains a subset of your data (typically 1-128 MB).
- Ray Data stores blocks in Ray's distributed object store.
- Ray Data applies operations to blocks in parallel across the cluster.
- Block size affects performance—too small causes overhead, too large causes memory issues.


```python
# Create a simple dataset to understand blocks
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

#### 2. Execution model

Ray Data uses **lazy execution** by default, meaning it doesn't execute operations immediately but plans and optimizes before execution.

**Lazy execution benefits:**
- **Optimization**: Ray Data can optimize the entire pipeline before execution
- **Memory efficiency**: Only loads necessary data into memory
- **Fault tolerance**: Can restart from intermediate points if failures occur

<div class="alert alert-block alert-warning">
<b> Understanding execution:</b><br>
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

## Part 3: Extract: reading data

The **Extract** phase involves reading data from various sources. Ray Data provides built-in connectors for many common data sources and makes it easy to scale data reading across a distributed cluster, especially for the **multimodal data** that powers modern AI applications.

### The multimodal data revolution

Today's AI applications process vastly more complex data than traditional ETL pipelines:

** Tip: The scale of modern data**

- **Unstructured data growth**: Outpaces structured data by 10x+ in most organizations.
- **Video processing**: Companies like OpenAI (Sora), Pinterest, and Apple process petabytes of multimodal data daily.
- **Foundation models**: Require processing millions of images, videos, and documents.
- **AI-powered processing**: Every aspect of data processing is becoming AI-enhanced.

### How Ray Data reads data under the hood

When you read data with Ray Data, the following happens:

1. **File discovery**: Ray Data discovers all files matching your path pattern.
2. **Task creation**: Ray Data distributes files across Ray tasks (typically one file per task).
3. **Parallel reading**: Multiple tasks read files simultaneously across the cluster.
4. **Block creation**: Each task creates data blocks stored in Ray's object store.
5. **Lazy planning**: Ray Data creates the dataset but doesn't load the data until needed.

This architecture enables Ray Data to efficiently handle both traditional structured data and modern unstructured formats that power AI applications.

**Note: Built-in data sources**

- **Structured**: Parquet, CSV, JSON, Arrow
- **Unstructured**: Images, videos, audio, binary files
- **Databases**: MongoDB, MySQL, PostgreSQL, Snowflake
- **Cloud storage**: S3, GCS, Azure Blob Storage
- **Data lakes**: Delta Lake, Iceberg (with RayTurbo)
- **ML formats**: TensorFlow Records, PyTorch datasets
- **Memory**: Python lists, NumPy arrays, Pandas DataFrames

### Enterprise-grade data connectivity

For enterprise environments, **Anyscale** provides additional connectors and optimizations:
- **Enhanced security**: Integration with enterprise identity systems
- **Governance controls**: Data lineage and access controls
- **Performance optimization**: RayTurbo's streaming metadata fetching provides up to **4.5x faster** data loading
- **Hybrid deployment**: Support for Kubernetes, on-premises, and multi-cloud environments


### Accessing TPC-H benchmark data

This example uses the industry-standard TPC-H benchmark dataset. This provides realistic enterprise-scale data that's used by companies worldwide to evaluate data processing systems and represents real business scenarios with complex relationships between customers, orders, suppliers, and products.



```python
# Using TPC-H benchmark dataset - industry standard for data processing
# TPC-H is the gold standard benchmark for decision support systems and analytics

# TPC-H S3 data location
TPCH_S3_PATH = "s3://ray-benchmark-data/tpch/parquet/sf10"

# TPC-H schema overview
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
# Read TPC-H customer master data (traditional structured data processing)
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
# Geographic reference data - nations table
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
# Customer demographics by nation - join analysis
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
# Read TPC-H high-volume transactional data (orders and line items)

# Read orders table (header information)
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
# Read line items table (detailed transaction data - largest table in TPC-H)
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

## Part 4: Transform - processing data

The **Transform** phase is where the real data processing happens. Ray Data provides several transformation operations that you can apply to datasets, and understanding how they work under the hood is key to building efficient ETL pipelines that power modern AI applications.

### Transformations for the AI era

Modern AI workloads require more than traditional data transformations. Ray Data is designed for the era of **compound AI systems** and **agentic workflows** where:

**Tip: AI-powered transformations**

- **Multimodal processing**: Simultaneously process text, images, video, and audio.
- **Model inference**: Embed ML models directly into transformation pipelines.
- **GPU acceleration**: Seamlessly utilize both CPU and GPU resources.
- **Compound AI**: Orchestrate multiple models and traditional ML within single workflows.
- **AI-enhanced ETL**: Use AI to optimize every aspect of data processing.

### How Ray Data processes transformations

When you apply transformations with Ray Data:

1. **Task distribution**: Ray Data distributes transformations across Ray tasks/actors.
2. **Block-level processing**: Each task processes one or more blocks independently.
3. **Streaming execution**: Blocks flow through the pipeline without waiting for all data.
4. **Operator fusion**: Compatible operations are automatically combined for efficiency.
5. **Heterogeneous compute**: Intelligently schedules CPU and GPU work.
6. **Fault tolerance**: Ray Data automatically retries failed tasks.

This architecture enables Ray Data to handle everything from traditional business logic to cutting-edge AI inference within the same pipeline.

**Note: Transformation categories**

- **Row-wise operations**: `map()` - Transform individual rows
- **Batch operations**: `map_batches()` - Transform groups of rows (ideal for ML inference)
- **Filtering**: `filter()` - Remove rows based on conditions
- **Aggregations**: `groupby()` - Group and aggregate data
- **Joins**: `join()` - Combine datasets
- **AI operations**: Embed models for inference, embeddings, and feature extraction
- **Shuffling**: `random_shuffle()`, `sort()` - Reorder data

While most other data frameworks support similar map operations through UDFs, with Ray Data, these are treated as first-class supported features. Instead of sending arbitrary operations to partitions, Ray Data has several key advantages:
- Each task can have task-level concurrency and hardware allocation set instead of global settings for all UDFs.
- These tasks support PyArrow, pandas, and NumPy format, which provides easy integrations to the rest of the Python ecosystem.
- These tasks support stateful actors, which supports initializing expensive steps like downloading an AI model, only once per replica instead of per-invocation.
- For more advanced use cases, you can run Ray Core inside of the task, supporting nested parallelism algorithms. This is useful for HPC-style applications with complicated compute tasks on top of big data.

**Tip: GPU optimizations**

- **Nvidia RAPIDS**: `map_batches()` - You can speed up pandas ETL operations using the Nvidia cuDF library to run the slower sections of ETL logic onto GPUs.
- **Batch inference**: `map_batches()` - You can use GPU AI batch inference for unstructured data ingestion or LLM processing, amongst many other use cases.
- **AI training**: Many data pipelines, such as time series analysis, train many small models over sections of the data. You can train these smaller ML models, such as XGBoost models, using GPUs for faster performance.

This tutorial focuses on traditional CPU-based ETL workloads, but there are other templates available for batch inference using GPUs if you're interested in learning more.

### Practical ETL transformations

This section implements common ETL transformations using the e-commerce data:

#### 1. Data enrichment with business logic



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
# ML-ready feature engineering (preparing enterprise data for model training/inference)
print("ML-Ready Feature Engineering (Next-Generation Capabilities):")
print("Adding ML features for predictive analytics on enterprise transaction data...")

enriched_orders = traditional_enriched.map_batches(
    ml_ready_feature_engineering_tpch,
    batch_format="pandas",
    batch_size=10000
)
enriched_orders.limit(25).to_pandas()
```

#### 2. Aggregations and analytics

Aggregations are essential for creating summary statistics and business metrics. Ray Data's `groupby()` operations distribute the computation across the cluster.

**Caution: Under the hood - GroupBy operations**

When you perform a GroupBy operation, Ray Data:

1. **Shuffle phase**: Redistributes data so all records with the same key end up on the same node.
2. **Local aggregation**: Each node performs aggregation on its subset of data.
3. **Result collection**: Collects final aggregated results.

You can make several new datasets by aggregating data together.



```python
from ray.data.aggregate import Count, Mean, Sum, Max

#Executive summary dashboard - typical BI metrics on enterprise data
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

#Operational analytics - business process optimization
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

#Priority-based analysis - enterprise order management
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
#Temporal business analysis - time-series insights
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

## Part 5: Load - writing data

The **Load** phase involves writing the processed data to destination systems. Ray Data supports writing to various formats and destinations, and understanding how this works helps you optimize for your use case.

### How Ray Data writes data

When you write data with Ray Data:

1. **Parallel writing**: Multiple tasks write data simultaneously across the cluster.
2. **Partitioned output**: Ray Data writes data as multiple files (one per block typically).
3. **Format optimization**: Ray Data optimizes the writing process for each format.
4. **Streaming writes**: Ray Data can write large datasets without loading everything into memory.

**Note: Supported output formats**

- **Files**: Parquet, CSV, JSON
- **Databases**: MongoDB, MySQL, PostgreSQL
- **Cloud storage**: S3, GCS, Azure Blob Storage
- **Lakehouse formats**: Delta Lake (coming soon), Iceberg, Hudi
- **Custom**: Implement your own writers using `FileBasedDatasource`



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

## Summary: Your journey with Ray Data ETL

You've completed a comprehensive journey through Ray Data for ETL. This section summarizes what you've learned and explores how to take your AI data pipelines to production.

**Tip: What you've mastered**

- **Ray Data fundamentals**: Blocks, lazy execution, streaming processing
- **Extract phase**: Reading from multiple data sources efficiently, including multimodal data
- **Transform phase**: Distributed data processing and feature engineering
- **Load phase**: Writing to various destinations with optimization
- **Production patterns**: Error handling, monitoring, and data quality
- **Performance optimization**: Understanding bottlenecks and solutions

### When to use Ray Data

**Ray Data excels across the full spectrum of data workloads:**

**Traditional ETL and business intelligence:**
- **High-volume transaction processing** for e-commerce, finance, and operations
- **Business intelligence** and executive reporting at scale
- **Data warehouse** loading and transformation pipelines
- **CPU cluster optimization** with pure Python performance (no JVM overhead)
- **Traditional analytics** that need to scale beyond single-node tools

**Modern ML and AI workloads:**
- **Feature engineering** for machine learning at scale
- **Batch inference** on foundation models and LLMs
- **Multimodal data processing** (text, images, video, audio)
- **GPU-accelerated pipelines** for AI applications
- **Real-time model serving** and inference workloads

**Ray Data's unified platform advantage:**
- **One system** for both traditional ETL and cutting-edge AI
- **Seamless evolution** from CPU-based analytics to GPU-powered AI
- **No migration** required as your data needs grow and change
- **Consistent APIs** whether processing structured business data or unstructured AI content

**Ray is proven at scale:**
- Processing **exabyte-scale** workloads
- **1M+ clusters** orchestrated monthly across the Ray ecosystem
- **$120M annual savings** achieved by leading enterprises
- **Traditional workloads** running alongside **next-generation AI** on the same platform

### From open source to enterprise: Anyscale platform

While Ray Data open source provides powerful capabilities, **Anyscale** offers a unified AI platform for production deployments:

**Note: Anyscale - The unified AI platform**

- **RayTurbo runtime**: Up to 5.1x performance improvements over open source
- **Enterprise governance**: Resource quotas, usage tracking, and advanced observability
- **AI anywhere**: Deploy on Kubernetes, hybrid cloud, or any infrastructure
- **LLM suite**: Complete capabilities for embeddings, fine-tuning, and serving
- **Marketplace ready**: Available on AWS and GCP Marketplaces

### Production deployment options

**Getting started:**
1. **Ray open source**: Perfect for development and smaller workloads
2. **Anyscale platform**: Enterprise features with RayTurbo optimizations
3. **Marketplace deployment**: One-click setup with AWS or GCP Marketplace

### Key architectural insights

Understanding how Ray Data works under the hood helps you build better pipelines:

1. **AI-native architecture**: Purpose-built for Python, GPUs, and multimodal data
2. **Streaming execution**: Process datasets larger than cluster memory
3. **Heterogeneous compute**: Seamlessly orchestrate CPUs, GPUs, and other accelerators
4. **Operator fusion**: Combines compatible operations for efficiency
5. **Enterprise scalability**: Proven to scale to 8,000+ nodes

### Production readiness checklist

Before deploying Ray Data pipelines to production:

- **Architecture**: Choose between Ray OSS and Anyscale based on your needs
- **Performance**: Consider RayTurbo for production workloads requiring maximum efficiency
- **Governance**: Implement enterprise controls for AI sprawl and cost management
- **Security**: Leverage enterprise identity integration and access controls
- **Monitoring**: Use advanced observability tools for optimization insights
- **Scalability**: Test with realistic data volumes and cluster sizes

### Join the Ray ecosystem

The Ray community is thriving with **1,000+ contributors** and growing:

1. **Community**: Join the Ray Slack community for support and discussions
2. **Learning**: Access Ray Summit sessions and technical deep-dives
3. **Contributing**: Contribute to the fastest-growing AI infrastructure project
4. **Enterprise support**: Explore Anyscale for production deployments

**Tip: One platform for all your data workloads**

You now have the knowledge to build production-ready, scalable data pipelines that handle everything from traditional business ETL to cutting-edge AI applications. Whether you're processing millions of e-commerce transactions for business intelligence or preparing multimodal data for foundation models, Ray Data provides a unified platform that scales with your needs.

**Start with traditional ETL today, evolve to AI tomorrow - all on the same platform.** Ray Data and Anyscale eliminate the complexity of managing multiple systems as your data requirements grow.


