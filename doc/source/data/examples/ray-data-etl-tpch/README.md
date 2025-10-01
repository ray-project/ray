# Ray Data for ETL: A Comprehensive Beginner's Guide

This notebook provides a complete introduction to Ray Data for Extract, Transform, Load (ETL) workflows. We'll cover both the practical aspects of building ETL pipelines and the underlying architecture that makes Ray Data powerful for distributed data processing.

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

Let's start by importing the necessary libraries and setting up our environment.



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

    2025-08-28 00:38:22,820	INFO worker.py:1771 -- Connecting to existing Ray cluster at address: 10.0.248.128:6379...
    2025-08-28 00:38:22,832	INFO worker.py:1942 -- Connected to Ray cluster. View the dashboard at [1m[32mhttps://session-g3cs331q2c6ppx12j9ir5htn6l.i.anyscaleuserdata.com [39m[22m
    2025-08-28 00:38:22,835	INFO packaging.py:380 -- Pushing file package 'gcs://_ray_pkg_08c553810358104c2d14814f4a773e46518ed2d6.zip' (0.11MiB) to Ray cluster...
    2025-08-28 00:38:22,836	INFO packaging.py:393 -- Successfully pushed file package 'gcs://_ray_pkg_08c553810358104c2d14814f4a773e46518ed2d6.zip'.


    Ray version: 2.49.0
    Ray cluster resources: {'anyscale/node-group:8CPU-32GB': 10.0, 'anyscale/provider:aws': 11.0, 'CPU': 88.0, 'object_store_memory': 105541541064.0, 'memory': 377957122048.0, 'anyscale/cpu_only:true': 11.0, 'node:10.0.246.59': 1.0, 'anyscale/region:us-west-2': 11.0, 'node:10.0.228.88': 1.0, 'node:10.0.195.252': 1.0, 'anyscale/node-group:head': 1.0, 'node:__internal_head__': 1.0, 'node:10.0.248.128': 1.0, 'node:10.0.222.37': 1.0, 'node:10.0.228.223': 1.0, 'node:10.0.234.204': 1.0, 'node:10.0.218.75': 1.0, 'node:10.0.198.227': 1.0, 'node:10.0.208.38': 1.0, 'node:10.0.238.242': 1.0}


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

Let's understand how Ray Data compares to other data processing tools across traditional and modern workloads:

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

Before diving into ETL examples, let's understand the fundamental concepts that power Ray Data.

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

    2025-08-28 00:38:23,206	INFO dataset.py:3248 -- Tip: Use `take_batch()` instead of `take() / show()` to return records in pandas or numpy batch format.
    2025-08-28 00:38:23,208	INFO logging.py:295 -- Registered dataset logger for dataset dataset_327_0
    2025-08-28 00:38:23,219	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_327_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:23,220	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_327_0: InputDataBuffer[Input] -> LimitOperator[limit=3]
    2025-08-28 00:38:23,222	WARNING resource_manager.py:134 -- ⚠️  Ray's object store is configured to use only 27.9% of available memory (98.3GiB out of 352.0GiB total). For optimal Ray Data performance, we recommend setting the object store to at least 50% of available memory. You can do this by setting the 'object_store_memory' parameter when calling ray.init() or by setting the RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION environment variable.


    Dataset: MaterializedDataset(
       num_blocks=200,
       num_rows=1000,
       schema={id: int64, name: string, value: double}
    )
    Number of blocks: 200
    Schema: Column  Type
    ------  ----
    id      int64
    name    string
    value   double
    
    First 3 rows:


    2025-08-28 00:38:24,741	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_327_0 execution finished in 1.52 seconds


    Row 0: {'id': 0, 'name': 'name_0', 'value': 0.10075406674150811}
    Row 1: {'id': 1, 'name': 'name_1', 'value': 0.05100088950394521}
    Row 2: {'id': 2, 'name': 'name_2', 'value': 0.467035054122913}


#### 2. Execution Model

Ray Data uses **lazy execution** by default, meaning operations are not executed immediately but are planned and optimized before execution.

**Lazy Execution Benefits:**
- **Optimization**: Ray Data can optimize the entire pipeline before execution
- **Memory efficiency**: Only necessary data is loaded into memory
- **Fault tolerance**: Can restart from intermediate points if failures occur

<div class="alert alert-block alert-warning">
<b> Understanding Execution:</b><br>
<b>Lazy:</b> Build a plan first, then execute (default)<br>
<b>Eager:</b> Execute operations immediately as they're called<br><br>
Lazy execution allows Ray Data to optimize your entire pipeline for better performance!
</div>

Ray Data also utilizes **streaming execution**, which helps combine the best of structured streaming and batch inference.  Streaming execution doesn’t wait for one operator to complete to start the next. Each operator takes in and outputs a stream of blocks. This approach allows you to process datasets that are too large to fit in your cluster’s memory and can be 

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

    2025-08-28 00:38:24,844	INFO logging.py:295 -- Registered dataset logger for dataset dataset_329_0
    2025-08-28 00:38:24,848	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_329_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:24,848	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_329_0: InputDataBuffer[Input] -> TaskPoolMapOperator[MapBatches(transform)] -> LimitOperator[limit=5]
    2025-08-28 00:38:25,332	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_329_0 execution finished in 0.48 seconds


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
    <li><b>Data Lakes:</b> Delta Lake, Iceberg (via RayTurbo)</li>
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


### Accessing TPC-H Benchmark Data for Our ETL Examples

We'll use the industry-standard TPC-H benchmark dataset. This provides realistic enterprise-scale data that's used by companies worldwide to evaluate data processing systems and represents real business scenarios with complex relationships between customers, orders, suppliers, and products.



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

    
     TPC-H Schema (8 Tables):
    	CUSTOMER:	Customer master data with demographics and market segments
    	ORDERS:		Order header information with dates, priorities, and status
    	LINEITEM:	Detailed line items for each order (largest table ~6B rows)
    	PART:		Parts catalog with specifications and retail prices
    	SUPPLIER:	Supplier information including contact details and geography
    	PARTSUPP:	Part-supplier relationships with costs and availability
    	NATION:		Nation reference data with geographic regions
    	REGION:		Regional groupings for geographic analysis



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

    /home/ray/anaconda3/lib/python3.12/site-packages/ray/data/_internal/datasource/parquet_datasource.py:750: FutureWarning: The default `file_extensions` for `read_parquet` will change from `None` to ['parquet'] after Ray 2.43, and your dataset contains files that don't match the new `file_extensions`. To maintain backwards compatibility, set `file_extensions=None` explicitly.
      warnings.warn(
    2025-08-28 00:38:25,879	INFO logging.py:295 -- Registered dataset logger for dataset dataset_332_0
    2025-08-28 00:38:25,894	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_332_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:25,895	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_332_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)] -> LimitOperator[limit=1]
    [36m(MapBatches(transform) pid=292427)[0m Error calculating size for column 'name': cannot call `vectorize` on size 0 inputs unless `otypes` is set
    [36m(MapBatches(transform) pid=292427)[0m Error calculating size for column 'category': cannot call `vectorize` on size 0 inputs unless `otypes` is set
    [36m(MapBatches(transform) pid=292427)[0m Error calculating size for column 'name': cannot call `vectorize` on size 0 inputs unless `otypes` is set
    [36m(MapBatches(transform) pid=292427)[0m Error calculating size for column 'category': cannot call `vectorize` on size 0 inputs unless `otypes` is set
    2025-08-28 00:38:28,263	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_332_0 execution finished in 2.37 seconds
    2025-08-28 00:38:28,271	INFO logging.py:295 -- Registered dataset logger for dataset dataset_334_0
    2025-08-28 00:38:28,279	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_334_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:28,280	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_334_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)] -> LimitOperator[limit=1] -> TaskPoolMapOperator[Project]


     TPC-H Customer Master Data (Traditional ETL):


    2025-08-28 00:38:29,861	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_334_0 execution finished in 1.58 seconds
    2025-08-28 00:38:29,867	INFO logging.py:295 -- Registered dataset logger for dataset dataset_335_0
    2025-08-28 00:38:29,876	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_335_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:29,876	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_335_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)->Project] -> AggregateNumRows[AggregateNumRows]


    Schema: Column        Type
    ------        ----
    c_custkey     int64
    c_name        string
    c_address     string
    c_nationkey   int64
    c_phone       string
    c_acctbal     double
    c_mktsegment  string
    c_comment     string


    2025-08-28 00:38:30,710	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_335_0 execution finished in 0.83 seconds
    2025-08-28 00:38:30,719	INFO logging.py:295 -- Registered dataset logger for dataset dataset_336_0
    2025-08-28 00:38:30,729	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_336_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:30,729	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_336_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)] -> LimitOperator[limit=25] -> TaskPoolMapOperator[Project]


    Total customers: 1,500,000
    Sample customer records:


    2025-08-28 00:38:32,123	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_336_0 execution finished in 1.39 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>c_custkey</th>
      <th>c_name</th>
      <th>c_address</th>
      <th>c_nationkey</th>
      <th>c_phone</th>
      <th>c_acctbal</th>
      <th>c_mktsegment</th>
      <th>c_comment</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Customer#000000001</td>
      <td>IVhzIApeRb ot,c,E</td>
      <td>15</td>
      <td>25-989-741-2988</td>
      <td>711.56</td>
      <td>BUILDING</td>
      <td>to the even, regular platelets. regular, ironi...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Customer#000000002</td>
      <td>XSTf4,NCwDVaWNe6tEgvwfmRchLXak</td>
      <td>13</td>
      <td>23-768-687-3665</td>
      <td>121.65</td>
      <td>AUTOMOBILE</td>
      <td>l accounts. blithely ironic theodolites integr...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Customer#000000003</td>
      <td>MG9kdTD2WBHm</td>
      <td>1</td>
      <td>11-719-748-3364</td>
      <td>7498.12</td>
      <td>AUTOMOBILE</td>
      <td>deposits eat slyly ironic, even instructions....</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Customer#000000004</td>
      <td>XxVSJsLAGtn</td>
      <td>4</td>
      <td>14-128-190-5944</td>
      <td>2866.83</td>
      <td>MACHINERY</td>
      <td>requests. final, regular ideas sleep final accou</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Customer#000000005</td>
      <td>KvpyuHCplrB84WgAiGV6sYpZq7Tj</td>
      <td>3</td>
      <td>13-750-942-6364</td>
      <td>794.47</td>
      <td>HOUSEHOLD</td>
      <td>n accounts will have to unwind. foxes cajole a...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Customer#000000006</td>
      <td>sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn</td>
      <td>20</td>
      <td>30-114-968-4951</td>
      <td>7638.57</td>
      <td>AUTOMOBILE</td>
      <td>tions. even deposits boost according to the sl...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Customer#000000007</td>
      <td>TcGe5gaZNgVePxU5kRrvXBfkasDTea</td>
      <td>18</td>
      <td>28-190-982-9759</td>
      <td>9561.95</td>
      <td>AUTOMOBILE</td>
      <td>ainst the ironic, express theodolites. express...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>Customer#000000008</td>
      <td>I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5</td>
      <td>17</td>
      <td>27-147-574-9335</td>
      <td>6819.74</td>
      <td>BUILDING</td>
      <td>among the slyly regular theodolites kindle bli...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>Customer#000000009</td>
      <td>xKiAFTjUsCuxfeleNqefumTrjS</td>
      <td>8</td>
      <td>18-338-906-3675</td>
      <td>8324.07</td>
      <td>FURNITURE</td>
      <td>r theodolites according to the requests wake t...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>Customer#000000010</td>
      <td>6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2</td>
      <td>5</td>
      <td>15-741-346-9870</td>
      <td>2753.54</td>
      <td>HOUSEHOLD</td>
      <td>es regular deposits haggle. fur</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>Customer#000000011</td>
      <td>PkWS 3HlXqwTuzrKg633BEi</td>
      <td>23</td>
      <td>33-464-151-3439</td>
      <td>-272.60</td>
      <td>BUILDING</td>
      <td>ckages. requests sleep slyly. quickly even pin...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>Customer#000000012</td>
      <td>9PWKuhzT4Zr1Q</td>
      <td>13</td>
      <td>23-791-276-1263</td>
      <td>3396.49</td>
      <td>HOUSEHOLD</td>
      <td>to the carefully final braids. blithely regul...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>13</td>
      <td>Customer#000000013</td>
      <td>nsXQu0oVjD7PM659uC3SRSp</td>
      <td>3</td>
      <td>13-761-547-5974</td>
      <td>3857.34</td>
      <td>BUILDING</td>
      <td>ounts sleep carefully after the close frays. c...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>14</td>
      <td>Customer#000000014</td>
      <td>KXkletMlL2JQEA</td>
      <td>1</td>
      <td>11-845-129-3851</td>
      <td>5266.30</td>
      <td>FURNITURE</td>
      <td>, ironic packages across the unus</td>
    </tr>
    <tr>
      <th>14</th>
      <td>15</td>
      <td>Customer#000000015</td>
      <td>YtWggXoOLdwdo7b0y,BZaGUQMLJMX1Y,EC,6Dn</td>
      <td>23</td>
      <td>33-687-542-7601</td>
      <td>2788.52</td>
      <td>HOUSEHOLD</td>
      <td>platelets. regular deposits detect asymptotes...</td>
    </tr>
    <tr>
      <th>15</th>
      <td>16</td>
      <td>Customer#000000016</td>
      <td>cYiaeMLZSMAOQ2 d0W,</td>
      <td>10</td>
      <td>20-781-609-3107</td>
      <td>4681.03</td>
      <td>FURNITURE</td>
      <td>kly silent courts. thinly regular theodolites ...</td>
    </tr>
    <tr>
      <th>16</th>
      <td>17</td>
      <td>Customer#000000017</td>
      <td>izrh 6jdqtp2eqdtbkswDD8SG4SzXruMfIXyR7</td>
      <td>2</td>
      <td>12-970-682-3487</td>
      <td>6.34</td>
      <td>AUTOMOBILE</td>
      <td>packages wake! blithely even pint</td>
    </tr>
    <tr>
      <th>17</th>
      <td>18</td>
      <td>Customer#000000018</td>
      <td>3txGO AiuFux3zT0Z9NYaFRnZt</td>
      <td>6</td>
      <td>16-155-215-1315</td>
      <td>5494.43</td>
      <td>BUILDING</td>
      <td>s sleep. carefully even instructions nag furio...</td>
    </tr>
    <tr>
      <th>18</th>
      <td>19</td>
      <td>Customer#000000019</td>
      <td>uc,3bHIx84H,wdrmLOjVsiqXCq2tr</td>
      <td>18</td>
      <td>28-396-526-5053</td>
      <td>8914.71</td>
      <td>HOUSEHOLD</td>
      <td>nag. furiously careful packages are slyly at ...</td>
    </tr>
    <tr>
      <th>19</th>
      <td>20</td>
      <td>Customer#000000020</td>
      <td>JrPk8Pqplj4Ne</td>
      <td>22</td>
      <td>32-957-234-8742</td>
      <td>7603.40</td>
      <td>FURNITURE</td>
      <td>g alongside of the special excuses-- fluffily ...</td>
    </tr>
    <tr>
      <th>20</th>
      <td>21</td>
      <td>Customer#000000021</td>
      <td>XYmVpr9yAHDEn</td>
      <td>8</td>
      <td>18-902-614-8344</td>
      <td>1428.25</td>
      <td>MACHINERY</td>
      <td>quickly final accounts integrate blithely fur...</td>
    </tr>
    <tr>
      <th>21</th>
      <td>22</td>
      <td>Customer#000000022</td>
      <td>QI6p41,FNs5k7RZoCCVPUTkUdYpB</td>
      <td>3</td>
      <td>13-806-545-9701</td>
      <td>591.98</td>
      <td>MACHINERY</td>
      <td>s nod furiously above the furiously ironic ide...</td>
    </tr>
    <tr>
      <th>22</th>
      <td>23</td>
      <td>Customer#000000023</td>
      <td>OdY W13N7Be3OC5MpgfmcYss0Wn6TKT</td>
      <td>3</td>
      <td>13-312-472-8245</td>
      <td>3332.02</td>
      <td>HOUSEHOLD</td>
      <td>deposits. special deposits cajole slyly. fluff...</td>
    </tr>
    <tr>
      <th>23</th>
      <td>24</td>
      <td>Customer#000000024</td>
      <td>HXAFgIAyjxtdqwimt13Y3OZO 4xeLe7U8PqG</td>
      <td>13</td>
      <td>23-127-851-8031</td>
      <td>9255.67</td>
      <td>MACHINERY</td>
      <td>into beans. fluffily final ideas haggle fluffily</td>
    </tr>
    <tr>
      <th>24</th>
      <td>25</td>
      <td>Customer#000000025</td>
      <td>Hp8GyFQgGHFYSilH5tBfe</td>
      <td>12</td>
      <td>22-603-468-3533</td>
      <td>7133.70</td>
      <td>FURNITURE</td>
      <td>y. accounts sleep ruthlessly according to the ...</td>
    </tr>
  </tbody>
</table>
</div>




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

    2025-08-28 00:38:32,246	INFO logging.py:295 -- Registered dataset logger for dataset dataset_338_0
    2025-08-28 00:38:32,257	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_338_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:32,258	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_338_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)->Project] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]
    2025-08-28 00:38:33,199	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: c_mktsegment: string
    count(): int64
    mean(c_acctbal): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:38:33,211	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_338_0 execution finished in 0.95 seconds
    2025-08-28 00:38:33,219	INFO logging.py:295 -- Registered dataset logger for dataset dataset_340_0
    2025-08-28 00:38:33,228	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_340_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:33,229	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_340_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)->Project] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=5] -> TaskPoolMapOperator[Project]


    Customer Market Segment Distribution:


    2025-08-28 00:38:34,066	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: c_mktsegment: string
    count(): int64
    mean(c_acctbal): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:38:34,099	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: c_mktsegment: string
    customer_count: int64
    avg_account_balance: double, new schema: . This may lead to unexpected behavior.
    2025-08-28 00:38:34,112	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_340_0 execution finished in 0.88 seconds


    {'c_mktsegment': 'AUTOMOBILE', 'customer_count': 300036, 'avg_account_balance': 4496.230541701662}
    {'c_mktsegment': 'BUILDING', 'customer_count': 300276, 'avg_account_balance': 4505.869852402457}
    {'c_mktsegment': 'FURNITURE', 'customer_count': 299496, 'avg_account_balance': 4500.162798034031}
    {'c_mktsegment': 'HOUSEHOLD', 'customer_count': 299751, 'avg_account_balance': 4499.8627405746765}
    {'c_mktsegment': 'MACHINERY', 'customer_count': 300441, 'avg_account_balance': 4492.427445488464}



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

    2025-08-28 00:38:34,219	INFO logging.py:295 -- Registered dataset logger for dataset dataset_343_0
    2025-08-28 00:38:34,227	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_343_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:34,227	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_343_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1] -> TaskPoolMapOperator[ReadFiles]


    TPC-H Nations Reference Data:
    Loading geographic data for customer demographics...


    2025-08-28 00:38:34,541	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_343_0 execution finished in 0.31 seconds
    2025-08-28 00:38:34,548	INFO logging.py:295 -- Registered dataset logger for dataset dataset_345_0
    2025-08-28 00:38:34,554	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_345_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:34,554	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_345_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[MapBatches(count_rows)]
    2025-08-28 00:38:34,811	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_345_0 execution finished in 0.26 seconds
    2025-08-28 00:38:34,817	INFO logging.py:295 -- Registered dataset logger for dataset dataset_346_0
    2025-08-28 00:38:34,825	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_346_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:34,825	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_346_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=5] -> TaskPoolMapOperator[ReadFiles]


    Total nations: 25
    
     Sample nation records:


    2025-08-28 00:38:35,119	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_346_0 execution finished in 0.29 seconds


    {'n_nationkey': 0, 'n_name': 'ALGERIA', 'n_regionkey': 0, 'n_comment': ' haggle. carefully final deposits detect slyly agai'}
    {'n_nationkey': 1, 'n_name': 'ARGENTINA', 'n_regionkey': 1, 'n_comment': 'al foxes promise slyly according to the regular accounts. bold requests alon'}
    {'n_nationkey': 2, 'n_name': 'BRAZIL', 'n_regionkey': 1, 'n_comment': 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special '}
    {'n_nationkey': 3, 'n_name': 'CANADA', 'n_regionkey': 1, 'n_comment': 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'}
    {'n_nationkey': 4, 'n_name': 'EGYPT', 'n_regionkey': 4, 'n_comment': 'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d'}



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

    2025-08-28 00:38:35,231	INFO logging.py:295 -- Registered dataset logger for dataset dataset_350_0
    2025-08-28 00:38:35,247	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_350_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:35,248	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_350_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)->Project], InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> JoinOperatorWithPolars[Join(num_partitions=100)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]


     Customer Demographics by Nation:
       Joining customer and nation data for geographic analysis...


    2025-08-28 00:38:40,128	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: n_name: string
    count(): int64
    mean(c_acctbal): double
    sum(c_acctbal): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:38:40,240	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_350_0 execution finished in 4.99 seconds
    2025-08-28 00:38:40,305	INFO logging.py:295 -- Registered dataset logger for dataset dataset_353_0
    2025-08-28 00:38:40,398	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_353_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:40,399	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_353_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(drop_columns)->Project], InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[ReadFiles] -> JoinOperatorWithPolars[Join(num_partitions=100)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project] -> AllToAllOperator[Sort] -> LimitOperator[limit=10]
    2025-08-28 00:38:48,896	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: n_name: string
    count(): int64
    mean(c_acctbal): double
    sum(c_acctbal): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:38:49,089	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: n_name: string
    customer_count: int64
    avg_balance: double
    total_balance: double, new schema: . This may lead to unexpected behavior.
    2025-08-28 00:38:50,143	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: n_name: string
    customer_count: int64
    avg_balance: double
    total_balance: double, new schema: . This may lead to unexpected behavior.
    2025-08-28 00:38:50,295	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_353_0 execution finished in 9.89 seconds


    {'n_name': 'ETHIOPIA', 'customer_count': 60471, 'avg_balance': 4512.16504423608, 'total_balance': 272855132.39}
    {'n_name': 'UNITED KINGDOM', 'customer_count': 60381, 'avg_balance': 4482.881766946556, 'total_balance': 270680883.97}
    {'n_name': 'FRANCE', 'customer_count': 60316, 'avg_balance': 4483.745413488959, 'total_balance': 270441588.36}
    {'n_name': 'INDONESIA', 'customer_count': 60236, 'avg_balance': 4506.17889501295, 'total_balance': 271434191.9200001}
    {'n_name': 'INDIA', 'customer_count': 60215, 'avg_balance': 4505.563116167067, 'total_balance': 271302483.03999996}
    {'n_name': 'GERMANY', 'customer_count': 60153, 'avg_balance': 4488.417533456352, 'total_balance': 269991779.89}
    {'n_name': 'IRAN', 'customer_count': 60101, 'avg_balance': 4512.225304737025, 'total_balance': 271189253.03999996}
    {'n_name': 'CHINA', 'customer_count': 60065, 'avg_balance': 4480.9787298759675, 'total_balance': 269149987.40999997}
    {'n_name': 'RUSSIA', 'customer_count': 60065, 'avg_balance': 4507.959750603513, 'total_balance': 270770602.42}
    {'n_name': 'IRAQ', 'customer_count': 60056, 'avg_balance': 4516.969680797922, 'total_balance': 271271131.15}



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

    2025-08-28 00:38:50,461	INFO logging.py:295 -- Registered dataset logger for dataset dataset_356_0
    2025-08-28 00:38:50,469	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_356_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:50,470	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_356_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1] -> TaskPoolMapOperator[ReadFiles]
    2025-08-28 00:38:51,744	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_356_0 execution finished in 1.27 seconds
    2025-08-28 00:38:51,752	INFO logging.py:295 -- Registered dataset logger for dataset dataset_359_0
    2025-08-28 00:38:51,759	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_359_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:51,760	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_359_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=3] -> TaskPoolMapOperator[ReadFiles]


     TPC-H Orders Data (Enterprise Transaction Processing):
    
     Sample order records:


    2025-08-28 00:38:53,657	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_359_0 execution finished in 1.90 seconds


    {'o_orderkey': 1, 'o_custkey': 369001, 'o_orderstatus': 'O', 'o_totalprice': 186600.18, 'o_orderdate': datetime.date(1996, 1, 2), 'o_orderpriority': '5-LOW', 'o_clerk': 'Clerk#000009506', 'o_shippriority': 0, 'o_comment': 'nstructions sleep furiously among '}
    {'o_orderkey': 2, 'o_custkey': 780017, 'o_orderstatus': 'O', 'o_totalprice': 66219.63, 'o_orderdate': datetime.date(1996, 12, 1), 'o_orderpriority': '1-URGENT', 'o_clerk': 'Clerk#000008792', 'o_shippriority': 0, 'o_comment': ' foxes. pending accounts at the pending, silent asymptot'}
    {'o_orderkey': 3, 'o_custkey': 1233140, 'o_orderstatus': 'F', 'o_totalprice': 270741.97, 'o_orderdate': datetime.date(1993, 10, 14), 'o_orderpriority': '5-LOW', 'o_clerk': 'Clerk#000009543', 'o_shippriority': 0, 'o_comment': 'sly final accounts boost. carefully regular ideas cajole carefully. depos'}



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

    2025-08-28 00:38:53,780	INFO logging.py:295 -- Registered dataset logger for dataset dataset_362_0
    2025-08-28 00:38:53,788	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_362_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:53,789	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_362_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1] -> TaskPoolMapOperator[ReadFiles]


    2025-08-28 00:38:55,331	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_362_0 execution finished in 1.54 seconds
    2025-08-28 00:38:55,338	INFO logging.py:295 -- Registered dataset logger for dataset dataset_364_0
    2025-08-28 00:38:55,345	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_364_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:55,345	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_364_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1] -> TaskPoolMapOperator[ReadFiles]


    
     TPC-H Line Items Data (Detailed Transaction Processing):


    2025-08-28 00:38:56,747	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_364_0 execution finished in 1.40 seconds
    2025-08-28 00:38:56,753	INFO logging.py:295 -- Registered dataset logger for dataset dataset_365_0
    2025-08-28 00:38:56,761	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_365_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:56,762	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_365_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> TaskPoolMapOperator[MapBatches(count_rows)]


        Schema: Column           Type
    ------           ----
    l_orderkey       int64
    l_partkey        int64
    l_suppkey        int64
    l_linenumber     int64
    l_quantity       int64
    l_extendedprice  double
    l_discount       double
    l_tax            double
    l_returnflag     string
    l_linestatus     string
    l_shipdate       date32[day]
    l_commitdate     date32[day]
    l_receiptdate    date32[day]
    l_shipinstruct   string
    l_shipmode       string
    l_comment        string


    2025-08-28 00:38:57,257	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_365_0 execution finished in 0.50 seconds
    2025-08-28 00:38:57,264	INFO logging.py:295 -- Registered dataset logger for dataset dataset_366_0
    2025-08-28 00:38:57,271	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_366_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:57,272	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_366_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=3] -> TaskPoolMapOperator[ReadFiles]


        Total line items: 59,986,052
    
     Sample line item records:


    2025-08-28 00:38:59,309	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_366_0 execution finished in 2.04 seconds


    {'l_orderkey': 6000001, 'l_partkey': 1278570, 'l_suppkey': 53607, 'l_linenumber': 1, 'l_quantity': 45, 'l_extendedprice': 69682.95, 'l_discount': 0.09, 'l_tax': 0.05, 'l_returnflag': 'N', 'l_linestatus': 'O', 'l_shipdate': datetime.date(1998, 10, 20), 'l_commitdate': datetime.date(1998, 9, 10), 'l_receiptdate': datetime.date(1998, 11, 15), 'l_shipinstruct': 'COLLECT COD', 'l_shipmode': 'SHIP', 'l_comment': 'es haggle blithely above the silent ac'}
    {'l_orderkey': 6000001, 'l_partkey': 921150, 'l_suppkey': 96178, 'l_linenumber': 2, 'l_quantity': 47, 'l_extendedprice': 55042.17, 'l_discount': 0.08, 'l_tax': 0.02, 'l_returnflag': 'N', 'l_linestatus': 'O', 'l_shipdate': datetime.date(1998, 9, 8), 'l_commitdate': datetime.date(1998, 8, 31), 'l_receiptdate': datetime.date(1998, 9, 15), 'l_shipinstruct': 'COLLECT COD', 'l_shipmode': 'AIR', 'l_comment': 'counts. furio'}
    {'l_orderkey': 6000002, 'l_partkey': 1832878, 'l_suppkey': 7933, 'l_linenumber': 1, 'l_quantity': 13, 'l_extendedprice': 23540.14, 'l_discount': 0.01, 'l_tax': 0.07, 'l_returnflag': 'N', 'l_linestatus': 'O', 'l_shipdate': datetime.date(1996, 10, 29), 'l_commitdate': datetime.date(1996, 11, 29), 'l_receiptdate': datetime.date(1996, 10, 31), 'l_shipinstruct': 'DELIVER IN PERSON', 'l_shipmode': 'AIR', 'l_comment': 'ackages haggle slyly. bold, exp'}



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

    2025-08-28 00:38:59,398	INFO logging.py:295 -- Registered dataset logger for dataset dataset_369_0
    2025-08-28 00:38:59,406	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_369_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:38:59,407	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_369_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]
    2025-08-28 00:39:02,044	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: o_orderpriority: string
    count(): int64
    mean(o_totalprice): double
    sum(o_totalprice): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:39:02,066	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_369_0 execution finished in 2.66 seconds
    2025-08-28 00:39:02,088	INFO logging.py:295 -- Registered dataset logger for dataset dataset_374_0
    2025-08-28 00:39:02,103	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_374_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:39:02,106	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_374_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[Map(<lambda>)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]
    2025-08-28 00:39:20,617	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_year: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:39:20,639	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_374_0 execution finished in 18.53 seconds


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
- For more advanced use cases, Ray Core can be run inside of the task, supporting nested parallelism algorithms. This is useful for HPC-style applications where we need to have complicated compute tasks on top of big data.


<div class="alert alert-block alert-success">
<b> GPU Optimizations:</b>
<ul>
    <li><b>Nvidia RAPIDS:</b> <code>map_batches()</code> - Pandas ETL operations can be sped up using the Nvidia cuDF library to run the slower sections of ETL logic onto GPUs.</li>
    <li><b>Batch Inference:</b> <code>map_batches()</code> - GPU AI batch inference can be used for unstructured data ingestion or LLM processing, amongst many other use cases</li>
    <li><b>AI Training:</b> - Many data pipelines, such as time series analysis, train many small models over sections of the data. These smaller ML models, such as XGBoost models, can be trained using GPUs for faster performance</li>
</ul>
</div>

In this tutorial, we are going to focus on traditional CPU-based ETL workloads, but there are other templates available for batch inference using GPUs if you are interested in learning further.

### Practical ETL Transformations

Let's implement common ETL transformations using our e-commerce data:

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

    2025-08-28 00:39:20,752	INFO logging.py:295 -- Registered dataset logger for dataset dataset_377_0
    2025-08-28 00:39:20,760	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_377_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:39:20,761	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_377_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)] -> LimitOperator[limit=25]


    Traditional ETL Results:


    2025-08-28 00:39:26,971	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_377_0 execution finished in 6.21 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>o_orderkey</th>
      <th>o_custkey</th>
      <th>o_orderstatus</th>
      <th>o_totalprice</th>
      <th>o_orderdate</th>
      <th>o_orderpriority</th>
      <th>o_clerk</th>
      <th>o_shippriority</th>
      <th>o_comment</th>
      <th>order_year</th>
      <th>...</th>
      <th>quarter_name</th>
      <th>month_name</th>
      <th>revenue_tier</th>
      <th>priority_weight</th>
      <th>weighted_revenue</th>
      <th>is_urgent</th>
      <th>is_large_order</th>
      <th>requires_expedited_processing</th>
      <th>days_to_process</th>
      <th>is_peak_season</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4423681</td>
      <td>325555</td>
      <td>O</td>
      <td>59347.44</td>
      <td>1996-12-08</td>
      <td>1-URGENT</td>
      <td>Clerk#000000386</td>
      <td>0</td>
      <td>ng the accounts. slyly express pinto beans sleep</td>
      <td>1996</td>
      <td>...</td>
      <td>Q4</td>
      <td>December</td>
      <td>Medium</td>
      <td>1.0</td>
      <td>59347.440</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>1803</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4423682</td>
      <td>1075774</td>
      <td>F</td>
      <td>48526.32</td>
      <td>1993-06-03</td>
      <td>2-HIGH</td>
      <td>Clerk#000006158</td>
      <td>0</td>
      <td>deposits. special requests integrate blithely...</td>
      <td>1993</td>
      <td>...</td>
      <td>Q2</td>
      <td>June</td>
      <td>Small</td>
      <td>0.8</td>
      <td>38821.056</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>519</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4423683</td>
      <td>1015111</td>
      <td>F</td>
      <td>211218.00</td>
      <td>1994-09-03</td>
      <td>1-URGENT</td>
      <td>Clerk#000008441</td>
      <td>0</td>
      <td>ackages. carefully express pains boost. bold d...</td>
      <td>1994</td>
      <td>...</td>
      <td>Q3</td>
      <td>September</td>
      <td>Large</td>
      <td>1.0</td>
      <td>211218.000</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>976</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4423684</td>
      <td>1448251</td>
      <td>F</td>
      <td>73936.26</td>
      <td>1994-06-24</td>
      <td>5-LOW</td>
      <td>Clerk#000007302</td>
      <td>0</td>
      <td>fully around the unusual accounts. care</td>
      <td>1994</td>
      <td>...</td>
      <td>Q2</td>
      <td>June</td>
      <td>Medium</td>
      <td>0.2</td>
      <td>14787.252</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>905</td>
      <td>False</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4423685</td>
      <td>224824</td>
      <td>O</td>
      <td>60869.04</td>
      <td>1995-12-04</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000005898</td>
      <td>0</td>
      <td>pending accounts integrate q</td>
      <td>1995</td>
      <td>...</td>
      <td>Q4</td>
      <td>December</td>
      <td>Medium</td>
      <td>0.4</td>
      <td>24347.616</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>1433</td>
      <td>True</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4423686</td>
      <td>94375</td>
      <td>O</td>
      <td>55521.21</td>
      <td>1998-04-11</td>
      <td>1-URGENT</td>
      <td>Clerk#000006090</td>
      <td>0</td>
      <td>o beans wake slyly along the even packa</td>
      <td>1998</td>
      <td>...</td>
      <td>Q2</td>
      <td>April</td>
      <td>Medium</td>
      <td>1.0</td>
      <td>55521.210</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>2292</td>
      <td>False</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4423687</td>
      <td>637748</td>
      <td>P</td>
      <td>96743.73</td>
      <td>1995-04-13</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000003578</td>
      <td>0</td>
      <td>he silent, sly packages sleep accordi</td>
      <td>1995</td>
      <td>...</td>
      <td>Q2</td>
      <td>April</td>
      <td>Medium</td>
      <td>0.4</td>
      <td>38697.492</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>1198</td>
      <td>False</td>
    </tr>
    <tr>
      <th>7</th>
      <td>4423712</td>
      <td>1115005</td>
      <td>P</td>
      <td>309802.49</td>
      <td>1995-04-16</td>
      <td>1-URGENT</td>
      <td>Clerk#000009508</td>
      <td>0</td>
      <td>ounts. furiously bold accou</td>
      <td>1995</td>
      <td>...</td>
      <td>Q2</td>
      <td>April</td>
      <td>Enterprise</td>
      <td>1.0</td>
      <td>309802.490</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>1201</td>
      <td>False</td>
    </tr>
    <tr>
      <th>8</th>
      <td>4423713</td>
      <td>364759</td>
      <td>F</td>
      <td>279348.18</td>
      <td>1992-02-08</td>
      <td>1-URGENT</td>
      <td>Clerk#000005668</td>
      <td>0</td>
      <td>as. instructions about the quickly ironic foxe</td>
      <td>1992</td>
      <td>...</td>
      <td>Q1</td>
      <td>February</td>
      <td>Large</td>
      <td>1.0</td>
      <td>279348.180</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>38</td>
      <td>False</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4423714</td>
      <td>4279</td>
      <td>F</td>
      <td>122838.22</td>
      <td>1993-09-12</td>
      <td>5-LOW</td>
      <td>Clerk#000009293</td>
      <td>0</td>
      <td>ss accounts. blithe</td>
      <td>1993</td>
      <td>...</td>
      <td>Q3</td>
      <td>September</td>
      <td>Medium</td>
      <td>0.2</td>
      <td>24567.644</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>620</td>
      <td>False</td>
    </tr>
    <tr>
      <th>10</th>
      <td>4423715</td>
      <td>1391221</td>
      <td>F</td>
      <td>240515.93</td>
      <td>1992-12-24</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000002392</td>
      <td>0</td>
      <td>s; carefully bold packages solve slyly. specia...</td>
      <td>1992</td>
      <td>...</td>
      <td>Q4</td>
      <td>December</td>
      <td>Large</td>
      <td>0.6</td>
      <td>144309.558</td>
      <td>False</td>
      <td>True</td>
      <td>True</td>
      <td>358</td>
      <td>True</td>
    </tr>
    <tr>
      <th>11</th>
      <td>4423716</td>
      <td>236656</td>
      <td>F</td>
      <td>182232.10</td>
      <td>1992-11-02</td>
      <td>5-LOW</td>
      <td>Clerk#000006940</td>
      <td>0</td>
      <td>regular pinto beans. regula</td>
      <td>1992</td>
      <td>...</td>
      <td>Q4</td>
      <td>November</td>
      <td>Large</td>
      <td>0.2</td>
      <td>36446.420</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>306</td>
      <td>True</td>
    </tr>
    <tr>
      <th>12</th>
      <td>4423717</td>
      <td>944855</td>
      <td>O</td>
      <td>55194.24</td>
      <td>1996-10-05</td>
      <td>1-URGENT</td>
      <td>Clerk#000003250</td>
      <td>0</td>
      <td>elets! sly requests wake carefully</td>
      <td>1996</td>
      <td>...</td>
      <td>Q4</td>
      <td>October</td>
      <td>Medium</td>
      <td>1.0</td>
      <td>55194.240</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>1739</td>
      <td>False</td>
    </tr>
    <tr>
      <th>13</th>
      <td>4423718</td>
      <td>1163669</td>
      <td>F</td>
      <td>145746.18</td>
      <td>1994-03-24</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000006591</td>
      <td>0</td>
      <td>s wake furiously express excuses. regular, pen...</td>
      <td>1994</td>
      <td>...</td>
      <td>Q1</td>
      <td>March</td>
      <td>Medium</td>
      <td>0.6</td>
      <td>87447.708</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>813</td>
      <td>False</td>
    </tr>
    <tr>
      <th>14</th>
      <td>4423719</td>
      <td>773638</td>
      <td>O</td>
      <td>140131.93</td>
      <td>1997-02-22</td>
      <td>2-HIGH</td>
      <td>Clerk#000000979</td>
      <td>0</td>
      <td>kages nag along the pending ideas. even, expre...</td>
      <td>1997</td>
      <td>...</td>
      <td>Q1</td>
      <td>February</td>
      <td>Medium</td>
      <td>0.8</td>
      <td>112105.544</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>1879</td>
      <td>False</td>
    </tr>
    <tr>
      <th>15</th>
      <td>4423744</td>
      <td>506038</td>
      <td>F</td>
      <td>416004.61</td>
      <td>1993-03-21</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000008859</td>
      <td>0</td>
      <td>t pinto beans x-ray carefully furiously regula...</td>
      <td>1993</td>
      <td>...</td>
      <td>Q1</td>
      <td>March</td>
      <td>Enterprise</td>
      <td>0.4</td>
      <td>166401.844</td>
      <td>False</td>
      <td>True</td>
      <td>True</td>
      <td>445</td>
      <td>False</td>
    </tr>
    <tr>
      <th>16</th>
      <td>4423745</td>
      <td>1477240</td>
      <td>O</td>
      <td>227181.93</td>
      <td>1995-09-30</td>
      <td>2-HIGH</td>
      <td>Clerk#000001033</td>
      <td>0</td>
      <td>ly whithout the final deposits;</td>
      <td>1995</td>
      <td>...</td>
      <td>Q3</td>
      <td>September</td>
      <td>Large</td>
      <td>0.8</td>
      <td>181745.544</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
      <td>1368</td>
      <td>False</td>
    </tr>
    <tr>
      <th>17</th>
      <td>4423746</td>
      <td>1471403</td>
      <td>O</td>
      <td>153590.22</td>
      <td>1995-06-25</td>
      <td>5-LOW</td>
      <td>Clerk#000008571</td>
      <td>0</td>
      <td>es. accounts wake furiously about the dep</td>
      <td>1995</td>
      <td>...</td>
      <td>Q2</td>
      <td>June</td>
      <td>Large</td>
      <td>0.2</td>
      <td>30718.044</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>1271</td>
      <td>False</td>
    </tr>
    <tr>
      <th>18</th>
      <td>4423747</td>
      <td>866546</td>
      <td>O</td>
      <td>24009.81</td>
      <td>1997-06-09</td>
      <td>5-LOW</td>
      <td>Clerk#000008660</td>
      <td>0</td>
      <td>foxes. theodolites according to the furious, r</td>
      <td>1997</td>
      <td>...</td>
      <td>Q2</td>
      <td>June</td>
      <td>Small</td>
      <td>0.2</td>
      <td>4801.962</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>1986</td>
      <td>False</td>
    </tr>
    <tr>
      <th>19</th>
      <td>4423748</td>
      <td>528055</td>
      <td>O</td>
      <td>141626.74</td>
      <td>1996-06-11</td>
      <td>1-URGENT</td>
      <td>Clerk#000000618</td>
      <td>0</td>
      <td>ly regular sentiments integrate unusual reques...</td>
      <td>1996</td>
      <td>...</td>
      <td>Q2</td>
      <td>June</td>
      <td>Medium</td>
      <td>1.0</td>
      <td>141626.740</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>1623</td>
      <td>False</td>
    </tr>
    <tr>
      <th>20</th>
      <td>4423749</td>
      <td>994843</td>
      <td>F</td>
      <td>107573.21</td>
      <td>1992-07-30</td>
      <td>2-HIGH</td>
      <td>Clerk#000008555</td>
      <td>0</td>
      <td>its wake furiously blit</td>
      <td>1992</td>
      <td>...</td>
      <td>Q3</td>
      <td>July</td>
      <td>Medium</td>
      <td>0.8</td>
      <td>86058.568</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>211</td>
      <td>False</td>
    </tr>
    <tr>
      <th>21</th>
      <td>4423750</td>
      <td>1308635</td>
      <td>O</td>
      <td>52547.58</td>
      <td>1997-07-25</td>
      <td>5-LOW</td>
      <td>Clerk#000003457</td>
      <td>0</td>
      <td>ffily ideas. stealthily even packages nag. car...</td>
      <td>1997</td>
      <td>...</td>
      <td>Q3</td>
      <td>July</td>
      <td>Medium</td>
      <td>0.2</td>
      <td>10509.516</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2032</td>
      <td>False</td>
    </tr>
    <tr>
      <th>22</th>
      <td>4423751</td>
      <td>1216394</td>
      <td>F</td>
      <td>106017.64</td>
      <td>1993-04-22</td>
      <td>2-HIGH</td>
      <td>Clerk#000007135</td>
      <td>0</td>
      <td>slyly. slyly slow sheaves sleep enticingly. pe...</td>
      <td>1993</td>
      <td>...</td>
      <td>Q2</td>
      <td>April</td>
      <td>Medium</td>
      <td>0.8</td>
      <td>84814.112</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>477</td>
      <td>False</td>
    </tr>
    <tr>
      <th>23</th>
      <td>4423776</td>
      <td>421148</td>
      <td>O</td>
      <td>107647.22</td>
      <td>1997-07-08</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000007691</td>
      <td>0</td>
      <td>pending, silent dolphins according to the furi...</td>
      <td>1997</td>
      <td>...</td>
      <td>Q3</td>
      <td>July</td>
      <td>Medium</td>
      <td>0.6</td>
      <td>64588.332</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2015</td>
      <td>False</td>
    </tr>
    <tr>
      <th>24</th>
      <td>4423777</td>
      <td>1229882</td>
      <td>F</td>
      <td>130557.34</td>
      <td>1992-05-05</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000009091</td>
      <td>0</td>
      <td>ickly. slyly silent instructions serve blithely</td>
      <td>1992</td>
      <td>...</td>
      <td>Q2</td>
      <td>May</td>
      <td>Medium</td>
      <td>0.6</td>
      <td>78334.404</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>125</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
<p>25 rows × 24 columns</p>
</div>




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

    2025-08-28 00:39:27,181	INFO logging.py:295 -- Registered dataset logger for dataset dataset_379_0
    2025-08-28 00:39:27,191	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_379_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:39:27,191	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_379_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> LimitOperator[limit=3]


    
     ML-Ready Feature Engineering (Next-Generation Capabilities):
       Adding ML features for predictive analytics on enterprise transaction data...
    
     ML-Ready Data Sample:


    2025-08-28 00:39:32,975	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_379_0 execution finished in 5.78 seconds
    2025-08-28 00:39:32,987	INFO logging.py:295 -- Registered dataset logger for dataset dataset_380_0
    2025-08-28 00:39:32,998	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_380_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:39:32,998	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_380_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> LimitOperator[limit=25]


    {'o_orderkey': np.int64(24000001), 'o_custkey': np.int64(1494782), 'o_orderstatus': 'O', 'o_totalprice': np.float64(73193.66), 'o_orderdate': Timestamp('1997-03-03 00:00:00'), 'o_orderpriority': '1-URGENT', 'o_clerk': 'Clerk#000008224', 'o_shippriority': np.int64(0), 'o_comment': 'deas. carefully final pinto beans cajole a', 'order_year': np.int32(1997), 'order_quarter': np.int32(1), 'order_month': np.int32(3), 'order_day_of_week': np.int32(0), 'is_weekend': np.False_, 'quarter_name': 'Q1', 'month_name': 'March', 'revenue_tier': 'Medium', 'priority_weight': np.float64(1.0), 'weighted_revenue': np.float64(73193.66), 'is_urgent': np.True_, 'is_large_order': np.False_, 'requires_expedited_processing': np.True_, 'days_to_process': np.int64(1888), 'is_peak_season': np.False_, 'days_since_epoch': np.int64(1888), 'month_sin': np.float64(1.0), 'month_cos': np.float64(6.123233995736766e-17), 'quarter_sin': np.float64(1.0), 'quarter_cos': np.float64(6.123233995736766e-17), 'is_priority_1': np.int64(1), 'is_priority_2': np.int64(0), 'is_priority_3': np.int64(0), 'log_total_price': np.float64(11.20087774646869), 'revenue_per_priority': np.float64(73193.66), 'weekend_large_order': np.int64(0), 'year_normalized': np.float64(0.8333333333333334), 'seasonal_revenue_multiplier': np.float64(1.0), 'customer_id_mod_100': np.int64(82)}
    {'o_orderkey': np.int64(24000002), 'o_custkey': np.int64(793336), 'o_orderstatus': 'F', 'o_totalprice': np.float64(67691.93), 'o_orderdate': Timestamp('1993-01-21 00:00:00'), 'o_orderpriority': '1-URGENT', 'o_clerk': 'Clerk#000004358', 'o_shippriority': np.int64(0), 'o_comment': ' deposits. finally ironic ', 'order_year': np.int32(1993), 'order_quarter': np.int32(1), 'order_month': np.int32(1), 'order_day_of_week': np.int32(3), 'is_weekend': np.False_, 'quarter_name': 'Q1', 'month_name': 'January', 'revenue_tier': 'Medium', 'priority_weight': np.float64(1.0), 'weighted_revenue': np.float64(67691.93), 'is_urgent': np.True_, 'is_large_order': np.False_, 'requires_expedited_processing': np.True_, 'days_to_process': np.int64(386), 'is_peak_season': np.False_, 'days_since_epoch': np.int64(386), 'month_sin': np.float64(0.49999999999999994), 'month_cos': np.float64(0.8660254037844387), 'quarter_sin': np.float64(1.0), 'quarter_cos': np.float64(6.123233995736766e-17), 'is_priority_1': np.int64(1), 'is_priority_2': np.int64(0), 'is_priority_3': np.int64(0), 'log_total_price': np.float64(11.122737022132414), 'revenue_per_priority': np.float64(67691.93), 'weekend_large_order': np.int64(0), 'year_normalized': np.float64(0.16666666666666666), 'seasonal_revenue_multiplier': np.float64(1.0), 'customer_id_mod_100': np.int64(36)}
    {'o_orderkey': np.int64(24000003), 'o_custkey': np.int64(82214), 'o_orderstatus': 'O', 'o_totalprice': np.float64(59537.05), 'o_orderdate': Timestamp('1998-05-24 00:00:00'), 'o_orderpriority': '2-HIGH', 'o_clerk': 'Clerk#000008735', 'o_shippriority': np.int64(0), 'o_comment': 'ymptotes wake. furiously daring courts cajole slyly across the ironic Tir', 'order_year': np.int32(1998), 'order_quarter': np.int32(2), 'order_month': np.int32(5), 'order_day_of_week': np.int32(6), 'is_weekend': np.True_, 'quarter_name': 'Q2', 'month_name': 'May', 'revenue_tier': 'Medium', 'priority_weight': np.float64(0.8), 'weighted_revenue': np.float64(47629.64000000001), 'is_urgent': np.True_, 'is_large_order': np.False_, 'requires_expedited_processing': np.True_, 'days_to_process': np.int64(2335), 'is_peak_season': np.False_, 'days_since_epoch': np.int64(2335), 'month_sin': np.float64(0.49999999999999994), 'month_cos': np.float64(-0.8660254037844387), 'quarter_sin': np.float64(1.2246467991473532e-16), 'quarter_cos': np.float64(-1.0), 'is_priority_1': np.int64(0), 'is_priority_2': np.int64(1), 'is_priority_3': np.int64(0), 'log_total_price': np.float64(10.994370882941736), 'revenue_per_priority': np.float64(47629.64000000001), 'weekend_large_order': np.int64(0), 'year_normalized': np.float64(1.0), 'seasonal_revenue_multiplier': np.float64(1.0), 'customer_id_mod_100': np.int64(14)}


    2025-08-28 00:39:38,035	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_380_0 execution finished in 5.04 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>o_orderkey</th>
      <th>o_custkey</th>
      <th>o_orderstatus</th>
      <th>o_totalprice</th>
      <th>o_orderdate</th>
      <th>o_orderpriority</th>
      <th>o_clerk</th>
      <th>o_shippriority</th>
      <th>o_comment</th>
      <th>order_year</th>
      <th>...</th>
      <th>quarter_cos</th>
      <th>is_priority_1</th>
      <th>is_priority_2</th>
      <th>is_priority_3</th>
      <th>log_total_price</th>
      <th>revenue_per_priority</th>
      <th>weekend_large_order</th>
      <th>year_normalized</th>
      <th>seasonal_revenue_multiplier</th>
      <th>customer_id_mod_100</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4423681</td>
      <td>325555</td>
      <td>O</td>
      <td>59347.44</td>
      <td>1996-12-08</td>
      <td>1-URGENT</td>
      <td>Clerk#000000386</td>
      <td>0</td>
      <td>ng the accounts. slyly express pinto beans sleep</td>
      <td>1996</td>
      <td>...</td>
      <td>1.000000e+00</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>10.991181</td>
      <td>59347.440</td>
      <td>0</td>
      <td>0.666667</td>
      <td>1.2</td>
      <td>55</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4423682</td>
      <td>1075774</td>
      <td>F</td>
      <td>48526.32</td>
      <td>1993-06-03</td>
      <td>2-HIGH</td>
      <td>Clerk#000006158</td>
      <td>0</td>
      <td>deposits. special requests integrate blithely...</td>
      <td>1993</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>10.789882</td>
      <td>38821.056</td>
      <td>0</td>
      <td>0.166667</td>
      <td>1.0</td>
      <td>74</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4423683</td>
      <td>1015111</td>
      <td>F</td>
      <td>211218.00</td>
      <td>1994-09-03</td>
      <td>1-URGENT</td>
      <td>Clerk#000008441</td>
      <td>0</td>
      <td>ackages. carefully express pains boost. bold d...</td>
      <td>1994</td>
      <td>...</td>
      <td>-1.836970e-16</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>12.260651</td>
      <td>211218.000</td>
      <td>1</td>
      <td>0.333333</td>
      <td>1.0</td>
      <td>11</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4423684</td>
      <td>1448251</td>
      <td>F</td>
      <td>73936.26</td>
      <td>1994-06-24</td>
      <td>5-LOW</td>
      <td>Clerk#000007302</td>
      <td>0</td>
      <td>fully around the unusual accounts. care</td>
      <td>1994</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>11.210972</td>
      <td>14787.252</td>
      <td>0</td>
      <td>0.333333</td>
      <td>1.0</td>
      <td>51</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4423685</td>
      <td>224824</td>
      <td>O</td>
      <td>60869.04</td>
      <td>1995-12-04</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000005898</td>
      <td>0</td>
      <td>pending accounts integrate q</td>
      <td>1995</td>
      <td>...</td>
      <td>1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>11.016496</td>
      <td>24347.616</td>
      <td>0</td>
      <td>0.500000</td>
      <td>1.2</td>
      <td>24</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4423686</td>
      <td>94375</td>
      <td>O</td>
      <td>55521.21</td>
      <td>1998-04-11</td>
      <td>1-URGENT</td>
      <td>Clerk#000006090</td>
      <td>0</td>
      <td>o beans wake slyly along the even packa</td>
      <td>1998</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>10.924538</td>
      <td>55521.210</td>
      <td>0</td>
      <td>1.000000</td>
      <td>1.0</td>
      <td>75</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4423687</td>
      <td>637748</td>
      <td>P</td>
      <td>96743.73</td>
      <td>1995-04-13</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000003578</td>
      <td>0</td>
      <td>he silent, sly packages sleep accordi</td>
      <td>1995</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>11.479831</td>
      <td>38697.492</td>
      <td>0</td>
      <td>0.500000</td>
      <td>1.0</td>
      <td>48</td>
    </tr>
    <tr>
      <th>7</th>
      <td>4423712</td>
      <td>1115005</td>
      <td>P</td>
      <td>309802.49</td>
      <td>1995-04-16</td>
      <td>1-URGENT</td>
      <td>Clerk#000009508</td>
      <td>0</td>
      <td>ounts. furiously bold accou</td>
      <td>1995</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>12.643693</td>
      <td>309802.490</td>
      <td>1</td>
      <td>0.500000</td>
      <td>1.0</td>
      <td>5</td>
    </tr>
    <tr>
      <th>8</th>
      <td>4423713</td>
      <td>364759</td>
      <td>F</td>
      <td>279348.18</td>
      <td>1992-02-08</td>
      <td>1-URGENT</td>
      <td>Clerk#000005668</td>
      <td>0</td>
      <td>as. instructions about the quickly ironic foxe</td>
      <td>1992</td>
      <td>...</td>
      <td>6.123234e-17</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>12.540218</td>
      <td>279348.180</td>
      <td>1</td>
      <td>0.000000</td>
      <td>1.0</td>
      <td>59</td>
    </tr>
    <tr>
      <th>9</th>
      <td>4423714</td>
      <td>4279</td>
      <td>F</td>
      <td>122838.22</td>
      <td>1993-09-12</td>
      <td>5-LOW</td>
      <td>Clerk#000009293</td>
      <td>0</td>
      <td>ss accounts. blithe</td>
      <td>1993</td>
      <td>...</td>
      <td>-1.836970e-16</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>11.718632</td>
      <td>24567.644</td>
      <td>0</td>
      <td>0.166667</td>
      <td>1.0</td>
      <td>79</td>
    </tr>
    <tr>
      <th>10</th>
      <td>4423715</td>
      <td>1391221</td>
      <td>F</td>
      <td>240515.93</td>
      <td>1992-12-24</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000002392</td>
      <td>0</td>
      <td>s; carefully bold packages solve slyly. specia...</td>
      <td>1992</td>
      <td>...</td>
      <td>1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>12.390546</td>
      <td>144309.558</td>
      <td>0</td>
      <td>0.000000</td>
      <td>1.2</td>
      <td>21</td>
    </tr>
    <tr>
      <th>11</th>
      <td>4423716</td>
      <td>236656</td>
      <td>F</td>
      <td>182232.10</td>
      <td>1992-11-02</td>
      <td>5-LOW</td>
      <td>Clerk#000006940</td>
      <td>0</td>
      <td>regular pinto beans. regula</td>
      <td>1992</td>
      <td>...</td>
      <td>1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>12.113042</td>
      <td>36446.420</td>
      <td>0</td>
      <td>0.000000</td>
      <td>1.2</td>
      <td>56</td>
    </tr>
    <tr>
      <th>12</th>
      <td>4423717</td>
      <td>944855</td>
      <td>O</td>
      <td>55194.24</td>
      <td>1996-10-05</td>
      <td>1-URGENT</td>
      <td>Clerk#000003250</td>
      <td>0</td>
      <td>elets! sly requests wake carefully</td>
      <td>1996</td>
      <td>...</td>
      <td>1.000000e+00</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>10.918632</td>
      <td>55194.240</td>
      <td>0</td>
      <td>0.666667</td>
      <td>1.0</td>
      <td>55</td>
    </tr>
    <tr>
      <th>13</th>
      <td>4423718</td>
      <td>1163669</td>
      <td>F</td>
      <td>145746.18</td>
      <td>1994-03-24</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000006591</td>
      <td>0</td>
      <td>s wake furiously express excuses. regular, pen...</td>
      <td>1994</td>
      <td>...</td>
      <td>6.123234e-17</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>11.889629</td>
      <td>87447.708</td>
      <td>0</td>
      <td>0.333333</td>
      <td>1.0</td>
      <td>69</td>
    </tr>
    <tr>
      <th>14</th>
      <td>4423719</td>
      <td>773638</td>
      <td>O</td>
      <td>140131.93</td>
      <td>1997-02-22</td>
      <td>2-HIGH</td>
      <td>Clerk#000000979</td>
      <td>0</td>
      <td>kages nag along the pending ideas. even, expre...</td>
      <td>1997</td>
      <td>...</td>
      <td>6.123234e-17</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>11.850347</td>
      <td>112105.544</td>
      <td>0</td>
      <td>0.833333</td>
      <td>1.0</td>
      <td>38</td>
    </tr>
    <tr>
      <th>15</th>
      <td>4423744</td>
      <td>506038</td>
      <td>F</td>
      <td>416004.61</td>
      <td>1993-03-21</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000008859</td>
      <td>0</td>
      <td>t pinto beans x-ray carefully furiously regula...</td>
      <td>1993</td>
      <td>...</td>
      <td>6.123234e-17</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>12.938454</td>
      <td>166401.844</td>
      <td>1</td>
      <td>0.166667</td>
      <td>1.0</td>
      <td>38</td>
    </tr>
    <tr>
      <th>16</th>
      <td>4423745</td>
      <td>1477240</td>
      <td>O</td>
      <td>227181.93</td>
      <td>1995-09-30</td>
      <td>2-HIGH</td>
      <td>Clerk#000001033</td>
      <td>0</td>
      <td>ly whithout the final deposits;</td>
      <td>1995</td>
      <td>...</td>
      <td>-1.836970e-16</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>12.333511</td>
      <td>181745.544</td>
      <td>1</td>
      <td>0.500000</td>
      <td>1.0</td>
      <td>40</td>
    </tr>
    <tr>
      <th>17</th>
      <td>4423746</td>
      <td>1471403</td>
      <td>O</td>
      <td>153590.22</td>
      <td>1995-06-25</td>
      <td>5-LOW</td>
      <td>Clerk#000008571</td>
      <td>0</td>
      <td>es. accounts wake furiously about the dep</td>
      <td>1995</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>11.942050</td>
      <td>30718.044</td>
      <td>0</td>
      <td>0.500000</td>
      <td>1.0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>18</th>
      <td>4423747</td>
      <td>866546</td>
      <td>O</td>
      <td>24009.81</td>
      <td>1997-06-09</td>
      <td>5-LOW</td>
      <td>Clerk#000008660</td>
      <td>0</td>
      <td>foxes. theodolites according to the furious, r</td>
      <td>1997</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>10.086259</td>
      <td>4801.962</td>
      <td>0</td>
      <td>0.833333</td>
      <td>1.0</td>
      <td>46</td>
    </tr>
    <tr>
      <th>19</th>
      <td>4423748</td>
      <td>528055</td>
      <td>O</td>
      <td>141626.74</td>
      <td>1996-06-11</td>
      <td>1-URGENT</td>
      <td>Clerk#000000618</td>
      <td>0</td>
      <td>ly regular sentiments integrate unusual reques...</td>
      <td>1996</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>11.860957</td>
      <td>141626.740</td>
      <td>0</td>
      <td>0.666667</td>
      <td>1.0</td>
      <td>55</td>
    </tr>
    <tr>
      <th>20</th>
      <td>4423749</td>
      <td>994843</td>
      <td>F</td>
      <td>107573.21</td>
      <td>1992-07-30</td>
      <td>2-HIGH</td>
      <td>Clerk#000008555</td>
      <td>0</td>
      <td>its wake furiously blit</td>
      <td>1992</td>
      <td>...</td>
      <td>-1.836970e-16</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>11.585936</td>
      <td>86058.568</td>
      <td>0</td>
      <td>0.000000</td>
      <td>1.0</td>
      <td>43</td>
    </tr>
    <tr>
      <th>21</th>
      <td>4423750</td>
      <td>1308635</td>
      <td>O</td>
      <td>52547.58</td>
      <td>1997-07-25</td>
      <td>5-LOW</td>
      <td>Clerk#000003457</td>
      <td>0</td>
      <td>ffily ideas. stealthily even packages nag. car...</td>
      <td>1997</td>
      <td>...</td>
      <td>-1.836970e-16</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>10.869493</td>
      <td>10509.516</td>
      <td>0</td>
      <td>0.833333</td>
      <td>1.0</td>
      <td>35</td>
    </tr>
    <tr>
      <th>22</th>
      <td>4423751</td>
      <td>1216394</td>
      <td>F</td>
      <td>106017.64</td>
      <td>1993-04-22</td>
      <td>2-HIGH</td>
      <td>Clerk#000007135</td>
      <td>0</td>
      <td>slyly. slyly slow sheaves sleep enticingly. pe...</td>
      <td>1993</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>11.571370</td>
      <td>84814.112</td>
      <td>0</td>
      <td>0.166667</td>
      <td>1.0</td>
      <td>94</td>
    </tr>
    <tr>
      <th>23</th>
      <td>4423776</td>
      <td>421148</td>
      <td>O</td>
      <td>107647.22</td>
      <td>1997-07-08</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000007691</td>
      <td>0</td>
      <td>pending, silent dolphins according to the furi...</td>
      <td>1997</td>
      <td>...</td>
      <td>-1.836970e-16</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>11.586624</td>
      <td>64588.332</td>
      <td>0</td>
      <td>0.833333</td>
      <td>1.0</td>
      <td>48</td>
    </tr>
    <tr>
      <th>24</th>
      <td>4423777</td>
      <td>1229882</td>
      <td>F</td>
      <td>130557.34</td>
      <td>1992-05-05</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000009091</td>
      <td>0</td>
      <td>ickly. slyly silent instructions serve blithely</td>
      <td>1992</td>
      <td>...</td>
      <td>-1.000000e+00</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>11.779575</td>
      <td>78334.404</td>
      <td>0</td>
      <td>0.000000</td>
      <td>1.0</td>
      <td>82</td>
    </tr>
  </tbody>
</table>
<p>25 rows × 38 columns</p>
</div>



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

Let's make several new datasets by aggregating data together.



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

    2025-08-28 00:39:38,211	INFO logging.py:295 -- Registered dataset logger for dataset dataset_383_0
    2025-08-28 00:39:38,221	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_383_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:39:38,221	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_383_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]


    Executive Dashboard (Traditional BI on TPC-H):


    2025-08-28 00:40:01,086	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_quarter: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    sum(weighted_revenue): double
    mean(is_urgent): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:40:01,123	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_383_0 execution finished in 22.90 seconds
    2025-08-28 00:40:01,130	INFO logging.py:295 -- Registered dataset logger for dataset dataset_385_0
    2025-08-28 00:40:01,139	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_385_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:40:01,139	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_385_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=25] -> TaskPoolMapOperator[Project]


    Quarterly Business Performance:


    2025-08-28 00:40:23,711	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_quarter: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    sum(weighted_revenue): double
    mean(is_urgent): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:40:23,895	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_quarter: int64
    total_orders: int64
    total_revenue: double
    avg_order_value: double
    weighted_revenue: double
    urgent_order_percentage: double, new schema: PandasBlockSchema(names=[], types=[]). This may lead to unexpected behavior.
    2025-08-28 00:40:23,930	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_385_0 execution finished in 22.79 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_quarter</th>
      <th>total_orders</th>
      <th>total_revenue</th>
      <th>avg_order_value</th>
      <th>weighted_revenue</th>
      <th>urgent_order_percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>3939930</td>
      <td>5.954367e+11</td>
      <td>151128.743367</td>
      <td>3.573891e+11</td>
      <td>0.399966</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3971040</td>
      <td>5.999666e+11</td>
      <td>151085.499182</td>
      <td>3.600278e+11</td>
      <td>0.400345</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>3648472</td>
      <td>5.509344e+11</td>
      <td>151004.153324</td>
      <td>3.307208e+11</td>
      <td>0.400452</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>3440558</td>
      <td>5.199605e+11</td>
      <td>151126.804122</td>
      <td>3.120457e+11</td>
      <td>0.400078</td>
    </tr>
  </tbody>
</table>
</div>




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

    2025-08-28 00:40:24,067	INFO logging.py:295 -- Registered dataset logger for dataset dataset_388_0
    2025-08-28 00:40:24,082	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_388_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:40:24,083	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_388_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]


    Operational Analytics (Enterprise Process Optimization):


    2025-08-28 00:40:45,640	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: revenue_tier: string
    count(): int64
    sum(o_totalprice): double
    mean(priority_weight): double
    mean(requires_expedited_processing): double
    sum(is_peak_season): int64, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:40:45,689	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_388_0 execution finished in 21.61 seconds
    2025-08-28 00:40:45,696	INFO logging.py:295 -- Registered dataset logger for dataset dataset_390_0
    2025-08-28 00:40:45,704	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_390_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:40:45,705	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_390_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=25] -> TaskPoolMapOperator[Project]


    Performance by Revenue Tier:


    2025-08-28 00:41:07,447	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: revenue_tier: string
    count(): int64
    sum(o_totalprice): double
    mean(priority_weight): double
    mean(requires_expedited_processing): double
    sum(is_peak_season): int64, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:41:07,626	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: revenue_tier: string
    order_volume: int64
    total_revenue: double
    avg_priority_weight: double
    expedited_processing_rate: double
    peak_season_orders: int64, new schema: PandasBlockSchema(names=[], types=[]). This may lead to unexpected behavior.
    2025-08-28 00:41:07,655	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_390_0 execution finished in 21.95 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>revenue_tier</th>
      <th>order_volume</th>
      <th>total_revenue</th>
      <th>avg_priority_weight</th>
      <th>expedited_processing_rate</th>
      <th>peak_season_orders</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Enterprise</td>
      <td>854969</td>
      <td>2.868012e+11</td>
      <td>0.600440</td>
      <td>1.000000</td>
      <td>130174</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Large</td>
      <td>6322157</td>
      <td>1.351880e+12</td>
      <td>0.600089</td>
      <td>0.745465</td>
      <td>962126</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Medium</td>
      <td>5706652</td>
      <td>5.680665e+11</td>
      <td>0.600149</td>
      <td>0.400155</td>
      <td>867669</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Small</td>
      <td>2116222</td>
      <td>5.955084e+10</td>
      <td>0.600166</td>
      <td>0.400108</td>
      <td>322330</td>
    </tr>
  </tbody>
</table>
</div>




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

    2025-08-28 00:41:07,751	INFO logging.py:295 -- Registered dataset logger for dataset dataset_393_0
    2025-08-28 00:41:07,760	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_393_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:41:07,761	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_393_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]


    
     Priority-Based Analysis (Order Management Insights):


    2025-08-28 00:41:29,471	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: o_orderpriority: string
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    mean(is_large_order): double
    mean(is_weekend): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:41:29,508	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_393_0 execution finished in 21.75 seconds
    2025-08-28 00:41:29,514	INFO logging.py:295 -- Registered dataset logger for dataset dataset_396_0
    2025-08-28 00:41:29,524	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_396_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:41:29,524	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_396_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project] -> AllToAllOperator[Sort] -> LimitOperator[limit=25]
    2025-08-28 00:41:50,905	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: o_orderpriority: string
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    mean(is_large_order): double
    mean(is_weekend): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:41:51,040	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: o_orderpriority: string
    priority_orders: int64
    priority_revenue: double
    avg_order_value: double
    large_order_rate: double
    weekend_order_rate: double, new schema: PandasBlockSchema(names=[], types=[]). This may lead to unexpected behavior.
    2025-08-28 00:41:51,414	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: PandasBlockSchema(names=['o_orderpriority', 'priority_orders', 'priority_revenue', 'avg_order_value', 'large_order_rate', 'weekend_order_rate'], types=[dtype('O'), dtype('int64'), dtype('float64'), dtype('float64'), dtype('float64'), dtype('float64')]), new schema: PandasBlockSchema(names=[], types=[]). This may lead to unexpected behavior.
    2025-08-28 00:41:51,489	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_396_0 execution finished in 21.96 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>o_orderpriority</th>
      <th>priority_orders</th>
      <th>priority_revenue</th>
      <th>avg_order_value</th>
      <th>large_order_rate</th>
      <th>weekend_order_rate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1-URGENT</td>
      <td>3003093</td>
      <td>4.537567e+11</td>
      <td>151096.441152</td>
      <td>0.299924</td>
      <td>0.286203</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2-HIGH</td>
      <td>3000061</td>
      <td>4.534886e+11</td>
      <td>151159.783341</td>
      <td>0.300221</td>
      <td>0.285980</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4-NOT SPECIFIED</td>
      <td>3000260</td>
      <td>4.532757e+11</td>
      <td>151078.819384</td>
      <td>0.299571</td>
      <td>0.286167</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3-MEDIUM</td>
      <td>2998940</td>
      <td>4.529252e+11</td>
      <td>151028.424989</td>
      <td>0.299468</td>
      <td>0.286202</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5-LOW</td>
      <td>2997646</td>
      <td>4.528520e+11</td>
      <td>151069.216228</td>
      <td>0.299619</td>
      <td>0.285374</td>
    </tr>
  </tbody>
</table>
</div>




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

    2025-08-28 00:41:51,602	INFO logging.py:295 -- Registered dataset logger for dataset dataset_399_0
    2025-08-28 00:41:51,611	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_399_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:41:51,611	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_399_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> LimitOperator[limit=1]


    Temporal Analysis (Time-Series Business Intelligence):


    2025-08-28 00:42:12,674	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_year: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    mean(is_peak_season): double
    mean(is_large_order): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:42:12,710	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_399_0 execution finished in 21.10 seconds
    2025-08-28 00:42:12,717	INFO logging.py:295 -- Registered dataset logger for dataset dataset_402_0
    2025-08-28 00:42:12,726	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_402_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:42:12,727	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_402_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project] -> AllToAllOperator[Sort] -> LimitOperator[limit=25]


    Year-over-Year Performance:


    2025-08-28 00:42:34,273	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_year: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    mean(is_peak_season): double
    mean(is_large_order): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:42:34,415	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_year: int64
    yearly_orders: int64
    yearly_revenue: double
    avg_order_value: double
    peak_season_rate: double
    large_order_percentage: double, new schema: PandasBlockSchema(names=[], types=[]). This may lead to unexpected behavior.
    2025-08-28 00:42:34,873	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: PandasBlockSchema(names=['order_year', 'yearly_orders', 'yearly_revenue', 'avg_order_value', 'peak_season_rate', 'large_order_percentage'], types=[dtype('int64'), dtype('int64'), dtype('float64'), dtype('float64'), dtype('float64'), dtype('float64')]), new schema: PandasBlockSchema(names=[], types=[]). This may lead to unexpected behavior.
    2025-08-28 00:42:34,944	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_402_0 execution finished in 22.21 seconds





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_year</th>
      <th>yearly_orders</th>
      <th>yearly_revenue</th>
      <th>avg_order_value</th>
      <th>peak_season_rate</th>
      <th>large_order_percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1992</td>
      <td>2281205</td>
      <td>3.444725e+11</td>
      <td>151004.621657</td>
      <td>0.167060</td>
      <td>0.299643</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1993</td>
      <td>2276638</td>
      <td>3.440619e+11</td>
      <td>151127.204812</td>
      <td>0.167049</td>
      <td>0.299833</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1994</td>
      <td>2275919</td>
      <td>3.440890e+11</td>
      <td>151186.860472</td>
      <td>0.167508</td>
      <td>0.300342</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1995</td>
      <td>2275575</td>
      <td>3.437713e+11</td>
      <td>151070.064800</td>
      <td>0.166866</td>
      <td>0.299505</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1996</td>
      <td>2281938</td>
      <td>3.447880e+11</td>
      <td>151094.386277</td>
      <td>0.166771</td>
      <td>0.299821</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1997</td>
      <td>2275511</td>
      <td>3.436590e+11</td>
      <td>151024.990596</td>
      <td>0.166723</td>
      <td>0.299462</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1998</td>
      <td>1333214</td>
      <td>2.014564e+11</td>
      <td>151105.820581</td>
      <td>0.000000</td>
      <td>0.299689</td>
    </tr>
  </tbody>
</table>
</div>



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

    2025-08-28 00:42:35,135	INFO logging.py:295 -- Registered dataset logger for dataset dataset_404_0
    2025-08-28 00:42:35,148	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_404_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:42:35,149	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_404_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)->Write]
    2025-08-28 00:42:35,170	WARNING progress_bar.py:120 -- Truncating long operator name to 100 characters. To disable this behavior, set `ray.data.DataContext.get_current().DEFAULT_ENABLE_PROGRESS_BAR_NAME_TRUNCATION = False`.


     Writing TPC-H processed data to various formats...
     Writing enriched TPC-H orders to Parquet...


    2025-08-28 00:43:02,396	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_404_0 execution finished in 27.25 seconds
    2025-08-28 00:43:02,448	INFO dataset.py:4871 -- Data sink Parquet finished. 15000000 rows and 8.0GB data written.
    2025-08-28 00:43:02,454	INFO logging.py:295 -- Registered dataset logger for dataset dataset_407_0
    2025-08-28 00:43:02,474	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_407_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:43:02,475	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_407_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project->Write]
    2025-08-28 00:43:23,835	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_quarter: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    sum(weighted_revenue): double
    mean(is_urgent): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:43:23,994	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_407_0 execution finished in 21.52 seconds
    2025-08-28 00:43:24,033	INFO dataset.py:4871 -- Data sink Parquet finished. 4 rows and 8.7KB data written.
    2025-08-28 00:43:24,039	INFO logging.py:295 -- Registered dataset logger for dataset dataset_410_0
    2025-08-28 00:43:24,053	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_410_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:43:24,054	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_410_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project->Write]
    2025-08-28 00:43:45,353	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: revenue_tier: string
    count(): int64
    sum(o_totalprice): double
    mean(priority_weight): double
    mean(requires_expedited_processing): double
    sum(is_peak_season): int64, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:43:45,512	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_410_0 execution finished in 21.46 seconds
    2025-08-28 00:43:45,556	INFO dataset.py:4871 -- Data sink Parquet finished. 4 rows and 9.1KB data written.
    2025-08-28 00:43:45,565	INFO logging.py:295 -- Registered dataset logger for dataset dataset_413_0
    2025-08-28 00:43:45,579	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_413_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:43:45,580	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_413_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project->Write]
    2025-08-28 00:44:07,091	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: o_orderpriority: string
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    mean(is_large_order): double
    mean(is_weekend): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:44:07,253	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_413_0 execution finished in 21.67 seconds
    2025-08-28 00:44:07,294	INFO dataset.py:4871 -- Data sink Parquet finished. 5 rows and 8.6KB data written.
    2025-08-28 00:44:07,300	INFO logging.py:295 -- Registered dataset logger for dataset dataset_416_0
    2025-08-28 00:44:07,313	INFO streaming_executor.py:159 -- Starting execution of Dataset dataset_416_0. Full logs are in /tmp/ray/session_2025-08-27_22-21-09_261930_2324/logs/ray-data
    2025-08-28 00:44:07,314	INFO streaming_executor.py:160 -- Execution plan of Dataset dataset_416_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ListFiles] -> LimitOperator[limit=1000] -> TaskPoolMapOperator[ReadFiles] -> TaskPoolMapOperator[MapBatches(traditional_etl_enrichment_tpch)->MapBatches(ml_ready_feature_engineering_tpch)] -> AllToAllOperator[Aggregate] -> TaskPoolMapOperator[Project->Write]
    2025-08-28 00:44:28,398	WARNING streaming_executor_state.py:790 -- Operator produced a RefBundle with a different schema than the previous one. Previous schema: order_year: int64
    count(): int64
    sum(o_totalprice): double
    mean(o_totalprice): double
    mean(is_peak_season): double
    mean(is_large_order): double, new schema: None. This may lead to unexpected behavior.
    2025-08-28 00:44:28,553	INFO streaming_executor.py:279 -- ✔️  Dataset dataset_416_0 execution finished in 21.24 seconds
    2025-08-28 00:44:28,594	INFO dataset.py:4871 -- Data sink Parquet finished. 7 rows and 8.4KB data written.


    [36m(autoscaler +10m44s)[0m Tip: use `ray status` to view detailed cluster status. To disable these messages, set RAY_SCHEDULER_EVENTS=0.


## Summary: Your Journey with Ray Data ETL

Congratulations! You've completed a comprehensive journey through Ray Data for ETL. Let's summarize what you've learned and explore how to take your AI data pipelines to production.

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
3. **Marketplace Deployment**: One-click setup via AWS or GCP Marketplace

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
