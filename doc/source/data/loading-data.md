---
myst:
  html_meta:
    description: "Load data into Ray Data from local and cloud storage, compressed files, URIs, single-node libraries, distributed DataFrames, and Hugging Face."
---

(loading_data)=

# Loading data

Ray Data loads data from various sources. This guide shows you how to [read files](#reading-files) such as images, [load in-memory data](#loading-data-from-other-libraries) such as pandas DataFrames, and [read databases](#reading-databases) such as MySQL.

(reading-files)=

## Read files

Ray Data reads files in a variety of formats from shared local storage or cloud storage. For the full list of supported formats, see the {ref}`Loading Data API <loading-data-api>`.

:::::{tab-set}

::::{tab-item} Parquet

To read Parquet files, call {func}`~ray.data.read_parquet`.

```{testcode}
import ray

ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

print(ds.schema())
```

```{testoutput}
Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string
```

:::{tip}
When you read Parquet files, you can use column pruning to filter columns efficiently at the file scan level. For more on this projection pushdown feature, see {ref}`Parquet column pruning <parquet_column_pruning>`.
:::

::::

::::{tab-item} Images

To read raw images, call {func}`~ray.data.read_images`. Ray Data represents images as NumPy ndarrays.

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages/")

print(ds.schema())
```

```{testoutput}
Column  Type
------  ----
image   ArrowTensorTypeV2(shape=(32, 32, 3), dtype=uint8)
```

::::

::::{tab-item} Text

To read lines of text, call {func}`~ray.data.read_text`.

```{testcode}
import ray

ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

print(ds.schema())
```

```{testoutput}
Column  Type
------  ----
text    string
```

::::

::::{tab-item} CSV

To read CSV files, call {func}`~ray.data.read_csv`.

```{testcode}
import ray

ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

print(ds.schema())
```

```{testoutput}
Column             Type
------             ----
sepal length (cm)  double
sepal width (cm)   double
petal length (cm)  double
petal width (cm)   double
target             int64
```

::::

::::{tab-item} Binary

To read raw binary files, call {func}`~ray.data.read_binary_files`.

```{testcode}
import ray

ds = ray.data.read_binary_files("s3://anonymous@ray-example-data/documents")

print(ds.schema())
```

```{testoutput}
Column  Type
------  ----
bytes   binary
```

::::

::::{tab-item} TFRecords

To read TFRecords files, call {func}`~ray.data.read_tfrecords`.

```{testcode}
import ray

ds = ray.data.read_tfrecords("s3://anonymous@ray-example-data/iris.tfrecords")

print(ds.schema())
```

```{testoutput}
:options: +MOCK

Column        Type
------        ----
label         binary
petal.length  float
sepal.width   float
petal.width   float
sepal.length  float
```

::::

::::{tab-item} Zarr

To read a Zarr v2 store, call {func}`~ray.data.read_zarr`.

```python
import ray

ds = ray.data.read_zarr("s3://anonymous@ray-example-data/mnist-tiny.zarr")
```

::::

:::::

(reading-files-from-shared-local-storage)=

### Read files from shared local storage

To read files from a shared local filesystem, put the files on storage such as NFS, and mount that storage at the same path on every Ray node. Then, call a function such as {func}`~ray.data.read_parquet` with the mounted path. Paths can point to files or directories.

:::{warning}
Don't use the deprecated `local://` scheme. Use cloud storage or a shared filesystem path that's available on every Ray node instead.
:::

To read formats other than Parquet, see the {ref}`Loading Data API <loading-data-api>`.

```{testcode}
:skipif: True

import ray

ds = ray.data.read_parquet("/mnt/cluster_storage/iris.parquet")

print(ds.schema())
```

```{testoutput}
Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string
```

(reading-files-from-cloud-storage)=

### Read files from cloud storage

To read files in cloud storage, authenticate all nodes with your cloud service provider. Then, call a function such as {func}`~ray.data.read_parquet` and specify URIs with the appropriate scheme. URIs can point to buckets, folders, or objects.

To read formats other than Parquet, see the {ref}`Loading Data API <loading-data-api>`.

::::{tab-set}

:::{tab-item} S3

To read files from Amazon S3, specify URIs with the `s3://` scheme.

```{testcode}
import ray

ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

print(ds.schema())
```

```{testoutput}
Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string
```

Ray Data relies on PyArrow for authentication with Amazon S3. To configure credentials that work with PyArrow, see the PyArrow [S3 filesystem docs](https://arrow.apache.org/docs/python/filesystems.html#s3).

:::

:::{tab-item} GCS

To read files from Google Cloud Storage, install the [Filesystem interface to Google Cloud Storage](https://gcsfs.readthedocs.io/en/latest/):

```console
pip install gcsfs
```

Then, create a `GCSFileSystem` and specify URIs with the `gs://` scheme.

```{testcode}
:skipif: True

import ray

filesystem = gcsfs.GCSFileSystem(project="my-google-project")
ds = ray.data.read_parquet(
    "gs://...",
    filesystem=filesystem
)

print(ds.schema())
```

```{testoutput}
Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string
```

Ray Data relies on PyArrow for authentication with Google Cloud Storage. To configure credentials that work with PyArrow, see the PyArrow [GCS filesystem docs](https://arrow.apache.org/docs/python/filesystems.html#google-cloud-storage-file-system).

:::

:::{tab-item} Azure Blob Storage

To read files from Azure Blob Storage, install the [Filesystem interface to Azure-Datalake Gen1 and Gen2 Storage](https://pypi.org/project/adlfs/):

```console
pip install adlfs
```

Then, create an `AzureBlobFileSystem` and specify URIs with the `az://` scheme.

```{testcode}
:skipif: True

import adlfs
import ray

ds = ray.data.read_parquet(
    "az://ray-example-data/iris.parquet",
    adlfs.AzureBlobFileSystem(account_name="azureopendatastorage")
)

print(ds.schema())
```

```{testoutput}
Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string
```

Ray Data relies on PyArrow for authentication with Azure Blob Storage. To configure credentials that work with PyArrow, see the PyArrow [fsspec-compatible filesystems docs](https://arrow.apache.org/docs/python/filesystems.html#using-fsspec-compatible-filesystems-with-arrow).

:::

::::

(reading-files-from-the-hadoop-distributed-file-system)=

### Read files from the Hadoop Distributed File System

To read files from the Hadoop Distributed File System (HDFS), install the Hadoop client on every relevant Ray node and set `HADOOP_HOME`, `JAVA_HOME`, and `CLASSPATH` so that [PyArrow can load the native HDFS library and the Hadoop Java client](https://arrow.apache.org/docs/python/filesystems.html#hadoop-file-system-hdfs). If `libhdfs.so` isn't under `$HADOOP_HOME/lib/native`, also set `ARROW_LIBHDFS_DIR`. Then, pass a fully qualified `hdfs://` URI to a supported read API. The following example reads Parquet data from HDFS:

```{testcode}
:skipif: True

import ray

ds = ray.data.read_parquet("hdfs://hostname:8020/path/to/data")
```

:::{warning}
PyArrow HDFS embeds a Java Virtual Machine (JVM) in the Python process. On Linux, its signal handling can conflict with Ray and cause the process to exit with `SIGSEGV` or `SIGABRT` and create an `hs_err_pid*.log` file. See {ref}`troubleshoot-pyarrow-hdfs-jvm-crashes` for the HotSpot signal-chaining configuration and the last-resort fallback.
:::

(handling-compressed-files)=

### Handle compressed files

To read a compressed file, specify `compression` in `arrow_open_stream_args`. You can use any [codec supported by Arrow](https://arrow.apache.org/docs/python/generated/pyarrow.CompressedInputStream.html).

```{testcode}
import ray

ds = ray.data.read_csv(
    "s3://anonymous@ray-example-data/iris.csv.gz",
    arrow_open_stream_args={"compression": "gzip"},
)
```

(downloading-files-from-uris)=

### Download files from URIs

If you have a metadata table with a column of URIs, you can download the files that those URIs reference.

To download the files in bulk, use the {func}`~ray.data.Dataset.with_column` method with the {func}`~ray.data.expressions.download` expression. Ray Data handles the parallel download of the files that the URLs in your dataset reference, so you don't need to manage async code in your own transformations.

The following example shows how to download a batch of images from URLs listed in a Parquet file:

```{testcode}
import pyarrow.fs

import ray
from ray.data.expressions import download

# Read a Parquet file containing a column of image URLs
ds = ray.data.read_parquet("s3://anonymous@ray-example-data/imagenet/metadata_file.parquet")

# Use `with_column` and `download` to download the images in parallel.
# This creates a new column 'bytes' with the downloaded file contents.
ds = ds.with_column(
    "bytes",
    download(
        "image_url",
        filesystem=pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2"),
    ),
)

ds.take(1)
```

(loading-data-from-other-libraries)=

## Load data from other libraries

Ray Data creates datasets from data in single-node data libraries, distributed DataFrame libraries, Hugging Face, and ML libraries.

(loading-data-from-single-node-data-libraries)=

### Load data from single-node data libraries

Ray Data interoperates with libraries such as pandas, NumPy, and Arrow.

::::{tab-set}

:::{tab-item} Python objects

To create a {class}`~ray.data.dataset.Dataset` from Python objects, call {func}`~ray.data.from_items` and pass a list of `Dict`. Ray Data treats each `Dict` as a row.

```{testcode}
import ray

ds = ray.data.from_items([
    {"food": "spam", "price": 9.34},
    {"food": "ham", "price": 5.37},
    {"food": "eggs", "price": 0.94}
])

print(ds)
```

```{testoutput}
shape: (3, 2)
╭────────┬────────╮
│ food   ┆ price  │
│ ---    ┆ ---    │
│ string ┆ double │
╞════════╪════════╡
│ spam   ┆ 9.34   │
│ ham    ┆ 5.37   │
│ eggs   ┆ 0.94   │
╰────────┴────────╯
(Showing 3 of 3 rows)
```

You can also create a {class}`~ray.data.dataset.Dataset` from a list of regular Python objects. In the schema, the column name defaults to `item`.

```{testcode}
import ray

ds = ray.data.from_items([1, 2, 3, 4, 5])

print(ds)
```

```{testoutput}
shape: (5, 1)
╭───────╮
│ item  │
│ ---   │
│ int64 │
╞═══════╡
│ 1     │
│ 2     │
│ 3     │
│ 4     │
│ 5     │
╰───────╯
(Showing 5 of 5 rows)
```

:::

:::{tab-item} NumPy

To create a {class}`~ray.data.dataset.Dataset` from a NumPy array, call {func}`~ray.data.from_numpy`. Ray Data treats the outer axis as the row dimension.

```{testcode}
import numpy as np
import ray

array = np.arange(3)
ds = ray.data.from_numpy(array)

print(ds)
```

```{testoutput}
shape: (3, 1)
╭───────╮
│ data  │
│ ---   │
│ int64 │
╞═══════╡
│ 0     │
│ 1     │
│ 2     │
╰───────╯
(Showing 3 of 3 rows)
```

:::

:::{tab-item} pandas

To create a {class}`~ray.data.dataset.Dataset` from a pandas DataFrame, call {func}`~ray.data.from_pandas`.

```{testcode}
import pandas as pd
import ray

df = pd.DataFrame({
    "food": ["spam", "ham", "eggs"],
    "price": [9.34, 5.37, 0.94]
})
ds = ray.data.from_pandas(df)

print(ds)
```

```{testoutput}
shape: (3, 2)
╭────────┬────────╮
│ food   ┆ price  │
│ ---    ┆ ---    │
│ object ┆ double │
╞════════╪════════╡
│ spam   ┆ 9.34   │
│ ham    ┆ 5.37   │
│ eggs   ┆ 0.94   │
╰────────┴────────╯
(Showing 3 of 3 rows)
```

:::

:::{tab-item} PyArrow

To create a {class}`~ray.data.dataset.Dataset` from an Arrow table, call {func}`~ray.data.from_arrow`.

```{testcode}
import pyarrow as pa

table = pa.table({
    "food": ["spam", "ham", "eggs"],
    "price": [9.34, 5.37, 0.94]
})
ds = ray.data.from_arrow(table)

print(ds)
```

```{testoutput}
shape: (3, 2)
╭────────┬────────╮
│ food   ┆ price  │
│ ---    ┆ ---    │
│ string ┆ double │
╞════════╪════════╡
│ spam   ┆ 9.34   │
│ ham    ┆ 5.37   │
│ eggs   ┆ 0.94   │
╰────────┴────────╯
(Showing 3 of 3 rows)
```

:::

::::

(loading_datasets_from_distributed_df)=
(loading-data-from-distributed-dataframe-libraries)=

### Load data from distributed DataFrame libraries

Ray Data interoperates with distributed data processing frameworks such as [Daft](https://www.daft.ai), {ref}`Dask <dask-on-ray>`, {ref}`Spark <spark-on-ray>`, {ref}`Modin <modin-on-ray>`, and {ref}`Mars <mars-on-ray>`.

:::{note}
The Ray community provides these operations but might not actively maintain them. If you run into issues, [create a GitHub issue](https://github.com/ray-project/ray/issues).
:::

::::{tab-set}

:::{tab-item} Daft

To create a {class}`~ray.data.dataset.Dataset` from a [Daft DataFrame](https://docs.daft.ai/en/stable/api/dataframe/), call {func}`~ray.data.from_daft`. This function runs the Daft DataFrame and constructs a `Dataset` backed by the Arrow data that your Daft query produces.

```{testcode}
:skipif: True

import daft
import ray

df = daft.from_pydict({"int_col": [i for i in range(10000)], "str_col": [str(i) for i in range(10000)]})
ds = ray.data.from_daft(df)

ds.show(3)
```

```{testoutput}
{'int_col': 0, 'str_col': '0'}
{'int_col': 1, 'str_col': '1'}
{'int_col': 2, 'str_col': '2'}
```

:::

:::{tab-item} Dask

To create a {class}`~ray.data.dataset.Dataset` from a [Dask DataFrame](https://docs.dask.org/en/stable/dataframe.html), call {func}`~ray.data.from_dask`. This function constructs a `Dataset` backed by the distributed pandas DataFrame partitions that underlie the Dask DataFrame.

<!--
We skip the code snippet below because `from_dask` doesn't work with PyArrow
14 and later. For more information, see https://github.com/ray-project/ray/issues/54837
-->

```{testcode}
:skipif: True

import dask.dataframe as dd
import pandas as pd
import ray

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
ddf = dd.from_pandas(df, npartitions=4)
# Create a Dataset from a Dask DataFrame.
ds = ray.data.from_dask(ddf)

ds.show(3)
```

```{testoutput}
{'col1': 0, 'col2': '0'}
{'col1': 1, 'col2': '1'}
{'col1': 2, 'col2': '2'}
```

:::

:::{tab-item} Spark

To create a {class}`~ray.data.dataset.Dataset` from a [Spark DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html), call {func}`~ray.data.from_spark`. This function creates a `Dataset` backed by the distributed Spark DataFrame partitions that underlie the Spark DataFrame.

<!--
TODO: This code snippet might not work correctly. We should test it.
-->

```{testcode}
:skipif: True

import ray
import raydp

spark = raydp.init_spark(app_name="Spark -> Datasets Example",
                        num_executors=2,
                        executor_cores=2,
                        executor_memory="500MB")
df = spark.createDataFrame([(i, str(i)) for i in range(10000)], ["col1", "col2"])
ds = ray.data.from_spark(df)

ds.show(3)
```

```{testoutput}
{'col1': 0, 'col2': '0'}
{'col1': 1, 'col2': '1'}
{'col1': 2, 'col2': '2'}
```

:::

:::{tab-item} Iceberg

To create a {class}`~ray.data.dataset.Dataset` from an [Iceberg table](https://iceberg.apache.org), call {func}`~ray.data.read_iceberg`. This function creates a `Dataset` backed by the distributed files that underlie the Iceberg table.

```{testcode}
:skipif: True

import ray
from pyiceberg.expressions import EqualTo

ds = ray.data.read_iceberg(
    table_identifier="db_name.table_name",
    row_filter=EqualTo("column_name", "literal_value"),
    catalog_kwargs={"name": "default", "type": "glue"}
)
ds.show(3)
```

```{testoutput}
:options: +MOCK

{'col1': 0, 'col2': '0'}
{'col1': 1, 'col2': '1'}
{'col1': 2, 'col2': '2'}
```

:::

:::{tab-item} Modin

To create a {class}`~ray.data.dataset.Dataset` from a Modin DataFrame, call {func}`~ray.data.from_modin`. This function constructs a `Dataset` backed by the distributed pandas DataFrame partitions that underlie the Modin DataFrame.

```{testcode}
import modin.pandas as md
import pandas as pd
import ray

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
mdf = md.DataFrame(df)
# Create a Dataset from a Modin DataFrame.
ds = ray.data.from_modin(mdf)

ds.show(3)
```

```{testoutput}
{'col1': 0, 'col2': '0'}
{'col1': 1, 'col2': '1'}
{'col1': 2, 'col2': '2'}
```

:::

:::{tab-item} Mars

To create a {class}`~ray.data.dataset.Dataset` from a Mars DataFrame, call {func}`~ray.data.from_mars`. This function constructs a `Dataset` backed by the distributed pandas DataFrame partitions that underlie the Mars DataFrame.

```{testcode}
:skipif: True

import mars
import mars.dataframe as md
import pandas as pd
import ray

cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
mdf = md.DataFrame(df, num_partitions=8)
# Create a tabular Dataset from a Mars DataFrame.
ds = ray.data.from_mars(mdf)

ds.show(3)
```

```{testoutput}
{'col1': 0, 'col2': '0'}
{'col1': 1, 'col2': '1'}
{'col1': 2, 'col2': '2'}
```

:::

::::

(loading_huggingface_datasets)=
(loading-hugging-face-datasets)=

### Load Hugging Face datasets

To read datasets from the Hugging Face Hub, use {func}`~ray.data.read_parquet` or another read function with the `HfFileSystem` filesystem. This approach performs and scales better than loading datasets into memory first.

First, install the required dependencies:

```console
pip install huggingface_hub
```

Set your Hugging Face token to authenticate. You can read public datasets without a token, but Hugging Face rate limits are more aggressive without one. To read Hugging Face datasets without a token, set the filesystem argument to `HfFileSystem()`.

```console
export HF_TOKEN=<YOUR HUGGING FACE TOKEN>
```

Most Hugging Face datasets store their data in Parquet files, so you can read directly from the dataset path:

```{testcode}
:skipif: True

import os
import ray
from huggingface_hub import HfFileSystem

ds = ray.data.read_parquet(
    "hf://datasets/wikimedia/wikipedia",
    file_extensions=["parquet"],
    filesystem=HfFileSystem(token=os.environ["HF_TOKEN"]),
)

print(f"Dataset count: {ds.count()}")
print(ds.schema())
```

```{testoutput}
Dataset count: 61614907
Column  Type
------  ----
id      string
url     string
title   string
text    string
```

:::{tip}
If you get serialization errors when reading from Hugging Face filesystems, try upgrading `huggingface_hub` to version 1.1.6 or later. For more details, see [GitHub issue 59029](https://github.com/ray-project/ray/issues/59029).
:::

(loading_datasets_from_ml_libraries)=
(loading-data-from-ml-libraries)=

### Load data from ML libraries

Ray Data interoperates with Hugging Face, PyTorch, and TensorFlow datasets.

:::::{tab-set}

::::{tab-item} Hugging Face

To load a Hugging Face dataset into Ray Data, use the Hugging Face Hub `HfFileSystem` with {func}`~ray.data.read_parquet`, {func}`~ray.data.read_csv`, or {func}`~ray.data.read_json`. Hugging Face datasets are often backed by these file formats, so this approach performs efficient distributed reads directly from the Hub.

```{testcode}
:skipif: True

import ray.data
from huggingface_hub import HfFileSystem

path = "hf://datasets/Salesforce/wikitext/wikitext-2-raw-v1/"
fs = HfFileSystem()
ds = ray.data.read_parquet(path, filesystem=fs)
print(ds.take(5))
```

```{testoutput}
:options: +MOCK

[{'text': '...'}, {'text': '...'}]
```

::::

::::{tab-item} PyTorch

To convert a PyTorch dataset to a Ray Dataset, call {func}`~ray.data.from_torch`.

<!-- The mirror for CIFAR10 has historically been unreliable, so we skip the test. -->

```{testcode}
:skipif: True

import ray
from torch.utils.data import Dataset
from torchvision import datasets
from torchvision.transforms import ToTensor

tds = datasets.CIFAR10(root="data", train=True, download=True, transform=ToTensor())
ds = ray.data.from_torch(tds)

print(ds)
```

```{testoutput}
:options: +MOCK

Downloading https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz to data/cifar-10-python.tar.gz
100%|███████████████████████| 170498071/170498071 [00:07<00:00, 23494838.54it/s]
Extracting data/cifar-10-python.tar.gz to data
Dataset(num_rows=50000, schema={item: object})
```

::::

::::{tab-item} TensorFlow

To convert a TensorFlow dataset to a Ray Dataset, call {func}`~ray.data.from_tf`.

:::{warning}
{func}`~ray.data.from_tf` doesn't support parallel reads. Only use this function with small datasets such as MNIST or CIFAR.
:::

```{testcode}
:skipif: True

import ray
import tensorflow_datasets as tfds

tf_ds, _ = tfds.load("cifar10", split=["train", "test"])
ds = ray.data.from_tf(tf_ds)

print(ds)
```

<!--
The following `testoutput` is mocked to avoid illustrating download logs like
"Downloading and preparing dataset 162.17 MiB".
-->

```{testoutput}
:options: +MOCK

MaterializedDataset(
   num_blocks=...,
   num_rows=50000,
   schema={
      id: binary,
      image: ArrowTensorTypeV2(shape=(32, 32, 3), dtype=uint8),
      label: int64
   }
)
```

::::

:::::

(reading-databases)=

## Read databases

Ray Data reads from databases such as MySQL, PostgreSQL, MongoDB, and BigQuery.

(reading_sql)=
(reading-sql-databases)=

### Read SQL databases

Call {func}`~ray.data.read_sql` to read data from a database that provides a [Python DB API2-compliant](https://peps.python.org/pep-0249/) connector.

::::{tab-set}

:::{tab-item} MySQL

To read from MySQL, install [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/). It's the first-party MySQL database connector.

```console
pip install mysql-connector-python
```

Then, define your connection logic and query the database.

```{testcode}
:skipif: True

import mysql.connector

import ray

def create_connection():
    return mysql.connector.connect(
        user="admin",
        password=...,
        host="example-mysql-database.c2c2k1yfll7o.us-west-2.rds.amazonaws.com",
        connection_timeout=30,
        database="example",
    )

# Get all movies
dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
# Get movies after the year 1980
dataset = ray.data.read_sql(
    "SELECT title, score FROM movie WHERE year >= 1980", create_connection
)
# Get the number of movies per year
dataset = ray.data.read_sql(
    "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
)
```

:::

:::{tab-item} PostgreSQL

To read from PostgreSQL, install [Psycopg 2](https://www.psycopg.org/docs). It's the most popular PostgreSQL database connector.

```console
pip install psycopg2-binary
```

Then, define your connection logic and query the database.

```{testcode}
:skipif: True

import psycopg2

import ray

def create_connection():
    return psycopg2.connect(
        user="postgres",
        password=...,
        host="example-postgres-database.c2c2k1yfll7o.us-west-2.rds.amazonaws.com",
        dbname="example",
    )

# Get all movies
dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
# Get movies after the year 1980
dataset = ray.data.read_sql(
    "SELECT title, score FROM movie WHERE year >= 1980", create_connection
)
# Get the number of movies per year
dataset = ray.data.read_sql(
    "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
)
```

:::

:::{tab-item} Snowflake

To read from Snowflake, install the [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector).

```console
pip install snowflake-connector-python
```

Then, define your connection logic and query the database.

```{testcode}
:skipif: True

import snowflake.connector

import ray

def create_connection():
    return snowflake.connector.connect(
        user=...,
        password=...,
        account="ZZKXUVH-IPB52023",
        database="example",
    )

# Get all movies
dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
# Get movies after the year 1980
dataset = ray.data.read_sql(
    "SELECT title, score FROM movie WHERE year >= 1980", create_connection
)
# Get the number of movies per year
dataset = ray.data.read_sql(
    "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
)
```

:::

:::{tab-item} Databricks

To read from Databricks, set the `DATABRICKS_TOKEN` environment variable to your Databricks warehouse access token.

```console
export DATABRICKS_TOKEN=...
```

If you're not running your program on the Databricks runtime, also set the `DATABRICKS_HOST` environment variable.

```console
export DATABRICKS_HOST=adb-<workspace-id>.<random-number>.azuredatabricks.net
```

Then, call {func}`ray.data.read_databricks_tables` to read from the Databricks SQL warehouse.

```{testcode}
:skipif: True

import ray

dataset = ray.data.read_databricks_tables(
    warehouse_id='...',  # Databricks SQL warehouse ID
    catalog='catalog_1',  # Unity catalog name
    schema='db_1',  # Schema name
    query="SELECT title, score FROM movie WHERE year >= 1980",
)
```

:::

:::{tab-item} BigQuery

To read from BigQuery, install the [Python Client for Google BigQuery](https://cloud.google.com/python/docs/reference/bigquery/latest) and the [Python Client for Google BigQueryStorage](https://cloud.google.com/python/docs/reference/bigquerystorage/latest).

```console
pip install google-cloud-bigquery
pip install google-cloud-bigquery-storage
```

To read data from BigQuery, call {func}`~ray.data.read_bigquery` and specify the project ID and either a dataset or a query.

```{testcode}
:skipif: True

import ray

# Read the entire dataset. Do not specify query.
ds = ray.data.read_bigquery(
    project_id="my_gcloud_project_id",
    dataset="bigquery-public-data.ml_datasets.iris",
)

# Read from a SQL query of the dataset. Do not specify dataset.
ds = ray.data.read_bigquery(
    project_id="my_gcloud_project_id",
    query = "SELECT * FROM `bigquery-public-data.ml_datasets.iris` LIMIT 50",
)

# Write back to BigQuery
ds.write_bigquery(
    project_id="my_gcloud_project_id",
    dataset="destination_dataset.destination_table",
    overwrite_table=True,
)
```

:::

::::

(reading_mongodb)=

### Read from MongoDB

To read data from MongoDB, call {func}`~ray.data.read_mongo` and specify the source URI, database, and collection. You can also pass an [aggregation pipeline](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/) to run against the collection. Without one, Ray Data reads the entire collection.

```{testcode}
:skipif: True

import ray

# Read a local MongoDB.
ds = ray.data.read_mongo(
    uri="mongodb://localhost:27017",
    database="my_db",
    collection="my_collection",
    pipeline=[{"$match": {"col": {"$gte": 0, "$lt": 10}}}, {"$sort": "sort_col"}],
)

# Reading a remote MongoDB is the same.
ds = ray.data.read_mongo(
    uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
    database="my_db",
    collection="my_collection",
    pipeline=[{"$match": {"col": {"$gte": 0, "$lt": 10}}}, {"$sort": "sort_col"}],
)

# Write back to MongoDB.
ds.write_mongo(
    MongoDatasource(),
    uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
    database="my_db",
    collection="my_collection",
)
```

(reading-from-kafka)=

## Read from Kafka

Ray Data reads from message queues such as Kafka.

(reading_kafka)=

To read data from Kafka topics, call {func}`~ray.data.read_kafka` and specify the topic names and broker addresses. Ray Data performs bounded reads between a start and end offset. You can specify offsets as integers, as `"earliest"` or `"latest"` strings, or as `datetime` objects for time-based ranges.

First, install the required dependencies:

```console
pip install confluent-kafka
```

Then, specify your Kafka configuration and read from topics.

```{testcode}
:skipif: True

import ray

# Read from a single topic with offset range
ds = ray.data.read_kafka(
    topics="my-topic",
    bootstrap_servers="localhost:9092",
    start_offset=0,
    end_offset=1000,
)

# Read from multiple topics
ds = ray.data.read_kafka(
    topics=["topic1", "topic2"],
    bootstrap_servers="localhost:9092",
    start_offset="earliest",
    end_offset="latest",
)

# Read messages within a datetime range (datetimes with no timezone info are treated as UTC)
from datetime import datetime
ds = ray.data.read_kafka(
    topics="my-topic",
    bootstrap_servers="localhost:9092",
    start_offset=datetime(2025, 1, 1),
    end_offset=datetime(2025, 1, 2),
)

# Read with authentication (Confluent/librdkafka options)
ds = ray.data.read_kafka(
    topics="secure-topic",
    bootstrap_servers="localhost:9092",
    consumer_config={
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "your-username",
        "sasl.password": "your-password",
    },
)

print(ds.schema())
```

```{testoutput}
Column          Type
------          ----
offset          int64
key             binary
value           binary
topic           string
partition       int32
timestamp       int64
timestamp_type  int32
headers         map<string, binary>
```

(creating-synthetic-data)=

## Create synthetic data

Synthetic datasets are useful for testing and benchmarking.

::::{tab-set}

:::{tab-item} Integer range

To create a synthetic {class}`~ray.data.Dataset` from a range of integers, call {func}`~ray.data.range`. Ray Data stores the integer range in a single column called `id`.

```{testcode}
import ray

ds = ray.data.range(10000)

print(ds.schema())
```

```{testoutput}
Column  Type
------  ----
id      int64
```

:::

:::{tab-item} Tensor range

To create a synthetic {class}`~ray.data.Dataset` containing arrays, call {func}`~ray.data.range_tensor`. Ray Data packs an integer range into ndarrays of the provided shape. In the schema, the column name defaults to `data`.

```{testcode}
import ray

ds = ray.data.range_tensor(10, shape=(64, 64))

print(ds.schema())
```

```{testoutput}
Column  Type
------  ----
data    ArrowTensorTypeV2(shape=(64, 64), dtype=int64)
```

:::

::::

(loading-other-datasources)=

## Load other datasources

If Ray Data can't load your data, subclass {class}`~ray.data.Datasource`. Then, construct an instance of your custom datasource and pass it to {func}`~ray.data.read_datasource`. To write results, you might also need to subclass {class}`ray.data.Datasink`. Then, create an instance of your custom datasink and pass it to {func}`~ray.data.Dataset.write_datasink`. For more details, see {ref}`Advanced: Read and write custom file types <custom_datasource>`.

```{testcode}
:skipif: True

# Read from a custom datasource.
ds = ray.data.read_datasource(YourCustomDatasource(), **read_args)

# Write to a custom datasink.
ds.write_datasink(YourCustomDatasink())
```

## Community-maintained connectors

The community maintains the following connectors, which integrate Ray Data with additional data systems:

* [Apache Doris Ray Connector](https://github.com/jiangxt2/ray-doris): Reads and writes data between Ray Data and [Apache Doris](https://doris.apache.org/).
* [Kinetica Ray Connector](https://github.com/kineticadb/kinetica-ray): Reads and writes data between Ray Data and [Kinetica](https://www.kinetica.com/).

## Performance considerations

By default, Ray Data decides the number of output blocks from all read tasks dynamically, based on input data size and available resources. This default should work well in most cases. To override it, set the `override_num_blocks` argument. Ray Data decides internally how many read tasks to run concurrently to make the best use of the cluster, from 1 to `override_num_blocks` tasks. The higher the `override_num_blocks` value, the smaller the data blocks in the dataset, and the more opportunities for parallel execution.

To tune the number of output blocks and find other ways to optimize read performance, see {ref}`Optimize reads <optimizing-reads>`.
