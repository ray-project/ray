# Not tested in CI currently!
# flake8: noqa

import ray

# fmt: off
# __from_spark_begin__
import raydp

spark = raydp.init_spark(app_name="Spark -> Datastreams Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="500MB")
df = spark.createDataFrame([(i, str(i)) for i in range(10000)], ["col1", "col2"])
# Create a tabular Datastream from a Spark DataFrame.
ds = ray.data.from_spark(df)
# -> MaterializedDatastream(num_blocks=10, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_spark_end__
# fmt: on

# fmt: off
# __read_parquet_s3_with_fs_begin__
import pyarrow as pa

# Create a tabular Datastream by reading a Parquet file from a private S3 bucket.
# NOTE: This example is not runnable as-is; add in a path to your private bucket and the
# required S3 credentials!
ds = ray.data.read_parquet(
    "s3://some/private/bucket",
    filesystem=pa.fs.S3FileSystem(
        region="us-west-2",
        access_key="XXXX",
        secret_key="XXXX",
    ),
)
# __read_parquet_s3_with_fs_end__
# fmt: on

# fmt: off
# __read_parquet_hdfs_begin__
# Create a tabular Datastream by reading a Parquet file from HDFS using HDFS connection
# automatically constructed based on the URI.
# NOTE: This example is not runnable as-is; you'll need to point it at your HDFS
# cluster/data.
ds = ray.data.read_parquet("hdfs://<host:port>/path/to/file.parquet")
# __read_parquet_hdfs_end__
# fmt: on

# TODO(Clark): Find clean way to start local HDFS cluster in the below example (that
# works in CI).

# fmt: off
# __read_parquet_hdfs_with_fs_begin__
import pyarrow as pa

# Create a tabular Datastream by reading a Parquet file from HDFS, manually specifying a
# configured HDFS connection via a Pyarrow HDFSFileSystem instance.
# NOTE: This example is not runnable as-is; you'll need to point it at your HDFS
# cluster/data.
ds = ray.data.read_parquet(
    "hdfs://path/to/file.parquet",
    filesystem=pa.fs.HDFSFileSystem(host="localhost", port=9000, user="bob"),
)
# __read_parquet_hdfs_with_fs_end__
# fmt: on

# TODO(Clark): Find open data for below GCS example.

# fmt: off
# __read_parquet_gcs_begin__
import gcsfs

# Create a tabular Datastream by reading a Parquet file from GCS, passing the configured
# GCSFileSystem.
# NOTE: This example is not runnable as-is; you need to point it at your GCS bucket
# and configure your GCP project and credentials.
path = "gs://path/to/file.parquet"
filesystem = gcsfs.GCSFileSystem(project="my-google-project")
ds = ray.data.read_parquet(path, filesystem=filesystem)
# __read_parquet_gcs_end__
# fmt: on


# fmt: off
# __validate_parquet_gcs_begin__
print(filesystem.ls(path))
# ['path/to/file.parquet']
print(filesystem.open(path))
# <File-like object GCSFileSystem, path/to/file.parquet>
# __validate_parquet_gcs_end__
# fmt: on

# fmt: off
# __read_parquet_az_begin__
import adlfs

# Create a tabular Datastream by reading a Parquet file from Azure Blob Storage, passing
# the configured AzureBlobFileSystem.
path = (
    "az://nyctlc/yellow/puYear=2009/puMonth=1/"
    "part-00019-tid-8898858832658823408-a1de80bd-eed3-4d11-b9d4-fa74bfbd47bc-426333-4"
    ".c000.snappy.parquet"
)
ds = ray.data.read_parquet(
    path,
    filesystem=adlfs.AzureBlobFileSystem(account_name="azureopendatastorage")
)
# __read_parquet_az_end__
# fmt: on

# fmt: off
# __from_mars_begin__
import mars
import mars.dataframe as md
import pandas as pd

cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
mdf = md.DataFrame(df, num_partitions=8)
# Create a tabular Datastream from a Mars DataFrame.
ds = ray.data.from_mars(mdf)
# -> MaterializedDatastream(num_blocks=8, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_mars_end__
# fmt: on
