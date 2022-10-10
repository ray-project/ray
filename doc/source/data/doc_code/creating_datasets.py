# flake8: noqa

# fmt: off
# __creating_datasets_import_begin__
import ray
# __creating_datasets_import_end__
# fmt: on

# fmt: off
# __gen_synth_int_range_begin__
# Create a Dataset of Python objects.
ds = ray.data.range(10000)
# -> Dataset(num_blocks=200, num_rows=10000, schema=<class 'int'>)

ds.take(5)
# -> [0, 1, 2, 3, 4]
# __gen_synth_int_range_end__
# fmt: on

# fmt: off
# __gen_synth_tabular_range_begin__
# Create a Dataset of Arrow records.
ds = ray.data.range_table(10000)
# -> Dataset(num_blocks=200, num_rows=10000, schema={value: int64})

ds.take(5)
# -> [{'value': 0}, {'value': 1}, {'value': 2}, {'value': 3}, {'value': 4}]
# __gen_synth_tabular_range_end__
# fmt: on

# fmt: off
# __gen_synth_tensor_range_begin__
# Create a Dataset of tensors.
ds = ray.data.range_tensor(100 * 64 * 64, shape=(64, 64))
# -> Dataset(
#       num_blocks=200,
#       num_rows=409600,
#       schema={value: <ArrowTensorType: shape=(64, 64), dtype=int64>}
#    )

ds.take(2)
# -> [array([[0, 0, 0, ..., 0, 0, 0],
#         [0, 0, 0, ..., 0, 0, 0],
#         [0, 0, 0, ..., 0, 0, 0],
#         ...,
#         [0, 0, 0, ..., 0, 0, 0],
#         [0, 0, 0, ..., 0, 0, 0],
#         [0, 0, 0, ..., 0, 0, 0]]),
#  array([[1, 1, 1, ..., 1, 1, 1],
#         [1, 1, 1, ..., 1, 1, 1],
#         [1, 1, 1, ..., 1, 1, 1],
#         ...,
#         [1, 1, 1, ..., 1, 1, 1],
#         [1, 1, 1, ..., 1, 1, 1],
#         [1, 1, 1, ..., 1, 1, 1]])]
# __gen_synth_tensor_range_end__
# fmt: on

# fmt: off
# __from_items_begin__
# Create a Dataset of tabular (Arrow) records.
ds = ray.data.from_items([{"col1": i, "col2": str(i)} for i in range(10000)])
# -> Dataset(num_blocks=200, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_items_end__
# fmt: on

# fmt: off
# __from_pandas_begin__
import pandas as pd

# Create a tabular Dataset from a Pandas DataFrame.
df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
ds = ray.data.from_pandas(df)
# -> Dataset(num_blocks=1, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_pandas_end__
# fmt: on

# fmt: off
# __from_pandas_mult_begin__
import pandas as pd

data = list(range(10000))
num_chunks = 10
chunk_size = len(data) // num_chunks
chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
dfs = [
    pd.DataFrame({"col1": list(chunk), "col2": list(map(str, chunk))})
    for chunk in chunks
]
# Create a tabular Dataset from multiple Pandas DataFrames.
ds = ray.data.from_pandas(dfs)
# -> Dataset(num_blocks=10, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_pandas_mult_end__
# fmt: on

# fmt: off
# __from_numpy_begin__
import numpy as np

# Create a tensor Dataset from a 1D NumPy ndarray.
arr = np.arange(100)
ds = ray.data.from_numpy(arr)
# -> Dataset(
#        num_blocks=1,
#        num_rows=100,
#        schema={value: <ArrowTensorType: shape=(), dtype=int64>},
#    )

# Each element is a scalar ndarray.
ds.show(3)
# -> {'value': array(0)}
# -> {'value': array(1)}
# -> {'value': array(2)}

# Create a tensor Dataset from a 3D NumPy ndarray.
arr = np.ones((3, 4, 4))
# The outer dimension is treated as the row dimension.
ds = ray.data.from_numpy(arr)
# -> Dataset(
#        num_blocks=1,
#        num_rows=3,
#        schema={value: <ArrowTensorType: shape=(4, 4), dtype=int64>},
#    )

ds.show(2)
# -> {'value': array([[1., 1., 1., 1.],
#        [1., 1., 1., 1.],
#        [1., 1., 1., 1.],
#        [1., 1., 1., 1.]])}
# -> {'value': array([[1., 1., 1., 1.],
#        [1., 1., 1., 1.],
#        [1., 1., 1., 1.],
#        [1., 1., 1., 1.]])}
# __from_numpy_end__
# fmt: on

# fmt: off
# __read_images_begin__
ds = ray.data.read_images("example://image-datasets/simple")
# -> Dataset(num_blocks=3, num_rows=3, 
#            schema={__value__: ArrowTensorType(shape=(32, 32, 3), dtype=uint8)})

ds.take(1)
# -> [array([[[ 88,  70,  68],
#            [103,  88,  85],
#            [112,  96,  97],
#            ...,
#            [168, 151,  81],
#            [167, 149,  83],
#            [166, 148,  82]]], dtype=uint8)]
# __read_images_end__
# fmt: on

# fmt: off
# __from_numpy_mult_begin__
import numpy as np

# Create a tensor Dataset from multiple 3D NumPy ndarray.
arrs = [np.random.rand(2, 4, 4) for _ in range(4)]
# The outer dimension is treated as the row dimension.
ds = ray.data.from_numpy(arrs)
# -> Dataset(
#        num_blocks=4,
#        num_rows=8,
#        schema={value: <ArrowTensorType: shape=(4, 4), dtype=int64>},
#    )

ds.show(2)
# -> {'value': array([[0.06587483, 0.67808656, 0.76461924, 0.83428549],
#        [0.04932103, 0.25112165, 0.26476714, 0.24599738],
#        [0.67624391, 0.58689537, 0.12594709, 0.94663371],
#        [0.32435665, 0.97719096, 0.03234169, 0.71563231]])}
# -> {'value': array([[0.98570318, 0.65956399, 0.82168898, 0.09798336],
#        [0.22426704, 0.34209978, 0.02605247, 0.48200137],
#        [0.17312096, 0.38789983, 0.42663678, 0.92652456],
#        [0.80787394, 0.92437162, 0.11185822, 0.3319638 ]])}
# __from_numpy_mult_end__
# fmt: on

# fmt: off
# __from_arrow_begin__
import pyarrow as pa

# Create a tabular Dataset from an Arrow Table.
t = pa.table({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
ds = ray.data.from_arrow(t)
# -> Dataset(num_blocks=1, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_arrow_end__
# fmt: on

# fmt: off
# __from_arrow_mult_begin__
import arrow as pd

data = list(range(10000))
num_chunks = 10
chunk_size = len(data) // num_chunks
chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
ts = [
    pa.table({"col1": list(chunk), "col2": list(map(str, chunk))})
    for chunk in chunks
]
# Create a tabular Dataset from multiple Arrow Tables.
ds = ray.data.from_arrow(ts)
# -> Dataset(num_blocks=10, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_arrow_mult_end__
# fmt: on

# fmt: off
# __from_dask_begin__
import pandas as pd
import dask.dataframe as dd

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
ddf = dd.from_pandas(df, npartitions=4)
# Create a tabular Dataset from a Dask DataFrame.
ds = ray.data.from_dask(ddf)
# -> Dataset(num_blocks=10, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_dask_end__
# fmt: on

# fmt: off
# __from_spark_begin__
import raydp

spark = raydp.init_spark(app_name="Spark -> Datasets Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="500MB")
df = spark.createDataFrame([(i, str(i)) for i in range(10000)], ["col1", "col2"])
# Create a tabular Dataset from a Spark DataFrame.
ds = ray.data.from_dask(df)
# -> Dataset(num_blocks=10, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_spark_end__
# fmt: on

# fmt: off
# __from_modin_begin__
import modin.pandas as md

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
mdf = md.DataFrame(df)
# Create a tabular Dataset from a Modin DataFrame.
ds = ray.data.from_modin(mdf)
# -> Dataset(num_blocks=8, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_modin_end__
# fmt: on

# fmt: off
# __from_mars_begin__
import mars
import mars.dataframe as md

cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
mdf = md.DataFrame(df, num_partitions=8)
# Create a tabular Dataset from a Mars DataFrame.
ds = ray.data.from_mars(mdf)
# -> Dataset(num_blocks=8, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_mars_end__
# fmt: on

# fmt: off
# __read_parquet_begin__
# Create a tabular Dataset by reading a Parquet file.
ds = ray.data.read_parquet("example://iris.parquet")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        }
#    )

ds.show(2)
# -> {
#     'sepal.length': 5.1,
#     'sepal.width': 3.5,
#     'petal.length': 1.4,
#     'petal.width': 0.2,
#     'variety': 'Setosa',
# }
# -> {
#     'sepal.length': 4.9,
#     'sepal.width': 3.0,
#     'petal.length': 1.4,
#     'petal.width': 0.2,
#     'variety': 'Setosa',
# }
# __read_parquet_end__
# fmt: on

# fmt: off
# __read_parquet_pushdown_begin__
import pyarrow as pa

# Create a tabular Dataset by reading a Parquet file, pushing column selection and row
# filtering down to the file scan.
ds = ray.data.read_parquet(
    "example://iris.parquet",
    columns=["sepal.length", "variety"],
    filter=pa.dataset.field("sepal.length") > 5.0,
).fully_executed()  # Force a full read of the file.
# -> Dataset(num_blocks=1, num_rows=118, schema={sepal.length: double, variety: string})

ds.show(2)
# -> {'sepal.length': 5.1, 'variety': 'Setosa'}
#    {'sepal.length': 5.4, 'variety': 'Setosa'}
# __read_parquet_pushdown_end__
# fmt: on

# fmt: off
# __read_csv_begin__
# Create a tabular Dataset by reading a CSV file.
ds = ray.data.read_csv("example://iris.csv")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        }
#    )

ds.show(2)
# -> {
#     'sepal.length': 5.1,
#     'sepal.width': 3.5,
#     'petal.length': 1.4,
#     'petal.width': 0.2,
#     'variety': 'Setosa',
# }
# -> {
#     'sepal.length': 4.9,
#     'sepal.width': 3.0,
#     'petal.length': 1.4,
#     'petal.width': 0.2,
#     'variety': 'Setosa',
# }
# __read_csv_end__
# fmt: on

# fmt: off
# __read_json_begin__
# Create a tabular Dataset by reading a JSON file.
ds = ray.data.read_json("example://iris.json")
# -> Dataset(
#        num_blocks=1,
#        num_rows=150,
#        schema={
#            sepal.length: double,
#            sepal.width: double,
#            petal.length: double,
#            petal.width: double,
#            variety: string,
#        }
#    )

ds.show(2)
# -> {
#     'sepal.length': 5.1,
#     'sepal.width': 3.5,
#     'petal.length': 1.4,
#     'petal.width': 0.2,
#     'variety': 'Setosa',
# }
# -> {
#     'sepal.length': 4.9,
#     'sepal.width': 3.0,
#     'petal.length': 1.4,
#     'petal.width': 0.2,
#     'variety': 'Setosa',
# }
# __read_json_end__
# fmt: on

# fmt: off
# __read_numpy_begin__
# Create a tensor Dataset by reading a NumPy file.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(
#       num_blocks=1,
#       num_rows=3,
#       schema={__RAY_TC__: <ArrowTensorType: shape=(28, 28), dtype=uint8>},
#   )

ds.show(2)
# [array([[0, ...]]), array([[0, ...]])]
# __read_numpy_end__
# fmt: on

# fmt: off
# __read_text_begin__
# Create a tabular Dataset by reading a text file.
ds = ray.data.read_text("example://sms_spam_collection_subset.txt")
# -> Dataset(num_blocks=1, num_rows=10, schema=<class 'str'>)

ds.show(3)
# -> ham     Go until jurong point, crazy.. Available only in bugis n great world la e
#            buffet... Cine there got amore wat...
#    ham     Ok lar... Joking wif u oni...
#    spam    Free entry in 2 a wkly comp to win FA Cup final tkts 21st May 2005. Text FA
#            to 87121 to receive entry question(std txt rate)T&C's apply
#            08452810075over18's
# __read_text_end__
# fmt: on

# fmt: off
# __read_binary_begin__
from io import BytesIO
import PIL

# Create a tabular Dataset by reading a binary file.
ds = ray.data.read_binary_files("example://mnist_subset_partitioned/0/1.png")
# -> Dataset(num_blocks=1, num_rows=1, schema=<class 'bytes'>)

ds = ds.map(lambda bytes_: np.asarray(PIL.Image.open(BytesIO(bytes_)).convert("L")))
# -> Dataset(
#        num_blocks=1,
#        num_rows=1,
#        schema={__RAY_TC__: <ArrowTensorType: shape=(28, 28), dtype=uint8>},
#    )

ds.show(3)
# -> ham     Go until jurong point, crazy.. Available only in bugis n great world la e
#            buffet... Cine there got amore wat...
#    ham     Ok lar... Joking wif u oni...
#    spam    Free entry in 2 a wkly comp to win FA Cup final tkts 21st May 2005. Text FA
#            to 87121 to receive entry question(std txt rate)T&C's apply
#            08452810075over18's
# __read_binary_end__
# fmt: on

# fmt: off
# __read_parquet_s3_begin__
# Create a tabular Dataset by reading a Parquet file from S3.
ds = ray.data.read_parquet("s3://ursa-labs-taxi-data/2009/01/data.parquet")
# -> Dataset(
#        num_blocks=1,
#        num_rows=14092413,
#        schema={
#            vendor_id: string,
#            pickup_at: timestamp[us],
#            dropoff_at: timestamp[us],
#            passenger_count: int8,
#            trip_distance: float,
#            pickup_longitude: float,
#            pickup_latitude: float,
#            ...,
#        },
#    )

ds.show(2)
# -> {
#        'vendor_id': 'VTS',
#        'pickup_at': datetime.datetime(2009, 1, 4, 2, 52),
#        'dropoff_at': datetime.datetime(2009, 1, 4, 3, 2),
#        'passenger_count': 1,
#        'trip_distance': 2.630000114440918,
#        'pickup_longitude': -73.99195861816406,
#        'pickup_latitude': 40.72156524658203,
#        ...,
#    }
#    {
#        'vendor_id': 'VTS',
#        'pickup_at': datetime.datetime(2009, 1, 4, 3, 31),
#        'dropoff_at': datetime.datetime(2009, 1, 4, 3, 38),
#        'passenger_count': 3,
#        'trip_distance': 4.550000190734863,
#        'pickup_longitude': -73.98210144042969,
#        'pickup_latitude': 40.736289978027344,
#        ...,
#    }
# __read_parquet_s3_end__
# fmt: on

# fmt: off
# __read_parquet_s3_with_fs_begin__
import pyarrow as pa

# Create a tabular Dataset by reading a Parquet file from a private S3 bucket.
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
# Create a tabular Dataset by reading a Parquet file from HDFS using HDFS connection
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

# Create a tabular Dataset by reading a Parquet file from HDFS, manually specifying a
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

# Create a tabular Dataset by reading a Parquet file from GCS, passing the configured
# GCSFileSystem.
# NOTE: This example is not runnable as-is; you'll need to point it at your GCS bucket
# and configure your GCP project and credentials.
ds = ray.data.read_parquet(
    "gs://path/to/file.parquet",
    filesystem=gcsfs.GCSFileSystem(project="my-google-project"),
)
# __read_parquet_gcs_end__
# fmt: on

# fmt: off
# __read_parquet_az_begin__
import adlfs

# Create a tabular Dataset by reading a Parquet file from Azure Blob Storage, passing
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

# __read_tfrecords_begin__
# Create a tabular Dataset by reading a TFRecord file.
ds = ray.data.read_tfrecords("example://iris.tfrecords")
# Dataset(
#     num_blocks=1,
#     num_rows=150,
#     schema={
#         sepal.length: float64,
#         sepal.width: float64,
#         petal.length: float64,
#         petal.width: float64,
#         label: object,
#     },
# )
ds.show(1)
# {
#     "sepal.length": 5.099999904632568,
#     "sepal.width": 3.5,
#     "petal.length": 1.399999976158142,
#     "petal.width": 0.20000000298023224,
#     "label": b"Setosa",
# }
# __read_tfrecords_end__
