# flake8: noqa

# fmt: off
# __creating_datastreams_import_begin__
import ray
# __creating_datastreams_import_end__
# fmt: on

# For tfrecords
ray.init(runtime_env={"pip": ["tensorflow_metadata"]})

# fmt: off
# __gen_synth_int_range_begin__
# Create a Datastream of Python objects.
ds = ray.data.range(10000)
# -> Datastream(num_blocks=200, num_rows=10000, schema=<class 'int'>)

ds.take(5)
# -> [0, 1, 2, 3, 4]
# __gen_synth_int_range_end__
# fmt: on

# fmt: off
# __gen_synth_tabular_range_begin__
# Create a Datastream of Arrow records.
ds = ray.data.range(10000)
# -> Datastream(num_blocks=200, num_rows=10000, schema={id: int64})

ds.take(5)
# -> [{'value': 0}, {'value': 1}, {'value': 2}, {'value': 3}, {'value': 4}]
# __gen_synth_tabular_range_end__
# fmt: on

# fmt: off
# __gen_synth_tensor_range_begin__
# Create a Datastream of tensors.
ds = ray.data.range_tensor(100 * 64 * 64, shape=(64, 64))
# -> Datastream(
#       num_blocks=200,
#       num_rows=409600,
#       schema={__value__: numpy.ndarray(shape=(64, 64), dtype=int64)}
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
# Create a Datastream of tabular (Arrow) records.
ds = ray.data.from_items([{"col1": i, "col2": str(i)} for i in range(10000)])
# -> MaterializedDatastream(num_blocks=200, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_items_end__
# fmt: on

# fmt: off
# __from_pandas_begin__
import pandas as pd

# Create a tabular Datastream from a Pandas DataFrame.
df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
ds = ray.data.from_pandas(df)
# -> MaterializedDatastream(num_blocks=1, num_rows=10000, schema={col1: int64, col2: object})

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
# Create a tabular Datastream from multiple Pandas DataFrames.
ds = ray.data.from_pandas(dfs)
# -> MaterializedDatastream(num_blocks=10, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_pandas_mult_end__
# fmt: on

# fmt: off
# __from_numpy_begin__
import numpy as np

# Create a tensor Datastream from a 3D NumPy ndarray.
arr = np.ones((3, 4, 4))
# The outer dimension is treated as the row dimension.
ds = ray.data.from_numpy(arr)
# -> MaterializedDatastream(
#        num_blocks=1,
#        num_rows=3,
#        schema={__value__: numpy.ndarray(shape=(4, 4), dtype=double)}
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
# -> Datastream(num_blocks=3, num_rows=3, 
#            schema={image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8)})

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

# Create a tensor Datastream from multiple 3D NumPy ndarray.
arrs = [np.random.rand(2, 4, 4) for _ in range(4)]
# The outer dimension is treated as the row dimension.
ds = ray.data.from_numpy(arrs)
# -> MaterializedDatastream(
#        num_blocks=4,
#        num_rows=8,
#        schema={__value__: numpy.ndarray(shape=(4, 4), dtype=double)}
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

# Create a tabular Datastream from an Arrow Table.
t = pa.table({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
ds = ray.data.from_arrow(t)
# -> MaterializedDatastream(num_blocks=1, num_rows=10000, schema={col1: int64, col2: string})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_arrow_end__
# fmt: on

# fmt: off
# __from_arrow_mult_begin__
import pyarrow as pa

data = list(range(10000))
num_chunks = 10
chunk_size = len(data) // num_chunks
chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
ts = [
    pa.table({"col1": list(chunk), "col2": list(map(str, chunk))})
    for chunk in chunks
]
# Create a tabular Datastream from multiple Arrow Tables.
ds = ray.data.from_arrow(ts)
# -> MaterializedDatastream(num_blocks=10, num_rows=10000, schema={col1: int64, col2: string})

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
# Create a tabular Datastream from a Dask DataFrame.
ds = ray.data.from_dask(ddf)
# -> MaterializedDatastream(num_blocks=10, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_dask_end__
# fmt: on

# fmt: off
# __from_modin_begin__
import modin.pandas as md

df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
mdf = md.DataFrame(df)
# Create a tabular Datastream from a Modin DataFrame.
ds = ray.data.from_modin(mdf)
# -> MaterializedDatastream(num_blocks=8, num_rows=10000, schema={col1: int64, col2: object})

ds.show(3)
# -> {'col1': 0, 'col2': '0'}
# -> {'col1': 1, 'col2': '1'}
# -> {'col1': 2, 'col2': '2'}
# __from_modin_end__
# fmt: on

# fmt: off
# __read_parquet_begin__
# Create a tabular Datastream by reading a Parquet file.
ds = ray.data.read_parquet("example://iris.parquet")
# -> Datastream(
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

# Create a tabular Datastream by reading a Parquet file, pushing column selection and row
# filtering down to the file scan.
ds = ray.data.read_parquet(
    "example://iris.parquet",
    columns=["sepal.length", "variety"],
    filter=pa.dataset.field("sepal.length") > 5.0,
).materialize()  # Force a full read of the file.
# -> Datastream(num_blocks=1, num_rows=118, schema={sepal.length: double, variety: string})

ds.show(2)
# -> {'sepal.length': 5.1, 'variety': 'Setosa'}
#    {'sepal.length': 5.4, 'variety': 'Setosa'}
# __read_parquet_pushdown_end__
# fmt: on

# fmt: off
# __read_csv_begin__
# Create a tabular Datastream by reading a CSV file.
ds = ray.data.read_csv("example://iris.csv")
# -> Datastream(
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
# Create a tabular Datastream by reading a JSON file.
ds = ray.data.read_json("example://iris.json")
# -> Datastream(
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
# Create a tensor Datastream by reading a NumPy file.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Datastream(
#       num_blocks=1,
#       num_rows=3,
#       schema={__value__: numpy.ndarray(shape=(28, 28), dtype=uint8)}
#   )

ds.show(2)
# [array([[0, ...]]), array([[0, ...]])]
# __read_numpy_end__
# fmt: on

# fmt: off
# __read_text_begin__
# Create a tabular Datastream by reading a text file.
ds = ray.data.read_text("example://sms_spam_collection_subset.txt")
# -> Datastream(num_blocks=1, num_rows=10, schema=<class 'str'>)

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
import PIL.Image

# Create a tabular Datastream by reading a binary file.
ds = ray.data.read_binary_files("example://mnist_subset_partitioned/0/1.png")
# -> Datastream(num_blocks=1, num_rows=1, schema=<class 'bytes'>)

ds = ds.map(lambda bytes_: {"images": np.asarray(PIL.Image.open(BytesIO(bytes_["bytes"])).convert("L"))})
# -> Datastream(
#        num_blocks=1,
#        num_rows=1,
#        schema={__value__: numpy.ndarray(shape=(28, 28), dtype=uint8)}
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
# Create a tabular Datastream by reading a Parquet file from S3.
ds = ray.data.read_parquet("s3://anonymous@air-example-data/ursa-labs-taxi-data/by_year/2019/01/data.parquet")
# -> Datastream(
#        num_blocks=1,
#        num_rows=7667792,
#        schema={
#            vendor_id: string,
#            pickup_at: timestamp[us],
#            dropoff_at: timestamp[us],
#            passenger_count: int8,
#            trip_distance: float,
#            rate_code_id: string,
#            store_and_fwd_flag: string,
#            ...,
#        },
#    )

ds.show(2)
# -> {
#        'vendor_id': '1',
#        'pickup_at': datetime.datetime(2019, 1, 1, 0, 46, 40),
#        'dropoff_at': datetime.datetime(2019, 1, 1, 0, 53, 20),
#        'passenger_count': 1,
#        'trip_distance': 1.5,
#        'rate_code_id': '1',
#        'store_and_fwd_flag': 'N', 
#        ...,
#    }
#    {
#        'vendor_id': '1',
#        'pickup_at': datetime.datetime(2019, 1, 1, 0, 59, 47)
#        'dropoff_at': datetime.datetime(2019, 1, 1, 1, 18, 59),
#        'passenger_count': 1,
#        'trip_distance': 2.5999999046325684,
#        'rate_code_id': '1',
#        'store_and_fwd_flag': 'N', 
#        ...,
#    }
# __read_parquet_s3_end__
# fmt: on

# fmt: off
# __read_compressed_begin__
# Read a gzip-compressed CSV file from S3.
ds = ray.data.read_csv(
    "s3://anonymous@air-example-data/gzip_compressed.csv",
    arrow_open_stream_args={"compression": "gzip"},
)
# __read_compressed_end__
# fmt: on

# fmt: off
# __read_tfrecords_begin__
# Create a tabular Datastream by reading a TFRecord file.
ds = ray.data.read_tfrecords("example://iris.tfrecords")
# Datastream(
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
# fmt: on
