# flake8: noqa

# fmt: off
# __create_range_begin__
import ray

# Create a Dataset of tensors.
ds = ray.data.range_tensor(10000, shape=(64, 64))
# -> Dataset(
#       num_blocks=200,
#       num_rows=10000,
#       schema={__value__: <ArrowTensorType: shape=(64, 64), dtype=int64>}
#    )

ds.take(2)
# -> [array([[0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            ...,
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0]]),
#     array([[1, 1, 1, ..., 1, 1, 1],
#            [1, 1, 1, ..., 1, 1, 1],
#            [1, 1, 1, ..., 1, 1, 1],
#            ...,
#            [1, 1, 1, ..., 1, 1, 1],
#            [1, 1, 1, ..., 1, 1, 1],
#            [1, 1, 1, ..., 1, 1, 1]])]
# __create_range_end__

# __create_pandas_begin__
import ray

import pandas as pd
import numpy as np

# Start with a tabular base dataset.
ds = ray.data.range_table(1000)

# Create a single TensorArray column.
def single_col_udf(batch: pd.DataFrame) -> pd.DataFrame:
    bs = len(batch)

    # Lists of ndarrays are automatically cast to TensorArray.
    arr = [np.zeros((128, 128, 3)) for _ in range(bs)]

    ## Alternatively, manually construct a TensorArray from a single ndarray.
    # from ray.data.extensions.tensor_extension import TensorArray
    # arr = TensorArray(np.zeros((bs, 128, 128, 3), dtype=np.int64))

    return pd.DataFrame({"__value__": arr})

ds.map_batches(single_col_udf)
# -> Dataset(num_blocks=17, num_rows=1000, schema={__value__: TensorDtype(shape=(128, 128, 3), dtype=int64)})
# __create_pandas_end__

# __create_pandas_2_begin__
# Create multiple TensorArray columns.
def multi_col_udf(batch: pd.DataFrame) -> pd.DataFrame:
    bs = len(batch)

    # Lists of ndarrays are automatically cast to TensorArray.
    images = [np.zeros((128, 128, 3), dtype=np.int64) for _ in range(bs)]
    embeds = [np.zeros((256,), dtype=np.uint8) for _ in range(bs)]

    ## Alternatively, manually construct a TensorArray from a single ndarray.
    # images = TensorArray(np.zeros((bs, 128, 128, 3), dtype=np.int64))
    # embeds = TensorArray(np.zeros((bs, 256,), dtype=np.uint8))

    return pd.DataFrame({"images": images, "embeds": embeds})

ds.map_batches(multi_col_udf)
# -> Dataset(num_blocks=17, num_rows=1000, schema={image: TensorDtype(shape=(128, 128, 3), dtype=int64), embed: TensorDtype(shape=(256,), dtype=uint8)})
# __create_pandas_2_end__

# __create_numpy_begin__
import ray

# From in-memory numpy data.
ray.data.from_numpy(np.zeros((1000, 128, 128, 3), dtype=np.int64))
# -> Dataset(num_blocks=1, num_rows=1000,
#            schema={__value__: <ArrowTensorType: shape=(128, 128, 3), dtype=int64>})

# From saved numpy files.
ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(num_blocks=1, num_rows=3,
#            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})
# __create_numpy_end__

# __create_parquet_1_begin__
import ray

# Reading previously saved Tensor data works out of the box.
ray.data.read_parquet("example://parquet_images_mini")
# -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

ds.take(1)
# -> [{'image':
#         array([[[ 92,  71,  57],
#                 [107,  87,  72],
#                 ...,
#                 [141, 161, 185],
#                 [139, 158, 184]],
#                
#                ...,
#                
#                [[135, 135, 109],
#                 [135, 135, 108],
#                 ...,
#                 [167, 150,  89],
#                 [165, 146,  90]]], dtype=uint8),
#      'label': 'cat',
#     }]
# __create_parquet_1_end__

import shutil
shutil.rmtree("/tmp/some_path", ignore_errors=True)

# __create_parquet_2_begin__
import ray
import numpy as np
import pandas as pd

path = "/tmp/some_path"

# Create a DataFrame with a list of serialized ndarrays as a column.
# Note that we do not cast it to a tensor array, so each element in the
# column is an opaque blob of bytes.
arr = np.arange(24).reshape((3, 2, 2, 2))
df = pd.DataFrame({
    "one": [1, 2, 3],
    "two": [tensor.tobytes() for tensor in arr]})

# Write the dataset to Parquet. The tensor column will be written as an
# array of opaque byte blobs.
ds = ray.data.from_pandas([df])
ds.write_parquet(path)

# Read the Parquet files into a new Dataset, with the serialized tensors
# automatically cast to our tensor column extension type.
ds = ray.data.read_parquet(
    path, tensor_column_schema={"two": (np.int, (2, 2, 2))})

# The new column is represented with as a Tensor extension type.
print(ds.schema())
# -> one: int64
#    two: extension<arrow.py_extension_type<ArrowTensorType>>
# __create_parquet_2_end__

shutil.rmtree(path)

# __create_parquet_3_begin__
import pickle
import pyarrow as pa
from ray.data.extensions import TensorArray

path = "/tmp/some_path"

# Create a DataFrame with a list of pickled ndarrays as a column.
arr = np.arange(24).reshape((3, 2, 2, 2))
df = pd.DataFrame({
    "one": [1, 2, 3],
    "two": [pickle.dumps(tensor) for tensor in arr]})

# Write the dataset to Parquet. The tensor column will be written as an
# array of opaque byte blobs.
ds = ray.data.from_pandas([df])
ds.write_parquet(path)

# Manually deserialize the tensor pickle bytes and cast to our tensor
# extension type. For the sake of efficiency, we directly construct a
# TensorArray rather than .astype() casting on the mutated column with
# TensorDtype.
def cast_udf(block: pa.Table) -> pa.Table:
    block = block.to_pandas()
    block["two"] = TensorArray([pickle.loads(a) for a in block["two"]])
    return pa.Table.from_pandas(block)

# Read the Parquet files into a new Dataset, applying the casting UDF
# on-the-fly within the underlying read tasks.
ds = ray.data.read_parquet(path, _block_udf=cast_udf)

# The new column is represented with as a Tensor extension type.
print(ds.schema())
# -> one: int64
#    two: extension<arrow.py_extension_type<ArrowTensorType>>
# __create_parquet_3_end__

# __create_images_begin__
from ray.data.datasource import ImageFolderDatasource

ray.data.read_datasource(ImageFolderDatasource(), paths=["example://image-folder"])
# -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

ds.take(1)
# -> [{'image':
#         array([[[ 92,  71,  57],
#                 [107,  87,  72],
#                 ...,
#                 [141, 161, 185],
#                 [139, 158, 184]],
#
#                ...,
#
#                [[135, 135, 109],
#                 [135, 135, 108],
#                 ...,
#                 [167, 150,  89],
#                 [165, 146,  90]]], dtype=uint8),
#      'label': 'cat',
#     }]
# __create_images_end__


# __consume_native_begin__
import ray

# Read a single-column example dataset.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(num_blocks=1, num_rows=3,
#            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

# This returns batches in numpy.ndarray format.
next(ds.iter_batches())
# -> array([[[0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            ...,
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0]],
#
#           ...,
#
#           [[0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            ...,
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0]]], dtype=uint8)
# __consume_native_end__

# __consume_pandas_begin__
import ray

# Read a single-column example dataset.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(num_blocks=1, num_rows=3,
#            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

# This returns batches in pandas.DataFrame format.
next(ds.iter_batches(batch_format="pandas"))
# ->                                            __value__
# 0  [[  0,   0,   0,   0,   0,   0,   0,   0,   0,...
# 1  [[  0,   0,   0,   0,   0,   0,   0,   0,   0,...
# 2  [[  0,   0,   0,   0,   0,   0,   0,   0,   0,...
# __consume_pandas_end__

# __consume_pyarrow_begin__
import ray

# Read a single-column example dataset.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(num_blocks=1, num_rows=3,
#            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

# This returns batches in pyarrow.Table format.
next(ds.iter_batches(batch_format="pyarrow"))
# pyarrow.Table
# __value__: extension<arrow.py_extension_type<ArrowTensorType>>
# ----
# __value__: [[[0,0,0,0,0,0,0,0,0,0,...],...,[0,0,0,0,0,0,0,0,0,0,...]]]
# __consume_pyarrow_end__

# __consume_pyarrow_2_begin__
# Read a multi-column example dataset.
ray.data.read_parquet("example://parquet_images_mini")
# -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

# This returns batches in pyarrow.Table format.
next(ds.iter_batches(batch_format="pyarrow"))
# pyarrow.Table
# image: extension<arrow.py_extension_type<ArrowTensorType>>
# label: string
# ----
# image: [[[92,71,57,107,87,72,113,97,85,122,...,85,170,152,88,167,150,89,165,146,90]]]
# label: [["cat"]]
# __consume_pyarrow_2_end__

# __consume_numpy_begin__
import ray

# Read a single-column example dataset.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(num_blocks=1, num_rows=3,
#            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

# This returns batches in np.ndarray format.
next(ds.iter_batches(batch_format="numpy"))
# -> array([[[0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            ...,
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0]],
#
#           ...,
#
#           [[0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0],
#            ...,
#            [0, 0, 0, ..., 0, 0, 0],
#            [0, 0, 0, ..., 0, 0, 0]]], dtype=uint8)
# __consume_numpy_end__

# __consume_numpy_2_begin__
# Read a multi-column example dataset.
ray.data.read_parquet("example://parquet_images_mini")
# -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

# This returns batches in Dict[str, np.ndarray] format.
next(ds.iter_batches(batch_format="numpy"))
# -> {'image': array([[[[ 92,  71,  57],
#                       [107,  87,  72],
#                       ...,
#                       [141, 161, 185],
#                       [139, 158, 184]],
#
#                      ...,
#
#                      [[135, 135, 109],
#                       [135, 135, 108],
#                       ...,
#                       [167, 150,  89],
#                       [165, 146,  90]]]], dtype=uint8),
#     'label': array(['cat'], dtype=object)}
# __consume_numpy_2_end__


# __pandas_begin__
import ray
from ray.data.extensions import TensorDtype

import pandas as pd
import numpy as np


# Create a DataFrame with a list of ndarrays as a column.
df = pd.DataFrame({
    "one": [1, 2, 3],
    "two": list(np.arange(24).reshape((3, 2, 2, 2)))})
# Note the opaque np.object dtype for this column.
print(df.dtypes)
# -> one     int64
#    two    object
#    dtype: object

# Cast column to our TensorDtype Pandas extension type.
df["two"] = df["two"].astype(TensorDtype())

# Note that the column dtype is now TensorDtype instead of
# np.object.
print(df.dtypes)
# -> one          int64
#    two    TensorDtype
#    dtype: object

# Pandas is now aware of this tensor column, and we can do the
# typical DataFrame operations on this column.
col = 2 * df["two"]
# The ndarrays underlying the tensor column will be manipulated,
# but the column itself will continue to be a Pandas type.
print(type(col))
# -> pandas.core.series.Series
print(col)
# -> 0   [[[ 2  4]
#          [ 6  8]]
#         [[10 12]
#           [14 16]]]
#    1   [[[18 20]
#          [22 24]]
#         [[26 28]
#          [30 32]]]
#    2   [[[34 36]
#          [38 40]]
#         [[42 44]
#          [46 48]]]
#    Name: two, dtype: TensorDtype

# Once you do an aggregation on that column that returns a single
# row's value, you get back our TensorArrayElement type.
tensor = col.mean()
print(type(tensor))
# -> ray.data.extensions.tensor_extension.TensorArrayElement
print(tensor)
# -> array([[[18., 20.],
#            [22., 24.]],
#           [[26., 28.],
#            [30., 32.]]])

# This is a light wrapper around a NumPy ndarray, and can easily
# be converted to an ndarray.
type(tensor.to_numpy())
# -> numpy.ndarray

# In addition to doing Pandas operations on the tensor column,
# you can now put the DataFrame directly into a Dataset.
ds = ray.data.from_pandas([df])
# Internally, this column is represented with the corresponding
# Arrow tensor extension type.
print(ds.schema())
# -> one: int64
#    two: extension<arrow.py_extension_type<ArrowTensorType>>
# __pandas_end__

shutil.rmtree("/tmp/some_path")

# __write_1_begin__
# Read a multi-column example dataset.
ds = ray.data.read_parquet("example://parquet_images_mini")
# -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

# You can write the dataset to Parquet.
ds.write_parquet("/tmp/some_path")

# And you can read it back.
read_ds = ray.data.read_parquet("/tmp/some_path")
print(read_ds.schema())
# -> image: extension<arrow.py_extension_type<ArrowTensorType>>
#    label: string
# __write_1_end__

shutil.rmtree("/tmp/some_path")

# __write_2_begin__
# Read a single-column example dataset.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Dataset(num_blocks=1, num_rows=3,
#            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

# You can write the dataset to Parquet.
ds.write_numpy("/tmp/some_path")

# And you can read it back.
read_ds = ray.data.read_numpy("/tmp/some_path")
print(read_ds.schema())
# -> __value__: extension<arrow.py_extension_type<ArrowTensorType>>
# __write_2_end__
