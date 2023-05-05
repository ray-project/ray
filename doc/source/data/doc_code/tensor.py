# flake8: noqa

import ray
from typing import Dict, Any

# fmt: off
# __create_range_begin__
# Create a Datastream of tensors.
ds = ray.data.range_tensor(10000, shape=(64, 64))
# -> Datastream(num_blocks=200, num_rows=10000,
#               schema={data: numpy.ndarray(shape=(64, 64), dtype=int64)})

ds.take(1)
# -> {'data': array([[0, 0, 0, ..., 0, 0, 0],
#                    [0, 0, 0, ..., 0, 0, 0],
#                    [0, 0, 0, ..., 0, 0, 0],
#                    ...,
#                    [0, 0, 0, ..., 0, 0, 0],
#                    [0, 0, 0, ..., 0, 0, 0],
#                    [0, 0, 0, ..., 0, 0, 0]])}
# __create_range_end__

# __create_pandas_2_begin__
import pandas as pd
import numpy as np

# Create multiple TensorArray columns.
def gen_image_and_embed(batch: pd.DataFrame) -> pd.DataFrame:
    bs = len(batch)

    # Lists of ndarrays are automatically cast to TensorArray.
    image = [np.zeros((128, 128, 3), dtype=np.int64) for _ in range(bs)]
    embed = [np.zeros((256,), dtype=np.uint8) for _ in range(bs)]
    return pd.DataFrame({"image": image, "embed": embed})

    ## Alternatively, manually construct TensorArrays from ndarray batches.
    # image = TensorArray(np.zeros((bs, 128, 128, 3), dtype=np.int64))
    # embed = TensorArray(np.zeros((bs, 256,), dtype=np.uint8))
    # return pd.DataFrame({"image": image, "embed": embed})

ds.map_batches(gen_image_and_embed, batch_format="pandas")
ds.materialize()
# -> Datastream(num_blocks=17, num_rows=1000,
#               schema={image: numpy.ndarray(shape=(128, 128, 3), dtype=int64),
#                       embed: numpy.ndarray(shape=(256,), dtype=uint8)})
# __create_pandas_2_end__

# __create_numpy_begin__
# From in-memory numpy data.
ray.data.from_numpy(np.zeros((1000, 128, 128, 3), dtype=np.int64))
# -> Datastream(num_blocks=1, num_rows=1000,
#               schema={data: numpy.ndarray(shape=(128, 128, 3), dtype=int64)})

# From saved numpy files.
ray.data.read_numpy("example://mnist_subset.npy")
# -> Datastream(num_blocks=1, num_rows=3,
#               schema={data: numpy.ndarray(shape=(28, 28), dtype=uint8)})
# __create_numpy_end__

# __create_parquet_1_begin__
# Reading previously saved Tensor data works out of the box.
ds = ray.data.read_parquet("example://parquet_images_mini")
# -> Datastream(num_blocks=3, num_rows=3,
#               schema={image: numpy.ndarray(shape=(128, 128, 3), dtype=uint8),
#                       label: string})

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

# Write the datastream to Parquet. The tensor column will be written as an
# array of opaque byte blobs.
ds = ray.data.from_pandas([df])
ds.write_parquet(path)

# Read the Parquet files into a new Datastream, with the serialized tensors
# automatically cast to our tensor column extension type.
ds = ray.data.read_parquet(
    path, tensor_column_schema={"two": (np.int_, (2, 2, 2))})

# The new column is represented with as a Tensor extension type.
print(ds.schema())
# -> one: int64
#    two: extension<arrow.py_extension_type<ArrowTensorType>>
# __create_parquet_2_end__

ds.materialize()
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

# Write the datastream to Parquet. The tensor column will be written as an
# array of opaque byte blobs.
ds = ray.data.from_pandas([df])
ds.write_parquet(path)

# Manually deserialize the tensor pickle bytes and cast to our tensor
# extension type.
def cast_udf(block: pa.Table) -> pa.Table:
    block = block.to_pandas()
    block["two"] = TensorArray([pickle.loads(a) for a in block["two"]])
    return pa.Table.from_pandas(block)

# Read the Parquet files into a new Datastream, applying the casting UDF
# on-the-fly within the underlying read tasks.
ds = ray.data.read_parquet(path, _block_udf=cast_udf)

# The new column is represented with as a Tensor extension type.
print(ds.schema())
# -> one: int64
#    two: extension<arrow.py_extension_type<ArrowTensorType>>
# __create_parquet_3_end__
ds.materialize()

# __create_images_begin__
ds = ray.data.read_images("example://image-datasets/simple")
# -> Datastream(num_blocks=3, num_rows=3, 
#               schema={data: numpy.ndarray(shape=(32, 32, 3), dtype=uint8)})

ds.take(1)
# -> [array([[[ 88,  70,  68],
#            [103,  88,  85],
#            [112,  96,  97],
#            ...,
#            [168, 151,  81],
#            [167, 149,  83],
#            [166, 148,  82]]], dtype=uint8)]
# __create_images_end__

# __consume_pandas_2_begin__
ds = ray.data.read_parquet("example://parquet_images_mini")
# -> Datastream(num_blocks=3, num_rows=3,
#               schema={image: numpy.ndarray(shape=(128, 128, 3), dtype=uint8),
#                       label: string})

def add_one(batch: pd.DataFrame) -> pd.DataFrame:
    batch["image"] += 1
    return batch

# This processes batches in pd.DataFrame format.
ds = ds.map_batches(add_one, batch_format="pandas")

# This returns pandas batches with List[np.ndarray] columns.
next(ds.iter_batches(batch_format="pandas"))
# ->                                             image label
# 0  [[[ 96,  76,  61], [ 92,  72,  57], [ 92,  72,...   cat
# 1  [[[ 38,  38,  39], [ 39,  39,  40], [ 39,  39,...   cat
# 2  [[[ 47,  39,  33], [ 43,  36,  29], [ 43,  36,...   dog
# __consume_pandas_2_end__

# __consume_pyarrow_2_begin__
from ray.data.extensions.tensor_extension import ArrowTensorArray

ds = ray.data.read_parquet("example://parquet_images_mini")
# -> Datastream(num_blocks=3, num_rows=3,
#               schema={image: numpy.ndarray(shape=(128, 128, 3), dtype=uint8),
#                       label: object})

def add_one(batch: pa.Table) -> pa.Table:

    def to_numpy(buf):
        if not isinstance(buf, np.ndarray):
            buf = buf.as_py()
        return buf

    np_col = np.array(
        [
            to_numpy(buf) for buf in batch.column("image")
        ]
    )
    np_col += 1

    return batch.set_column(
        batch._ensure_integer_index("image"),
        "image",
        ArrowTensorArray.from_numpy(np_col),
    )

# This processes batches in pyarrow.Table format.
ds = ds.map_batches(add_one, batch_format="pyarrow")

# This returns batches in pyarrow.Table format.
next(ds.iter_batches(batch_format="pyarrow"))
# pyarrow.Table
# image: extension<arrow.py_extension_type<ArrowTensorType>>
# label: string
# ----
# image: [[[92,71,57,107,87,72,113,97,85,122,...,85,170,152,88,167,150,89,165,146,90]]]
# label: [["cat"]]
# __consume_pyarrow_2_end__

# __consume_numpy_2_begin__
ds = ray.data.read_parquet("example://parquet_images_mini")
# -> Datastream(num_blocks=3, num_rows=3,
#               schema={image: numpy.ndarray(shape=(128, 128, 3), dtype=uint8),
#                       label: object})

def add_one(batch: Dict[str, Any]) -> Dict[str, Any]:
    assert isinstance(batch, dict)
    batch["image"] += 1
    return batch

# This processes batches in np.ndarray format.
ds = ds.map_batches(add_one, batch_format="numpy")

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


ds.materialize()
shutil.rmtree("/tmp/some_path")

# __write_1_begin__
# Read a multi-column example datastream.
ds = ray.data.read_parquet("example://parquet_images_mini")
# -> Datastream(num_blocks=3, num_rows=3,
#               schema={image: numpy.ndarray(shape=(128, 128, 3), dtype=uint8),
#                       label: object})

# You can write the datastream to Parquet.
ds.write_parquet("/tmp/some_path")

# And you can read it back.
read_ds = ray.data.read_parquet("/tmp/some_path")
print(read_ds.schema())
# -> image: extension<arrow.py_extension_type<ArrowTensorType>>
#    label: string
# __write_1_end__

read_ds.materialize()
shutil.rmtree("/tmp/some_path")

# __write_2_begin__
# Read a single-column example datastream.
ds = ray.data.read_numpy("example://mnist_subset.npy")
# -> Datastream(num_blocks=1, num_rows=3,
#               schema={data: numpy.ndarray(shape=(28, 28), dtype=uint8)})

# You can write the datastream to Parquet.
ds.write_numpy("/tmp/some_path", column="data")

# And you can read it back.
read_ds = ray.data.read_numpy("/tmp/some_path")
print(read_ds.schema())
# -> data: extension<arrow.py_extension_type<ArrowTensorType>>
# __write_2_end__

# fmt: off
# __create_variable_shaped_tensors_begin___
# Create a Datastream of variable-shaped tensors.
ragged_array = np.array([np.ones((2, 2)), np.ones((3, 3))], dtype=object)
df = pd.DataFrame({"feature": ragged_array, "label": [1, 1]})
ds = ray.data.from_pandas([df, df])
# -> Datastream(num_blocks=2, num_rows=4,
#               schema={feature: numpy.ndarray(shape=(None, None), dtype=float64), 
#                       label: int64})

ds.take(2)
# -> [{'feature': array([[1., 1.],
#                       [1., 1.]]), 
#       'label': 1}, 
#     {'feature': array([[1., 1., 1.],
#                        [1., 1., 1.],
#                        [1., 1., 1.]]), 
#       'label': 1}]
# __create_variable_shaped_tensors_end__

# fmt: off
# __tf_variable_shaped_tensors_begin___
# Convert Datastream to a TensorFlow Dataset.
tf_ds = ds.to_tf(
    batch_size=2,
    feature_columns="feature",
    label_columns="label"
)
# Iterate through the tf.RaggedTensors.
for ragged_tensor in tf_ds:
    print(ragged_tensor)
# -> (<tf.RaggedTensor [[[1.0, 1.0], [1.0, 1.0]],
#                      [[1.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 1.0, 1.0]]]>,
#     <tf.Tensor: shape=(2,), dtype=int64, numpy=array([1, 1])>)
# (<tf.RaggedTensor [[[1.0, 1.0], [1.0, 1.0]],
#                   [[1.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 1.0, 1.0]]]>,
#  <tf.Tensor: shape=(2,), dtype=int64, numpy=array([1, 1])>)
# __tf_variable_shaped_tensors_end__
