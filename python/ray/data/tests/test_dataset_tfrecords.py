import json
import os

import numpy as np
from pandas.api.types import is_int64_dtype, is_float_dtype, is_object_dtype
import pytest

import ray

from ray.tests.conftest import *  # noqa


def test_read_tfrecords(ray_start_regular_shared, tmp_path):
    import tensorflow as tf

    features = tf.train.Features(
        feature={
            "int64": tf.train.Feature(int64_list=tf.train.Int64List(value=[1])),
            "int64_list": tf.train.Feature(
                int64_list=tf.train.Int64List(value=[1, 2, 3, 4])
            ),
            "int64_empty": tf.train.Feature(int64_list=tf.train.Int64List(value=[])),
            "float": tf.train.Feature(float_list=tf.train.FloatList(value=[1.0])),
            "float_list": tf.train.Feature(
                float_list=tf.train.FloatList(value=[1.0, 2.0, 3.0, 4.0])
            ),
            "float_empty": tf.train.Feature(float_list=tf.train.FloatList(value=[])),
            "bytes": tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"abc"])),
            "bytes_list": tf.train.Feature(
                bytes_list=tf.train.BytesList(value=[b"abc", b"1234"])
            ),
            "bytes_empty": tf.train.Feature(bytes_list=tf.train.BytesList(value=[])),
        }
    )
    example = tf.train.Example(features=features)
    path = os.path.join(tmp_path, "data.tfrecords")
    with tf.io.TFRecordWriter(path=path) as writer:
        writer.write(example.SerializeToString())

    ds = ray.data.read_tfrecords(path)

    df = ds.to_pandas()
    # Protobuf serializes features in a non-deterministic order.
    assert is_int64_dtype(dict(df.dtypes)["int64"])
    assert is_object_dtype(dict(df.dtypes)["int64_list"])
    assert is_object_dtype(dict(df.dtypes)["int64_empty"])

    assert is_float_dtype(dict(df.dtypes)["float"])
    assert is_object_dtype(dict(df.dtypes)["float_list"])
    assert is_object_dtype(dict(df.dtypes)["float_empty"])

    assert is_object_dtype(dict(df.dtypes)["bytes"])
    assert is_object_dtype(dict(df.dtypes)["bytes_list"])
    assert is_object_dtype(dict(df.dtypes)["bytes_empty"])

    assert list(df["int64"]) == [1]
    assert np.array_equal(df["int64_list"][0], np.array([1, 2, 3, 4]))
    assert np.array_equal(df["int64_empty"][0], np.array([], dtype=np.int64))

    assert list(df["float"]) == [1.0]
    assert np.array_equal(df["float_list"][0], np.array([1.0, 2.0, 3.0, 4.0]))
    assert np.array_equal(df["float_empty"][0], np.array([], dtype=np.float32))

    assert list(df["bytes"]) == [b"abc"]
    assert np.array_equal(df["bytes_list"][0], np.array([b"abc", b"1234"]))
    assert np.array_equal(df["bytes_empty"][0], np.array([], dtype=np.bytes_))


def test_write_tfrecords(ray_start_regular_shared, tmp_path):
    """Test that write_tfrecords writes TFRecords correctly.

    Test this by writing a Dataset to a TFRecord (function under test),
    reading it back out into a tf.train.Example,
    and checking that the result is analogous to the original Dataset.
    """

    import tensorflow as tf

    # The dataset we will write to a .tfrecords file.
    ds = ray.data.from_items(
        [
            # Row one.
            {
                "int_item": 1,
                "int_list": [2, 2, 3],
                "int_empty": [],
                "float_item": 1.0,
                "float_list": [2.0, 3.0, 4.0],
                "float_empty": 1.0,
                "bytes_item": b"abc",
                "bytes_list": [b"def", b"1234"],
                "bytes_empty": None,
            },
            # Row two.
            {
                "int_item": 2,
                "int_list": [3, 3, 4],
                "int_empty": [9, 2],
                "float_item": 2.0,
                "float_list": [5.0, 6.0, 7.0],
                "float_empty": None,
                "bytes_item": b"ghi",
                "bytes_list": [b"jkl", b"5678"],
                "bytes_empty": b"hello",
            },
        ],
        # Here, we specify `parallelism=1` to ensure that all rows end up in the same
        # block, which is required for type inference involving
        # partially missing columns.
        parallelism=1,
    )

    # The corresponding tf.train.Example that we would expect to read
    # from this dataset.

    expected_records = [
        # Record one (corresponding to row one).
        tf.train.Example(
            features=tf.train.Features(
                feature={
                    "int_item": tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[1])
                    ),
                    "int_list": tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[2, 2, 3])
                    ),
                    "int_empty": tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[])
                    ),
                    "float_item": tf.train.Feature(
                        float_list=tf.train.FloatList(value=[1.0])
                    ),
                    "float_list": tf.train.Feature(
                        float_list=tf.train.FloatList(value=[2.0, 3.0, 4.0])
                    ),
                    "float_empty": tf.train.Feature(
                        float_list=tf.train.FloatList(value=[1.0])
                    ),
                    "bytes_item": tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[b"abc"])
                    ),
                    "bytes_list": tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[b"def", b"1234"])
                    ),
                    "bytes_empty": tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[])
                    ),
                }
            )
        ),
        # Record two (corresponding to row two).
        tf.train.Example(
            features=tf.train.Features(
                feature={
                    "int_item": tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[2])
                    ),
                    "int_list": tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[3, 3, 4])
                    ),
                    "int_empty": tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[9, 2])
                    ),
                    "float_item": tf.train.Feature(
                        float_list=tf.train.FloatList(value=[2.0])
                    ),
                    "float_list": tf.train.Feature(
                        float_list=tf.train.FloatList(value=[5.0, 6.0, 7.0])
                    ),
                    "float_empty": tf.train.Feature(
                        float_list=tf.train.FloatList(value=[])
                    ),
                    "bytes_item": tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[b"ghi"])
                    ),
                    "bytes_list": tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[b"jkl", b"5678"])
                    ),
                    "bytes_empty": tf.train.Feature(
                        bytes_list=tf.train.BytesList(value=[b"hello"])
                    ),
                }
            )
        ),
    ]

    # Perform the test.
    # Write the dataset to a .tfrecords file.
    ds.write_tfrecords(tmp_path)

    # Read the Examples back out from the .tfrecords file.
    # This follows the offical TFRecords tutorial:
    # https://www.tensorflow.org/tutorials/load_data/tfrecord#reading_a_tfrecord_file_2

    filenames = sorted(os.listdir(tmp_path))
    filepaths = [os.path.join(tmp_path, filename) for filename in filenames]
    raw_dataset = tf.data.TFRecordDataset(filepaths)

    tfrecords = []
    for raw_record in raw_dataset:
        example = tf.train.Example()
        example.ParseFromString(raw_record.numpy())
        tfrecords.append(example)

    assert tfrecords == expected_records


def test_readback_tfrecords(ray_start_regular_shared, tmp_path):
    """
    Test reading back TFRecords written using datasets.
    The dataset we read back should be the same that we wrote.
    """

    # The dataset we will write to a .tfrecords file.
    ds = ray.data.from_items(
        [
            # Row one.
            {
                "int_item": 1,
                "int_list": [2, 2, 3],
                "int_empty": [],
                "float_item": 1.0,
                "float_list": [2.0, 3.0, 4.0],
                "float_empty": 1.0,
                "bytes_item": b"abc",
                "bytes_list": [b"def", b"1234"],
                "bytes_empty": None,
            },
            # Row two.
            {
                "int_item": 2,
                "int_list": [3, 3, 4],
                "int_empty": [9, 2],
                "float_item": 2.0,
                "float_list": [5.0, 6.0, 7.0],
                "float_empty": None,
                "bytes_item": b"ghi",
                "bytes_list": [b"jkl", b"5678"],
                "bytes_empty": b"hello",
            },
        ],
        # Here and in the read_tfrecords call below, we specify `parallelism=1`
        # to ensure that all rows end up in the same block, which is required
        # for type inference involving partially missing columns.
        parallelism=1,
    )
    # Write the TFRecords.
    ds.write_tfrecords(tmp_path)
    # Read the TFRecords.
    readback_ds = ray.data.read_tfrecords(tmp_path)
    if not ray.data.context.DatasetContext.get_current().use_streaming_executor:
        assert ds.take() == readback_ds.take()
    else:
        # In streaming, we set batch_format to "default" (because calling
        # ds.dataset_format() will still invoke bulk execution and we want
        # to avoid that). As a result, it's receiving PandasRow (the defaut
        # batch format), which doesn't have the same ordering of columns as
        # the ArrowRow.
        from ray.data.block import BlockAccessor

        def get_rows(ds):
            rows = []
            for batch in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
                batch = BlockAccessor.for_block(BlockAccessor.batch_to_block(batch))
                for row in batch.iter_rows():
                    rows.append(row)
            return rows

        assert get_rows(ds) == get_rows(readback_ds)


def test_write_invalid_tfrecords(ray_start_regular_shared, tmp_path):
    """
    If we try to write a dataset with invalid TFRecord datatypes,
    ValueError should be raised.
    """

    ds = ray.data.from_items([{"item": None}])

    with pytest.raises(ValueError):
        ds.write_tfrecords(tmp_path)


def test_read_invalid_tfrecords(ray_start_regular_shared, tmp_path):
    file_path = os.path.join(tmp_path, "file.json")
    with open(file_path, "w") as file:
        json.dump({"number": 0, "string": "foo"}, file)

    # Expect RuntimeError raised when reading JSON as TFRecord file.
    with pytest.raises(RuntimeError, match="Failed to read TFRecord file"):
        ray.data.read_tfrecords(file_path).schema()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
