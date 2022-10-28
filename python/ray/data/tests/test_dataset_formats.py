import os
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from ray.data.datasource.file_meta_provider import _handle_read_os_error

from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.http import HTTPFileSystem

import ray
from ray.data._internal.arrow_block import ArrowRow
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource import (
    Datasource,
    DummyOutputDatasource,
    SimpleTensorFlowDatasource,
    SimpleTorchDatasource,
    WriteResult,
)

from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


def test_from_arrow(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow([pa.Table.from_pandas(df1), pa.Table.from_pandas(df2)])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ds.stats()

    # test from single pyarrow table
    ds = ray.data.from_arrow(pa.Table.from_pandas(df1))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ds.stats()


def test_from_arrow_refs(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow_refs(
        [ray.put(pa.Table.from_pandas(df1)), ray.put(pa.Table.from_pandas(df2))]
    )
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ds.stats()

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ds.stats()


def test_to_arrow_refs(ray_start_regular_shared):
    n = 5

    # Zero-copy.
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_table(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())], ignore_index=True
    )
    assert df.equals(dfds)

    # Conversion.
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())], ignore_index=True
    )
    assert df.equals(dfds)


def test_get_internal_block_refs(ray_start_regular_shared):
    blocks = ray.data.range(10, parallelism=10).get_internal_block_refs()
    assert len(blocks) == 10
    out = []
    for b in ray.get(blocks):
        out.extend(list(BlockAccessor.for_block(b).iter_rows()))
    out = sorted(out)
    assert out == list(range(10)), out


def test_fsspec_filesystem(ray_start_regular_shared, tmp_path):
    """Same as `test_parquet_write` but using a custom, fsspec filesystem.

    TODO (Alex): We should write a similar test with a mock PyArrow fs, but
    unfortunately pa.fs._MockFileSystem isn't serializable, so this may require
    some effort.
    """
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    path1 = os.path.join(str(tmp_path), "test1.parquet")
    pq.write_table(table, path1)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(str(tmp_path), "test2.parquet")
    pq.write_table(table, path2)

    fs = LocalFileSystem()

    ds = ray.data.read_parquet([path1, path2], filesystem=fs)

    # Test metadata-only parquet ops.
    assert ds._plan.execute()._num_computed() == 1
    assert ds.count() == 6

    out_path = os.path.join(tmp_path, "out")
    os.mkdir(out_path)

    ds._set_uuid("data")
    ds.write_parquet(out_path)

    ds_df1 = pd.read_parquet(os.path.join(out_path, "data_000000.parquet"))
    ds_df2 = pd.read_parquet(os.path.join(out_path, "data_000001.parquet"))
    ds_df = pd.concat([ds_df1, ds_df2])
    df = pd.concat([df1, df2])
    assert ds_df.equals(df)


def test_fsspec_http_file_system(ray_start_regular_shared, http_server, http_file):
    ds = ray.data.read_text(http_file, filesystem=HTTPFileSystem())
    assert ds.count() > 0
    # Test auto-resolve of HTTP file system when it is not provided.
    ds = ray.data.read_text(http_file)
    assert ds.count() > 0


def test_read_example_data(ray_start_regular_shared, tmp_path):
    ds = ray.data.read_csv("example://iris.csv")
    assert ds.count() == 150
    assert ds.take(1) == [
        {
            "sepal.length": 5.1,
            "sepal.width": 3.5,
            "petal.length": 1.4,
            "petal.width": 0.2,
            "variety": "Setosa",
        }
    ]


@pytest.mark.parametrize("pipelined", [False, True])
def test_write_datasource(ray_start_regular_shared, pipelined):
    output = DummyOutputDatasource()
    ds0 = ray.data.range(10, parallelism=2)
    ds = maybe_pipeline(ds0, pipelined)
    ds.write_datasource(output)
    if pipelined:
        assert output.num_ok == 2
    else:
        assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10

    ray.get(output.data_sink.set_enabled.remote(False))
    ds = maybe_pipeline(ds0, pipelined)
    with pytest.raises(ValueError):
        ds.write_datasource(output)
    if pipelined:
        assert output.num_ok == 2
    else:
        assert output.num_ok == 1
    assert output.num_failed == 1
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10


def test_tensorflow_datasource(ray_start_regular_shared):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]

    def dataset_factory():
        return tfds.load("mnist", split=["train"], as_supervised=True)[0]

    ray_dataset = ray.data.read_datasource(
        SimpleTensorFlowDatasource(), parallelism=1, dataset_factory=dataset_factory
    ).fully_executed()

    assert ray_dataset.num_blocks() == 1

    actual_data = ray_dataset.take_all()
    expected_data = list(tf_dataset)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)


def test_torch_datasource(ray_start_regular_shared, local_path):
    import torchvision

    # Download datasets to separate folders to prevent interference.
    torch_dataset_root = os.path.join(local_path, "torch")
    ray_dataset_root = os.path.join(local_path, "ray")

    torch_dataset = torchvision.datasets.MNIST(torch_dataset_root, download=True)
    expected_data = list(torch_dataset)

    def dataset_factory():
        return torchvision.datasets.MNIST(ray_dataset_root, download=True)

    ray_dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), parallelism=1, dataset_factory=dataset_factory
    )
    actual_data = list(next(ray_dataset.iter_batches(batch_size=None)))

    assert actual_data == expected_data


def test_torch_datasource_value_error(shutdown_only, local_path):
    import torchvision

    dataset = torchvision.datasets.MNIST(local_path, download=True)

    with pytest.raises(ValueError):
        # `dataset_factory` should be a function, not a Torch dataset.
        ray.data.read_datasource(
            SimpleTorchDatasource(),
            parallelism=1,
            dataset_factory=dataset,
        )


class NodeLoggerOutputDatasource(Datasource[Union[ArrowRow, int]]):
    """A writable datasource that logs node IDs of write tasks, for testing."""

    def __init__(self):
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True
                self.node_ids = set()

            def write(self, node_id: str, block: Block) -> str:
                block = BlockAccessor.for_block(block)
                if not self.enabled:
                    raise ValueError("disabled")
                self.rows_written += block.num_rows()
                self.node_ids.add(node_id)
                return "ok"

            def get_rows_written(self):
                return self.rows_written

            def get_node_ids(self):
                return self.node_ids

            def set_enabled(self, enabled):
                self.enabled = enabled

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        ray_remote_args: Dict[str, Any],
        **write_args,
    ) -> List[ObjectRef[WriteResult]]:
        data_sink = self.data_sink

        @ray.remote
        def write(b):
            node_id = ray.get_runtime_context().node_id.hex()
            return ray.get(data_sink.write.remote(node_id, b))

        tasks = []
        for b in blocks:
            tasks.append(write.options(**ray_remote_args).remote(b))
        return tasks

    def on_write_complete(self, write_results: List[WriteResult]) -> None:
        assert all(w == "ok" for w in write_results), write_results
        self.num_ok += 1

    def on_write_failed(
        self, write_results: List[ObjectRef[WriteResult]], error: Exception
    ) -> None:
        self.num_failed += 1


def test_write_datasource_ray_remote_args(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().node_id.hex()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    output = NodeLoggerOutputDatasource()
    ds = ray.data.range(100, parallelism=10)
    # Pin write tasks to
    ds.write_datasource(output, ray_remote_args={"resources": {"bar": 1}})
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 100

    node_ids = ray.get(output.data_sink.get_node_ids.remote())
    assert node_ids == {bar_node_id}


def test_read_s3_file_error(ray_start_regular_shared, s3_path):
    dummy_path = s3_path + "_dummy"
    error_message = "Please check that file exists and has properly configured access."
    with pytest.raises(OSError) as e:
        ray.data.read_parquet(dummy_path)
    assert error_message in str(e.value)
    with pytest.raises(OSError) as e:
        ray.data.read_binary_files(dummy_path)
    assert error_message in str(e.value)
    with pytest.raises(OSError) as e:
        ray.data.read_csv(dummy_path)
    assert error_message in str(e.value)
    with pytest.raises(OSError) as e:
        ray.data.read_json(dummy_path)
    assert error_message in str(e.value)
    with pytest.raises(OSError) as e:
        error = OSError(
            f"Error creating dataset. Could not read schema from {dummy_path}: AWS "
            "Error [code 15]: No response body.. Is this a 'parquet' file?"
        )
        _handle_read_os_error(error, dummy_path)
    assert error_message in str(e.value)


def test_read_tfrecords(ray_start_regular_shared, tmp_path):
    import tensorflow as tf

    features = tf.train.Features(
        feature={
            "int64": tf.train.Feature(int64_list=tf.train.Int64List(value=[1])),
            "int64_list": tf.train.Feature(
                int64_list=tf.train.Int64List(value=[1, 2, 3, 4])
            ),
            "float": tf.train.Feature(float_list=tf.train.FloatList(value=[1.0])),
            "float_list": tf.train.Feature(
                float_list=tf.train.FloatList(value=[1.0, 2.0, 3.0, 4.0])
            ),
            "bytes": tf.train.Feature(bytes_list=tf.train.BytesList(value=[b"abc"])),
            "bytes_list": tf.train.Feature(
                bytes_list=tf.train.BytesList(value=[b"abc", b"1234"])
            ),
        }
    )
    example = tf.train.Example(features=features)
    path = os.path.join(tmp_path, "data.tfrecords")
    with tf.io.TFRecordWriter(path=path) as writer:
        writer.write(example.SerializeToString())

    ds = ray.data.read_tfrecords(path)

    df = ds.to_pandas()
    # Protobuf serializes features in a non-deterministic order.
    assert dict(df.dtypes) == {
        "int64": np.int64,
        "int64_list": object,
        "float": np.float,
        "float_list": object,
        "bytes": object,
        "bytes_list": object,
    }
    assert list(df["int64"]) == [1]
    assert list(df["int64_list"]) == [[1, 2, 3, 4]]
    assert list(df["float"]) == [1.0]
    assert list(df["float_list"]) == [[1.0, 2.0, 3.0, 4.0]]
    assert list(df["bytes"]) == [b"abc"]
    assert list(df["bytes_list"]) == [[b"abc", b"1234"]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
