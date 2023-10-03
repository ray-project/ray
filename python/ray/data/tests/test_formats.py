import os
from typing import Iterable, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torchvision
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem

import ray
from ray._private.test_utils import wait_for_condition
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource import Datasource, DummyOutputDatasource, WriteResult
from ray.data.datasource.file_meta_provider import _handle_read_os_error
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef


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
    assert "FromArrow" in ds.stats()

    # test from single pyarrow table
    ds = ray.data.from_arrow(pa.Table.from_pandas(df1))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()


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
    assert "FromArrow" in ds.stats()

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()


def test_to_arrow_refs(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"id": list(range(n))})
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
        out.extend(extract_values("id", BlockAccessor.for_block(b).iter_rows(True)))
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
    assert ds._plan.execute()._num_computed() == 0
    assert ds.count() == 6

    out_path = os.path.join(tmp_path, "out")
    os.mkdir(out_path)

    ds._set_uuid("data")
    ds.write_parquet(out_path)

    ds_df1 = pd.read_parquet(os.path.join(out_path, "data_000000_000000.parquet"))
    ds_df2 = pd.read_parquet(os.path.join(out_path, "data_000001_000000.parquet"))
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


def test_write_datasource(ray_start_regular_shared):
    output = DummyOutputDatasource()
    ds = ray.data.range(10, parallelism=2)
    ds.write_datasource(output)
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10

    output.enabled = False
    ds = ray.data.range(10, parallelism=2)
    with pytest.raises(ValueError):
        ds.write_datasource(output, ray_remote_args={"max_retries": 0})
    assert output.num_ok == 1
    assert output.num_failed == 1
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10


def test_from_tf(ray_start_regular_shared):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    expected_data = list(tf_dataset)
    assert len(actual_data) == len(expected_data)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)


def test_from_torch(shutdown_only, tmp_path):
    torch_dataset = torchvision.datasets.MNIST(tmp_path, download=True)
    expected_data = list(torch_dataset)

    ray_dataset = ray.data.from_torch(torch_dataset)

    actual_data = extract_values("item", list(ray_dataset.take_all()))
    assert actual_data == expected_data

    import torch

    class IterMNIST(torch.utils.data.IterableDataset):
        def __len__(self):
            return len(torch_dataset)

        def __iter__(self):
            return iter(torch_dataset)

    iter_torch_dataset = IterMNIST()
    ray_dataset = ray.data.from_torch(iter_torch_dataset)

    actual_data = extract_values("item", list(ray_dataset.take_all()))
    assert actual_data == expected_data


class NodeLoggerOutputDatasource(Datasource):
    """A writable datasource that logs node IDs of write tasks, for testing."""

    def __init__(self):
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.node_ids = set()

            def write(self, node_id: str, block: Block) -> str:
                block = BlockAccessor.for_block(block)
                self.rows_written += block.num_rows()
                self.node_ids.add(node_id)
                return "ok"

            def get_rows_written(self):
                return self.rows_written

            def get_node_ids(self):
                return self.node_ids

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        **write_args,
    ) -> WriteResult:
        data_sink = self.data_sink

        def write(b):
            node_id = ray.get_runtime_context().get_node_id()
            return data_sink.write.remote(node_id, b)

        tasks = []
        for b in blocks:
            tasks.append(write(b))
        ray.get(tasks)
        return "ok"

    def on_write_complete(self, write_results: List[WriteResult]) -> None:
        assert all(w == "ok" for w in write_results), write_results
        self.num_ok += 1

    def on_write_failed(
        self, write_results: List[ObjectRef[WriteResult]], error: Exception
    ) -> None:
        self.num_failed += 1


def test_write_datasource_ray_remote_args(ray_start_cluster):
    ray.shutdown()
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    output = NodeLoggerOutputDatasource()
    ds = ray.data.range(100, parallelism=10)
    # Pin write tasks to node with "bar" resource.
    ds.write_datasource(output, ray_remote_args={"resources": {"bar": 1}})
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 100

    node_ids = ray.get(output.data_sink.get_node_ids.remote())
    assert node_ids == {bar_node_id}


def test_read_s3_file_error(shutdown_only, s3_path):
    dummy_path = s3_path + "_dummy"
    error_message = "Please check that file exists and has properly configured access."
    with pytest.raises(OSError, match=error_message):
        ray.data.read_parquet(dummy_path)
    with pytest.raises(OSError, match=error_message):
        ray.data.read_binary_files(dummy_path)
    with pytest.raises(OSError, match=error_message):
        ray.data.read_csv(dummy_path)
    with pytest.raises(OSError, match=error_message):
        ray.data.read_json(dummy_path)
    with pytest.raises(OSError, match=error_message):
        error = OSError(
            f"Error creating dataset. Could not read schema from {dummy_path}: AWS "
            "Error [code 15]: No response body.. Is this a 'parquet' file?"
        )
        _handle_read_os_error(error, dummy_path)


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


def test_get_reader(shutdown_only):
    # Note: if you get TimeoutErrors here, try installing required dependencies
    # with `pip install -U "ray[default]"`.
    ray.init()

    head_node_id = ray.get_runtime_context().get_node_id()

    # Issue read so `_get_reader` being executed.
    ray.data.range(10).materialize()

    # Verify `_get_reader` being executed on same node (head node).
    def verify_get_reader():
        from ray.util.state import list_tasks

        task_states = list_tasks(filters=[("name", "=", "_get_reader")])
        # Verify only one task being executed on same node.
        assert len(task_states) == 1
        assert task_states[0]["name"] == "_get_reader"
        assert task_states[0]["node_id"] == head_node_id
        return True

    wait_for_condition(verify_get_reader, timeout=20)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
