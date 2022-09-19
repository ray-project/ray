import os
from io import BytesIO
from typing import Any, Dict, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from ray.data.datasource.file_meta_provider import _handle_read_os_error

import requests
import snappy
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.http import HTTPFileSystem

import ray
from ray.data.tests.util import gen_bin_files, Counter
from ray.data._internal.arrow_block import ArrowRow
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource import (
    BaseFileMetadataProvider,
    Datasource,
    DummyOutputDatasource,
    FastFileMetadataProvider,
    PartitionStyle,
    PathPartitionEncoder,
    PathPartitionFilter,
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


def test_read_binary_files(ray_start_regular_shared):
    with gen_bin_files(10) as (_, paths):
        ds = ray.data.read_binary_files(paths, parallelism=10)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item
        # Test metadata ops.
        assert ds.count() == 10
        assert "bytes" in str(ds.schema()), ds
        assert "bytes" in str(ds), ds


def test_read_binary_files_with_fs(ray_start_regular_shared):
    with gen_bin_files(10) as (tempdir, paths):
        # All the paths are absolute, so we want the root file system.
        fs, _ = pa.fs.FileSystem.from_uri("/")
        ds = ray.data.read_binary_files(paths, filesystem=fs, parallelism=10)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item


def test_read_binary_files_with_paths(ray_start_regular_shared):
    with gen_bin_files(10) as (_, paths):
        ds = ray.data.read_binary_files(paths, include_paths=True, parallelism=10)
        for i, (path, item) in enumerate(ds.iter_rows()):
            assert path == paths[i]
            expected = open(paths[i], "rb").read()
            assert expected == item


# TODO(Clark): Hitting S3 in CI is currently broken due to some AWS
# credentials issue, unskip this test once that's fixed or once ported to moto.
@pytest.mark.skip(reason="Shouldn't hit S3 in CI")
def test_read_binary_files_s3(ray_start_regular_shared):
    ds = ray.data.read_binary_files(["s3://anyscale-data/small-files/0.dat"])
    item = ds.take(1).pop()
    expected = requests.get(
        "https://anyscale-data.s3.us-west-2.amazonaws.com/small-files/0.dat"
    ).content
    assert item == expected


def test_read_text(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")
    with open(os.path.join(path, "file3.txt"), "w") as f:
        f.write("ray\n")
    ds = ray.data.read_text(path)
    assert sorted(ds.take()) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 5


def test_read_text_meta_provider(
    ray_start_regular_shared,
    tmp_path,
):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    path = os.path.join(path, "file1.txt")
    with open(path, "w") as f:
        f.write("hello\n")
        f.write("world\n")
        f.write("goodbye\n")
        f.write("ray\n")
    ds = ray.data.read_text(path, meta_provider=FastFileMetadataProvider())
    assert sorted(ds.take()) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 5

    with pytest.raises(NotImplementedError):
        ray.data.read_text(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_read_text_partitioned_with_filter(
    ray_start_regular_shared,
    tmp_path,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_text(dataframe, path, **kwargs):
        dataframe.to_string(path, index=False, header=False, **kwargs)

    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(tmp_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            df_to_text,
        )
        df_to_text(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.txt"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filter_fn=skip_unpartitioned,
        )
        ds = ray.data.read_text(base_dir, partition_filter=partition_path_filter)
        assert_base_partitioned_ds(
            ds,
            schema="<class 'str'>",
            num_computed=None,
            sorted_values=["1 a", "1 b", "1 c", "3 e", "3 f", "3 g"],
            ds_take_transform_fn=lambda t: t,
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


def test_read_binary_snappy(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy")
    os.mkdir(path)
    with open(os.path.join(path, "file"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(
        path,
        arrow_open_stream_args=dict(compression="snappy"),
    )
    assert sorted(ds.take()) == [byte_str]


def test_read_binary_snappy_inferred(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy_inferred")
    os.mkdir(path)
    with open(os.path.join(path, "file.snappy"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(path)
    assert sorted(ds.take()) == [byte_str]


def test_read_binary_meta_provider(
    ray_start_regular_shared,
    tmp_path,
):
    path = os.path.join(tmp_path, "test_binary_snappy")
    os.mkdir(path)
    path = os.path.join(path, "file")
    with open(path, "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(
        path,
        arrow_open_stream_args=dict(compression="snappy"),
        meta_provider=FastFileMetadataProvider(),
    )
    assert sorted(ds.take()) == [byte_str]

    with pytest.raises(NotImplementedError):
        ray.data.read_binary_files(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_read_binary_snappy_partitioned_with_filter(
    ray_start_regular_shared,
    tmp_path,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_binary(dataframe, path, **kwargs):
        with open(path, "wb") as f:
            df_string = dataframe.to_string(index=False, header=False, **kwargs)
            byte_str = df_string.encode()
            bytes = BytesIO(byte_str)
            snappy.stream_compress(bytes, f)

    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        base_dir = os.path.join(tmp_path, style.value)
        partition_path_encoder = PathPartitionEncoder.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
        )
        write_base_partitioned_df(
            partition_keys,
            partition_path_encoder,
            df_to_binary,
        )
        df_to_binary(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.snappy"))
        partition_path_filter = PathPartitionFilter.of(
            style=style,
            base_dir=base_dir,
            field_names=partition_keys,
            filter_fn=skip_unpartitioned,
        )
        ds = ray.data.read_binary_files(
            base_dir,
            partition_filter=partition_path_filter,
            arrow_open_stream_args=dict(compression="snappy"),
        )
        assert_base_partitioned_ds(
            ds,
            count=2,
            num_rows=2,
            schema="<class 'bytes'>",
            num_computed=None,
            sorted_values=[b"1 a\n1 b\n1 c", b"3 e\n3 f\n3 g"],
            ds_take_transform_fn=lambda t: t,
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


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


def test_torch_datasource_value_error(ray_start_regular_shared, local_path):
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


def test_read_text_remote_args(ray_start_cluster, tmp_path):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().node_id.hex()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")

    ds = ray.data.read_text(
        path, parallelism=2, ray_remote_args={"resources": {"bar": 1}}
    )

    blocks = ds.get_internal_block_refs()
    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {bar_node_id}, locations
    assert sorted(ds.take()) == ["goodbye", "hello", "world"]


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
