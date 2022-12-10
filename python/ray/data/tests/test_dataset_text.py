import os

import pandas as pd
import pytest

import ray
from ray.data.tests.util import Counter
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    PartitionStyle,
    PathPartitionEncoder,
    PathPartitionFilter,
    Partitioning,
)

from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


def _to_lines(rows):
    return [row["text"] for row in rows]


def test_read_text_partitioning(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "country=us")
    os.mkdir(path)
    with open(os.path.join(path, "file.txt"), "w") as f:
        f.write("foo\nbar\nbaz")

    ds = ray.data.read_text(path, partitioning=Partitioning("hive"))

    df = ds.to_pandas()
    assert list(df.columns) == ["text", "country"]
    assert sorted(df["text"]) == ["bar", "baz", "foo"]
    assert list(df["country"]) == ["us", "us", "us"]


def test_empty_text_files(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    # 2 empty files.
    _ = open(os.path.join(path, "file1.txt"), "w")
    _ = open(os.path.join(path, "file2.txt"), "w")
    ds = ray.data.read_text(path)
    assert ds.count() == 0
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 2
    # 2 empty lines, one from each file.
    assert _to_lines(ds.take()) == ["", ""]


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
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "ray", "world"]
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
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 5

    with pytest.raises(NotImplementedError):
        ray.data.read_text(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_read_text_partitioned_with_filter(
    shutdown_only,
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
            schema="{text: string}",
            num_computed=None,
            sorted_values=["1 a", "1 b", "1 c", "3 e", "3 f", "3 g"],
            ds_take_transform_fn=_to_lines,
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


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
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "world"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
