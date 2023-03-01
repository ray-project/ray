import os
from io import BytesIO
import requests

import pandas as pd
import pyarrow as pa
import pytest
import snappy

import ray
from ray.data.tests.util import Counter
from ray.data.tests.util import gen_bin_files
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


def test_read_binary_files_partitioning(ray_start_regular_shared, tmp_path):
    os.mkdir(os.path.join(tmp_path, "country=us"))
    path = os.path.join(tmp_path, "country=us", "file.bin")
    with open(path, "wb") as f:
        f.write(b"foo")

    ds = ray.data.read_binary_files(path, partitioning=Partitioning("hive"))

    assert ds.take() == [{"bytes": b"foo", "country": "us"}]

    ds = ray.data.read_binary_files(
        path, include_paths=True, partitioning=Partitioning("hive")
    )

    assert ds.take() == [{"bytes": b"foo", "path": path, "country": "us"}]


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
