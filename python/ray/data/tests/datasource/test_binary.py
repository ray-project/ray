import os
from io import BytesIO

import pytest
import snappy

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values, gen_bin_files
from ray.tests.conftest import *  # noqa


def test_read_binary_files(ray_start_regular_shared):
    with gen_bin_files(10) as (_, paths):
        ds = ray.data.read_binary_files(paths)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item["bytes"]
        # Test metadata ops.
        assert ds.count() == 10
        assert "bytes" in str(ds.schema()), ds
        assert "bytes" in str(ds), ds


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
    assert sorted(extract_values("bytes", ds.take())) == [byte_str]


def test_read_binary_snappy_inferred(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy_inferred")
    os.mkdir(path)
    with open(os.path.join(path, "file.snappy"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(path)
    assert sorted(extract_values("bytes", ds.take())) == [byte_str]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
