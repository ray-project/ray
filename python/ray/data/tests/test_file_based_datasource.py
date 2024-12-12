import os
from typing import Iterator
from unittest import mock

import pyarrow
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
from ray.data.datasource import FileShuffleConfig
from ray.data.datasource.file_based_datasource import (
    OPEN_FILE_MAX_ATTEMPTS,
    FileBasedDatasource,
    _open_file_with_retry,
)
from ray.data.datasource.path_util import _is_local_windows_path


class MockFileBasedDatasource(FileBasedDatasource):
    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        builder = DelegatingBlockBuilder()
        builder.add({"data": f.readall()})
        yield builder.build()


def test_local_paths(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test.txt")
    with open(path, "w"):
        pass

    datasource = MockFileBasedDatasource(path)
    assert datasource.supports_distributed_reads

    datasource = MockFileBasedDatasource(f"local://{path}")
    assert not datasource.supports_distributed_reads


def test_local_paths_with_client_raises_error(ray_start_cluster_enabled, tmp_path):
    ray_start_cluster_enabled.add_node(num_cpus=1)
    ray_start_cluster_enabled.head_node._ray_params.ray_client_server_port = "10004"
    ray_start_cluster_enabled.head_node.start_ray_client_server()
    ray.init("ray://localhost:10004")

    path = os.path.join(tmp_path, "test.txt")
    with open(path, "w"):
        pass

    with pytest.raises(ValueError):
        MockFileBasedDatasource(f"local://{path}")


def test_include_paths(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test.txt")
    with open(path, "w"):
        pass

    datasource = MockFileBasedDatasource(path, include_paths=True)
    ds = ray.data.read_datasource(datasource)

    paths = [row["path"] for row in ds.take_all()]
    assert paths == [path]


def test_file_extensions(ray_start_regular_shared, tmp_path):
    csv_path = os.path.join(tmp_path, "file.csv")
    with open(csv_path, "w") as file:
        file.write("spam")

    txt_path = os.path.join(tmp_path, "file.txt")
    with open(txt_path, "w") as file:
        file.write("ham")

    datasource = MockFileBasedDatasource([csv_path, txt_path], file_extensions=None)
    ds = ray.data.read_datasource(datasource)
    assert sorted(ds.input_files()) == sorted([csv_path, txt_path])

    datasource = MockFileBasedDatasource([csv_path, txt_path], file_extensions=["csv"])
    ds = ray.data.read_datasource(datasource)
    assert ds.input_files() == [csv_path]


def test_open_file_with_retry(ray_start_regular_shared):
    class FlakyFileOpener:
        def __init__(self, max_attempts: int):
            self.retry_attempts = 0
            self.max_attempts = max_attempts

        def open(self):
            self.retry_attempts += 1
            if self.retry_attempts < self.max_attempts:
                raise OSError(
                    "When creating key x in bucket y: AWS Error SLOW_DOWN during "
                    "PutObject operation: Please reduce your request rate."
                )
            return "dummy"

    original_max_attempts = OPEN_FILE_MAX_ATTEMPTS
    try:
        # Test openning file successfully after retries.
        opener = FlakyFileOpener(3)
        assert _open_file_with_retry("dummy", lambda: opener.open()) == "dummy"

        # Test exhausting retries and failed eventually.
        ray.data.datasource.file_based_datasource.OPEN_FILE_MAX_ATTEMPTS = 3
        opener = FlakyFileOpener(4)
        with pytest.raises(OSError):
            _open_file_with_retry("dummy", lambda: opener.open())
    finally:
        ray.data.datasource.file_based_datasource.OPEN_FILE_MAX_ATTEMPTS = (
            original_max_attempts
        )


def test_windows_path():
    with mock.patch("sys.platform", "win32"):
        assert _is_local_windows_path("c:/some/where")
        assert _is_local_windows_path("c:\\some\\where")
        assert _is_local_windows_path("c:\\some\\where/mixed")


@pytest.mark.parametrize("shuffle", [True, False, "file"])
def test_invalid_shuffle_arg_raises_error(ray_start_regular_shared, shuffle):
    with pytest.raises(ValueError):
        FileBasedDatasource("example://iris.csv", shuffle=shuffle)


@pytest.mark.parametrize("shuffle", [None, "files"])
def test_valid_shuffle_arg_does_not_raise_error(ray_start_regular_shared, shuffle):
    FileBasedDatasource("example://iris.csv", shuffle=shuffle)


def test_read_parquet_deterministic_shuffling(restore_data_context, tmp_path):
    def write_parquet_file(path, file_index):
        """Write a dummy Parquet file with test data."""
        # Create a dummy dataset with unique data for each file
        data = {
            "col1": range(10 * file_index, 10 * (file_index + 1)),
            "col2": ["foo", "bar"] * 5,
        }
        table = pyarrow.Table.from_pydict(data)
        pq.write_table(table, path)

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True
    # Use the current working directory

    # Create temporary Parquet files for testing in the current directory
    paths = [tmp_path / f"test_file_{i}.parquet" for i in range(5)]
    for i, path in enumerate(paths):
        # Write dummy Parquet files
        write_parquet_file(path, i)

    # Convert paths to strings for read_parquet
    string_paths = [str(path) for path in paths]

    # Read with deterministic shuffling
    shuffle_config = FileShuffleConfig(seed=42)
    ds1 = ray.data.read_parquet(string_paths, shuffle=shuffle_config)
    ds2 = ray.data.read_parquet(string_paths, shuffle=shuffle_config)

    # Verify deterministic behavior
    assert ds1.take_all() == ds2.take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
