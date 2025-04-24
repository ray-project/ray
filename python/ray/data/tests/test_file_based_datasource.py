import os
from typing import Iterator

import pyarrow
import pytest

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource


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


def test_flaky_datasource(ray_start_regular_shared, tmp_path):
    """Test that flaky read tasks are retried for both the
    default set of retried errors and a custom set of retried errors."""
    csv_path = os.path.join(tmp_path, "file.csv")
    with open(csv_path, "w") as file:
        file.write("spam")

    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    default_retried_error = ray.data.context.DEFAULT_RETRIED_IO_ERRORS[0]
    custom_retried_error = "AWS Error ACCESS_DENIED"

    class FlakyFileBasedDatasource(MockFileBasedDatasource):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            CounterActor = ray.remote(Counter)
            # This actor ref is shared across all read tasks.
            self.counter = CounterActor.remote()

        def _read_stream(self, f: "pyarrow.NativeFile", path: str):
            count = ray.get(self.counter.increment.remote())
            if count == 1:
                raise RuntimeError(default_retried_error)
            elif count == 2:
                raise RuntimeError(custom_retried_error)
            else:
                yield super()._read_stream(f, path)

    ray.data.DataContext.get_current().retried_io_errors.append(custom_retried_error)

    datasource = FlakyFileBasedDatasource([csv_path])
    ds = ray.data.read_datasource(datasource)
    assert len(ds.take()) == 20


@pytest.mark.parametrize("shuffle", [True, False, "file"])
def test_invalid_shuffle_arg_raises_error(ray_start_regular_shared, shuffle):
    with pytest.raises(ValueError):
        FileBasedDatasource("example://iris.csv", shuffle=shuffle)


@pytest.mark.parametrize("shuffle", [None, "files"])
def test_valid_shuffle_arg_does_not_raise_error(ray_start_regular_shared, shuffle):
    FileBasedDatasource("example://iris.csv", shuffle=shuffle)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
