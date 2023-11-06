import os

import pyarrow
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource import BlockBasedFileDatasink


class MockFileDatasink(BlockBasedFileDatasink):
    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        file.write(b"")


@pytest.mark.parametrize("num_rows", [0, 1])
def test_write_preserves_user_directory(num_rows, tmp_path, ray_start_regular_shared):
    ds = ray.data.range(num_rows)
    path = os.path.join(tmp_path, "test")
    os.mkdir(path)  # User-created directory

    ds.write_datasource(MockFileDatasink(path=path))

    assert os.path.isdir(path)


def test_write_creates_dir(tmp_path, ray_start_regular_shared):
    ds = ray.data.range(1)
    path = os.path.join(tmp_path, "test")

    ds.write_datasource(MockFileDatasink(path=path, try_create_dir=True))

    assert os.path.isdir(path)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
