import pytest
import os
import ray
from typing import Any, Callable, Dict
import pyarrow
from ray.data.block import BlockAccessor
from ray.data.datasource import FileBasedDatasource

class MockFileBasedDatasource(FileBasedDatasource):

    def _write_block(self, f: "pyarrow.NativeFile", block: BlockAccessor, **writer_args):
        f.write(b"")


@pytest.mark.parametrize("num_rows", [0, 1])
def test_write_preserves_user_directory(num_rows, tmp_path, ray_start_regular_shared):
    ds = ray.data.range(num_rows)
    path = os.path.join(tmp_path, "test")
    os.mkdir(path)  # User-created directory

    ds.write_datasource(MockFileBasedDatasource(), dataset_uuid=ds._uuid, path=path)

    assert os.path.isdir(path)


def test_write_creates_dir(tmp_path, ray_start_regular_shared):
    ds = ray.data.range(1)
    path = os.path.join(tmp_path, "test")

    ds.write_datasource(MockFileBasedDatasource(), dataset_uuid=ds._uuid, path=path, try_create_dir=True)

    assert os.path.isdir(path)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
