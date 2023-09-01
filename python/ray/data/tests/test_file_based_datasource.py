import os

import pyarrow
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource import FileBasedDatasource
from ray.data.datasource.file_based_datasource import (
    OPEN_FILE_MAX_ATTEMPTS,
    _open_file_with_retry,
)


class MockFileBasedDatasource(FileBasedDatasource):
    def _write_block(
        self, f: "pyarrow.NativeFile", block: BlockAccessor, **writer_args
    ):
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

    ds.write_datasource(
        MockFileBasedDatasource(), dataset_uuid=ds._uuid, path=path, try_create_dir=True
    )

    assert os.path.isdir(path)


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
