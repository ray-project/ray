import os
from unittest import mock

import pyarrow
import pytest

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import (
    OPEN_FILE_MAX_ATTEMPTS,
    FileBasedDatasource,
    _open_file_with_retry,
)
from ray.data.datasource.path_util import _is_local_windows_path


class MockFileBasedDatasource(FileBasedDatasource):
    def _read_file(self, f: "pyarrow.NativeFile", path: str) -> Block:
        builder = DelegatingBlockBuilder()
        builder.add({"data": f.readall()})
        return builder.build()


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
