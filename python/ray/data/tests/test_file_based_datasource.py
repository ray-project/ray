from unittest import mock

import pytest

import ray
from ray.data.datasource.file_based_datasource import (
    OPEN_FILE_MAX_ATTEMPTS,
    _open_file_with_retry,
)
from ray.data.datasource.path_util import _is_local_windows_path


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
