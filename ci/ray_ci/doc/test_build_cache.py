import sys
import os
import pytest
import tempfile
from unittest import mock

from ci.ray_ci.doc.build_cache import BuildCache


@mock.patch("subprocess.check_output")
def test_get_cache(mock_check_output):
    mock_check_output.return_value = b"file1\nfile2\nfile3"
    assert BuildCache("/path/to/cache")._get_cache() == {"file1", "file2", "file3"}


@mock.patch("os.environ", {"BUILDKITE_COMMIT": "12345"})
def test_zip_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        files = set()
        for i in range(3):
            file_name = f"file_{i}.txt"
            with open(os.path.join(temp_dir, file_name), "w") as file:
                file.write("hi")
            files.add(file_name)

        assert BuildCache(temp_dir)._zip_cache(files) == "12345.tgz"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
