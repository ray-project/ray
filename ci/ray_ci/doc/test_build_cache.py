import os
import pickle
import sys
import tempfile
import types
from unittest import mock

import pytest

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


def test_massage_cache():
    # SimpleNamespace (stdlib) so the subprocess interpreter can unpickle it.
    cache = types.SimpleNamespace(
        dependencies={
            "doc1": ["site-packages/dep1", "dep2"],
            "doc2": ["dep3", "site-packages/dep4"],
        }
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_path = os.path.join(temp_dir, "env_cache.pkl")
        with open(cache_path, "wb") as file:
            pickle.dump(cache, file)

        build_cache = BuildCache(temp_dir)
        # Production defaults to the doc-build image's `python`; run the massage
        # with this test's interpreter instead.
        build_cache._massage_cache("env_cache.pkl", python_executable=sys.executable)

        with open(cache_path, "rb") as file:
            cache = pickle.load(file)
            assert cache.dependencies == {
                "doc1": ["dep2"],
                "doc2": ["dep3"],
            }


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
