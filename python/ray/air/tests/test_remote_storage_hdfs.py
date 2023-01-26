"""Test remote_storage in a ci environment with real hdfs setup."""
import os

import pytest

from ray.air._internal.remote_storage import get_fs_and_path


def test_get_fs_and_path_hdfs():
    # set up the correct CLASSPATH for pyarrow to work.
    with open("/tmp/hdfs_classpath_env", "r") as f:
        os.environ["CLASSPATH"] = f.readline()
    with open("/tmp/hdfs_path_env", "r") as f:
        os.environ["PATH"] = f.readline()
    hostname = os.getenv("CONTAINER_ID")
    hdfs_uri = f"hdfs://{hostname}:8020/test/"
    # do it twice should yield the same result
    _, path = get_fs_and_path(hdfs_uri)
    _, cached_path = get_fs_and_path(hdfs_uri)
    assert path == cached_path


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
