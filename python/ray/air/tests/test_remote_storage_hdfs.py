"""Test remote_storage in a ci environment with real hdfs setup."""
import os

import pytest

from ray.air._internal.remote_storage import get_fs_and_path


@pytest.fixture
def setup_hdfs():
    """Set env vars required by pyarrow to talk to hdfs correctly.

    Returns hostname and port needed for the hdfs uri."""

    # the following file is written in `install-hdfs.sh`.
    with open("/tmp/hdfs_env", "r") as f:
        for line in f.readlines():
            line = line.rstrip("\n")
            tokens = line.split("=", maxsplit=1)
            os.environ[tokens[0]] = tokens[1]
    import sys

    sys.path.insert(0, os.path.join(os.environ["HADOOP_HOME"], "bin"))
    hostname = os.getenv("CONTAINER_ID")
    port = os.getenv("HDFS_PORT")
    yield hostname, port


def test_get_fs_and_path_hdfs(setup_hdfs):
    hostname, port = setup_hdfs
    hdfs_uri = f"hdfs://{hostname}:{port}/test/"
    # do it twice should yield the same result
    _, path = get_fs_and_path(hdfs_uri)
    _, cached_path = get_fs_and_path(hdfs_uri)
    assert path == cached_path


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
