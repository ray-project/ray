import os
import urllib
from pathlib import Path
import pyarrow.fs
import pytest

import ray
import ray.internal.storage as storage
from ray.tests.conftest import *  # noqa


def _custom_fs(uri):
    parsed_uri = urllib.parse.urlparse(uri)
    return pyarrow.fs.FileSystem.from_uri(parsed_uri.path)


def path_eq(a, b):
    return Path(a).resolve() == Path(b).resolve()


def test_get_filesystem_local(shutdown_only, tmp_path):
    path = os.path.join(str(tmp_path), "foo/bar")
    ray.init(storage=path)
    fs, prefix = storage.get_filesystem()
    assert path_eq(path, prefix), (path, prefix)
    assert isinstance(fs, pyarrow.fs.LocalFileSystem), fs


def test_get_custom_filesystem(shutdown_only, tmp_path):
    ray.init(
        storage=os.path.join("custom://ray.test.test_storage_custom_fs", str(tmp_path))
    )
    fs, prefix = storage.get_filesystem()
    assert path_eq(prefix, tmp_path), prefix
    assert isinstance(fs, pyarrow.fs.LocalFileSystem), fs


def test_get_filesystem_s3(shutdown_only):
    # TODO(ekl) we could use moto to test the API directly against S3 APIs. For now,
    # hook a little into the internals to test the S3 path.
    ray.init(storage=None)
    storage._init_storage("s3://bucket/foo/bar", False)
    fs, prefix = storage._init_filesystem(
        create_valid_file=False, check_valid_file=False
    )
    assert path_eq(prefix, "bucket/foo/bar"), prefix
    assert isinstance(fs, pyarrow.fs.S3FileSystem), fs


def test_get_filesystem_invalid(shutdown_only, tmp_path):
    with pytest.raises(pyarrow.lib.ArrowInvalid):
        ray.init(storage="blahblah://bad")


def test_get_filesystem_remote_workers(shutdown_only, tmp_path):
    path = os.path.join(str(tmp_path), "foo/bar")
    ray.init(storage=path, num_gpus=1)

    @ray.remote
    def check():
        fs, prefix = storage.get_filesystem()
        assert fs is not None
        return "ok"

    assert ray.get(check.remote()) == "ok"
    os.unlink(os.path.join(path, "_valid"))

    @ray.remote(num_gpus=1)  # Force a new worker.
    def check():
        storage.get_filesystem()  # Crash since the valid file is deleted.

    with pytest.raises(RuntimeError):
        ray.get(check.remote())


def test_put_get(shutdown_only, tmp_path):
    path = str(tmp_path)
    ray.init(storage=path, num_gpus=1)
    client = storage.get_client("ns")
    client2 = storage.get_client("ns2")
    client.put("foo/bar", b"hello")
    client.put("baz", b"goodbye")
    client2.put("baz", b"goodbye!")
    assert client.get("foo/bar") == b"hello"
    assert client.get("baz") == b"goodbye"
    assert client2.get("baz") == b"goodbye!"

    client.delete("baz")
    assert client.get("baz") is None


def test_directory_traversal_attack(shutdown_only, tmp_path):
    path = str(tmp_path)
    ray.init(storage=path, num_gpus=1)
    client = storage.get_client("foo")
    client.put("data", b"hello")
    client2 = storage.get_client("foo/bar")
    # Should not be able to access '../data'.
    with pytest.raises(ValueError):
        client2.get("../data")


def test_list_basic(shutdown_only, tmp_path):
    path = str(tmp_path)
    ray.init(storage=path, num_gpus=1)
    client = storage.get_client("ns")
    client.put("foo/bar1", b"hello")
    client.put("foo/bar2", b"hello")
    client.put("baz/baz1", b"goodbye!")
    d1 = client.list("")
    assert sorted([f.base_name for f in d1]) == ["baz", "foo"], d1
    d2 = client.list("foo")
    assert sorted([f.base_name for f in d2]) == ["bar1", "bar2"], d2
    with pytest.raises(FileNotFoundError):
        client.list("invalid")
    with pytest.raises(NotADirectoryError):
        client.list("foo/bar1")


def test_get_info_basic(shutdown_only, tmp_path):
    path = str(tmp_path)
    ray.init(storage=path, num_gpus=1)
    client = storage.get_client("ns")
    client.put("foo/bar1", b"hello")
    assert client.get_info("foo/bar1").base_name == "bar1"
    assert client.get_info("foo/bar2") is None
    assert client.get_info("foo").base_name == "foo"
    assert client.get_info("").base_name == "ns"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
