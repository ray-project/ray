import os
import urllib
import pyarrow.fs
import pytest

import ray
import ray.internal.storage as storage
from ray.tests.conftest import *  # noqa


def _custom_fs(uri):
    parsed_uri = urllib.parse.urlparse(uri)
    return pyarrow.fs.FileSystem.from_uri(parsed_uri.path)


def test_get_filesystem_local(shutdown_only, tmp_path):
    path = os.path.join(str(tmp_path), "foo/bar")
    ray.init(storage=path)
    fs, prefix = storage.get_filesystem()
    assert path == prefix, (path, prefix)
    assert isinstance(fs, pyarrow.fs.LocalFileSystem), fs


def test_get_custom_filesystem(shutdown_only, tmp_path):
    ray.init(
        storage=os.path.join("custom://ray.test.test_storage_custom_fs", str(tmp_path))
    )
    fs, prefix = storage.get_filesystem()
    assert prefix == str(tmp_path), prefix
    assert isinstance(fs, pyarrow.fs.LocalFileSystem), fs


def test_get_filesystem_s3(shutdown_only):
    # TODO(ekl) we could use moto to test the API directly against S3 APIs. For now,
    # hook a bit more into the internals to test the S3 path.
    ray.init(storage=None)
    storage.impl._init_storage("s3://bucket/foo/bar", False)
    fs, prefix = storage.impl._init_filesystem(
        create_valid_file=False, check_valid_file=False
    )
    assert prefix == "bucket/foo/bar", prefix
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
    path = os.path.join(str(tmp_path), "foo/bar")
    ray.init(storage=path, num_gpus=1)
    storage.put("ns", "foo/bar", b"hello")
    storage.put("ns", "baz", b"goodbye")
    storage.put("ns2", "baz", b"goodbye!")
    assert storage.get("ns", "foo/bar") == b"hello"
    assert storage.get("ns", "baz") == b"goodbye"
    assert storage.get("ns2", "baz") == b"goodbye!"

    storage.delete("ns", "baz")
    assert storage.get("ns", "baz") is None


def test_list_basic(shutdown_only, tmp_path):
    path = os.path.join(str(tmp_path), "foo/bar")
    ray.init(storage=path, num_gpus=1)
    storage.put("ns", "foo/bar1", b"hello")
    storage.put("ns", "foo/bar2", b"hello")
    storage.put("ns", "baz/baz1", b"goodbye!")
    d1 = storage.list("ns", "")
    assert sorted([f.base_name for f in d1]) == ["baz", "foo"], d1
    d2 = storage.list("ns", "foo")
    assert sorted([f.base_name for f in d2]) == ["bar1", "bar2"], d2
    with pytest.raises(FileNotFoundError):
        storage.list("ns", "invalid")
    with pytest.raises(NotADirectoryError):
        storage.list("ns", "foo/bar1")


def test_get_info_basic(shutdown_only, tmp_path):
    path = os.path.join(str(tmp_path), "foo/bar")
    ray.init(storage=path, num_gpus=1)
    storage.put("ns", "foo/bar1", b"hello")
    assert storage.get_info("ns", "foo/bar1").base_name == "bar1"
    assert storage.get_info("ns", "foo/bar2") is None
    assert storage.get_info("ns", "foo").base_name == "foo"
    assert storage.get_info("ns", "").base_name == ""


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
