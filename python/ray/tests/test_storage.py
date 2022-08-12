import os
import subprocess
import urllib
from pathlib import Path

import pyarrow.fs
import pytest

import ray
import ray._private.storage as storage
from ray._private.test_utils import simulate_storage
from ray.tests.conftest import *  # noqa


def _custom_fs(uri):
    parsed_uri = urllib.parse.urlparse(uri)
    return pyarrow.fs.FileSystem.from_uri(parsed_uri.path)


def path_eq(a, b):
    # NOTE: ".resolve()" does not work properly for paths other than
    # local filesystem paths. For example, for S3, it turns "<s3_bucket>/foo"
    # into "<workdir>/<s3_bucket>/foo". But for the purpose of this function,
    # it works fine as well as both paths are resolved in the same way.
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
    root = "bucket/foo/bar"
    with simulate_storage("s3", root) as s3_uri:
        ray.init(storage=s3_uri)
        fs, prefix = storage.get_filesystem()
        assert path_eq(prefix, root), prefix
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


@pytest.mark.parametrize("storage_type", ["s3", "fs"])
def test_put_get(shutdown_only, tmp_path, storage_type):
    with simulate_storage(storage_type) as storage_uri:
        ray.init(storage=storage_uri, num_gpus=1)
        client = storage.get_client("ns")
        client2 = storage.get_client("ns2")
        assert client.get("foo/bar") is None
        client.put("foo/bar", b"hello")
        client.put("baz", b"goodbye")
        client2.put("baz", b"goodbye!")
        assert client.get("foo/bar") == b"hello"
        assert client.get("baz") == b"goodbye"
        assert client2.get("baz") == b"goodbye!"

        # delete file
        assert client.delete("baz")
        assert client.get("baz") is None
        assert not client.delete("non_existing")

        # delete dir
        n_files = 3
        for i in range(n_files):
            assert client2.get(f"foo/bar{i}") is None
        for i in range(n_files):
            client2.put(f"foo/bar{i}", f"hello{i}".encode())
        for i in range(n_files):
            assert client2.get(f"foo/bar{i}") == f"hello{i}".encode()
        assert client2.delete_dir("foo")
        for i in range(n_files):
            assert client2.get(f"foo/bar{i}") is None
        assert not client2.delete_dir("non_existing")


@pytest.mark.parametrize("storage_type", ["s3", "fs"])
def test_directory_traversal_attack(shutdown_only, storage_type):
    with simulate_storage(storage_type) as storage_uri:
        ray.init(storage=storage_uri, num_gpus=1)
        client = storage.get_client("foo")
        client.put("data", b"hello")
        client2 = storage.get_client("foo/bar")
        # Should not be able to access '../data'.
        with pytest.raises(ValueError):
            client2.get("../data")


@pytest.mark.parametrize("storage_type", ["s3", "fs"])
def test_list_basic(shutdown_only, storage_type):
    with simulate_storage(storage_type) as storage_uri:
        ray.init(storage=storage_uri, num_gpus=1)
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


@pytest.mark.parametrize("storage_type", ["s3", "fs"])
def test_get_info_basic(shutdown_only, storage_type):
    with simulate_storage(storage_type) as storage_uri:
        ray.init(storage=storage_uri, num_gpus=1)
        client = storage.get_client("ns")
        client.put("foo/bar1", b"hello")
        assert client.get_info("foo/bar1").base_name == "bar1"
        assert client.get_info("foo/bar2") is None
        assert client.get_info("foo").base_name == "foo"
        assert client.get_info("").base_name == "ns"


@pytest.mark.parametrize("storage_type", ["s3", "fs"])
def test_connecting_to_cluster(shutdown_only, storage_type):
    with simulate_storage(storage_type) as storage_uri:
        try:
            subprocess.check_call(["ray", "start", "--head", "--storage", storage_uri])
            ray.init(address="auto")
            from ray._private.storage import _storage_uri

            # make sure driver is using the same storage when connecting to a cluster
            assert _storage_uri == storage_uri
        finally:
            subprocess.check_call(["ray", "stop"])


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
