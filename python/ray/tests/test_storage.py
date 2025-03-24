# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import urllib
from pathlib import Path

from packaging.version import parse as parse_version
import pyarrow.fs
import pytest

import ray
import ray._private.storage as storage
from ray._private.test_utils import simulate_storage
from ray._private.arrow_utils import (
    add_creatable_buckets_param_if_s3_uri,
    get_pyarrow_version,
)
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


def test_storage_not_set(shutdown_only):
    ray.init()
    with pytest.raises(
        RuntimeError, match=r".*No storage URI has been configured for the cluster.*"
    ):
        fs, prefix = storage.get_filesystem()


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


def test_escape_storage_uri_with_runtime_env(shutdown_only):
    # https://github.com/ray-project/ray/issues/41568
    # Test to make sure we can successfully start worker process
    # when storage uri contains ?,& and we use runtime env and that the
    # moto mocking actually works with the escaped uri
    with simulate_storage("s3") as s3_uri:
        assert "?" in s3_uri
        assert "&" in s3_uri
        ray.init(storage=s3_uri, runtime_env={"env_vars": {"TEST_ENV": "1"}})

        client = storage.get_client("foo")
        client.put("bar", b"baz")

        @ray.remote
        def f():
            client = storage.get_client("foo")
            return client.get("bar")

        assert ray.get(f.remote()) == b"baz"


def test_storage_uri_semicolon(shutdown_only):
    with simulate_storage("s3") as s3_uri:
        # test that ';' can be used instead of '&'
        s3_uri.replace("&", ";")
        ray.init(storage=s3_uri, runtime_env={"env_vars": {"TEST_ENV": "1"}})
        client = storage.get_client("foo")
        client.put("bar", b"baz")

        @ray.remote
        def f():
            client = storage.get_client("foo")
            return client.get("bar")

        assert ray.get(f.remote()) == b"baz"


def test_storage_uri_special(shutdown_only):
    # Test various non-ascii characters that can appear in a URI
    # test that '$', '+', ' ' are passed through
    with simulate_storage("s3", region="$value+value value") as s3_uri:
        ray.init(storage=s3_uri, runtime_env={"env_vars": {"TEST_ENV": "1"}})
        client = storage.get_client("foo")
        # url parsing: '+' becomes ' '
        assert client.fs.region == "$value value value"
        client.put("bar", b"baz")

        @ray.remote
        def f():
            client = storage.get_client("foo")
            return client.get("bar").decode() + ";" + client.fs.region

        assert ray.get(f.remote()) == "baz;$value value value"


def test_storage_uri_unicode(shutdown_only):
    # test unicode characters in URI
    with simulate_storage("s3", region="üs-öst-2") as s3_uri:
        ray.init(storage=s3_uri, runtime_env={"env_vars": {"TEST_ENV": "1"}})
        client = storage.get_client("foo")
        client.put("bar", b"baz")

        @ray.remote
        def f():
            client = storage.get_client("foo")
            return client.get("bar")

        assert ray.get(f.remote()) == b"baz"


def test_get_filesystem_invalid(shutdown_only, tmp_path):
    with pytest.raises(pyarrow.lib.ArrowInvalid):
        ray.init(storage="blahblah://bad")


@pytest.mark.skipif(
    sys.platform == "win32", reason="Fails on Windows + Deprecating storage"
)
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
        with pytest.raises(
            FileNotFoundError if storage_type == "s3" else NotADirectoryError
        ):
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


def test_add_creatable_buckets_param_if_s3_uri():
    if get_pyarrow_version() >= parse_version("9.0.0"):
        # Test that the allow_bucket_creation=true query arg is added to an S3 URI.
        uri = "s3://bucket/foo"
        assert (
            add_creatable_buckets_param_if_s3_uri(uri)
            == "s3://bucket/foo?allow_bucket_creation=true"
        )

        # Test that query args are merged (i.e. existing query args aren't dropped).
        uri = "s3://bucket/foo?bar=baz"
        assert (
            add_creatable_buckets_param_if_s3_uri(uri)
            == "s3://bucket/foo?allow_bucket_creation=true&bar=baz"
        )

        # Test that existing allow_bucket_creation=false query arg isn't overridden.
        uri = "s3://bucket/foo?allow_bucket_creation=false"
        assert (
            add_creatable_buckets_param_if_s3_uri(uri)
            == "s3://bucket/foo?allow_bucket_creation=false"
        )
    else:
        # Test that the allow_bucket_creation=true query arg is not added to an S3 URI,
        # since we're using Arrow < 9.
        uri = "s3://bucket/foo"
        assert add_creatable_buckets_param_if_s3_uri(uri) == uri

    # Test that non-S3 URI is unchanged.
    uri = "gcs://bucket/foo"
    assert add_creatable_buckets_param_if_s3_uri(uri) == "gcs://bucket/foo"


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
