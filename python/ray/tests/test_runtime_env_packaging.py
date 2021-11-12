import os
from pathlib import Path
import random
from shutil import rmtree
import string
import sys
import tempfile
import uuid

import pytest
from ray.ray_constants import KV_NAMESPACE_PACKAGE
from ray.experimental.internal_kv import (_internal_kv_del,
                                          _internal_kv_exists)
from ray._private.runtime_env.packaging import (
    _dir_travel, get_uri_for_directory, _get_excludes,
    upload_package_if_needed)


def random_string(size: int = 10):
    return "".join(random.choice(string.ascii_uppercase) for _ in range(size))


@pytest.fixture
def empty_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def random_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        subdir = path / "subdir"
        subdir.mkdir(parents=True)
        for _ in range(10):
            p1 = path / random_string(10)
            with p1.open("w") as f1:
                f1.write(random_string(100))
            p2 = path / random_string(10)
            with p2.open("w") as f2:
                f2.write(random_string(200))
        yield tmp_dir


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
class TestGetURIForDirectory:
    def test_invalid_directory(self):
        with pytest.raises(ValueError):
            get_uri_for_directory("/does/not/exist")

        with pytest.raises(ValueError):
            get_uri_for_directory("does/not/exist")

    def test_determinism(self, random_dir):
        # Check that it's deterministic for same data.
        uris = {get_uri_for_directory(random_dir) for _ in range(10)}
        assert len(uris) == 1

        # Add one file, should be different now.
        with open(Path(random_dir) / f"test_{random_string}", "w") as f:
            f.write(random_string())

        assert {get_uri_for_directory(random_dir)} != uris

    def test_relative_paths(self, random_dir):
        # Check that relative or absolute paths result in the same URI.
        p = Path(random_dir)
        relative_uri = get_uri_for_directory(os.path.relpath(p))
        absolute_uri = get_uri_for_directory(p.resolve())
        assert relative_uri == absolute_uri

    def test_excludes(self, random_dir):
        # Excluding a directory should modify the URI.
        included_uri = get_uri_for_directory(random_dir)
        excluded_uri = get_uri_for_directory(random_dir, excludes=["subdir"])
        assert included_uri != excluded_uri

        # Excluding a directory should be the same as deleting it.
        rmtree((Path(random_dir) / "subdir").resolve())
        deleted_uri = get_uri_for_directory(random_dir)
        assert deleted_uri == excluded_uri

    def test_empty_directory(self):
        try:
            os.mkdir("d1")
            os.mkdir("d2")
            assert get_uri_for_directory("d1") == get_uri_for_directory("d2")
        finally:
            os.rmdir("d1")
            os.rmdir("d2")

    def test_uri_hash_length(self, random_dir):
        uri = get_uri_for_directory(random_dir)
        hex_hash = uri.split("_")[-1][:-len(".zip")]
        assert len(hex_hash) == 16


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
class TestUploadPackageIfNeeded:
    def test_create_upload_once(self, empty_dir, random_dir,
                                ray_start_regular):
        uri = get_uri_for_directory(random_dir)
        uploaded = upload_package_if_needed(uri, empty_dir, random_dir)
        assert uploaded
        assert _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)

        uploaded = upload_package_if_needed(uri, empty_dir, random_dir)
        assert not uploaded
        assert _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)

        # Delete the URI from the internal_kv. This should trigger re-upload.
        _internal_kv_del(uri, namespace=KV_NAMESPACE_PACKAGE)
        assert not _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)
        uploaded = upload_package_if_needed(uri, empty_dir, random_dir)
        assert uploaded


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
def test_travel():
    with tempfile.TemporaryDirectory() as tmp_dir:
        dir_paths = set()
        file_paths = set()
        item_num = 0
        excludes = []
        root = Path(tmp_dir) / "test"

        def construct(path, excluded=False, depth=0):
            nonlocal item_num
            path.mkdir(parents=True)
            if not excluded:
                dir_paths.add(str(path))
            if depth > 8:
                return
            if item_num > 500:
                return
            dir_num = random.randint(0, 10)
            file_num = random.randint(0, 10)
            for _ in range(dir_num):
                uid = str(uuid.uuid4()).split("-")[0]
                dir_path = path / uid
                exclud_sub = random.randint(0, 5) == 0
                if not excluded and exclud_sub:
                    excludes.append(str(dir_path.relative_to(root)))
                if not excluded:
                    construct(dir_path, exclud_sub or excluded, depth + 1)
                item_num += 1
            if item_num > 1000:
                return

            for _ in range(file_num):
                uid = str(uuid.uuid4()).split("-")[0]
                with (path / uid).open("w") as f:
                    v = random.randint(0, 1000)
                    f.write(str(v))
                    if not excluded:
                        if random.randint(0, 5) == 0:
                            excludes.append(
                                str((path / uid).relative_to(root)))
                        else:
                            file_paths.add((str(path / uid), str(v)))
                item_num += 1

        construct(root)
        exclude_spec = _get_excludes(root, excludes)
        visited_dir_paths = set()
        visited_file_paths = set()

        def handler(path):
            if path.is_dir():
                visited_dir_paths.add(str(path))
            else:
                with open(path) as f:
                    visited_file_paths.add((str(path), f.read()))

        _dir_travel(root, [exclude_spec], handler)
        assert file_paths == visited_file_paths
        assert dir_paths == visited_dir_paths


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
