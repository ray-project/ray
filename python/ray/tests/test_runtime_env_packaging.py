import os
from pathlib import Path
import random
from shutil import copytree, rmtree, make_archive
import string
import sys
import tempfile
from filecmp import dircmp
import uuid

import pytest
from ray.ray_constants import KV_NAMESPACE_PACKAGE
from ray.experimental.internal_kv import (_internal_kv_del,
                                          _internal_kv_exists)
from ray._private.runtime_env.packaging import (
    _dir_travel, get_uri_for_directory, _get_excludes,
    upload_package_if_needed, parse_uri, Protocol,
    get_top_level_dir_from_compressed_package, remove_dir_from_filepaths,
    unzip_package)

TOP_LEVEL_DIR_NAME = "top_level"
ARCHIVE_NAME = "archive.zip"


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


@pytest.fixture
def random_zip_file_without_top_level_dir(random_dir):
    path = Path(random_dir)
    make_archive(path / ARCHIVE_NAME[:ARCHIVE_NAME.rfind(".")], "zip", path)
    yield str(path / ARCHIVE_NAME)


@pytest.fixture
def random_zip_file_with_top_level_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        top_level_dir = path / TOP_LEVEL_DIR_NAME
        top_level_dir.mkdir(parents=True)
        next_level_dir = top_level_dir
        for _ in range(10):
            p1 = next_level_dir / random_string(10)
            with p1.open("w") as f1:
                f1.write(random_string(100))
            p2 = next_level_dir / random_string(10)
            with p2.open("w") as f2:
                f2.write(random_string(200))
            dir1 = next_level_dir / random_string(15)
            dir1.mkdir(parents=True)
            dir2 = next_level_dir / random_string(15)
            dir2.mkdir(parents=True)
            next_level_dir = dir2
        make_archive(path / ARCHIVE_NAME[:ARCHIVE_NAME.rfind(".")], "zip",
                     path, TOP_LEVEL_DIR_NAME)
        yield str(path / ARCHIVE_NAME)


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
class TestGetTopLevelDirFromCompressedPackage:
    def test_get_top_level_valid(self, random_zip_file_with_top_level_dir):
        top_level_dir_name = get_top_level_dir_from_compressed_package(
            str(random_zip_file_with_top_level_dir))
        assert top_level_dir_name == TOP_LEVEL_DIR_NAME

    def test_get_top_level_invalid(self,
                                   random_zip_file_without_top_level_dir):
        top_level_dir_name = get_top_level_dir_from_compressed_package(
            str(random_zip_file_without_top_level_dir))
        assert top_level_dir_name is None


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
class TestRemoveDirFromFilepaths:
    def test_valid_removal(self, random_zip_file_with_top_level_dir):
        # This test copies the TOP_LEVEL_DIR_NAME directory, and then it
        # shifts the contents of the copied directory into the base tmp_path
        # directory. Then it compares the contents of tmp_path with the
        # TOP_LEVEL_DIR_NAME directory to ensure that they match.

        archive_path = random_zip_file_with_top_level_dir
        tmp_path = archive_path[:archive_path.rfind("/")]
        original_dir_path = os.path.join(tmp_path, TOP_LEVEL_DIR_NAME)
        copy_dir_path = os.path.join(tmp_path, TOP_LEVEL_DIR_NAME + "_copy")
        copytree(original_dir_path, copy_dir_path)
        remove_dir_from_filepaths(tmp_path, TOP_LEVEL_DIR_NAME + "_copy")
        dcmp = dircmp(tmp_path, f"{tmp_path}/{TOP_LEVEL_DIR_NAME}")

        # Since this test uses the tmp_path as the target directory, and since
        # the tmp_path also contains the zip file and the top level directory,
        # make sure that the only difference between the tmp_path's contents
        # and the top level directory's contents are the zip file from the
        # Pytest fixture and the top level directory itself. This implies that
        # all files have been extracted from the top level directory and moved
        # into the tmp_path.
        assert set(dcmp.left_only) == {ARCHIVE_NAME, TOP_LEVEL_DIR_NAME}

        # Make sure that all the subdirectories and files have been moved to
        # the target directory
        assert len(dcmp.right_only) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("remove_top_level_directory", [False, True])
@pytest.mark.parametrize("unlink_zip", [False, True])
class TestUnzipPackage:
    def dcmp_helper(self, remove_top_level_directory, unlink_zip, tmp_subdir,
                    tmp_path, archive_path):
        dcmp = None
        if remove_top_level_directory:
            dcmp = dircmp(f"{tmp_subdir}", f"{tmp_path}/{TOP_LEVEL_DIR_NAME}")
        else:
            dcmp = dircmp(f"{tmp_subdir}/{TOP_LEVEL_DIR_NAME}",
                          f"{tmp_path}/{TOP_LEVEL_DIR_NAME}")
        assert len(dcmp.left_only) == 0
        assert len(dcmp.right_only) == 0

        if unlink_zip:
            assert not Path(archive_path).is_file()
        else:
            assert Path(archive_path).is_file()

    def test_unzip_package(self, random_zip_file_with_top_level_dir,
                           remove_top_level_directory, unlink_zip):
        archive_path = random_zip_file_with_top_level_dir
        tmp_path = archive_path[:archive_path.rfind("/")]
        tmp_subdir = f"{tmp_path}/{TOP_LEVEL_DIR_NAME}_tmp"

        unzip_package(
            package_path=archive_path,
            target_dir=tmp_subdir,
            remove_top_level_directory=remove_top_level_directory,
            unlink_zip=unlink_zip)

        self.dcmp_helper(remove_top_level_directory, unlink_zip, tmp_subdir,
                         tmp_path, archive_path)

    def test_unzip_with_matching_subdirectory_names(
            self, remove_top_level_directory, unlink_zip):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            top_level_dir = path / TOP_LEVEL_DIR_NAME
            top_level_dir.mkdir(parents=True)
            next_level_dir = top_level_dir
            for _ in range(10):
                dir1 = next_level_dir / TOP_LEVEL_DIR_NAME
                dir1.mkdir(parents=True)
                next_level_dir = dir1
            make_archive(path / ARCHIVE_NAME[:ARCHIVE_NAME.rfind(".")], "zip",
                         path, TOP_LEVEL_DIR_NAME)
            archive_path = str(path / ARCHIVE_NAME)

            tmp_path = archive_path[:archive_path.rfind("/")]
            tmp_subdir = f"{tmp_path}/{TOP_LEVEL_DIR_NAME}_tmp"

            unzip_package(
                package_path=archive_path,
                target_dir=tmp_subdir,
                remove_top_level_directory=remove_top_level_directory,
                unlink_zip=unlink_zip)

            self.dcmp_helper(remove_top_level_directory, unlink_zip,
                             tmp_subdir, tmp_path, archive_path)


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


@pytest.mark.parametrize(
    "parsing_tuple",
    [("gcs://file.zip", Protocol.GCS, "file.zip"),
     ("s3://bucket/file.zip", Protocol.S3, "s3_bucket_file.zip"),
     ("https://test.com/file.zip", Protocol.HTTPS, "https_test_com_file.zip"),
     ("gs://bucket/file.zip", Protocol.GS, "gs_bucket_file.zip")])
def test_parsing(parsing_tuple):
    uri, protocol, package_name = parsing_tuple
    parsed_protocol, parsed_package_name = parse_uri(uri)

    assert protocol == parsed_protocol
    assert package_name == parsed_package_name


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
