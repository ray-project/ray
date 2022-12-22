import os
import random
import shutil
import socket
import string
import sys
import uuid
from filecmp import dircmp
from pathlib import Path
from shutil import copytree, make_archive, rmtree

import pytest

from ray._private.gcs_utils import GcsClient
from ray._private.ray_constants import KV_NAMESPACE_PACKAGE
from ray._private.runtime_env.packaging import (
    GCS_STORAGE_MAX_SIZE,
    MAC_OS_ZIP_HIDDEN_DIR_NAME,
    Protocol,
    _dir_travel,
    _get_excludes,
    _store_package_in_gcs,
    get_local_dir_from_uri,
    get_top_level_dir_from_compressed_package,
    get_uri_for_directory,
    get_uri_for_package,
    is_whl_uri,
    is_zip_uri,
    parse_uri,
    remove_dir_from_filepaths,
    unzip_package,
    upload_package_if_needed,
)
from ray.experimental.internal_kv import (
    _initialize_internal_kv,
    _internal_kv_del,
    _internal_kv_exists,
    _internal_kv_get,
    _internal_kv_reset,
)

TOP_LEVEL_DIR_NAME = "top_level"
ARCHIVE_NAME = "archive.zip"


def random_string(size: int = 10):
    return "".join(random.choice(string.ascii_uppercase) for _ in range(size))


@pytest.fixture
def random_dir(tmp_path) -> Path:
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    for _ in range(10):
        p1 = tmp_path / random_string(10)
        with p1.open("w") as f1:
            f1.write(random_string(100))
        p2 = tmp_path / random_string(10)
        with p2.open("w") as f2:
            f2.write(random_string(200))
    yield tmp_path


@pytest.fixture
def short_path_dir():
    """A directory with a short path.

    This directory is used to test the case where a socket file is in the
    directory.  Socket files have a maximum length of 108 characters, so the
    path from the built-in pytest fixture tmp_path is too long.
    """
    dir = Path("short_path")
    dir.mkdir()
    yield dir
    shutil.rmtree(str(dir))


@pytest.fixture
def random_zip_file_without_top_level_dir(random_dir):
    make_archive(
        random_dir / ARCHIVE_NAME[: ARCHIVE_NAME.rfind(".")], "zip", random_dir
    )
    yield str(random_dir / ARCHIVE_NAME)


@pytest.fixture
def random_zip_file_with_top_level_dir(tmp_path):
    path = tmp_path
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

    # Add __MACOSX directory. This is a hidden directory that is created by
    # macOS when zipping a directory.
    macos_dir = path / MAC_OS_ZIP_HIDDEN_DIR_NAME
    macos_dir.mkdir(parents=True)
    with (macos_dir / "file").open("w") as f:
        f.write("macos file")

    make_archive(
        path / ARCHIVE_NAME[: ARCHIVE_NAME.rfind(".")],
        "zip",
        path,
        TOP_LEVEL_DIR_NAME,
    )
    yield str(path / ARCHIVE_NAME)


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
        with open(random_dir / f"test_{random_string()}", "w") as f:
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
        hex_hash = uri.split("_")[-1][: -len(".zip")]
        assert len(hex_hash) == 16

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Unix sockets not available on windows",
    )
    def test_unopenable_files_skipped(self, random_dir, short_path_dir):
        """Test that unopenable files can be present in the working_dir.

        Some files such as `.sock` files are unopenable. This test ensures that
        we skip those files when generating the content hash. Previously this
        would raise an exception, see #25411.
        """

        # Create a socket file.
        sock = socket.socket(socket.AF_UNIX)
        sock.bind(str(short_path_dir / "test_socket"))

        # Check that opening the socket raises an exception.
        with pytest.raises(OSError):
            (short_path_dir / "test_socket").open()

        # Check that the hash can still be generated without errors.
        get_uri_for_directory(short_path_dir)


class TestUploadPackageIfNeeded:
    def test_create_upload_once(self, tmp_path, random_dir, ray_start_regular):
        uri = get_uri_for_directory(random_dir)
        uploaded = upload_package_if_needed(uri, tmp_path, random_dir)
        assert uploaded
        assert _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)

        uploaded = upload_package_if_needed(uri, tmp_path, random_dir)
        assert not uploaded
        assert _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)

        # Delete the URI from the internal_kv. This should trigger re-upload.
        _internal_kv_del(uri, namespace=KV_NAMESPACE_PACKAGE)
        assert not _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)
        uploaded = upload_package_if_needed(uri, tmp_path, random_dir)
        assert uploaded


class TestStorePackageInGcs:
    class DisconnectedClient(GcsClient):
        """Mock GcsClient that fails cannot put in the GCS."""

        def __init__(self, *args, **kwargs):
            pass

        def internal_kv_put(self, *args, **kwargs):
            raise RuntimeError("Cannot reach GCS!")

    def raise_runtime_error(self, *args, **kwargs):
        raise RuntimeError("Raised a runtime error!")

    def test_upload_succeeds(self, ray_start_regular):
        """Check function behavior when upload succeeds."""

        uri = "gcs://test.zip"
        bytes = b"test"

        assert len(bytes) < GCS_STORAGE_MAX_SIZE
        assert not _internal_kv_exists(uri, namespace=KV_NAMESPACE_PACKAGE)
        assert _store_package_in_gcs(uri, bytes) == len(bytes)
        assert bytes == _internal_kv_get(uri, namespace=KV_NAMESPACE_PACKAGE)

    def test_upload_fails(self):
        """Check that function throws useful error when upload fails."""

        uri = "gcs://test.zip"
        bytes = b"test"

        assert len(bytes) < GCS_STORAGE_MAX_SIZE

        _internal_kv_reset()
        _initialize_internal_kv(self.DisconnectedClient())
        with pytest.raises(RuntimeError, match="Failed to store package in the GCS"):
            _store_package_in_gcs(uri, bytes)

    def test_package_size_too_large(self):
        """Check that function throws useful error when package is too large."""

        uri = "gcs://test.zip"
        bytes = b"a" * (GCS_STORAGE_MAX_SIZE + 1)

        with pytest.raises(ValueError, match="Package size"):
            _store_package_in_gcs(uri, bytes)


class TestGetTopLevelDirFromCompressedPackage:
    def test_get_top_level_valid(self, random_zip_file_with_top_level_dir):
        top_level_dir_name = get_top_level_dir_from_compressed_package(
            str(random_zip_file_with_top_level_dir)
        )
        assert top_level_dir_name == TOP_LEVEL_DIR_NAME

    def test_get_top_level_invalid(self, random_zip_file_without_top_level_dir):
        top_level_dir_name = get_top_level_dir_from_compressed_package(
            str(random_zip_file_without_top_level_dir)
        )
        assert top_level_dir_name is None


class TestRemoveDirFromFilepaths:
    def test_valid_removal(self, random_zip_file_with_top_level_dir):
        # This test copies the TOP_LEVEL_DIR_NAME directory, and then it
        # shifts the contents of the copied directory into the base tmp_path
        # directory. Then it compares the contents of tmp_path with the
        # TOP_LEVEL_DIR_NAME directory to ensure that they match.

        archive_path = random_zip_file_with_top_level_dir
        tmp_path = archive_path[: archive_path.rfind(os.path.sep)]
        original_dir_path = os.path.join(tmp_path, TOP_LEVEL_DIR_NAME)
        copy_dir_path = os.path.join(tmp_path, TOP_LEVEL_DIR_NAME + "_copy")
        copytree(original_dir_path, copy_dir_path)
        remove_dir_from_filepaths(tmp_path, TOP_LEVEL_DIR_NAME + "_copy")
        dcmp = dircmp(tmp_path, os.path.join(tmp_path, TOP_LEVEL_DIR_NAME))

        # Since this test uses the tmp_path as the target directory, and since
        # the tmp_path also contains the zip file and the top level directory,
        # make sure that the only difference between the tmp_path's contents
        # and the top level directory's contents are the zip file from the
        # Pytest fixture and the top level directory itself. This implies that
        # all files have been extracted from the top level directory and moved
        # into the tmp_path.
        assert set(dcmp.left_only) == {
            ARCHIVE_NAME,
            TOP_LEVEL_DIR_NAME,
            MAC_OS_ZIP_HIDDEN_DIR_NAME,
        }

        # Make sure that all the subdirectories and files have been moved to
        # the target directory
        assert len(dcmp.right_only) == 0


@pytest.mark.parametrize("remove_top_level_directory", [False, True])
@pytest.mark.parametrize("unlink_zip", [False, True])
class TestUnzipPackage:
    def dcmp_helper(
        self, remove_top_level_directory, unlink_zip, tmp_subdir, tmp_path, archive_path
    ):
        dcmp = None
        if remove_top_level_directory:
            dcmp = dircmp(tmp_subdir, os.path.join(tmp_path, TOP_LEVEL_DIR_NAME))
        else:
            dcmp = dircmp(
                os.path.join(tmp_subdir, TOP_LEVEL_DIR_NAME),
                os.path.join(tmp_path, TOP_LEVEL_DIR_NAME),
            )
        assert len(dcmp.left_only) == 0
        assert len(dcmp.right_only) == 0

        if unlink_zip:
            assert not Path(archive_path).is_file()
        else:
            assert Path(archive_path).is_file()

    def test_unzip_package(
        self, random_zip_file_with_top_level_dir, remove_top_level_directory, unlink_zip
    ):
        archive_path = random_zip_file_with_top_level_dir
        tmp_path = archive_path[: archive_path.rfind(os.path.sep)]
        tmp_subdir = os.path.join(tmp_path, TOP_LEVEL_DIR_NAME + "_tmp")

        unzip_package(
            package_path=archive_path,
            target_dir=tmp_subdir,
            remove_top_level_directory=remove_top_level_directory,
            unlink_zip=unlink_zip,
        )

        self.dcmp_helper(
            remove_top_level_directory, unlink_zip, tmp_subdir, tmp_path, archive_path
        )

    def test_unzip_with_matching_subdirectory_names(
        self,
        remove_top_level_directory,
        unlink_zip,
        tmp_path,
    ):
        path = tmp_path
        top_level_dir = path / TOP_LEVEL_DIR_NAME
        top_level_dir.mkdir(parents=True)
        next_level_dir = top_level_dir
        for _ in range(10):
            dir1 = next_level_dir / TOP_LEVEL_DIR_NAME
            dir1.mkdir(parents=True)
            next_level_dir = dir1
        make_archive(
            path / ARCHIVE_NAME[: ARCHIVE_NAME.rfind(".")],
            "zip",
            path,
            TOP_LEVEL_DIR_NAME,
        )
        archive_path = str(path / ARCHIVE_NAME)

        tmp_path = archive_path[: archive_path.rfind(os.path.sep)]
        tmp_subdir = os.path.join(tmp_path, TOP_LEVEL_DIR_NAME + "_tmp")

        unzip_package(
            package_path=archive_path,
            target_dir=tmp_subdir,
            remove_top_level_directory=remove_top_level_directory,
            unlink_zip=unlink_zip,
        )

        self.dcmp_helper(
            remove_top_level_directory,
            unlink_zip,
            tmp_subdir,
            tmp_path,
            archive_path,
        )

    def test_unzip_package_with_multiple_top_level_dirs(
        self,
        remove_top_level_directory,
        unlink_zip,
        random_zip_file_without_top_level_dir,
    ):
        """Test unzipping a package with multiple top level directories (not counting __MACOSX).

        Tests that we don't remove the top level directory, regardless of the
        value of remove_top_level_directory.
        """
        archive_path = random_zip_file_without_top_level_dir
        tmp_path = archive_path[: archive_path.rfind(os.path.sep)]
        target_dir = os.path.join(tmp_path, "target_dir")
        print(os.listdir(tmp_path))

        # tmp_path
        # ├── target_dir
        # └── archive.zip

        unzip_package(
            package_path=archive_path,
            target_dir=target_dir,
            remove_top_level_directory=remove_top_level_directory,
            unlink_zip=unlink_zip,
        )
        print(os.listdir(target_dir))
        dcmp = dircmp(tmp_path, target_dir)
        print(dcmp.report())
        # assert False
        assert dcmp.left_only == ["target_dir"]
        # A side effect of the test structure is that archive.zip is itself
        # added to the zip file because it is in the same directory we're zipping.
        assert dcmp.right_only == ([ARCHIVE_NAME] if unlink_zip else [])

        if unlink_zip:
            assert not Path(archive_path).is_file()
        else:
            assert Path(archive_path).is_file()


class TestParseUri:
    @pytest.mark.parametrize(
        "parsing_tuple",
        [
            ("gcs://file.zip", Protocol.GCS, "file.zip"),
            ("s3://bucket/file.zip", Protocol.S3, "s3_bucket_file.zip"),
            ("https://test.com/file.zip", Protocol.HTTPS, "https_test_com_file.zip"),
            ("gs://bucket/file.zip", Protocol.GS, "gs_bucket_file.zip"),
            ("hdfs://namenode/file.zip", Protocol.HDFS, "hdfs_namenode_file.zip"),
        ],
    )
    def test_parsing_basic(self, parsing_tuple):
        uri, protocol, package_name = parsing_tuple
        parsed_protocol, parsed_package_name = parse_uri(uri)

        assert protocol == parsed_protocol
        assert package_name == parsed_package_name

    @pytest.mark.parametrize(
        "parsing_tuple",
        [
            (
                "https://username:PAT@github.com/repo/archive/commit_hash.zip",
                "https_username_PAT_github_com_repo_archive_commit_hash.zip",
            ),
            (
                (
                    "https://un:pwd@gitlab.com/user/repo/-/"
                    "archive/commit_hash/repo-commit_hash.zip"
                ),
                (
                    "https_un_pwd_gitlab_com_user_repo_-_"
                    "archive_commit_hash_repo-commit_hash.zip"
                ),
            ),
        ],
    )
    def test_parse_private_git_https_uris(self, parsing_tuple):
        raw_uri, parsed_uri = parsing_tuple
        parsed_protocol, parsed_package_name = parse_uri(raw_uri)
        assert parsed_protocol == Protocol.HTTPS
        assert parsed_package_name == parsed_uri


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_travel(tmp_path):
    dir_paths = set()
    file_paths = set()
    item_num = 0
    excludes = []
    root = tmp_path / "test"

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
            v = random.randint(0, 1000)
            with (path / uid).open("w") as f:
                f.write(str(v))
            if not excluded:
                if random.randint(0, 5) == 0:
                    excludes.append(str((path / uid).relative_to(root)))
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


def test_is_whl_uri():
    assert is_whl_uri("gcs://my-package.whl")
    assert not is_whl_uri("gcs://asdf.zip")
    assert not is_whl_uri("invalid_format")


def test_is_zip_uri():
    assert is_zip_uri("s3://my-package.zip")
    assert is_zip_uri("gcs://asdf.zip")
    assert not is_zip_uri("invalid_format")
    assert not is_zip_uri("gcs://a.whl")


def test_get_uri_for_package():
    assert get_uri_for_package(Path("/tmp/my-pkg.whl")) == "gcs://my-pkg.whl"


def test_get_local_dir_from_uri():
    uri = "gcs://<working_dir_content_hash>.zip"
    assert get_local_dir_from_uri(uri, "base_dir") == Path(
        "base_dir/<working_dir_content_hash>"
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
