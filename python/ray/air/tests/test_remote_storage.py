import os
import threading
from unittest.mock import patch

import pytest
import shutil
import tempfile

from ray.air._internal.remote_storage import (
    upload_to_uri,
    download_from_uri,
)
from ray.tune.utils.file_transfer import _get_recursive_files_and_stats


@pytest.fixture
def temp_data_dirs():
    tmp_source = os.path.realpath(tempfile.mkdtemp())
    tmp_target = os.path.realpath(tempfile.mkdtemp())

    os.makedirs(os.path.join(tmp_source, "subdir", "nested"))
    os.makedirs(os.path.join(tmp_source, "subdir_exclude", "something"))

    files = [
        "level0.txt",
        "level0_exclude.txt",
        "subdir/level1.txt",
        "subdir/level1_exclude.txt",
        "subdir/nested/level2.txt",
        "subdir_nested_level2_exclude.txt",
        "subdir_exclude/something/somewhere.txt",
    ]

    for file in files:
        with open(os.path.join(tmp_source, file), "w") as f:
            f.write("Data")

    yield tmp_source, tmp_target

    shutil.rmtree(tmp_source)
    shutil.rmtree(tmp_target)


def assert_file(exists: bool, root: str, path: str):
    full_path = os.path.join(root, path)

    if exists:
        assert os.path.exists(full_path)
    else:
        assert not os.path.exists(full_path)


def test_upload_no_exclude(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    upload_to_uri(tmp_source, "memory:///target/dir1")
    download_from_uri("memory:///target/dir1", tmp_target)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_upload_exclude_files(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    upload_to_uri(tmp_source, "memory:///target/dir2", exclude=["*_exclude.txt"])
    download_from_uri("memory:///target/dir2", tmp_target)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_upload_exclude_dirs(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    upload_to_uri(tmp_source, "memory:///target/dir3", exclude=["*_exclude/*"])
    download_from_uri("memory:///target/dir3", tmp_target)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_upload_exclude_multi(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    upload_to_uri(
        tmp_source, "memory:///target/dir4", exclude=["*_exclude.txt", "*_exclude/*"]
    )
    download_from_uri("memory:///target/dir4", tmp_target)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_upload_exclude_multimatch(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    upload_to_uri(tmp_source, "memory:///target/dir5", exclude=["*_exclude*"])
    download_from_uri("memory:///target/dir5", tmp_target)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_get_recursive_files_race_con(temp_data_dirs):
    tmp_source, _ = temp_data_dirs

    def run(event):
        lst = os.lstat

        def waiting_lstat(*args, **kwargs):
            event.wait()
            return lst(*args, **kwargs)

        with patch("os.lstat", wraps=waiting_lstat):
            _get_recursive_files_and_stats(tmp_source)

    event = threading.Event()

    get_thread = threading.Thread(target=run, args=(event,))
    get_thread.start()

    os.remove(os.path.join(tmp_source, "level0.txt"))
    event.set()

    get_thread.join()

    assert_file(False, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "level0_exclude.txt")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
