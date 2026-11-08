"""Test AIR internal utilities (under ray.air._internal)."""

import os
from pathlib import Path

import pytest

import ray
from ray.air._internal.filelock import RAY_LOCKFILE_DIR, TempFileLock
from ray.air._internal.uri_utils import URI


def test_temp_file_lock(tmp_path, monkeypatch):
    """Test that the directory where temp file locks are saved can be configured
    via the env variable that configures the global Ray temp dir."""
    monkeypatch.setenv("RAY_TMPDIR", str(tmp_path))
    assert str(tmp_path) in ray._common.utils.get_default_system_temp_dir()
    with TempFileLock(path="abc.txt"):
        assert RAY_LOCKFILE_DIR in os.listdir(tmp_path)
        assert os.listdir(tmp_path / RAY_LOCKFILE_DIR)


def test_multiple_file_locks(tmp_path, monkeypatch):
    """Test that a new file lock is created for unique paths."""
    monkeypatch.setenv("RAY_TMPDIR", str(tmp_path))
    with TempFileLock(path="abc.txt"):
        with TempFileLock(path="subdir/abc.txt"):
            assert RAY_LOCKFILE_DIR in os.listdir(tmp_path)
            # We should have 2 locks, one for abc.txt and one for subdir/abc.txt
            assert len(os.listdir(tmp_path / RAY_LOCKFILE_DIR)) == 2


def test_uri_rstrip_subpath_preserves_matching_prefixes():
    """Test that URI.rstrip_subpath only strips the trailing subpath suffix."""
    uri = URI("s3://my-exp-bucket/exp/trial/exp?param=1")
    assert str(uri.rstrip_subpath(Path("exp"))) == "s3://my-exp-bucket/exp/trial?param=1"

    local_uri = URI("/tmp/a/b/a")
    assert str(local_uri.rstrip_subpath(Path("a"))) == "/tmp/a/b"

    with pytest.raises(AssertionError):
        _ = URI("/tmp/abc").rstrip_subpath(Path("bc"))


def test_uri_parent_raises_when_no_valid_parent():
    """Test that URI.parent raises AssertionError when parent is Path('.') or self."""
    s3_uri = URI("s3://bucket/a?param=1")
    assert str(s3_uri.parent) == "s3://bucket?param=1"
    with pytest.raises(AssertionError, match="has no valid parent URI"):
        _ = s3_uri.parent.parent

    with pytest.raises(AssertionError, match="has no valid parent URI"):
        _ = URI("relative_dir").parent

    with pytest.raises(AssertionError, match="has no valid parent URI"):
        _ = URI("/").parent


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
