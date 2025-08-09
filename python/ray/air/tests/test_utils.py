"""Test AIR internal utilities (under ray.air._internal)."""

import os

import pytest

import ray
from ray.air._internal.filelock import RAY_LOCKFILE_DIR, TempFileLock


def test_temp_file_lock(tmp_path, monkeypatch):
    """Test that the directory where temp file locks are saved can be configured
    via the env variable that configures the global Ray temp dir."""
    monkeypatch.setenv("RAY_TMPDIR", str(tmp_path))
    assert str(tmp_path) in ray._common.utils.get_user_temp_dir()
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
