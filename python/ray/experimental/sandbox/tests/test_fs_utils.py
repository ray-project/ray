import sys

import pytest

from ray.experimental.sandbox._internal import fs_utils


def test_rmtree_removes_read_only_dirs(tmp_path):
    """Images like UBI ship 0555 directories, which keep even their owner
    from deleting what's in them. A directory its owner can't even list
    doesn't stop it either."""
    locked = tmp_path / "tree" / "usr" / "bin"
    locked.mkdir(parents=True)
    (locked / "tool").write_text("x")
    locked.chmod(0o555)
    hidden = tmp_path / "tree" / "hidden"
    (hidden / "deeper").mkdir(parents=True)
    (hidden / "deeper" / "file").write_text("x")
    hidden.chmod(0o000)

    fs_utils.rmtree(str(tmp_path / "tree"))
    assert not (tmp_path / "tree").exists()


def test_rmtree_reports_errors_like_shutil(tmp_path):
    """Like shutil.rmtree, it raises for a missing path unless told to
    ignore errors."""
    missing = str(tmp_path / "missing")
    with pytest.raises(FileNotFoundError):
        fs_utils.rmtree(missing)
    fs_utils.rmtree(missing, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
