import os
import sys

import pytest
from ray._private.log_monitor import LogFileInfo


def _create_file_info(log_path):
    return LogFileInfo(
        filename=log_path,
        size_when_last_opened=0,
        file_position=0,
        file_handle=None,
        is_err_file=False,
        job_id=None,
        worker_pid=None,
    )


@pytest.mark.skipif(
    sys.platform == "win32", reason="Relies on POSIX truncate semantics"
)
def test_reopen_same_inode_truncation_seeks_beginning(tmp_path):
    """Truncating a file should rewind and reopen the reader on the same inode."""

    log_path = tmp_path / "worker.log"

    with open(log_path, "w") as f:
        for i in range(100):
            print(f"Log line {i}", file=f)

    file_info = _create_file_info(log_path)

    file_info.reopen_if_necessary()
    for i in range(50):
        line = file_info.file_handle.readline().strip()
        assert line == f"Log line {i}".encode("utf-8")

    original_inode = os.stat(log_path).st_ino
    original_position = file_info.file_handle.tell()
    file_info.file_position = original_position

    with open(log_path, "w") as f:
        print("Truncated log line 0", file=f)

    assert os.stat(log_path).st_ino == original_inode
    assert os.path.getsize(log_path) < original_position

    file_info.reopen_if_necessary()

    assert file_info.file_position == 0
    assert file_info.file_handle.tell() == 0
    assert file_info.size_when_last_opened == os.path.getsize(log_path)
    assert file_info.file_handle.readline().strip() == b"Truncated log line 0"
    file_info.file_handle.close()


@pytest.mark.skipif(
    sys.platform == "win32", reason="Relies on POSIX truncate semantics"
)
def test_reopen_same_inode_truncation_with_rewrite_larger_than_position(tmp_path):
    """Truncation should rewind even if rewritten content exceeds the old position."""

    log_path = tmp_path / "worker.log"

    with open(log_path, "w") as f:
        for i in range(100):
            print(f"Old log line {i}", file=f)

    file_info = _create_file_info(log_path)
    file_info.reopen_if_necessary()

    for i in range(10):
        line = file_info.file_handle.readline().strip()
        assert line == f"Old log line {i}".encode("utf-8")

    original_inode = os.stat(log_path).st_ino
    original_position = file_info.file_handle.tell()
    original_size = file_info.size_when_last_opened
    file_info.file_position = original_position

    with open(log_path, "w") as f:
        for i in range(20):
            print(f"New log line {i}", file=f)

    new_size = os.path.getsize(log_path)
    assert os.stat(log_path).st_ino == original_inode
    assert original_position < new_size < original_size

    file_info.reopen_if_necessary()

    assert file_info.file_position == 0
    assert file_info.file_handle.tell() == 0
    assert file_info.size_when_last_opened == new_size
    assert file_info.file_handle.readline().strip() == b"New log line 0"
    file_info.file_handle.close()


def test_reopen_same_inode_growth_keeps_size_when_last_opened(tmp_path):
    """Growing a file in place should not hide unread data after a close/reopen cycle."""

    log_path = tmp_path / "worker.log"

    with open(log_path, "w") as f:
        for i in range(20):
            print(f"Log line {i}", file=f)

    file_info = _create_file_info(log_path)
    file_info.reopen_if_necessary()
    original_size = file_info.size_when_last_opened

    for i in range(5):
        line = file_info.file_handle.readline().strip()
        assert line == f"Log line {i}".encode("utf-8")

    file_info.file_position = file_info.file_handle.tell()

    with open(log_path, "a") as f:
        for i in range(20, 30):
            print(f"Log line {i}", file=f)

    assert os.path.getsize(log_path) > original_size

    file_info.reopen_if_necessary()

    assert file_info.size_when_last_opened == original_size
    file_info.file_handle.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
