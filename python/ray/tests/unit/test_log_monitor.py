import os
import sys
from unittest.mock import Mock

import pytest

from ray._private.log_monitor import LogFileInfo, LogMonitor


@pytest.mark.parametrize("is_autoscaler_v2", [False, True])
@pytest.mark.parametrize(
    "directory_name",
    [
        "logs",
        "logs[1]",
        pytest.param(
            "logs*",
            marks=pytest.mark.skipif(sys.platform == "win32", reason="Invalid path"),
        ),
        pytest.param(
            "logs?",
            marks=pytest.mark.skipif(sys.platform == "win32", reason="Invalid path"),
        ),
    ],
)
def test_log_monitor_treats_directory_as_literal(
    tmp_path, monkeypatch, directory_name, is_autoscaler_v2
):
    monkeypatch.setattr(
        "ray._private.log_monitor.RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED", True
    )
    filenames = {
        "worker-abc-01000000-123.out",
        "worker-abc-01000000-123.err",
        "java-worker-abc-01000000-124.log",
        "raylet.err",
        "gcs_server.err",
        "monitor.log",
        "events/event_AUTOSCALER.log",
        "tpu_logs/libtpu.log",
        "runtime_env_setup-01000000.log",
    }
    logs_dir = tmp_path / directory_name
    for directory in (logs_dir, tmp_path / "logs1"):
        for filename in filenames:
            path = directory / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"{filename}\n")
    expected = filenames - (
        {"monitor.log"} if is_autoscaler_v2 else {"events/event_AUTOSCALER.log"}
    )
    publisher = Mock()
    monitor = LogMonitor("127.0.0.1", str(logs_dir), publisher, lambda _: True)
    monitor.is_autoscaler_v2 = is_autoscaler_v2

    monitor.update_log_filenames()
    monitor.update_log_filenames()

    assert monitor.log_filenames == {str(logs_dir / name) for name in expected}
    try:
        monitor.open_closed_files()
        assert monitor.check_log_files_and_publish_updates()
        assert publisher.publish_logs.call_count == len(expected)
        assert {
            call.args[0]["lines"][0] for call in publisher.publish_logs.call_args_list
        } == expected
    finally:
        for file_info in monitor.open_file_infos:
            file_info.file_handle.close()


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
