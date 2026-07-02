import os
import sys
import types

import pytest

from ray._private import ray_constants
from ray._private.log_monitor import LogFileInfo, LogMonitor


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


class SentinelException(Exception):
    pass


def test_log_monitor_passive_publish_suppression(tmp_path, monkeypatch):
    """Verify that LogMonitor suppresses log publishing when GCS is in passive mode."""
    log_path = tmp_path / "worker-0-1234.out"
    with open(log_path, "w") as f:
        print("Test log message line", file=f)

    file_info = _create_file_info(log_path)
    file_info.worker_pid = 1234
    file_info.job_id = "01000000"
    file_info.is_err_file = False
    file_info.actor_name = None
    file_info.task_name = None
    file_info.reopen_if_necessary()

    mock_gcs_client = types.SimpleNamespace()
    # Simulate GCS running in passive mode on this head node
    mock_gcs_client.is_gcs_leader = lambda: False

    called = {"publish": False}

    def mock_publish_logs(data):
        called["publish"] = True

    mock_gcs_client.publish_logs = mock_publish_logs

    log_monitor = LogMonitor.__new__(LogMonitor)
    log_monitor.ip = "127.0.0.1"
    log_monitor.logs_dir = str(tmp_path)
    log_monitor.gcs_client = mock_gcs_client
    log_monitor.open_file_infos = [file_info]
    log_monitor.closed_file_infos = []
    log_monitor.max_files_open = 1000
    log_monitor.log_filenames = set()

    # Enable leader election config
    monkeypatch.setattr(ray_constants, "RAY_LEADER_ELECT", True)

    def mock_sleep(seconds):
        raise SentinelException("Loop suppressed")

    import ray._private.log_monitor as log_monitor_module

    monkeypatch.setattr(log_monitor_module.time, "sleep", mock_sleep)

    log_monitor.should_update_filenames = lambda last: False
    log_monitor.open_closed_files = lambda: None

    with pytest.raises(SentinelException):
        log_monitor.run()

    # Verify log publishing to GCS was suppressed in passive mode
    assert not called["publish"]
    file_info.file_handle.close()


def test_log_monitor_active_publish_resume(tmp_path, monkeypatch):
    """Verify that LogMonitor publishes logs when GCS is in active mode."""
    log_path = tmp_path / "worker-0-5678.out"
    with open(log_path, "w") as f:
        print("Active log message line", file=f)

    file_info = _create_file_info(log_path)
    file_info.worker_pid = 5678
    file_info.job_id = "01000000"
    file_info.is_err_file = False
    file_info.actor_name = None
    file_info.task_name = None
    file_info.reopen_if_necessary()

    mock_gcs_client = types.SimpleNamespace()
    # Simulate GCS running in active leader mode on this head node
    mock_gcs_client.is_gcs_leader = lambda: True

    called = {"publish": False}

    def mock_publish_logs(data):
        called["publish"] = True
        assert data["lines"] == ["Active log message line\n"]

    mock_gcs_client.publish_logs = mock_publish_logs

    log_monitor = LogMonitor.__new__(LogMonitor)
    log_monitor.ip = "127.0.0.1"
    log_monitor.logs_dir = str(tmp_path)
    log_monitor.gcs_client = mock_gcs_client
    log_monitor.open_file_infos = [file_info]
    log_monitor.closed_file_infos = []
    log_monitor.max_files_open = 1000
    log_monitor.log_filenames = set()

    # Enable leader election config
    monkeypatch.setattr(ray_constants, "RAY_LEADER_ELECT", True)

    def mock_sleep(seconds):
        raise SentinelException("Loop active run finished")

    import ray._private.log_monitor as log_monitor_module

    monkeypatch.setattr(log_monitor_module.time, "sleep", mock_sleep)

    log_monitor.should_update_filenames = lambda last: False
    log_monitor.open_closed_files = lambda: None

    with pytest.raises(SentinelException):
        log_monitor.run()

    # Verify log publishing to GCS resumed in active mode
    assert called["publish"]
    file_info.file_handle.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
