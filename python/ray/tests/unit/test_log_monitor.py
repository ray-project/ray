import logging
import os
import sys
from unittest.mock import MagicMock

import pytest

import ray._private.ray_constants as ray_constants
from ray._private import log_monitor as log_monitor_module
from ray._private.log_monitor import (
    PASSIVE_GCS_POLL_INTERVAL_S,
    LogFileInfo,
    LogMonitor,
)
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.exceptions import GcsPassiveError, RpcError

LOG_MONITOR_LOGGER = "ray._private.log_monitor"


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


def _passive_gcs_rejection():
    """The error a passive GCS raises, as check_status() translates it."""
    return GcsPassiveError(
        "GCS server is in passive (read-only) mode.",
        rpc_code=GRPC_STATUS_CODE_UNAVAILABLE,
    )


@pytest.fixture
def make_log_monitor(tmp_path, monkeypatch):
    monitors = []

    def factory(*, leader_election=True):
        monkeypatch.setattr(
            ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", leader_election
        )
        monitor = LogMonitor(
            node_ip_address="127.0.0.1",
            logs_dir=str(tmp_path),
            gcs_client=MagicMock(),
            is_proc_alive_fn=lambda pid: True,
        )
        monitors.append(monitor)
        return monitor

    yield factory
    for monitor in monitors:
        for file_info in monitor.open_file_infos:
            if file_info.file_handle is not None:
                file_info.file_handle.close()


@pytest.fixture
def log_monitor(make_log_monitor):
    return make_log_monitor()


def _write_worker_log(monitor, line):
    with open(os.path.join(monitor.logs_dir, "worker-01000000-1234.out"), "a") as f:
        print(line, file=f)


def _log_and_drain(monitor, line):
    """Write one worker log line and run a single pass of the monitor's loop."""
    _write_worker_log(monitor, line)
    monitor.update_log_filenames()
    monitor.open_closed_files()
    return monitor.check_log_files_and_publish_updates()


def _published_lines(monitor):
    return [
        line
        for call in monitor.gcs_client.publish_logs.call_args_list
        for line in call.args[0]["lines"]
    ]


@pytest.fixture
def monitor_logs(caplog):
    """Capture this module's logs.

    The "ray" logger does not propagate to the root logger caplog listens on, so
    caplog.at_level() alone records nothing.
    """
    logger = logging.getLogger(LOG_MONITOR_LOGGER)
    logger.addHandler(caplog.handler)
    with caplog.at_level(logging.INFO, logger=LOG_MONITOR_LOGGER):
        yield caplog
    logger.removeHandler(caplog.handler)


def _count_logged(caplog, needle):
    return sum(needle in record.getMessage() for record in caplog.records)


def test_passive_head_drops_log_lines_instead_of_replaying_them_on_promotion(
    log_monitor,
):
    # Nothing subscribes to a passive GCS, so a backlog would only flood drivers
    # with stale lines the moment this head is promoted.
    log_monitor.gcs_client.publish_logs.side_effect = _passive_gcs_rejection()
    assert _log_and_drain(log_monitor, "written while passive") is True

    log_monitor.gcs_client.publish_logs.side_effect = None
    assert _log_and_drain(log_monitor, "written after promotion") is True

    delivered = log_monitor.gcs_client.publish_logs.call_args_list[-1].args[0]
    assert delivered["lines"] == ["written after promotion"]


def test_a_passive_gcs_is_reported_once_not_once_per_batch(log_monitor, monitor_logs):
    log_monitor.gcs_client.publish_logs.side_effect = _passive_gcs_rejection()

    for i in range(3):
        _log_and_drain(log_monitor, f"line {i}")

    assert len(_published_lines(log_monitor)) == 3
    assert _count_logged(monitor_logs, "GCS is in passive mode") == 1


def test_each_leadership_change_is_reported_once(log_monitor, monitor_logs):
    log_monitor.gcs_client.publish_logs.side_effect = _passive_gcs_rejection()
    _log_and_drain(log_monitor, "passive")
    _log_and_drain(log_monitor, "still passive")

    log_monitor.gcs_client.publish_logs.side_effect = None
    _log_and_drain(log_monitor, "promoted")
    _log_and_drain(log_monitor, "still active")

    log_monitor.gcs_client.publish_logs.side_effect = _passive_gcs_rejection()
    _log_and_drain(log_monitor, "demoted")

    assert _count_logged(monitor_logs, "GCS is in passive mode") == 2
    assert _count_logged(monitor_logs, "Resuming publishing") == 1
    assert log_monitor._publish_passive_latch.waiting_for_promotion


class _StopLoop(Exception):
    pass


@pytest.mark.parametrize(
    "passive, expected_sleep",
    [(True, PASSIVE_GCS_POLL_INTERVAL_S), (False, 0.1)],
)
def test_poll_interval_adapts_to_leadership(
    log_monitor, monkeypatch, passive, expected_sleep
):
    if passive:
        log_monitor.gcs_client.publish_logs.side_effect = _passive_gcs_rejection()
    else:
        log_monitor.gcs_client.publish_logs.side_effect = None
        log_monitor._publish_passive_latch.waiting_for_promotion = False

    _write_worker_log(log_monitor, "line")

    slept = []

    def stop_at_first_sleep(seconds):
        slept.append(seconds)
        raise _StopLoop()

    monkeypatch.setattr(log_monitor_module.time, "sleep", stop_at_first_sleep)

    with pytest.raises(_StopLoop):
        log_monitor.run()

    assert slept == [expected_sleep]
    if passive:
        # Backing off must not turn into hoarding: the line was still consumed.
        assert _published_lines(log_monitor) == ["line"]


def test_other_publish_failures_are_still_reported(log_monitor, monitor_logs):
    # An unreachable GCS also yields UNAVAILABLE; only the passive one is benign.
    log_monitor.gcs_client.publish_logs.side_effect = RpcError(
        "Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )

    _log_and_drain(log_monitor, "written while the GCS is down")

    assert _count_logged(monitor_logs, "Failed to publish log messages") == 1
    assert not log_monitor._publish_passive_latch.waiting_for_promotion


def test_the_feature_flag_makes_the_passive_handling_unreachable(
    make_log_monitor, monitor_logs, monkeypatch
):
    monitor = make_log_monitor(leader_election=False)
    monitor.gcs_client.publish_logs.side_effect = _passive_gcs_rejection()
    _write_worker_log(monitor, "written on a cluster without leader election")

    slept = []

    def stop_at_first_sleep(seconds):
        slept.append(seconds)
        raise _StopLoop()

    monkeypatch.setattr(log_monitor_module.time, "sleep", stop_at_first_sleep)

    with pytest.raises(_StopLoop):
        monitor.run()

    # Every new branch stays out of the way: generic handling, no latch, no backoff.
    assert _count_logged(monitor_logs, "Failed to publish log messages") == 1
    assert _count_logged(monitor_logs, "GCS is in passive mode") == 0
    assert not monitor._publish_passive_latch.waiting_for_promotion
    assert slept == [0.1]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
