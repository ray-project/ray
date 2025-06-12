import os
import shutil
import sys
import time
import uuid

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import check_call_ray


def unix_socket_create_path(name):
    unix = sys.platform != "win32"
    return os.path.join(ray._common.utils.get_user_temp_dir(), name) if unix else None


def unix_socket_verify(unix_socket):
    if sys.platform != "win32":
        assert os.path.exists(unix_socket), "Socket not found: " + unix_socket


def unix_socket_delete(unix_socket):
    unix = sys.platform != "win32"
    return os.remove(unix_socket) if unix else None


@pytest.fixture
def delete_default_temp_dir():
    def delete_default_temp_dir_once():
        shutil.rmtree(ray._common.utils.get_ray_temp_dir(), ignore_errors=True)
        return not os.path.exists(ray._common.utils.get_ray_temp_dir())

    wait_for_condition(delete_default_temp_dir_once)
    yield


def test_tempdir_created_successfully(delete_default_temp_dir, shutdown_only):
    temp_dir = os.path.join(ray._common.utils.get_user_temp_dir(), uuid.uuid4().hex)
    ray.init(_temp_dir=temp_dir)
    assert os.path.exists(temp_dir), "Specified temp dir not found."
    assert not os.path.exists(
        ray._common.utils.get_ray_temp_dir()
    ), "Default temp dir should not exist."
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_tempdir_commandline(delete_default_temp_dir):
    temp_dir = os.path.join(ray._common.utils.get_user_temp_dir(), uuid.uuid4().hex)
    check_call_ray(
        [
            "start",
            "--head",
            "--temp-dir=" + temp_dir,
            "--port",
            "0",
        ]
    )
    assert os.path.exists(temp_dir), "Specified temp dir not found."
    assert not os.path.exists(
        ray._common.utils.get_ray_temp_dir()
    ), "Default temp dir should not exist."
    check_call_ray(["stop"])
    shutil.rmtree(
        temp_dir,
        ignore_errors=True,
    )


def test_tempdir_long_path():
    if sys.platform != "win32":
        # Test AF_UNIX limits for sockaddr_un->sun_path on POSIX OSes
        maxlen = 104 if sys.platform.startswith("darwin") else 108
        temp_dir = os.path.join(ray._common.utils.get_user_temp_dir(), "z" * maxlen)
        with pytest.raises(OSError):
            ray.init(_temp_dir=temp_dir)  # path should be too long


def test_raylet_tempfiles(shutdown_only):
    expected_socket_files = (
        {"plasma_store", "raylet"} if sys.platform != "win32" else set()
    )

    ray.init(num_cpus=0)
    node = ray._private.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    log_files_expected = {
        "log_monitor.log",
        "monitor.log",
        "raylet.out",
        "raylet.err",
        "gcs_server.out",
        "gcs_server.err",
        "dashboard.log",
        "dashboard_agent.log",
    }

    def check_all_log_file_exists():
        log_files = set(os.listdir(node.get_logs_dir_path()))
        for expected in log_files_expected:
            if expected not in log_files:
                raise RuntimeError(f"File {expected} not found!")
        return True

    wait_for_condition(check_all_log_file_exists)
    # Get the list of log files again since the previous one
    # might have the stale information.
    log_files = set(os.listdir(node.get_logs_dir_path()))
    assert log_files_expected.issubset(log_files)
    assert log_files.issuperset(log_files_expected)

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files.issuperset(expected_socket_files)
    ray.shutdown()

    ray.init(num_cpus=2)
    node = ray._private.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    time.sleep(3)  # wait workers to start
    log_files = set(os.listdir(node.get_logs_dir_path()))

    assert log_files.issuperset(log_files_expected)

    # Check numbers of worker log file.
    assert sum(1 for filename in log_files if filename.startswith("worker")) == 4

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files.issuperset(expected_socket_files)


def test_tempdir_privilege(shutdown_only):
    tmp_dir = ray._common.utils.get_ray_temp_dir()
    os.makedirs(tmp_dir, exist_ok=True)
    os.chmod(tmp_dir, 0o000)
    ray.init(num_cpus=1)
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    assert os.path.exists(session_dir), "Specified socket path not found."


def test_session_dir_uniqueness():
    session_dirs = set()
    for i in range(2):
        ray.init(num_cpus=1)
        session_dirs.add(ray._private.worker._global_node.get_session_dir_path)
        ray.shutdown()
    assert len(session_dirs) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
