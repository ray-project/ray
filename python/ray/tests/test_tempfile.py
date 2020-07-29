import os
import shutil
import sys
import time

import pytest
import ray
from ray.cluster_utils import Cluster
from ray.test_utils import check_call_ray


def unix_socket_create_path(name):
    unix = sys.platform != "win32"
    return os.path.join(ray.utils.get_user_temp_dir(), name) if unix else None


def unix_socket_verify(unix_socket):
    if sys.platform != "win32":
        assert os.path.exists(unix_socket), "Socket not found: " + unix_socket


def unix_socket_delete(unix_socket):
    unix = sys.platform != "win32"
    return os.remove(unix_socket) if unix else None


def test_conn_cluster():
    # plasma_store_socket_name
    with pytest.raises(Exception) as exc_info:
        ray.init(
            address="127.0.0.1:6379",
            plasma_store_socket_name=os.path.join(
                ray.utils.get_user_temp_dir(), "this_should_fail"))
    assert exc_info.value.args[0] == (
        "When connecting to an existing cluster, "
        "plasma_store_socket_name must not be provided.")

    # raylet_socket_name
    with pytest.raises(Exception) as exc_info:
        ray.init(
            address="127.0.0.1:6379",
            raylet_socket_name=os.path.join(ray.utils.get_user_temp_dir(),
                                            "this_should_fail"))
    assert exc_info.value.args[0] == (
        "When connecting to an existing cluster, "
        "raylet_socket_name must not be provided.")

    # temp_dir
    with pytest.raises(Exception) as exc_info:
        ray.init(
            address="127.0.0.1:6379",
            temp_dir=os.path.join(ray.utils.get_user_temp_dir(),
                                  "this_should_fail"))
    assert exc_info.value.args[0] == (
        "When connecting to an existing cluster, "
        "temp_dir must not be provided.")


def test_tempdir(shutdown_only):
    shutil.rmtree(ray.utils.get_ray_temp_dir(), ignore_errors=True)
    ray.init(
        temp_dir=os.path.join(ray.utils.get_user_temp_dir(),
                              "i_am_a_temp_dir"))
    assert os.path.exists(
        os.path.join(ray.utils.get_user_temp_dir(),
                     "i_am_a_temp_dir")), "Specified temp dir not found."
    assert not os.path.exists(
        ray.utils.get_ray_temp_dir()), ("Default temp dir should not exist.")
    shutil.rmtree(
        os.path.join(ray.utils.get_user_temp_dir(), "i_am_a_temp_dir"),
        ignore_errors=True)


def test_tempdir_commandline():
    shutil.rmtree(ray.utils.get_ray_temp_dir(), ignore_errors=True)
    check_call_ray([
        "start", "--head", "--temp-dir=" + os.path.join(
            ray.utils.get_user_temp_dir(), "i_am_a_temp_dir2")
    ])
    assert os.path.exists(
        os.path.join(ray.utils.get_user_temp_dir(),
                     "i_am_a_temp_dir2")), "Specified temp dir not found."
    assert not os.path.exists(
        ray.utils.get_ray_temp_dir()), "Default temp dir should not exist."
    check_call_ray(["stop"])
    shutil.rmtree(
        os.path.join(ray.utils.get_user_temp_dir(), "i_am_a_temp_dir2"),
        ignore_errors=True)


def test_tempdir_long_path():
    if sys.platform != "win32":
        # Test AF_UNIX limits for sockaddr_un->sun_path on POSIX OSes
        maxlen = 104 if sys.platform.startswith("darwin") else 108
        temp_dir = os.path.join(ray.utils.get_user_temp_dir(), "z" * maxlen)
        with pytest.raises(OSError):
            ray.init(temp_dir=temp_dir)  # path should be too long


def test_raylet_socket_name(shutdown_only):
    sock1 = unix_socket_create_path("i_am_a_temp_socket_1")
    ray.init(raylet_socket_name=sock1)
    unix_socket_verify(sock1)
    ray.shutdown()
    try:
        unix_socket_delete(sock1)
    except OSError:
        pass  # It could have been removed by Ray.
    cluster = Cluster(True)
    sock2 = unix_socket_create_path("i_am_a_temp_socket_2")
    cluster.add_node(raylet_socket_name=sock2)
    unix_socket_verify(sock2)
    cluster.shutdown()
    try:
        unix_socket_delete(sock2)
    except OSError:
        pass  # It could have been removed by Ray.


def test_temp_plasma_store_socket(shutdown_only):
    sock1 = unix_socket_create_path("i_am_a_temp_socket_1")
    ray.init(plasma_store_socket_name=sock1)
    unix_socket_verify(sock1)
    ray.shutdown()
    try:
        unix_socket_delete(sock1)
    except OSError:
        pass  # It could have been removed by Ray.
    cluster = Cluster(True)
    sock2 = unix_socket_create_path("i_am_a_temp_socket_2")
    cluster.add_node(plasma_store_socket_name=sock2)
    unix_socket_verify(sock2)
    cluster.shutdown()
    try:
        unix_socket_delete(sock2)
    except OSError:
        pass  # It could have been removed by Ray.


def test_raylet_tempfiles(shutdown_only):
    expected_socket_files = ({"plasma_store", "raylet"}
                             if sys.platform != "win32" else set())

    ray.init(num_cpus=0)
    node = ray.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    log_files = set(os.listdir(node.get_logs_dir_path()))
    log_files_expected = {
        "log_monitor.out", "log_monitor.err", "plasma_store.out",
        "plasma_store.err", "monitor.out", "monitor.err", "redis-shard_0.out",
        "redis-shard_0.err", "redis.out", "redis.err", "raylet.out",
        "raylet.err", "gcs_server.out", "gcs_server.err"
    }

    for expected in log_files_expected:
        assert expected in log_files
    assert log_files_expected.issubset(log_files)
    assert log_files.issuperset(log_files_expected)

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files == expected_socket_files
    ray.shutdown()

    ray.init(num_cpus=2)
    node = ray.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    time.sleep(3)  # wait workers to start
    log_files = set(os.listdir(node.get_logs_dir_path()))

    assert log_files.issuperset(log_files_expected)

    # Check numbers of worker log file.
    assert sum(
        1 for filename in log_files if filename.startswith("worker")) == 4

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files == expected_socket_files


def test_tempdir_privilege(shutdown_only):
    os.chmod(ray.utils.get_ray_temp_dir(), 0o000)
    ray.init(num_cpus=1)
    session_dir = ray.worker._global_node.get_session_dir_path()
    assert os.path.exists(session_dir), "Specified socket path not found."


def test_session_dir_uniqueness():
    session_dirs = set()
    for i in range(2):
        ray.init(num_cpus=1)
        session_dirs.add(ray.worker._global_node.get_session_dir_path)
        ray.shutdown()
    assert len(session_dirs) == 2


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
