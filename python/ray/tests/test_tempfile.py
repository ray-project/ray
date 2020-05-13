import os
import shutil
import time
import pytest
import ray
import ray.ray_constants as ray_constants
import subprocess
from ray.cluster_utils import Cluster


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
    subprocess.check_call([
        "ray", "start", "--head", "--temp-dir=" + os.path.join(
            ray.utils.get_user_temp_dir(), "i_am_a_temp_dir2")
    ])
    assert os.path.exists(
        os.path.join(ray.utils.get_user_temp_dir(),
                     "i_am_a_temp_dir2")), "Specified temp dir not found."
    assert not os.path.exists(
        ray.utils.get_ray_temp_dir()), "Default temp dir should not exist."
    subprocess.check_call(["ray", "stop"])
    shutil.rmtree(
        os.path.join(ray.utils.get_user_temp_dir(), "i_am_a_temp_dir2"),
        ignore_errors=True)


def test_tempdir_long_path():
    temp_dir = os.path.join(ray.utils.get_user_temp_dir(), "z" * 108)
    with pytest.raises(OSError):
        ray.init(temp_dir=temp_dir)  # path should be too long


def test_raylet_socket_name(shutdown_only):
    ray.init(
        raylet_socket_name=os.path.join(ray.utils.get_user_temp_dir(),
                                        "i_am_a_temp_socket"))
    assert os.path.exists(
        os.path.join(ray.utils.get_user_temp_dir(),
                     "i_am_a_temp_socket")), "Specified socket path not found."
    ray.shutdown()
    try:
        os.remove(
            os.path.join(ray.utils.get_user_temp_dir(), "i_am_a_temp_socket"))
    except OSError:
        pass  # It could have been removed by Ray.
    cluster = Cluster(True)
    cluster.add_node(
        raylet_socket_name=os.path.join(ray.utils.get_user_temp_dir(),
                                        "i_am_a_temp_socket_2"))
    assert os.path.exists(
        os.path.join(
            ray.utils.get_user_temp_dir(),
            "i_am_a_temp_socket_2")), "Specified socket path not found."
    cluster.shutdown()
    try:
        os.remove(
            os.path.join(ray.utils.get_user_temp_dir(),
                         "i_am_a_temp_socket_2"))
    except OSError:
        pass  # It could have been removed by Ray.


def test_temp_plasma_store_socket(shutdown_only):
    ray.init(
        plasma_store_socket_name=os.path.join(ray.utils.get_user_temp_dir(),
                                              "i_am_a_temp_socket"))
    assert os.path.exists(
        os.path.join(ray.utils.get_user_temp_dir(),
                     "i_am_a_temp_socket")), "Specified socket path not found."
    ray.shutdown()
    try:
        os.remove(
            os.path.join(ray.utils.get_user_temp_dir(), "i_am_a_temp_socket"))
    except OSError:
        pass  # It could have been removed by Ray.
    cluster = Cluster(True)
    cluster.add_node(
        plasma_store_socket_name=os.path.join(ray.utils.get_user_temp_dir(),
                                              "i_am_a_temp_socket_2"))
    assert os.path.exists(
        os.path.join(
            ray.utils.get_user_temp_dir(),
            "i_am_a_temp_socket_2")), "Specified socket path not found."
    cluster.shutdown()
    try:
        os.remove(
            os.path.join(ray.utils.get_user_temp_dir(),
                         "i_am_a_temp_socket_2"))
    except OSError:
        pass  # It could have been removed by Ray.


def test_raylet_tempfiles(shutdown_only):
    ray.init(num_cpus=0)
    node = ray.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    log_files = set(os.listdir(node.get_logs_dir_path()))
    log_files_expected = {
        "log_monitor.out", "log_monitor.err", "plasma_store.out",
        "plasma_store.err", "monitor.out", "monitor.err", "redis-shard_0.out",
        "redis-shard_0.err", "redis.out", "redis.err", "raylet.out",
        "raylet.err"
    }

    if ray_constants.GCS_SERVICE_ENABLED:
        log_files_expected.update({"gcs_server.out", "gcs_server.err"})
    else:
        log_files_expected.update({"raylet_monitor.out", "raylet_monitor.err"})

    assert log_files.issuperset(log_files_expected)

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files == {"plasma_store", "raylet"}
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
    assert socket_files == {"plasma_store", "raylet"}


def test_tempdir_privilege(shutdown_only):
    os.chmod(ray.utils.get_ray_temp_dir(), 0o000)
    ray.init(num_cpus=1)
    session_dir = ray.worker._global_node.get_session_dir_path()
    assert os.path.exists(session_dir), "Specified socket path not found."


def test_session_dir_uniqueness():
    session_dirs = set()
    for _ in range(3):
        ray.init(num_cpus=1)
        session_dirs.add(ray.worker._global_node.get_session_dir_path)
        ray.shutdown()
    assert len(session_dirs) == 3


if __name__ == "__main__":
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
