from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import shutil
import os
import time
import pytest
import ray
from ray.tests.cluster_utils import Cluster


@pytest.fixture
def backup_and_shutdown():
    """Protect the original /tmp/ray"""
    if os.path.exists("/tmp/ray"):
        shutil.move("/tmp/ray", "/tmp/__ray_backup")
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    if os.path.exists("/tmp/ray"):
        shutil.rmtree("/tmp/ray")
    if os.path.exists("/tmp/__ray_backup"):
        shutil.move("/tmp/__ray_backup", "/tmp/ray")


def test_conn_cluster(shutdown_only):
    # plasma_store_socket_name
    with pytest.raises(Exception) as exc_info:
        ray.init(
            redis_address="127.0.0.1:6379",
            plasma_store_socket_name="/tmp/this_should_fail")
    assert exc_info.value.args[0] == (
        "When connecting to an existing cluster, "
        "plasma_store_socket_name must not be provided.")

    # raylet_socket_name
    with pytest.raises(Exception) as exc_info:
        ray.init(
            redis_address="127.0.0.1:6379",
            raylet_socket_name="/tmp/this_should_fail")
    assert exc_info.value.args[0] == (
        "When connecting to an existing cluster, "
        "raylet_socket_name must not be provided.")

    # temp_dir
    with pytest.raises(Exception) as exc_info:
        ray.init(
            redis_address="127.0.0.1:6379", temp_dir="/tmp/this_should_fail")
    assert exc_info.value.args[0] == (
        "When connecting to an existing cluster, "
        "temp_dir must not be provided.")


def test_tempdir(backup_and_shutdown):
    ray.init(temp_dir="/tmp/i_am_a_temp_dir")
    assert os.path.exists(
        "/tmp/i_am_a_temp_dir"), "Specified temp dir not found."
    assert not os.path.exists("/tmp/ray"), "Default temp dir should not exist."
    ray.shutdown()
    shutil.rmtree("/tmp/i_am_a_temp_dir")


def test_tempdir_commandline(backup_and_shutdown):
    os.system("ray start --head --temp-dir=/tmp/i_am_a_temp_dir2")
    assert os.path.exists(
        "/tmp/i_am_a_temp_dir2"), "Specified temp dir not found."
    assert not os.path.exists("/tmp/ray"), "Default temp dir should not exist."
    os.system("ray stop")
    shutil.rmtree("/tmp/i_am_a_temp_dir2")


def test_socket_file_lifetime(shutdown_only):
    ray.init(num_cpus=1)
    node = ray.worker._global_node
    assert os.path.exists(node.plasma_store_socket_name)
    assert os.path.exists(node.raylet_socket_name)
    ray.shutdown()
    assert not os.path.exists(node.plasma_store_socket_name)
    assert not os.path.exists(node.raylet_socket_name)


def test_raylet_socket_name(shutdown_only):
    ray.init(raylet_socket_name="/tmp/i_am_a_temp_socket")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket"), "Specified socket path not found."
    ray.shutdown()
    assert not os.path.exists("/tmp/i_am_a_temp_socket"), (
        "The raylet socket file hasn't been removed on exit.")
    cluster = Cluster(True)
    cluster.add_node(raylet_socket_name="/tmp/i_am_a_temp_socket_2")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket_2"), "Specified socket path not found."
    cluster.shutdown()
    assert not os.path.exists("/tmp/i_am_a_temp_socket_2"), (
        "The raylet socket file hasn't been removed on exit.")


def test_temp_plasma_store_socket(shutdown_only):
    ray.init(plasma_store_socket_name="/tmp/i_am_a_temp_socket")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket"), "Specified socket path not found."
    ray.shutdown()
    assert not os.path.exists("/tmp/i_am_a_temp_socket"), (
        "The plasma store socket file hasn't been removed on exit.")
    cluster = Cluster(True)
    cluster.add_node(plasma_store_socket_name="/tmp/i_am_a_temp_socket_2")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket_2"), "Specified socket path not found."
    cluster.shutdown()
    assert not os.path.exists("/tmp/i_am_a_temp_socket_2"), (
        "The plasma store socket file hasn't been removed on exit.")


def test_raylet_tempfiles(shutdown_only):
    ray.init(num_cpus=0)
    node = ray.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    log_files = set(os.listdir(node.get_logs_dir_path()))
    assert log_files.issuperset({
        "log_monitor.out", "log_monitor.err", "plasma_store.out",
        "plasma_store.err", "monitor.out", "monitor.err", "raylet_monitor.out",
        "raylet_monitor.err", "redis-shard_0.out", "redis-shard_0.err",
        "redis.out", "redis.err", "raylet.out", "raylet.err"
    })  # with raylet logs
    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files == {"plasma_store", "raylet"}
    ray.shutdown()

    ray.init(num_cpus=2)
    node = ray.worker._global_node
    top_levels = set(os.listdir(node.get_session_dir_path()))
    assert top_levels.issuperset({"sockets", "logs"})
    time.sleep(3)  # wait workers to start
    log_files = set(os.listdir(node.get_logs_dir_path()))
    assert log_files.issuperset({
        "log_monitor.out", "log_monitor.err", "plasma_store.out",
        "plasma_store.err", "monitor.out", "monitor.err", "raylet_monitor.out",
        "raylet_monitor.err", "redis-shard_0.out", "redis-shard_0.err",
        "redis.out", "redis.err", "raylet.out", "raylet.err"
    })  # with raylet logs

    # Check numbers of worker log file.
    assert sum(
        1 for filename in log_files if filename.startswith("worker")) == 4

    socket_files = set(os.listdir(node.get_sockets_dir_path()))
    assert socket_files == {"plasma_store", "raylet"}


def test_raylet_dump_state(shutdown_only):
    ray.init(num_cpus=1)
    node = ray.worker._global_node
    time.sleep(15)  # wait raylet to dump state
    assert "debug_state.txt" in os.listdir(node.get_logs_dir_path())


def test_tempdir_privilege(shutdown_only):
    if not os.path.exists("/tmp/ray"):
        os.mkdir("/tmp/ray")
    os.chmod("/tmp/ray", 0o000)
    ray.init(num_cpus=1)
    session_dir = ray.worker._global_node.get_session_dir_path()
    assert os.path.exists(session_dir), "Specified socket path not found."


def test_session_dir_uniqueness(shutdown_only):
    session_dirs = set()
    for _ in range(3):
        ray.init(num_cpus=1)
        session_dirs.add(ray.worker._global_node.get_session_dir_path)
        ray.shutdown()
    assert len(session_dirs) == 3
