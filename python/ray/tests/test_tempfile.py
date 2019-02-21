from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import time
import pytest
import ray


def test_conn_cluster():
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


def test_tempdir():
    ray.init(temp_dir="/tmp/i_am_a_temp_dir")
    assert os.path.exists(
        "/tmp/i_am_a_temp_dir"), "Specified temp dir not found."
    ray.shutdown()
    shutil.rmtree("/tmp/i_am_a_temp_dir", ignore_errors=True)


def test_raylet_socket_name():
    ray.init(raylet_socket_name="/tmp/i_am_a_temp_socket")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket"), "Specified socket path not found."
    ray.shutdown()
    try:
        os.remove("/tmp/i_am_a_temp_socket")
    except Exception:
        pass


def test_temp_plasma_store_socket():
    ray.init(plasma_store_socket_name="/tmp/i_am_a_temp_socket")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket"), "Specified socket path not found."
    ray.shutdown()
    try:
        os.remove("/tmp/i_am_a_temp_socket")
    except Exception:
        pass


def test_raylet_tempfiles():
    ray.init(num_cpus=0)
    node = ray.worker._global_node
    top_levels = set(os.listdir(node.get_temp_dir_path()))
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
    top_levels = set(os.listdir(node.get_temp_dir_path()))
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
    ray.shutdown()
