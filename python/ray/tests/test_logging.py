import os
import ray


def test_log_rotation_config(ray_start_cluster):
    cluster = ray_start_cluster
    max_bytes = 100
    backup_count = 3

    # Create a cluster.
    cluster.add_node(
        num_cpus=0,
        _system_config={
            "log_rotation_max_bytes": max_bytes,
            "log_rotation_backup_count": backup_count
        })
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()
    for node in cluster.list_all_nodes():
        assert node._config["log_rotation_max_bytes"] == max_bytes
        assert node._config["log_rotation_backup_count"] == backup_count


def test_log_rotation(shutdown_only):
    max_bytes = 5
    backup_count = 3
    ray.init(
        num_cpus=0,
        _system_config={
            "log_rotation_max_bytes": max_bytes,
            "log_rotation_backup_count": backup_count
        })
    import time
    time.sleep(100)


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
