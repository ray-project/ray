import os
import ray


def set_logging_config(max_bytes, backup_count):
    os.environ["RAY_RAOTATION_MAX_BYTES"] = str(max_bytes)
    os.environ["RAY_ROTATION_BACKUP_COUNT"] = str(backup_count)


def test_log_rotation_config(ray_start_cluster):
    cluster = ray_start_cluster
    max_bytes = 100
    backup_count = 3

    # Create a cluster.
    set_logging_config(max_bytes, backup_count)
    head_node = cluster.add_node(num_cpus=0)
    # Set a different env var for a worker node.
    set_logging_config(0, 0)
    worker_node = cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()

    config = head_node.logging_config
    assert config["log_rotation_max_bytes"] == max_bytes
    assert config["log_rotation_backup_count"] == backup_count
    config = worker_node.logging_config
    assert config["log_rotation_max_bytes"] == 0
    assert config["log_rotation_backup_count"] == 0


def test_log_rotation(shutdown_only):
    max_bytes = 0
    backup_count = 0
    set_logging_config(max_bytes, backup_count)
    ray.init(num_cpus=0)
    import time
    time.sleep(100)


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
