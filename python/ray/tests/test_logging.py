import os
from collections import defaultdict
from pathlib import Path

import ray
from ray import ray_constants


def set_logging_config(max_bytes, backup_count):
    os.environ["RAY_ROTATION_MAX_BYTES"] = str(max_bytes)
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
    max_bytes = 1
    backup_count = 3
    set_logging_config(max_bytes, backup_count)
    ray.init(num_cpus=1)
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    log_rotating_component = [
        ray_constants.PROCESS_TYPE_DASHBOARD,
        ray_constants.PROCESS_TYPE_DASHBOARD_AGENT,
        ray_constants.PROCESS_TYPE_LOG_MONITOR,
        ray_constants.PROCESS_TYPE_MONITOR,
        ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER_DRIVER,
        ray_constants.PROCESS_TYPE_PYTHON_CORE_WORKER,
        # Below components are not log rotating now.
        # ray_constants.PROCESS_TYPE_RAYLET,
        # ray_constants.PROCESS_TYPE_GCS_SERVER,
        # ray_constants.PROCESS_TYPE_WORKER,
    ]

    # Run the basic workload.
    @ray.remote
    def f():
        for i in range(10):
            print(f"test {i}")

    ray.get(f.remote())

    paths = list(log_dir_path.iterdir())

    def component_exist(component, paths):
        for path in paths:
            filename = path.stem
            if component in filename:
                return True
        return False

    def component_file_size_small_enough(component):
        """Although max_bytes is 1, the file can have size that is big.
            For example, if the logger prints the traceback, it can be
            much bigger. So, we shouldn't make the assertion too tight.
        """
        small_enough_bytes = 512  # 512 bytes.
        for path in paths:
            if not component_exist(component, [path]):
                continue

            if path.stat().st_size > small_enough_bytes:
                return False
        return True

    for component in log_rotating_component:
        assert component_exist(component, paths)
        assert component_file_size_small_enough(component)

    # Check if the backup count is respected.
    file_cnts = defaultdict(int)
    for path in paths:
        filename = path.stem
        filename_without_suffix = filename.split(".")[0]
        file_cnts[filename_without_suffix] += 1
    for filename, file_cnt in file_cnts.items():
        # There could be backup_count + 1 files.
        # EX) *.log, *.log.* (as many as backup count).
        assert file_cnt <= backup_count + 1, (
            f"{filename} has files that are more than "
            f"backup count {backup_count}, file count: {file_cnt}")


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
