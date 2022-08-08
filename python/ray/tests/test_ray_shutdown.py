import sys
import time
import platform

import pytest
import ray

import psutil  # We must import psutil after ray because we bundle it with ray.

from ray._private.test_utils import (
    wait_for_condition,
    run_string_as_driver_nonblocking,
)


def get_all_ray_worker_processes():
    processes = psutil.process_iter(attrs=["pid", "name", "cmdline"])

    result = []
    for p in processes:
        cmd_line = p.info["cmdline"]
        if cmd_line is not None and len(cmd_line) > 0 and "ray::" in cmd_line[0]:
            result.append(p)
    return result


@pytest.fixture
def kill_all_ray_worker_process():
    # Avoiding the previous test doesn't kill the relevant process,
    # thus making the current test fail.
    ray_process = get_all_ray_worker_processes()
    for p in ray_process:
        try:
            p.kill()
        except Exception:
            pass
    yield


@pytest.fixture
def short_gcs_publish_timeout(monkeypatch):
    monkeypatch.setenv("RAY_MAX_GCS_PUBLISH_RETRIES", "3")
    yield


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown(
    kill_all_ray_worker_process, short_gcs_publish_timeout, shutdown_only
):
    """Make sure all ray workers are shutdown when driver is done."""

    ray.init()

    @ray.remote
    def f():
        import time

        time.sleep(10)

    num_cpus = int(ray.available_resources()["CPU"])
    tasks = [f.remote() for _ in range(num_cpus)]  # noqa
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    ray.shutdown()

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0, timeout=20)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_driver_dead(
    kill_all_ray_worker_process, short_gcs_publish_timeout, shutdown_only
):
    """Make sure all ray workers are shutdown when driver is killed."""

    driver = """
import ray
ray.init(_system_config={"gcs_rpc_server_reconnect_timeout_s": 1})
@ray.remote
def f():
    import time
    time.sleep(10)

num_cpus = int(ray.available_resources()["CPU"])
tasks = [f.remote() for _ in range(num_cpus)]
"""

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    assert p.poll() is None
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    # Kill the driver process.
    p.kill()
    p.wait()
    time.sleep(0.1)

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0, timeout=20)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_node_killed(
    kill_all_ray_worker_process, short_gcs_publish_timeout, ray_start_cluster
):
    """Make sure all ray workers when nodes are dead."""

    cluster = ray_start_cluster
    # head node.
    cluster.add_node(
        num_cpus=0, _system_config={"gcs_rpc_server_reconnect_timeout_s": 1}
    )
    ray.init(address="auto")

    num_worker_nodes = 2
    workers = []
    for _ in range(num_worker_nodes):
        workers.append(cluster.add_node(num_cpus=2))
    cluster.wait_for_nodes()

    @ray.remote
    def f():
        import time

        time.sleep(100)

    num_cpus = int(ray.available_resources()["CPU"])
    tasks = [f.remote() for _ in range(num_cpus)]  # noqa
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    for worker in workers:
        cluster.remove_node(worker)

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0, timeout=20)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_head_node_down(
    kill_all_ray_worker_process, short_gcs_publish_timeout, ray_start_cluster
):
    """Make sure all ray workers when head node is dead."""

    cluster = ray_start_cluster
    # head node.
    head = cluster.add_node(
        num_cpus=2, _system_config={"gcs_rpc_server_reconnect_timeout_s": 1}
    )

    # worker nodes.
    num_worker_nodes = 2
    for _ in range(num_worker_nodes):
        cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Start a driver.
    driver = """
import ray
ray.init(address="{}")
@ray.remote
def f():
    import time
    time.sleep(10)

num_cpus = int(ray.available_resources()["CPU"])
tasks = [f.remote() for _ in range(num_cpus)]
import time
time.sleep(100)
""".format(
        cluster.address
    )

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    wait_for_condition(lambda: p.poll() is None)
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    cluster.remove_node(head)

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0, timeout=20)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
