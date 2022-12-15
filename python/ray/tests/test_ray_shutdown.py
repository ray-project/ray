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

WAIT_TIMEOUT = 20


def get_all_ray_worker_processes():
    processes = [
        p.info["cmdline"] for p in psutil.process_iter(attrs=["pid", "name", "cmdline"])
    ]

    result = []
    for p in processes:
        if p is not None and len(p) > 0 and "ray::" in p[0]:
            result.append(p[0])
    return result


@pytest.fixture
def short_gcs_publish_timeout(monkeypatch):
    monkeypatch.setenv("RAY_MAX_GCS_PUBLISH_RETRIES", "3")
    yield


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown(short_gcs_publish_timeout, shutdown_only):
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

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_driver_dead(short_gcs_publish_timeout, shutdown_only):
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

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_node_killed(short_gcs_publish_timeout, ray_start_cluster):
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

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_head_node_down(short_gcs_publish_timeout, ray_start_cluster):
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

    wait_for_condition(
        lambda: len(get_all_ray_worker_processes()) == 0, timeout=WAIT_TIMEOUT
    )


def test_raylet_graceful_exit_upon_agent_exit(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=0)

    def get_raylet_agent_procs(worker):
        raylet = None
        for p in worker.live_processes():
            if p[0] == "raylet":
                raylet = p[1]
        assert raylet is not None

        children = psutil.Process(raylet.pid).children()
        assert len(children) == 1
        agent = psutil.Process(children[0].pid)
        return raylet, agent

    # Make sure raylet exits gracefully upon agent terminated by SIGTERM.
    worker = cluster.add_node(num_cpus=0)
    raylet, agent = get_raylet_agent_procs(worker)
    agent.terminate()
    exit_code = raylet.wait()
    # When the agent is terminated
    assert exit_code == 0

    # Make sure raylet exits gracefully upon agent terminated by SIGKILL.
    # TODO(sang): Make raylet exits ungracefully in this case. It is currently
    # not possible because we cannot detect the exit code of children process
    # from cpp code.
    worker = cluster.add_node(num_cpus=0)
    raylet, agent = get_raylet_agent_procs(worker)
    agent.kill()
    exit_code = raylet.wait()
    # When the agent is terminated
    assert exit_code == 0


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
