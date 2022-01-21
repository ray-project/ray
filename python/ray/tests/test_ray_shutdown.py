from datetime import datetime
import logging
import platform
import random
import sys
from threading import Event, Thread
import time

import pytest
import ray
import psutil  # We must import psutil after ray because we bundle it with ray.

from ray.cluster_utils import Cluster
from ray._private.test_utils import (wait_for_condition,
                                     run_string_as_driver_nonblocking)


def get_all_ray_worker_processes():
    processes = [
        p.info["cmdline"]
        for p in psutil.process_iter(attrs=["pid", "name", "cmdline"])
    ]

    result = []
    for p in processes:
        if p is not None and len(p) > 0 and "ray::" in p[0]:
            result.append(p)
    return result


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_ray_shutdown(shutdown_only):
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

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_driver_dead(shutdown_only):
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

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_node_killed(ray_start_cluster):
    """Make sure all ray workers when nodes are dead."""
    cluster = ray_start_cluster
    # head node.
    cluster.add_node(
        num_cpus=0, _system_config={"gcs_rpc_server_reconnect_timeout_s": 1})
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

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(platform.system() == "Windows", reason="Hang on Windows.")
def test_head_node_down(ray_start_cluster):
    """Make sure all ray workers when head node is dead."""
    cluster = ray_start_cluster
    # head node.
    head = cluster.add_node(
        num_cpus=2, _system_config={"gcs_rpc_server_reconnect_timeout_s": 1})

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
""".format(cluster.address)

    p = run_string_as_driver_nonblocking(driver)
    # Make sure the driver is running.
    time.sleep(1)
    wait_for_condition(lambda: p.poll() is None)
    wait_for_condition(lambda: len(get_all_ray_worker_processes()) > 0)

    cluster.remove_node(head)

    wait_for_condition(lambda: len(get_all_ray_worker_processes()) == 0)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Cluster unsupported on Windows.")
def test_concurrent_shutdown(monkeypatch):
    """Safe to shut down Ray with concurrent access to core workers."""
    monkeypatch.setenv("RAY_ENABLE_AUTO_CONNECT", "0")
    terminate = Event()

    @ray.remote(num_cpus=1)
    def work():
        time.sleep(random.uniform(0, 0.1))

    @ray.remote(num_cpus=1)
    class TestActor:
        def run(self):
            time.sleep(random.uniform(0, 0.1))

    def worker():
        while not terminate.is_set():
            try:
                if random.uniform(0, 1) < 0.5:
                    ray.wait([work.remote()], timeout=1)
                else:
                    actor = TestActor.remote()
                    ray.wait([actor.run.remote()], timeout=1)
            except Exception:
                if ray.is_initialized():
                    logging.exception(
                        "Ray task failed, likely during Ray shutdown")

    workers = [Thread(target=worker, name=f"worker_{i}") for i in range(4)]
    for w in workers:
        w.start()

    start = time.time()
    while time.time() - start < 60:
        address = ray.init(num_cpus=4)["address"]
        print(f"{datetime.now()} Started cluster at {address}")

        time.sleep(random.uniform(1, 5))

        print(f"{datetime.now()} Shutting down cluster at {address}")
        ray.shutdown()

        time.sleep(random.uniform(0, 1))

    terminate.set()
    for w in workers:
        w.join(timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
