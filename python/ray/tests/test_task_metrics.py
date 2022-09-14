from collections import defaultdict
import os
import time

import numpy as np
import pytest

import ray
from ray._private.test_utils import (
    fetch_prometheus_metrics,
    wait_for_condition,
)


METRIC_CONFIG = {
    "_system_config": {
        "metrics_report_interval_ms": 100,
    }
}


def tasks_by_state(info) -> dict:
    metrics_page = "localhost:{}".format(info["metrics_export_port"])
    print("Fetch metrics from", metrics_page)
    res = fetch_prometheus_metrics([metrics_page])
    if "ray_tasks" in res:
        states = defaultdict(int)
        for sample in res["ray_tasks"]:
            states[sample.labels["State"]] += sample.value
        print("Tasks by state: {}".format(states))
        return states
    else:
        return {}


def test_task_basic(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    @ray.remote
    def f():
        time.sleep(999)

    [f.remote() for _ in range(10)]

    expected = {
        "RUNNING": 2.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 8.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
    }
    # TODO(ekl) optimize the reporting interval to be faster for testing
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000
    )


def test_task_wait_on_deps(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    @ray.remote
    def f():
        time.sleep(999)

    @ray.remote
    def g(x):
        time.sleep(999)

    x = f.remote()
    [g.remote(x) for _ in range(5)]

    expected = {
        "RUNNING": 1.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 10.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000
    )


def test_actor_tasks_queued(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    @ray.remote
    class F:
        def f(self):
            time.sleep(999)

        def g(self):
            pass

    a = F.remote()
    [a.g.remote() for _ in range(10)]
    [a.f.remote() for _ in range(1)]  # Further tasks should be blocked on this one.
    [a.g.remote() for _ in range(9)]
    # TODO: add actor vs task type label and name label?

    expected = {
        "RUNNING": 1.0,
        "WAITING_FOR_EXECUTION": 9.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 11.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000
    )


def test_task_finish(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    @ray.remote
    def f():
        return "ok"

    @ray.remote
    def g():
        assert False

    (f.remote(), g.remote())

    expected = {
        "RUNNING": 0.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 2.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000
    )


def test_task_retry(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    @ray.remote(retry_exceptions=True)
    def f():
        assert False

    f.remote()
    time.sleep(1)  # Enough sleep so that retries have time to run.

    expected = {
        "RUNNING": 0.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 1.0,  # Only recorded as finished once.
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000
    )


def test_waiting_for_transfer(shutdown_only):
    cluster = ray.cluster_utils.Cluster()
    cluster.add_node(num_cpus=4, resources={"head": 1}, **METRIC_CONFIG)
    cluster.add_node(num_cpus=4, resources={"worker": 1})
    cluster.wait_for_nodes()
    info = ray.init(address=cluster.address)

    @ray.remote(resources={"head": 0.1})
    def gen():
        return np.ones(1024 * 1024 * 100)

    @ray.remote(resources={"worker": 0.1})
    def recv(x):
        return np.sum(x)

    while True:
        start = time.time()
        refs = [recv.remote(gen.remote()) for _ in range(100)]
        for _ in range(100):
            print(tasks_by_state(info))
            time.sleep(0.1)
        ray.get(refs)
        print("Time", time.time() - start)


# TODO(ekl) test wait on object store transfer (??)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
