from collections import defaultdict
import os

import pytest

import ray

from ray._private.test_utils import (
    fetch_prometheus_metrics,
    run_string_as_driver_nonblocking,
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


# TODO(ekl) in all these tests, we use run_string_as_driver_nonblocking to work around
# stats reporting issues if Ray is repeatedly restarted in unit tests.
def test_task_basic(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import time

ray.init("auto")

@ray.remote
def f():
    time.sleep(999)
a = [f.remote() for _ in range(10)]
ray.get(a)
"""
    proc = run_string_as_driver_nonblocking(driver)

    expected = {
        "RUNNING": 2.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 8.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
    }
    # TODO(ekl) optimize the reporting interval to be faster for testing
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=500
    )
    proc.kill()


def test_task_nested(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import time

ray.init("auto")

@ray.remote(num_cpus=0)
def wrapper():
    @ray.remote
    def f():
        time.sleep(999)

    ray.get([f.remote() for _ in range(10)])

w = wrapper.remote()
ray.get(w)
"""
    proc = run_string_as_driver_nonblocking(driver)

    expected = {
        "RUNNING": 3.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 8.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
    }
    # TODO(ekl) optimize the reporting interval to be faster for testing
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000
    )
    proc.kill()


def test_task_wait_on_deps(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import time

ray.init("auto")

@ray.remote
def f():
    time.sleep(999)

@ray.remote
def g(x):
    time.sleep(999)

x = f.remote()
a = [g.remote(x) for _ in range(5)]
ray.get(a)
"""
    proc = run_string_as_driver_nonblocking(driver)
    expected = {
        "RUNNING": 1.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 5.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=500
    )
    proc.kill()


def test_actor_tasks_queued(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import time

ray.init("auto")

@ray.remote
class F:
    def f(self):
        time.sleep(999)

    def g(self):
        pass

a = F.remote()
[a.g.remote() for _ in range(10)]
[a.f.remote() for _ in range(1)]  # Further tasks should be blocked on this one.
z = [a.g.remote() for _ in range(9)]
ray.get(z)
"""
    proc = run_string_as_driver_nonblocking(driver)
    expected = {
        "RUNNING": 1.0,
        "WAITING_FOR_EXECUTION": 9.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 11.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=500
    )
    proc.kill()


def test_task_finish(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import time

ray.init("auto")

@ray.remote
def f():
    return "ok"

@ray.remote
def g():
    assert False

f.remote()
g.remote()
time.sleep(999)
"""

    proc = run_string_as_driver_nonblocking(driver)
    expected = {
        "RUNNING": 0.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 2.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=500
    )
    proc.kill()


def test_task_retry(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import time

ray.init("auto")

@ray.remote(retry_exceptions=True)
def f():
    assert False

f.remote()
time.sleep(999)
"""

    proc = run_string_as_driver_nonblocking(driver)
    expected = {
        "RUNNING": 0.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 1.0,  # Only recorded as finished once.
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=500
    )
    proc.kill()


def test_concurrent_actor_tasks(shutdown_only):
    info = ray.init(num_cpus=2, **METRIC_CONFIG)

    driver = """
import ray
import asyncio

ray.init("auto")

@ray.remote(max_concurrency=30)
class A:
    async def f(self):
        await asyncio.sleep(300)

a = A.remote()
ray.get([a.f.remote() for _ in range(40)])
"""

    proc = run_string_as_driver_nonblocking(driver)
    expected = {
        "RUNNING": 30.0,
        "WAITING_FOR_EXECUTION": 10.0,
        "SCHEDULED": 0.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
        "FINISHED": 1.0,
    }
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=500
    )
    proc.kill()


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
