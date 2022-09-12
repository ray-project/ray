from collections import defaultdict
import os
import time

import pytest

import ray
from ray._private.test_utils import (
    fetch_prometheus_metrics,
    wait_for_condition,
)


def tasks_by_state(info) -> dict:
    metrics_page = "localhost:{}".format(info["metrics_export_port"])
    res = fetch_prometheus_metrics([metrics_page])
    if "ray_tasks" in res:
        states = defaultdict(int)
        for sample in res["ray_tasks"]:
            states[sample.labels["State"]] += sample.value
        print("Tasks by state: {}".format(states))
        return states
    else:
        print("No task metrics yet.")
        return {}


def test_task_basic(shutdown_only):
    info = ray.init(num_cpus=2)

    @ray.remote
    def f():
        time.sleep(999)

    refs = [f.remote() for _ in range(10)]

    expected = {
        "RUNNING": 2.0,
        "WAITING_FOR_EXECUTION": 0.0,
        "SCHEDULED": 8.0,
        "WAITING_FOR_DEPENDENCIES": 0.0,
    }
    # TODO(ekl) optimize the reporting interval to be faster for testing
    wait_for_condition(
        lambda: tasks_by_state(info) == expected, timeout=20, retry_interval_ms=2000)


# TODO(ekl) test wait on deps
# TODO(ekl) test actor tasks waiting for execution (queued vs running)
# TODO(ekl) test finished with success / error
# TODO(ekl) test wait on object store transfer (??)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
