from collections import defaultdict
from typing import Dict

import ray
from ray._private.test_utils import (
    raw_metrics,
    wait_for_condition,
)
from ray._private.worker import RayContext

_SYSTEM_CONFIG = {
    "task_events_report_interval_ms": 100,
    "metrics_report_interval_ms": 200,
}


def count_by_type(info: RayContext) -> Dict:
    res = raw_metrics(info)
    task_events_info = defaultdict(int)
    if "ray_gcs_task_manager_task_events_count" in res:
        for sample in res["ray_gcs_task_manager_task_events_count"]:
            print(sample)
            if "Type" in sample.labels and sample.labels["Type"] != "":
                task_events_info[sample.labels["Type"]] += sample.value
    return task_events_info


def test_status_task_events_metrics(shutdown_only):
    info = ray.init(num_cpus=1, _system_config=_SYSTEM_CONFIG)

    # Start a task
    @ray.remote
    def f():
        pass

    for _ in range(10):
        ray.get(f.remote())

    def verify():
        metric = count_by_type(info)
        assert metric["REPORTED"] >= 10, (
            "At least 10 tasks events should be reported. "
            "Could be more than 10 with multiple flush."
        )
        assert metric["STORED"] == 10, "10 task's events should be stored."

        return True

    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
    )
