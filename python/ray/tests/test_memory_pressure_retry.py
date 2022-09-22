from math import ceil
import sys
import time

import psutil
import pytest

import ray
from ray._private.test_utils import get_node_stats, wait_for_condition


memory_usage_threshold_fraction = 0.5
memory_monitor_interval_ms = 100
metrics_report_interval_ms = 100


@pytest.fixture
def ray_with_memory_monitor_no_oom_retries(shutdown_only):
    with ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold_fraction": memory_usage_threshold_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
            "metrics_report_interval_ms": metrics_report_interval_ms,
            "task_oom_retries": 0,
        },
    ):
        yield


@pytest.fixture
def ray_with_memory_monitor_two_oom_retries(shutdown_only):
    with ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold_fraction": memory_usage_threshold_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
            "metrics_report_interval_ms": metrics_report_interval_ms,
            "task_oom_retries": 2,
        },
    ):
        yield


# TODO(clarng): enable tests for actor retry
# @ray.remote(max_restarts = 1, max_task_retries=-1)
# class OneActorRetryAlloc:
#     def __init__(self):
#         self.leaks = []

#     def allocate(self, allocate_bytes: int, sleep_s = 0):
#         # divide by 8 as each element in the array occupies 8 bytes
#         new_list = [0] * ceil(allocate_bytes / 8)
#         self.leaks.append(new_list)
#         time.sleep(sleep_s)


@ray.remote(max_retries=0)
def no_retry_alloc_then_sleep(
    allocate_bytes: int,
):
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8
    chunks = [0] * ceil(bytes_per_chunk)
    time.sleep(1000)
    print(chunks)


@ray.remote(max_retries=1)
def one_retry_alloc_then_sleep(
    allocate_bytes: int,
):
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8
    chunks = [0] * ceil(bytes_per_chunk)
    time.sleep(1000)
    print(chunks)


def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> None:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = node_mem.total * pct - used
    assert bytes_needed > 0, "node has less memory than what is requested"
    return bytes_needed


def has_metric_tagged_with_value(tag, value) -> bool:
    raylet = ray.nodes()[0]
    reply = get_node_stats(raylet)
    for view in reply.view_data:
        for measure in view.measures:
            if tag in measure.tags:
                if hasattr(measure, "int_value"):
                    print(measure.int_value)
                    if measure.int_value == value:
                        return True
                if hasattr(measure, "double_value"):
                    print(measure.double_value)
                    if measure.double_value == value:
                        return True
    return False


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_no_oom_retry_use_task_retry_for_oom(ray_with_memory_monitor_no_oom_retries):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)

    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(one_retry_alloc_then_sleep.remote(bytes_to_alloc))

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.TaskEviction.Total",
        value=2.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_no_task_retry_dont_use_oom_retry_for_oom(
    ray_with_memory_monitor_two_oom_retries,
):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)

    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(no_retry_alloc_then_sleep.remote(bytes_to_alloc))

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.TaskEviction.Total",
        value=1.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_two_oom_retry_use_oom_retry_and_task_retry_for_oom(
    ray_with_memory_monitor_two_oom_retries,
):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)

    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(one_retry_alloc_then_sleep.remote(bytes_to_alloc))

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.TaskEviction.Total",
        value=4.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_no_oom_retry_no_task_retry_fail_immediately(
    ray_with_memory_monitor_no_oom_retries,
):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1.1)

    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(no_retry_alloc_then_sleep.remote(bytes_to_alloc))

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.TaskEviction.Total",
        value=1.0,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
