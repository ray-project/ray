from math import ceil
import sys
import time

import psutil
import pytest

import ray
import ray.experimental.state.api as state_api
from ray._private.test_utils import get_node_stats, wait_for_condition


@ray.remote(max_retries=-1)
def inf_retry(
    allocate_bytes: int, num_chunks: int = 10, allocate_interval_s: float = 0
):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    return end - start


@ray.remote(max_retries=0)
def no_retry(allocate_bytes: int, num_chunks: int = 10, allocate_interval_s: float = 0):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    return end - start


@ray.remote(max_retries=1)
def persistent_task():
    time.sleep(10000)


@ray.remote
class PersistentActor:
    def run(self):
        time.sleep(10000)


@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes: int, sleep_time_s: int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(allocate_bytes / 8)
        self.leaks.append(new_list)

        time.sleep(sleep_time_s / 1000)


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
                    if measure.int_value == value:
                        return True
                if hasattr(measure, "double_value"):
                    if measure.double_value == value:
                        return True
    return False


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_worker(shutdown_only):
    memory_usage_threshold_fraction = 0.7
    memory_monitor_interval_ms = 100
    metrics_report_interval_ms = 100

    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold_fraction": memory_usage_threshold_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
            "metrics_report_interval_ms": metrics_report_interval_ms,
        },
    )

    leaker = Leaker.remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.90)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(leaker.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))

    wait_for_condition(
        has_metric_tagged_with_value,
        timeout=10,
        retry_interval_ms=100,
        tag="MemoryManager.ActorOOM.Total",
        value=1.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_newest_worker(shutdown_only):
    memory_usage_threshold_fraction = 0.7
    memory_monitor_interval_ms = 100

    ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold_fraction": memory_usage_threshold_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
        },
    )

    leaker1 = Leaker.options(name="leaker1").remote()
    leaker2 = Leaker.options(name="leaker2").remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.55)
    ray.get(leaker1.allocate.remote(bytes_to_alloc, 0))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.65)
    ray.get(leaker2.allocate.remote(bytes_to_alloc, 0))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.8)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(leaker2.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3))

    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "leaker1" in actors

    print(state_api.list_actors())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
