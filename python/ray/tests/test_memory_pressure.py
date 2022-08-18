from math import ceil
import os
import sys
import time

import psutil
import pytest

import ray
from ray._private.test_utils import get_node_stats, wait_for_condition


memory_usage_threshold_fraction = 0.7
memory_monitor_interval_ms = 100


@pytest.fixture
def ray_with_memory_monitor(shutdown_only):
    metrics_report_interval_ms = 100

    with ray.init(
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "memory_usage_threshold_fraction": memory_usage_threshold_fraction,
            "memory_monitor_interval_ms": memory_monitor_interval_ms,
            "metrics_report_interval_ms": metrics_report_interval_ms,
            "low_memory_threshold_for_task_dispatch_throttling": 0.5,
            "low_memory_task_dispatch_token_refresh_interval_ms": 1000,
            "low_memory_task_dispatch_token_refresh_count": 1,
        },
    ):
        yield


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
def no_retry(
    allocate_bytes: int,
    num_chunks: int = 10,
    allocate_interval_s: float = 0,
    post_allocate_sleep_s: float = 0,
):
    start = time.time()
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    end = time.time()
    time.sleep(post_allocate_sleep_s)
    return end - start


@ray.remote
class Leaker:
    def __init__(self):
        self.leaks = []

    def allocate(self, allocate_bytes: int, sleep_time_s: int = 0):
        # divide by 8 as each element in the array occupies 8 bytes
        new_list = [0] * ceil(allocate_bytes / 8)
        self.leaks.append(new_list)

        time.sleep(sleep_time_s / 1000)

        return os.getpid()


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
def test_memory_pressure_kill_actor(ray_with_memory_monitor):
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
        tag="MemoryManager.ActorEviction.Total",
        value=1.0,
    )


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_task(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.95)
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(no_retry.remote(bytes_to_alloc))

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
def test_memory_pressure_kill_newest_worker(ray_with_memory_monitor):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction - 0.1
    )

    actor_ref = Leaker.options(name="actor").remote()
    ray.get(actor_ref.allocate.remote(bytes_to_alloc))

    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(no_retry.remote(allocate_bytes=bytes_to_alloc))

    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "actor" in actors


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_memory_pressure_kill_task_if_actor_submitted_task_first(
    ray_with_memory_monitor,
):
    actor_ref = Leaker.options(name="leaker1").remote()
    ray.get(actor_ref.allocate.remote(10))

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction - 0.1
    )
    task_ref = no_retry.remote(
        allocate_bytes=bytes_to_alloc, allocate_interval_s=0, post_allocate_sleep_s=1000
    )

    ray.get(actor_ref.allocate.remote(bytes_to_alloc))
    with pytest.raises(ray.exceptions.WorkerCrashedError) as _:
        ray.get(task_ref)

    actors = ray.util.list_named_actors()
    assert len(actors) == 1
    assert "leaker1" in actors


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_worker_dump(ray_with_memory_monitor):
    leakers = [Leaker.options(name=str(i)).remote() for i in range(7)]
    _ = [ray.get(leaker.allocate.remote(0)) for leaker in leakers]
    oom_actor = Leaker.options(name="oom_actor").remote()

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(1)
    with pytest.raises(ray.exceptions.RayActorError) as _:
        ray.get(
            oom_actor.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 3)
        )

@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_backpressure(ray_with_memory_monitor):
    # memory_users = [Leaker.options(name=str(i)).remote() for i in range(7)]
    hogger = Leaker.options(name="memory_hogger").remote()
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(memory_usage_threshold_fraction - 0.1)
    ray.get(
            hogger.allocate.remote(bytes_to_alloc)
        )

    for _ in range(10):
        refs = [no_retry.remote(0) for _ in range(10)]
        # refs = [user.allocate.remote(0) for user in memory_users]
        _ = [ray.get(ref) for ref in refs]

    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(memory_usage_threshold_fraction + 0.2)
    with pytest.raises(ray.exceptions.RayActorError) as _:
      ray.get(
              hogger.allocate.remote(bytes_to_alloc, memory_monitor_interval_ms * 10)
          )

    while True:
        refs = [no_retry.remote(0) for _ in range(10)]
        # refs = [user.allocate.remote(0) for user in memory_users]
        _ = [ray.get(ref) for ref in refs]

    # time.sleep(1000)

if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
