import sys
import time

import pytest

import ray

from ray.tests.test_memory_pressure import (
    ray_with_memory_monitor,
    allocate_memory,
    Leaker,
    get_additional_bytes_to_reach_memory_usage_pct,
    memory_usage_threshold_fraction,
)


# Utility for allocating locally (ray not involved)
def alloc_mem(bytes):
    chunks = 10
    mem = []
    bytes_per_chunk = bytes // 8 // chunks
    for _ in range(chunks):
        mem.append([0] * bytes_per_chunk)
    return mem


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_churn_long_running(
    ray_with_memory_monitor,
):
    long_running_bytes = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction - 0.1
    )
    allocate_memory.options(max_retries=1).remote(
        long_running_bytes, post_allocate_sleep_s=60
    )
    small_bytes = get_additional_bytes_to_reach_memory_usage_pct(
        memory_usage_threshold_fraction + 0.2
    )
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(allocate_memory.options(max_retries=1).remote(small_bytes))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_single_task_excessive_memory(
    ray_with_memory_monitor,
):
    bytes_to_alloc = get_additional_bytes_to_reach_memory_usage_pct(0.9)
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(allocate_memory.options(max_retries=1).remote(bytes_to_alloc))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_task_with_nested_task(
    ray_with_memory_monitor,
):
    with pytest.raises(
        (ray.exceptions.RayTaskError, ray.exceptions.OutOfMemoryError)
    ) as _:
        bytes1 = get_additional_bytes_to_reach_memory_usage_pct(
            memory_usage_threshold_fraction - 0.1
        )
        bytes2 = (
            get_additional_bytes_to_reach_memory_usage_pct(
                memory_usage_threshold_fraction + 0.2
            )
            - bytes1
        )
        ray.get(task_with_nested_task.remote(bytes1, bytes2, None, None))


@ray.remote
def task_with_nested_actor(fraction1, fraction2):
    leaker = Leaker.options(max_restarts=1, max_task_retries=1).remote()
    actor_bytes = get_additional_bytes_to_reach_memory_usage_pct(fraction1)
    ray.get(leaker.allocate.remote(actor_bytes))

    parent_bytes = get_additional_bytes_to_reach_memory_usage_pct(fraction2)
    dummy = alloc_mem(parent_bytes)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_task_with_nested_actor(
    ray_with_memory_monitor,
):
    with pytest.raises(
        (ray.exceptions.OutOfMemoryError, ray.exceptions.RayTaskError)
    ) as _:
        ray.get(task_with_nested_actor.remote(0.7, 0.9))


@ray.remote
class ActorWithNestedTask:
    def __init__(self):
        self.mem = []

    def alloc_local(self, num_bytes):
        self.mem = alloc_mem(num_bytes)

    def alloc_remote(self, num_bytes):
        ray.get(allocate_memory.options(max_retries=1).remote(num_bytes))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_actor_with_nested_task(
    ray_with_memory_monitor,
):
    leaker = ActorWithNestedTask.options(max_restarts=1, max_task_retries=1).remote()
    bytes_first = get_additional_bytes_to_reach_memory_usage_pct(0.45)
    ray.get(leaker.alloc_local.remote(bytes_first))

    bytes_second = get_additional_bytes_to_reach_memory_usage_pct(0.9)
    with pytest.raises(ray.exceptions.RayTaskError) as _:
        ray.get(leaker.alloc_remote.remote(bytes_second))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_actor_with_nested_task_two(
    ray_with_memory_monitor,
):
    leaker1 = ActorWithNestedTask.options(max_restarts=1, max_task_retries=1).remote()
    leaker2 = ActorWithNestedTask.options(max_restarts=1, max_task_retries=1).remote()
    bytes_first = get_additional_bytes_to_reach_memory_usage_pct(0.3)
    ray.get(leaker1.alloc_local.remote(bytes_first))
    bytes_second = get_additional_bytes_to_reach_memory_usage_pct(0.6)
    ray.get(leaker2.alloc_local.remote(bytes_second))

    bytes_third = get_additional_bytes_to_reach_memory_usage_pct(0.9)
    with pytest.raises(ray.exceptions.RayTaskError) as _:
        ray.get(leaker1.alloc_remote.remote(bytes_third))


# Used for syncing allocations
@ray.remote
class GlobalActor:
    def __init__(self):
        self.done = [False, False]

    def set_done(self, idx):
        self.done[idx] = True

    def both_done(self):
        return self.done[0] and self.done[1]


@ray.remote
def task_with_nested_task(bytes1, bytes2, barrier, instance_id):
    dummy = alloc_mem(bytes1)
    if barrier:
        ray.get(barrier.set_done.remote(instance_id))
        while not ray.get(barrier.both_done.remote()):
            time.sleep(1)
    ray.get(allocate_memory.options(max_retries=0).remote(bytes2))


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_deadlock_multiple_tasks_with_nested_task(
    ray_with_memory_monitor,
):
    bytes_first = get_additional_bytes_to_reach_memory_usage_pct(0.3)
    thirty_percent = get_additional_bytes_to_reach_memory_usage_pct(0.6) - bytes_first

    barrier = GlobalActor.options(max_restarts=1, max_task_retries=1).remote()

    first = task_with_nested_task.remote(bytes_first, thirty_percent, barrier, 0)
    second = task_with_nested_task.remote(thirty_percent, thirty_percent, barrier, 1)
    ray.get(first)
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(second)
